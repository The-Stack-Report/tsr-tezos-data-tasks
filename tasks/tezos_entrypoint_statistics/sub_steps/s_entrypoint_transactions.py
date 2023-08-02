import pandas as pd
from pathlib import Path
import logging
from ..queries import entrypoint_transactions_query_for_date
from task_utils.blocks_index import get_blocks_index
from task_utils.pg_db import dbConnection
import sqlalchemy
from shared_functions.tezos_transactions import add_addresses_from_tzkt_ids


# Prepares transactions df for entrypoint in batches per day
# entrypoint statistics function can take an optional max date so it doesn't have to query it from the db.
# Otherwise the task will query the max date from the database, which is slower.

def run(task_params={}, system_params={}):
    entrypoint = task_params.get("entrypoint")
    today_date = task_params.get("today_date")

    cache_dir = Path("cache")

    cache_entrypoint_dir = Path("cache") / "tezos_entrypoint_statistics" / entrypoint
    cache_entrypoint_dir.mkdir(parents=True, exist_ok=True)
    
    logging.info(f"Running entrypoint transactions for entrypoint: {entrypoint}")

    max_date = False
    max_date = task_params.get("max_date")

    # Construct queries that get the min and max date for the entrypoint
    logging.info(f"Querying for min and max date for entrypoint: {entrypoint}")
    min_query = sqlalchemy.text("""
        SELECT "Timestamp" as min_ts FROM public."TransactionOps"
        WHERE "Entrypoint" = :entrypoint
        ORDER BY "Timestamp" ASC LIMIT 1
        """)
    
    max_query = sqlalchemy.text("""
        SELECT "Timestamp" as max_ts
        FROM public."TransactionOps"
        WHERE "Entrypoint" = :entrypoint
        ORDER BY "Timestamp" DESC LIMIT 1
        """)

    print("Querying for min date:")
    min_date_local_cache_file = cache_entrypoint_dir / "min_date.csv"
    min_date_df = False
    if min_date_local_cache_file.exists():
        min_date_df = pd.read_csv(min_date_local_cache_file)
        min_date_df["min_ts"] = pd.to_datetime(min_date_df["min_ts"])
    else:
        min_date_df = pd.read_sql(sql=min_query, con=dbConnection, params={"entrypoint": entrypoint})
        min_date_df.to_csv(min_date_local_cache_file, index=False)


    min_date = min_date_df.iloc[0]["min_ts"]

    if max_date is None or max_date == False:
        print("Querying for max date:")
        max_date_df = pd.read_sql(sql=max_query, con=dbConnection, params={"entrypoint": entrypoint})
        print(max_date_df)
        max_date = max_date_df.iloc[0]["max_ts"]

    elif isinstance(max_date, str):
        logging.info("Converting provided task_param: max_date to pandas datetime.")
        max_date = pd.to_datetime(max_date)

    # Check if max date is today, if so, subtract one day to include only completed days
    if max_date.date() == pd.Timestamp.today().date():
        max_date = max_date - pd.Timedelta(days=1)

    min_date = min_date.tz_localize(None)
    max_date = max_date.tz_localize(None)

    date_range = pd.date_range(
        start=min_date,
        end=max_date,
        freq="D")

    cache_dir_tezos_accounts_index = Path("cache/tezos_accounts_index")

    tezos_accounts_df = pd.read_csv(
        cache_dir_tezos_accounts_index / f"accounts-{today_date}.csv"
    )

    # Prepare dict for mapping TzKT postgres table ids to tezos addresses
    accounts_by_id = dict(
        zip(
            tezos_accounts_df["Id"],
            tezos_accounts_df["Address"]
        )
    )

    # Get blocks index to use in more efficient date range queries
    blocks_index_df = get_blocks_index({
        "cache_dir": cache_dir / "blocks_index",
        "today_date": today_date
    })

    # Setup cache directories
    cache_per_date_dir = cache_entrypoint_dir / "per_date"
    cache_per_date_dir.mkdir(parents=True, exist_ok=True)

    # Iterate over date range to compile local cached files
    all_transactions_for_entrypoint = []
    for date in date_range:
        date_formatted = date.strftime("%Y-%m-%d")

        cached_transactions_file_path = cache_per_date_dir / f"{entrypoint}-transactions-{date_formatted}.csv"
        entrypoint_transactions_df = False

        blocks_index_df_filtered = blocks_index_df[blocks_index_df["date"] == date_formatted]
        min_level = blocks_index_df_filtered.iloc[0]["Level"]
        max_level = blocks_index_df_filtered.iloc[-1]["Level"]

        if cached_transactions_file_path.exists(): # Load transactions from cache
            print("Skipping date, already cached: ", date_formatted)
            entrypoint_transactions_df = pd.read_csv(cached_transactions_file_path)
        else: # Get transactions for date
            print(f"Querying for {entrypoint} transactions for date {date_formatted}")
            
            # Construct query
            q = entrypoint_transactions_query_for_date(
                entrypoint=entrypoint,
                date=date_formatted,
                min_level=min_level,
                max_level=max_level
                )
            
            # Run query
            entrypoint_transactions_df = pd.read_sql(
                    sql=q,
                    con=dbConnection,
                    params={"entrypoint": entrypoint}
                    )
            
            # Store query results to local cache
            entrypoint_transactions_df.to_csv(cached_transactions_file_path, index=False)
        
        print(f"date {date_formatted} - {entrypoint}")
        logging.info(f"Adding addresses from tzkt ids: {date_formatted}")
        
        # Enrich with actual tezos address values instead of database indexes
        entrypoint_transactions_df = add_addresses_from_tzkt_ids(
            transactions_df=entrypoint_transactions_df,
            accounts_by_id=accounts_by_id
        )
        all_transactions_for_entrypoint.append(entrypoint_transactions_df)

    all_transactions_for_entrypoint_df = pd.concat(all_transactions_for_entrypoint)

    print(all_transactions_for_entrypoint_df)

    output_file_path = cache_entrypoint_dir / f"{entrypoint}-transactions.csv"

    all_transactions_for_entrypoint_df.to_csv(
        output_file_path,
        index=False
    )