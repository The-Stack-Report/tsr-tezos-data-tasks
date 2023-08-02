import logging
import sqlalchemy
import pandas as pd
from task_utils.pg_db import dbConnection

# Leverage blocks timestamps to do faster queries filtered on date for transactions
# by comparing min/max block levels for date.


def get_blocks_index(params):
    cache_dir = params.get("cache_dir")
    today_date = params.get("today_date")

    blocks_timestamps_query = sqlalchemy.text(
        """
        SELECT
            "Level",
            "Timestamp"
        FROM public."Blocks"
        ORDER BY "Timestamp" ASC
        """
    )
    
    cache_dir.mkdir(parents=True, exist_ok=True)

    blocks_timestamps_local_cache_file = cache_dir / f"blocks_timestamps-{today_date}.csv"
    
    blocks_index_df = False
    if blocks_timestamps_local_cache_file.exists():
        logging.info("Reading blocks timestamps from local cache file.")
        blocks_index_df = pd.read_csv(blocks_timestamps_local_cache_file)
        logging.info("Converting timestamp column to pandas datetime.")
        blocks_index_df["Timestamp"] = pd.to_datetime(blocks_index_df["Timestamp"])
    else:
        logging.info("Querying for blocks timestamps.")
        blocks_index_df = pd.read_sql(
            sql=blocks_timestamps_query,
            con=dbConnection)
        blocks_index_df.to_csv(blocks_timestamps_local_cache_file, index=False)
    

    blocks_index_df["date"] = blocks_index_df["Timestamp"].dt.strftime("%Y-%m-%d")

    return blocks_index_df