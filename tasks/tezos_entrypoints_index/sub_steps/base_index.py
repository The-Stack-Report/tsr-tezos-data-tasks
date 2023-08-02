from ..queries import (
    top_entrypoints_query,
    distinct_entrypoints_query
)
from task_utils.pg_db import dbConnection
import pandas as pd
import logging

def get_base_index(cache_path, today_date):

    print("Getting base index")
    # Run exploratory queries to get all entrypoints and top entrypoints

    ############################
    #
    # Look for top entrypoints for day
    #

    top_entrypoints_for_day_cache_file_path = cache_path / f"tezos_top_entrypoints_query_result_{today_date}.csv"

    # Check if query ran for day already by looking for locally cached file with date.
    top_entrypoints_df = None
    if top_entrypoints_for_day_cache_file_path.exists():
        print("Using cache file")
        top_entrypoints_df = pd.read_csv(top_entrypoints_for_day_cache_file_path)
    else:
        logging.info("Running top entrypoints query")
        top_entrypoints_df = pd.read_sql(top_entrypoints_query, dbConnection)
        top_entrypoints_df.to_csv(top_entrypoints_for_day_cache_file_path, index=False)
    
    print(top_entrypoints_df)

    ############################
    #
    # Look for all distinct entrypoints for day
    #
    

    distinct_entrypoints_for_day_cache_file_path = cache_path / f"tezos_distinct_entrypoints_query_result_{today_date}.csv"

    # Check if query ran for day already by looking for locally cached file with date.
    all_entrypoints_df = None
    if distinct_entrypoints_for_day_cache_file_path.exists():
        print("Using cache file")
        all_entrypoints_df = pd.read_csv(distinct_entrypoints_for_day_cache_file_path)
    else:
        logging.info("Running distinct entrypoints query")
        all_entrypoints_df = pd.read_sql(distinct_entrypoints_query, dbConnection)
        all_entrypoints_df = all_entrypoints_df.drop_duplicates()
        all_entrypoints_df.to_csv(distinct_entrypoints_for_day_cache_file_path, index=False)
    

    ############################
    #
    # Combine distinct and top entrypoints in one dataframe
    #


    print(all_entrypoints_df)
    entrypoints_to_process = top_entrypoints_df['Entrypoint'].tolist()

    remaining_entrypoints_df = all_entrypoints_df[~all_entrypoints_df['Entrypoint'].isin(entrypoints_to_process)]

    entrypoints_to_process = all_entrypoints_df["Entrypoint"].tolist()

    entrypoints_to_process = set(entrypoints_to_process)
    
    print("Remaining entrypoints: ", len(remaining_entrypoints_df))
    print("Total entrypoints: ", len(entrypoints_to_process))

    sorted_combined_entrypoints = pd.concat([top_entrypoints_df, remaining_entrypoints_df], ignore_index=True)

    return sorted_combined_entrypoints
    
    # entrypoints_to_process = entrypoints_to_process[:10]