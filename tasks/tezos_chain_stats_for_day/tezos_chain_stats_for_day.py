import logging
from task_utils.pg_db import dbConnection
import pandas as pd
import sqlalchemy
from .queries import ops_for_date_flat_query
import datetime


TEST_PARAMS = {
    "day": "2021-01-01"
}

expected_system_params = {
    "mongodb": True,
    "s3_client": True,
    "s3_resource": True
}

def runTask(params, params2):
    print("Tezos chain stats for day task.")
    print(params)
    day = params.get("day")
    if day is None:
        raise Exception("Task tezos_chain_stats_for_day requires param day")
    logging.info(f"Running tezos chain stats for day {day}")
    # Construct dataset metadata



    # Query day data from TzKT postgres.
    dt = datetime.datetime.strptime(day, "%Y-%m-%d")
    q = ops_for_date_flat_query(dt)
    logging.info(f"Running query for day {day}")
    day_ops_df = pd.read_sql(q, dbConnection)
    logging.info(f"Found {len(day_ops_df)} operations for day {day}")
    logging.info(day_ops_df.head())
    print(day_ops_df)


    

    




    return True
    