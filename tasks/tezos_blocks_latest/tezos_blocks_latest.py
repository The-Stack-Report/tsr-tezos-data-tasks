from task_utils.pg_db import dbConnection
import pandas as pd
import sqlalchemy
from pathlib import Path
import logging

get_latest_blocks = sqlalchemy.text("""
SELECT * FROM public."Blocks"
ORDER BY "Id" DESC LIMIT 100
""")

def runTask(params):
    logging.info("Running latest blocks task")
    latest_blocks_df = pd.read_sql(get_latest_blocks, dbConnection)
    logging.info(latest_blocks_df)

    cache_path = Path("cache")
    cache_path.mkdir(parents=True, exist_ok=True)
    latest_blocks_df.to_csv(cache_path / "latest_blocks.csv", index=False)

    logging.info("Found max level: ")
    logging.info(latest_blocks_df["Level"].max())

    logging.info("Stored latest blocks in cache/latest_blocks.csv")
    logging.info("Returning task completion message to the worker.")
    return True
