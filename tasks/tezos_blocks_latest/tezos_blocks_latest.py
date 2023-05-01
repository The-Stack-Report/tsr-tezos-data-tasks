from task_utils.pg_db import dbConnection
import pandas as pd
import sqlalchemy
from pathlib import Path

get_latest_blocks = sqlalchemy.text("""
SELECT * FROM public."Blocks"
ORDER BY "Id" DESC LIMIT 100
""")

def runTask(params):
    print("Running latest blocks task")
    print(dbConnection)
    latest_blocks_df = pd.read_sql(get_latest_blocks, dbConnection)
    print(latest_blocks_df)

    cache_path = Path("cache")
    cache_path.mkdir(parents=True, exist_ok=True)
    latest_blocks_df.to_csv(cache_path / "latest_blocks.csv", index=False)

    print("Found max level: ")
    print(latest_blocks_df["Level"].max())

    print("Stored latest blocks in cache/latest_blocks.csv")
    print("Returning task completion message to the worker.")
    return True
