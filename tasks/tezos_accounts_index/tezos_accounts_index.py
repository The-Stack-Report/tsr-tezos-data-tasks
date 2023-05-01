from task_utils.pg_db import dbConnection
import pandas as pd
import sqlalchemy
from pathlib import Path


get_all_accounts = sqlalchemy.text("""
SELECT
"Id",
"Address"
FROM public."Accounts"
ORDER BY "Address"
""")

def runTask(params):
    print("Running tezos accounts index task")
    print(dbConnection)
    accounts_df = pd.read_sql(get_all_accounts, dbConnection)
    print(accounts_df)
    cache_path = Path("cache")
    cache_path.mkdir(parents=True, exist_ok=True)


    accounts_df.to_csv(cache_path / "accounts.csv", index=False)