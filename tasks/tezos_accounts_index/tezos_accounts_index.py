from task_utils.pg_db import dbConnection
import pandas as pd
import sqlalchemy

get_all_accounts = sqlalchemy.text("""
SELECT
"Id",
"Address"
FROM public."Accounts"
ORDER BY "Address"
LIMIT 100
""")

def runTask(params):
    print("Running tezos accounts index task")
    print(dbConnection)
    accounts_df = pd.read_sql(get_all_accounts, dbConnection)
    print(accounts_df)