import pandas as pd
from pathlib import Path
from task_utils.tezos_transactions import load_and_prepare_transactions_df

def run(task_params, system_params):
    entrypoint = task_params.get("entrypoint")
    mongodb = system_params.get("mongodb")
    today_date = task_params.get("today_date")
    print(f"Running entrypoint smart contracts for entrypoint: {entrypoint}")

    cache_entrypoint_dir = Path("cache") / "tezos_entrypoint_statistics" / entrypoint

    transactions_df = load_and_prepare_transactions_df(
        cache_entrypoint_dir / f"{entrypoint}-transactions.csv"
    )

    print(transactions_df)

    