import pandas as pd
from pathlib import Path
import json
from task_utils.tezos_usage_statistics import usage_statistics_from_transactions_df
from task_utils.tezos_transactions import load_and_prepare_transactions_df


def run(task_params={}, system_params={}):
    entrypoint = task_params.get("entrypoint")
    mongodb = system_params.get("mongodb")
    today_date = task_params.get("today_date")

    cache_entrypoint_dir = Path("cache") / "tezos_entrypoint_statistics" / entrypoint
    cache_entrypoint_dir.mkdir(parents=True, exist_ok=True)
    print(f"Running entrypoint summary for entrypoint: {entrypoint}")

    transactions_df = load_and_prepare_transactions_df(
        cache_entrypoint_dir / f"{entrypoint}-transactions.csv"
    )

    stats_total_for_entrypoint = usage_statistics_from_transactions_df(transactions_df)

    stats_total_for_entrypoint["date_description"] = f"totals up to date {today_date}"
    stats_total_for_entrypoint["date_formatted"] = today_date
    stats_total_for_entrypoint["entrypoint"] = entrypoint

    # TODO: Add top contracts
    # TODO: Add top accounts targeting
    # TODO: Add distribution of wallet vs contract accounts targeting entrypoint

    # Store stats total to json
    with open(cache_entrypoint_dir / f"{entrypoint}-statistics-summary.json", "w") as f:
        json.dump(stats_total_for_entrypoint, f, indent=4)