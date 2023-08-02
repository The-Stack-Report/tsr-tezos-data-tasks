import pandas as pd
from pathlib import path

from task_utils.tezos_usage_statistics import (
    network_json_from_transactions_df
)


def run(params):
    entrypoint = params.get("entrypoint")
    print(f"Running entrypoint network for entrypoint: {entrypoint}")


    network_for_entrypoint = network_json_from_transactions_df(all_transactions_for_entrypoint_df)