import pandas as pd
from pathlib import Path
from models.extended.dataset import Dataset
import logging
import json
import datetime
from shared_functions.tezos_usage_statistics import usage_statistics_from_transactions_df
from shared_functions.tezos_transactions import load_and_prepare_transactions_df


def run(task_params={}, system_params={}):
    entrypoint = task_params.get("entrypoint")
    mongodb = system_params.get("mongodb")

    cache_entrypoint_dir = Path("cache") / "tezos_entrypoint_statistics" / entrypoint
    cache_entrypoint_dir.mkdir(parents=True, exist_ok=True)

    print(f"Running entrypoint time series for entrypoint: {entrypoint}")

    ###############################
    #
    # Setup of dataset metadata
    #

    daily_time_series_dataset = Dataset(metadata_dict={
        "identifier": f"the-stack-report--tezos-entrypoint-time-series-{entrypoint}",
        "title": f"{entrypoint} - semantic analysis time series",
        "description": f"Time series of daily statistics for smart contract entrypoints named: {entrypoint}. Statistics include various values such as number of transactions, accounts & smart contracts involved.",
        "format": "CSV",
        "type": "TimeSeries",
        "url": f"https://the-stack-report.ams3.digitaloceanspaces.com/datasets/tezos/chain/semantic-entrypoints/tezos-semantic-entrypoint-time-series-{entrypoint}.csv",

        "__bucket": "the-stack-report",
        "__key": f"datasets/tezos/chain/semantic-entrypoints/tezos-semantic-entrypoint-time-series-{entrypoint}.csv"
    })

    daily_time_series_dataset.printProperties()
    transactions_df = load_and_prepare_transactions_df(
        cache_entrypoint_dir / f"{entrypoint}-transactions.csv"
    )

    min_date = transactions_df["Timestamp"].min()
    max_date = transactions_df["Timestamp"].max()

    min_date = pd.to_datetime(min_date)
    max_date = pd.to_datetime(max_date)

    print(f"Min date: {min_date}")
    print(f"Max date: {max_date}")

    date_range = pd.date_range(min_date, max_date, freq="D")

    stats_time_series = []
    for date in date_range:
        date_formatted = date.strftime("%Y-%m-%d")
        print(f"Processing date: {date_formatted}")


        transactions_for_day_df = transactions_df[transactions_df["date_formatted"] == date_formatted]
        stats_for_day = usage_statistics_from_transactions_df(transactions_for_day_df)
        stats_for_day["date"] = date_formatted
        stats_time_series.append(stats_for_day)

    stats_time_series_df = pd.DataFrame(stats_time_series)

    time_series_file_path = cache_entrypoint_dir / f"{entrypoint}-stats-time-series.csv"
    stats_time_series_df.to_csv(time_series_file_path, index=False)

    daily_time_series_dataset.setLocalFilePath(time_series_file_path)

    # Upload dataset & metadata to online storage
    logging.info(f"Uploading entrypoints time series datafile to S3")
    daily_time_series_dataset.uploadDatasetFile(storage_params=system_params)

    logging.info(f"Updating dataset metadata")
    daily_time_series_dataset.storeDatasetMetadata(mongodb)