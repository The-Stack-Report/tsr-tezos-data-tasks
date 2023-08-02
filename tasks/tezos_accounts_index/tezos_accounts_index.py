from task_utils.pg_db import dbConnection
import pandas as pd
import sqlalchemy
from pathlib import Path
import logging
from models.extended.dataset import Dataset
import datetime

get_all_accounts = sqlalchemy.text("""
SELECT
"Id",
"Address"
FROM public."Accounts"
ORDER BY "Address"
""")

expected_system_params = {
    "mongodb": True,
    "s3_client": True,
    "s3_resource": True
}

def runTask(task_params={}, system_params={}):
    logging.info("Running tezos accounts index task")

    # Extract secondary parameters for file and metadata storage
    mongodb = system_params["mongodb"]

    # Prepare local cache directory
    cache_path = Path("cache") / "tezos_accounts_index"
    cache_path.mkdir(parents=True, exist_ok=True)


    # Prepare metadata from co-located json file in repository
    current_dir = Path(__file__).parent.absolute()
    dataset = Dataset(from_file=current_dir / 'accounts_index_metadata.json')
    dataset.printProperties()

    # Prepare local cache file path
    today_formatted = datetime.datetime.now().strftime("%Y-%m-%d")
    cache_file_path = cache_path / f"accounts-{today_formatted}.csv"

    # Create actual dataset file and store to local cache
    if cache_file_path.exists():
        logging.info(f"Reading accounts index from local cache file {cache_file_path}")
        accounts_df = pd.read_csv(cache_file_path)
    else:
        logging.info(f"Querying for all accounts")
        accounts_df = pd.read_sql(get_all_accounts, dbConnection)
        logging.info(f"Saving accounts index to {cache_file_path}")
        accounts_df.to_csv(cache_file_path, index=False)
        logging.info(accounts_df)

    # Add data processing output to dataset
    dataset.setLocalFilePath(cache_file_path)

    # Upload dataset & metadata to online storage
    logging.info(f"Uploading accounts index to S3")
    dataset.uploadDatasetFile(storage_params=system_params)

    logging.info(f"Updating dataset metadata")
    dataset.storeDatasetMetadata(mongodb)

    return True

    

