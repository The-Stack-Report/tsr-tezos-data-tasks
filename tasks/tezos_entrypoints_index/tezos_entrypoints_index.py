from pathlib import Path
import logging
from models.extended.dataset import Dataset
import datetime
from .sub_steps.base_index import get_base_index

expected_system_params = {
    "mongodb": True,
    "s3_client": True,
    "s3_resource": True
}

def runTask(task_params={}, system_params={}):
    logging.info("Running tezos entrypoints index task")

    # Initialize cache directory
    cache_path = Path("cache/tezos_entrypoints_index")
    if not cache_path.exists():
        cache_path.mkdir(parents=True)


    today_date = datetime.datetime.utcnow().date().strftime("%Y-%m-%d")

    # Prepare metadata from co-located json file in repository
    current_dir = Path(__file__).parent.absolute()
    dataset = Dataset(from_file=current_dir / 'entrypoints_index_metadata.json')
    dataset.printProperties()

    # Get base index with naive sorting of top recent entrypoints first.
    all_entrypoints_df = get_base_index(
        cache_path=cache_path,
        today_date=today_date
    )
    

    entrypoints_index_file_path = cache_path / f"tezos_entrypoints_index_{today_date}.csv"
    all_entrypoints_df.to_csv(entrypoints_index_file_path, index=False)

    
    # Register output file to dataset object
    dataset.setLocalFilePath(entrypoints_index_file_path)

    # Upload dataset to S3 and store metadata in MongoDB
    logging.info("Uploading dataset to S3")
    dataset.uploadDatasetFile(storage_params=system_params)

    logging.info("Uploading dataset metadata to MongoDB")
    dataset.storeDatasetMetadata(mongodb=system_params["mongodb"])

    return True