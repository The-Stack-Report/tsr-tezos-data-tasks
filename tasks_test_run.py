from tasks import tasks, tasksByKey
import os
from test_utils.task_test_utils import runTask, findMissingImports
import logging
from test_utils.mongodb import db
from test_utils.s3_client import s3_client, s3_resource
import pandas as pd
import time
import datetime


# Tasks are meant to run within the context of a task worker:
# See: https://github.com/The-Stack-Report/tsr-data-services

# This script is used for development and testing purposes only
# Tasks are loaded from the tasks directory
# Database connection and s3 connection should be provided by the worker
# Here these are by this test script.


def runTasks(publish_connections):
    results = []
    for task in tasks:
        if hasattr(task, 'runTask'):
            task_result = runTask(task, secondary_params=publish_connections)
            results.append(task_result)
        else:
            print('>>> Task ',task.__name__,'does not have runTask function')
        print("Finished task: " + task.__name__)
    return results



def run_semantic_entrypoint_tasks(publish_connections):
    # Run through all entrypoints
    today_formatted = datetime.datetime.today().strftime("%Y-%m-%d")
    entrypoints_index_df = pd.read_csv(f"cache/tezos_entrypoints_index/tezos_entrypoints_index_{today_formatted}.csv")

    # entrypoints_index_df = entrypoints_index_df[200:203]
    print(entrypoints_index_df)
    time.sleep(1)
    for index, row in entrypoints_index_df.iterrows():
        entrypoint = row['Entrypoint']
        latest_call = row["latest_call"]

        # entrypoint statistics function can take an optional max date so it doesn't have to query it from the db.
        # Otherwise the task will query the max date from the database, which is slower.
        latest_call_date_formatted = False

        task_params = {
            "entrypoint": entrypoint,
            "today_date": today_formatted
        }
        
        # Check if latest call is string
        if latest_call is not None and isinstance(latest_call, str) and len(latest_call) > 9:
            latest_call_date_formatted = latest_call[0:10]
            task_params["max_date"] = latest_call_date_formatted
        
        
        print(latest_call_date_formatted)
        print("Running statistics task for entrypoint: " + entrypoint)
        runTask(
            task=tasksByKey['tezos_entrypoint_statistics'],
            task_params=task_params,
            system_params=publish_connections
            )


def run_tezos_wallet_account_tasks(publish_connections):
    print("Running Tezos wallet account tasks")
    today_formatted = datetime.datetime.today().strftime("%Y-%m-%d")
    accounts_index_df = pd.read_csv(f"cache/tezos_accounts_index/accounts-{today_formatted}.csv")
    print(accounts_index_df)
    wallet_accounts_df = accounts_index_df[accounts_index_df['Address'].str.startswith('tz1')]
    print(wallet_accounts_df)

    # wallet_accounts_df = wallet_accounts_df[0:3]
    # print(wallet_accounts_df)

    for index, row in wallet_accounts_df.iterrows():
        address = row['Address']
        print("Running tasks for wallet account: " + address)
        runTask(
            task=tasksByKey['tezos_wallet_statistics'],
            task_params={
                "address": address,
                "today_date": today_formatted
            },
            system_params=publish_connections
            )


run_all_tasks = True
run_all_tasks = False

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    publish_connections = {
        "mongodb": db,
        "s3_client": s3_client,
        "s3_resource": s3_resource
    }

    findMissingImports(tasks)


    if run_all_tasks:
        print("Running tests")
        logging.basicConfig(level=logging.INFO)
        results = runTasks(publish_connections)

        print("Results:")
        print(results)
        results_df = pd.DataFrame(results)

        results_with_errors_df = results_df[results_df['result'] == False]
        if len(results_with_errors_df) > 0:
            print("Tasks with errors:")
            print(results_with_errors_df)
            for index, row in results_with_errors_df.iterrows():
                print("Task: ", row['task'])
                print("❌ Error: ", row['error'])
            raise Exception("Some tasks failed")
    
    else:
        # Quickly test specific tasks
        # runTask(task=tasksByKey['tezos_contract_statistics'])
        # runTask(task=tasksByKey['tezos_chain_stats_for_day'], system_params=publish_connections)
        
        # runTask(task=tasksByKey['tezos_entrypoint_statistics'], task_params={"entrypoint": "ctez_to_tez"}, system_params=publish_connections)
        # runTask(task=tasksByKey['tezos_entrypoint_statistics'], task_params={"entrypoint": "sell"}, system_params=publish_connections)
        # runTask(task=tasksByKey['tezos_entrypoint_statistics'], task_params={"entrypoint": "transfer", "max_date": "2023-05-15"}, system_params=publish_connections)
        
        runTask(task=tasksByKey['tezos_accounts_index'], system_params=publish_connections)
        runTask(task=tasksByKey['tezos_entrypoints_index'], system_params=publish_connections)
        run_semantic_entrypoint_tasks(publish_connections)

        
