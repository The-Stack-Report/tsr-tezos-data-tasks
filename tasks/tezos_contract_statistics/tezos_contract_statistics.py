from task_utils.pg_db import dbConnection
import pandas as pd
import sqlalchemy
from pathlib import Path
from .queries import (
    create_contract_target_transactions_by_id_query,
    create_contract_sender_transactions_by_id_query
)
import logging

TEST_PARAMS = {
    "contract_address": "KT1LHHLso8zQWQWg1HUukajdxxbkGfNoHjh6"
}

accounts_df = False
accounts_by_id = False
accounts_by_address = False

def load_contract_accounts():
    global accounts_df
    global accounts_by_id
    if not accounts_by_id == False:
        logging.info("returning accounts_by_id from global python cache")
        return accounts_df
    else:
        logging.info("Reading accounts from cache directory")
        accounts_df = pd.read_csv("cache/accounts.csv")
        logging.info(f"Loaded {len(accounts_df)} accounts from cache index")
        accounts_by_id = dict(zip(accounts_df["Id"], accounts_df["Address"]))
        accounts_by_id[-1] = '__null__'
        return accounts_by_id

def load_contract_accounts_by_address():
    global accounts_df
    global accounts_by_address
    if not accounts_by_address == False:
        print("returning accounts_by_address")
        return accounts_by_address
    else:
        accounts_df = pd.read_csv("cache/accounts.csv")
        print(f"Loaded {len(accounts_df)} accounts from cache index")
        accounts_by_address = dict(zip(accounts_df["Address"], accounts_df["Id"]))
        accounts_by_address['__null__'] = -1
        return accounts_by_address        

def runTask(params={}):

    PLACEHOLDER = "NO_ADDRESS_SPECIFIED"
    logging.info(params)
    contract_address = params.get("contract_address")
    logging.info(f"Running tezos contracts statistics task for address: {contract_address}")

    accounts = load_contract_accounts()
    accounts_by_kt = load_contract_accounts_by_address()
    account_id = accounts_by_kt.get(contract_address)
    logging.info(f"Account id: {account_id} from address: {contract_address}")
    if account_id == None:
        logging.info("Account id not found.")
    if contract_address == PLACEHOLDER:
        logging.info("No address specified, exiting")
        raise Exception("No address specified, exiting")

    contract_target_transactions_by_id_query = create_contract_target_transactions_by_id_query(account_id)
    contract_sender_transactions_by_id_query = create_contract_sender_transactions_by_id_query(account_id)

    contract_target_transactions_df = pd.read_sql(contract_target_transactions_by_id_query, dbConnection)

    contract_sender_transactions_df = pd.read_sql(contract_sender_transactions_by_id_query, dbConnection)

    logging.info(contract_target_transactions_df)
    logging.info(contract_sender_transactions_df)


