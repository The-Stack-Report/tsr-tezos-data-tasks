import tasks.tezos_account_statistics as tezos_account_statistics
import tasks.tezos_accounts_index as tezos_accounts_index
import tasks.tezos_accounts_network as tezos_accounts_network
import tasks.tezos_blocks_latest as tezos_blocks_latest
import tasks.tezos_blocks_statistics as tezos_blocks_statistics
import tasks.tezos_contract_statistics as tezos_contract_statistics
import tasks.tezos_contracts_index as tezos_contracts_index

tasks = [
    tezos_account_statistics,
    tezos_accounts_index,
    tezos_accounts_network,
    tezos_blocks_latest,
    tezos_blocks_statistics,
    tezos_contract_statistics,
    tezos_contracts_index
]

tasksByKey = {}

for task in tasks:
    tasksByKey[task.__name__.split(".")[-1]] = task