import tasks.tezos_account_statistics as tezos_account_statistics
import tasks.tezos_accounts_index as tezos_accounts_index
import tasks.tezos_accounts_index_extended as tezos_accounts_index_extended
import tasks.tezos_accounts_network as tezos_accounts_network
import tasks.tezos_blocks_latest as tezos_blocks_latest
import tasks.tezos_blocks_statistics as tezos_blocks_statistics
import tasks.tezos_chain_stats_for_day as tezos_chain_stats_for_day
import tasks.tezos_chain_stats_time_series as tezos_chain_stats_time_series
import tasks.tezos_contract_statistics as tezos_contract_statistics
import tasks.tezos_contracts_index as tezos_contracts_index
import tasks.tezos_entrypoint_statistics as tezos_entrypoint_statistics
import tasks.tezos_entrypoints_index as tezos_entrypoints_index

tasks = [
    tezos_account_statistics,
    tezos_accounts_index,
    tezos_accounts_index_extended,
    tezos_accounts_network,
    tezos_blocks_latest,
    tezos_blocks_statistics,
    tezos_chain_stats_for_day,
    tezos_chain_stats_time_series,
    tezos_contract_statistics,
    tezos_contracts_index,
    tezos_entrypoint_statistics,
    tezos_entrypoints_index
]

tasksByKey = {}

for task in tasks:
    tasksByKey[task.__name__.split(".")[-1]] = task