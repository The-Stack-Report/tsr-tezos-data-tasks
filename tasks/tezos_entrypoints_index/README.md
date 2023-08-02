# Tezos entrypoints index task

Task that generates a table of all the entrypoint names used by smart contracts in the tezos protocol with some high-level statistics.

Dataset metadata is co-located in the task folder.

Temporary file storage is stored in the following cache path:

`cache/tezos_entrypoints_index`

First top entrypoints are queried based on recent transactions. Then all distinct entrypoints are queried from the transactions table. These are combined in an index with the top recent entrypoints first.

