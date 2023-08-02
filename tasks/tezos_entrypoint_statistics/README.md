# Tezos entrypoint statistics task

Task takes an entrypoint name as parameter and produces a variety of statistics datasets for that entrypoint.

Datasets produced:

- transactions-calling-{entrypoint} (TODO)
- the-stack-report--tezos-entrypoint-time-series-{entrypoint} (IMPLEMENTED)
- accounts-calling-{entrypoint} (TODO)
- smart-contracts-with-{entrypoint} (TODO)
- {entrypoint}-accounts-network (TODO)
- {entrypoint}-summary (TODO)

The transactions dataset is produced first and is used as the basis for the other datasets.

## Datasets output

**transactions-calling-{entrypoint}**

CSV table of transactions that called the entrypoint

**accounts-calling-{entrypoint}**

CSV table of accounts that called the entrypoint

**smart-contracts-with-{entrypoint}**

CSV table of smart contracts with the entrypoint called

**the-stack-report--tezos-entrypoint-time-series-{entrypoint}**

CSV table of entrypoint calls per day and other stats.

**{entrypoint}-accounts-network**

JSON Dictionary with nodes for accounts and directed edges for transactions between accounts that called the entrypoint.

**{entrypoint}-summary**

JSON Dictionary with summary statistics for the entrypoint. Mainly used for preview cards.

## Implementation notes

