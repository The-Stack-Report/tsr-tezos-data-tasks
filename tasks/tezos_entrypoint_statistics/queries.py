import sqlalchemy


def entrypoint_min_max_timestamp_query(entrypoint):
    return sqlalchemy.text("""
SELECT
    MIN("Timestamp") AS "MinTimestamp",
    MAX("Timestamp") AS "MaxTimestamp"
FROM
    public."TransactionOps"
WHERE
    "Entrypoint" = :entrypoint
AND "Status" = 1
AND "Timestamp" < CAST(CURRENT_DATE AS TIMESTAMP) - INTERVAL '1 day' + INTERVAL '23 hours 59 minutes 59 seconds'
""").bindparams(entrypoint=entrypoint)

def entrypoint_transactions_query(entrypoint):
    return sqlalchemy.text("""
SELECT
    "SenderId",
    "TargetId",
    "Entrypoint",
    "Status",
    "Timestamp"
FROM
    public."TransactionOps"
WHERE
    "Entrypoint" = :entrypoint
AND "Status" = 1
AND "Timestamp" < CAST(CURRENT_DATE AS TIMESTAMP) - INTERVAL '1 day' + INTERVAL '23 hours 59 minutes 59 seconds'
""").bindparams(entrypoint=entrypoint)


def entrypoint_transactions_query_for_date(entrypoint, date, min_level, max_level):
    return sqlalchemy.text(f"""
SELECT
    "Id",
    "Level",
    "TargetId",
    "Entrypoint",
    "Timestamp",
    "Amount",
    "Status",
    "OpHash",
    "Counter",
    "Nonce",
    "Errors",
    "InitiatorId",
    "SenderId",
    "SenderCodeHash",
    "TargetCodeHash",
    "BakerFee",
    "GasUsed"
FROM
    public."TransactionOps"
WHERE
    "Entrypoint" = :entrypoint
AND "Level" >= {min_level} AND "Level" <= {max_level}
AND "Status" = 1
""").bindparams(entrypoint=entrypoint)

#
# AND "Timestamp" >= '{date}'::date 
# AND "Timestamp" < ('{date}'::date + '1 day'::interval)
