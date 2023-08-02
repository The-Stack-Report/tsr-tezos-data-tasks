import sqlalchemy

top_entrypoints_query = sqlalchemy.text("""
SELECT
    "Entrypoint",
    COUNT(*) AS recent_calls,
    MAX("Timestamp") AS latest_call
FROM
    (
        SELECT
            *
        FROM
            public."TransactionOps"
        WHERE
            "Entrypoint" IS NOT NULL
        ORDER BY
            "Id" DESC
        LIMIT
            100000
    ) t
GROUP BY
    "Entrypoint"
ORDER BY
    recent_calls DESC,
    latest_call DESC
""")

distinct_entrypoints_query = sqlalchemy.text("""
SELECT DISTINCT
    "Entrypoint"
FROM public."TransactionOps"
WHERE "Entrypoint" IS NOT NULL
""")


def entrypoint_statistics_query(entrypoint):
    return sqlalchemy.text("""
SELECT
    "Entrypoint",
    COUNT(*) AS total_rows,
    MIN("Timestamp") AS first_call,
    MAX("Timestamp") AS latest_call
FROM
    (
        SELECT  
            *
        FROM

            public."TransactionOps"
        WHERE
            "Entrypoint" = :entrypoint
        AND
            "Timestamp" < CAST(CURRENT_DATE AS TIMESTAMP) - INTERVAL '1 day' + INTERVAL '23 hours 59 minutes 59 seconds'
    ) t
GROUP BY
    "Entrypoint"
ORDER BY
    total_rows DESC,
    latest_call DESC
""").bindparams(entrypoint=entrypoint)