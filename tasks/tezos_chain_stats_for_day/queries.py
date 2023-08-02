import sqlalchemy
import datetime

def ops_for_date_flat_query(dt):
    next_dt = dt + datetime.timedelta(days=1)
    dt_formatted = dt.strftime("%Y-%m-%d")
    next_dt_formatted = next_dt.strftime("%Y-%m-%d")

    print(f"Generating query from date: {dt_formatted} to date {next_dt_formatted}")
    return sqlalchemy.text(f"""SELECT
ops."Id",
ops."TargetId",
ops."Entrypoint",
ops."Amount",
ops."Timestamp",
ops."Status",
ops."OpHash",
ops."Errors",
ops."SenderId",
ops."InitiatorId",
ops."BakerFee",
ops."StorageFee",
ops."AllocationFee",
ops."GasUsed",
ops."GasLimit",
ops."Level"
FROM "TransactionOps" as ops
WHERE ops."Status" = 1
AND ops."Timestamp" BETWEEN '{dt_formatted}' AND '{next_dt_formatted}'
ORDER BY ops."Timestamp" ASC
""")
