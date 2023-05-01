import sqlalchemy

def create_contract_target_transactions_by_id_query(kt_address_id):
    return sqlalchemy.text(f"""
SELECT
ops."Id",
ops."TargetId",
ops."Entrypoint",
ops."Timestamp",
ops."Amount",
ops."Status",
ops."OpHash",
ops."Counter",
ops."Nonce",
ops."Errors",
ops."InitiatorId",
ops."SenderId",
ops."SenderCodeHash",
ops."TargetCodeHash",
ops."BakerFee",
ops."GasUsed"
FROM "TransactionOps" as ops
WHERE ops."TargetId" = {kt_address_id}
AND ops."Status" = 1
ORDER BY ops."Timestamp" ASC

    """)

def create_contract_sender_transactions_by_id_query(kt_address_id):
    return sqlalchemy.text(f"""
SELECT
ops."Id",
ops."Status",
ops."TargetId",
ops."SenderId",
ops."InitiatorId",
ops."OpHash",
ops."Timestamp",
ops."Amount",
ops."Counter",
ops."Errors",
ops."Entrypoint",
ops."SenderCodeHash"
FROM "TransactionOps" as ops
WHERE ops."SenderId" = {kt_address_id}
AND ops."Status" = 1
ORDER BY ops."Timestamp" ASC
""")