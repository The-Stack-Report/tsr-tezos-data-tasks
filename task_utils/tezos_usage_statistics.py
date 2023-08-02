import numpy as np

def base_stats():
    return {
        "transactions": 0,
        "senders": 0,
        "targets": 0
    }


def usage_statistics_from_transactions_df(transactions_df):
    expected_columns = [
        "Timestamp",
        "Entrypoint",
        "SenderAddress",
        "TargetAddress",
        "Status"
    ]
    for column in expected_columns:
        if column not in transactions_df.columns:
            raise Exception(f"Missing column {column} in transactions_df")
    

    stats = base_stats()

    if len(transactions_df) == 0:
        return stats
    
    stats["transactions"] = len(transactions_df)
    stats["senders"] = transactions_df["SenderAddress"].nunique()
    stats["targets"] = transactions_df["TargetAddress"].nunique()
    

    wallet_sender_transactions = transactions_df.loc[transactions_df["SenderAddress"].str.startswith("tz1")]
    contract_sender_transactions = transactions_df.loc[transactions_df["SenderAddress"].str.startswith("KT1")]

    stats["wallet_senders"] = wallet_sender_transactions["SenderAddress"].nunique()
    stats["contract_senders"] = contract_sender_transactions["SenderAddress"].nunique()

    wallet_target_transactions = transactions_df.loc[transactions_df["TargetAddress"].str.startswith("tz1")]
    contract_target_transactions = transactions_df.loc[transactions_df["TargetAddress"].str.startswith("KT1")]

    stats["wallet_targets"] = wallet_target_transactions["TargetAddress"].nunique()
    stats["contract_targets"] = contract_target_transactions["TargetAddress"].nunique()

    return stats

def base_network_json():
    return {
        "nodes": [],
        "edges": [],
        "senders": [],
        "targets": []
    }

def network_json_from_transactions_df(transactions_df):
    network_json = base_network_json()

    if len(transactions_df) == 0:
        return network_json
    
    senders = transactions_df["SenderAddress"].unique()
    targets = transactions_df["TargetAddress"].unique()
    transactions_df['sender-target'] = transactions_df['SenderAddress'] + transactions_df['TargetAddress']

    nodes = np.unique(np.concatenate((senders, targets)))

    network_json["nodes"] = nodes.tolist()
    network_json["edges"] = len(transactions_df)

    network_json["senders"] = senders.tolist()
    network_json["targets"] = targets.tolist()

    return network_json

