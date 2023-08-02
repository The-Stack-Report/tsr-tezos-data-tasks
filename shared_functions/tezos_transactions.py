import pandas as pd


def add_addresses_from_tzkt_ids(transactions_df, accounts_by_id):
    transactions_df["SenderAddress"] = transactions_df["SenderId"].apply(lambda x: accounts_by_id.get(x))
    transactions_df["TargetAddress"] = transactions_df["TargetId"].apply(lambda x: accounts_by_id.get(x))
    return transactions_df

def load_and_prepare_transactions_df(cache_path):
    transactions_df = pd.read_csv(cache_path)
    transactions_df["date"] = pd.to_datetime(transactions_df["Timestamp"]).dt.date
    transactions_df["date"] = pd.to_datetime(transactions_df["date"])
    transactions_df["date_formatted"] = transactions_df["date"].dt.strftime("%Y-%m-%d")

    return transactions_df