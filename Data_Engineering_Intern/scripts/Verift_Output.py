import pandas as pd

df_txn = pd.read_parquet("data/processed/transactions/ingest_date=2025-09-06/transactions_part0.parquet")
print(df_txn.head())
print(df_txn.info())
df_click = pd.read_parquet("data/processed/clickstream/ingest_date=2025-09-06/clickstream_part0.parquet")
print(df_click.head())
print(df_click.info())
df_raw_txn = pd.read_csv("data/raw/csv/transactions.csv")
print("\nRaw transactions:", len(df_raw_txn))
print("Processed transactions:", len(df_txn))
