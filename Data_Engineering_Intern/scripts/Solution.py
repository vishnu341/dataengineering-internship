import os
import pandas as pd
import requests
import json
import logging
from datetime import date
from google.cloud import storage

API_KEY = "55746bdfef49195045cfbdf0"   
BUCKET_NAME = "data-engineering-intern"  
CHUNK_SIZE = 50000 

def fetch_exchange_rates():
    url = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD"
    response = requests.get(url)
    data = response.json()

    today = date.today().strftime("%Y-%m-%d")
    save_path = f"data/raw/api_currency/{today}"
    os.makedirs(save_path, exist_ok=True)

    with open(f"{save_path}/rates.json", "w") as f:
        json.dump(data, f, indent=2)

    if response.status_code == 200 and data.get("result") == "success":
        logging.info("Successfully fetched exchange rates")
        return data["conversion_rates"]
    else:
        logging.warning("API Error: using empty rates")
        return {}


def standardize_columns(df):   
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
    return df


def clean_dataframe(df):
    df = standardize_columns(df)
    df = df.drop_duplicates()
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    return df


def clean_and_enrich_transactions(df, rates):    
    df = clean_dataframe(df)
    if "amount" in df.columns and "currency" in df.columns:
        df["amount_in_usd"] = df.apply(
            lambda row: row["amount"] / rates.get(row["currency"], 1)
            if row["currency"] in rates else row["amount"], axis=1
        )
    return df

def upload_to_gcs(local_file, bucket_name, gcs_path):
    client = storage.Client(project="data-engineering-intern")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_file)
    logging.info(f"Uploaded {local_file} → gs://{bucket_name}/{gcs_path}")

def run_pipeline():
    logging.basicConfig(level=logging.INFO)
    today = date.today().strftime("%Y-%m-%d")
    logging.info("Step 1: Fetching exchange rates...")
    rates = fetch_exchange_rates()
    logging.info("Step 2: Processing transactions.csv...")
    transactions_file = "data/raw/csv/transactions.csv"

    if not os.path.exists(transactions_file):
        logging.error("transactions.csv file missing in data/raw/csv/")
    else:
        processed_path = f"data/processed/transactions/ingest_date={today}"
        os.makedirs(processed_path, exist_ok=True)

        for i, chunk in enumerate(pd.read_csv(transactions_file, chunksize=CHUNK_SIZE)):
            df_clean = clean_and_enrich_transactions(chunk, rates)
            out_file = f"{processed_path}/transactions_part{i}.parquet"
            df_clean.to_parquet(out_file, index=False)
            logging.info(f"Saved cleaned chunk {i} → {out_file}")
            gcs_path = f"processed/transactions/ingest_date={today}/transactions_part{i}.parquet"
            upload_to_gcs(out_file, BUCKET_NAME, gcs_path)

    logging.info("Step 3: Processing clickstream.csv...")
    clickstream_file = "data/raw/csv/clickstream.csv"

    if not os.path.exists(clickstream_file):
        logging.error("clickstream.csv file missing in data/raw/csv/")
    else:
        processed_path = f"data/processed/clickstream/ingest_date={today}"
        os.makedirs(processed_path, exist_ok=True)

        for i, chunk in enumerate(pd.read_csv(clickstream_file, chunksize=CHUNK_SIZE)):
            df_clean = clean_dataframe(chunk)
            out_file = f"{processed_path}/clickstream_part{i}.parquet"
            df_clean.to_parquet(out_file, index=False)
            logging.info(f"Saved cleaned chunk {i} → {out_file}")
            gcs_path = f"processed/clickstream/ingest_date={today}/clickstream_part{i}.parquet"
            upload_to_gcs(out_file, BUCKET_NAME, gcs_path)

    logging.info("Pipeline finished successfully!")


if __name__ == "__main__":
    run_pipeline()