from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import os
import csv

# -------------------------------
# CONFIG
# -------------------------------
BUCKET_NAME = Variable.get("gcs_bucket")
CLICKSTREAM_FILE = Variable.get("clickstream_path")
TRANSACTIONS_FILE = Variable.get("transactions_path")
API_KEY = Variable.get("api_key")

# -------------------------------
# FUNCTIONS
# -------------------------------

def ingest_clickstream(**context):
    df = pd.read_csv(CLICKSTREAM_FILE)
    df.to_csv("/tmp/clickstream.csv", index=False)
    print("✅ Clickstream ingested")

def ingest_transactions(**context):
    df = pd.read_csv(TRANSACTIONS_FILE)
    df.to_csv("/tmp/transactions.csv", index=False)
    print("✅ Transactions ingested")

def ingest_currency_api(**context):
    url = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD"
    response = requests.get(url)
    data = response.json()
    with open("/tmp/rates.json", "w") as f:
        json.dump(data, f)
    print("✅ Currency API ingested")

def transform(**context):
    with open("/tmp/rates.json", "r") as f:
        rates = json.load(f).get("conversion_rates", {})

    # Clean transactions
    df_txn = pd.read_csv("/tmp/transactions.csv")
    df_txn.columns = [c.lower().replace(" ", "_") for c in df_txn.columns]
    df_txn["amount_in_usd"] = df_txn.apply(
        lambda row: row["amount"] / rates.get(row["currency"], 1)
        if row["currency"] in rates else row["amount"],
        axis=1
    )
    df_txn.to_parquet("/tmp/transactions_clean.parquet", index=False)

    # Clean clickstream
    df_click = pd.read_csv("/tmp/clickstream.csv")
    df_click.columns = [c.lower().replace(" ", "_") for c in df_click.columns]
    df_click.to_parquet("/tmp/clickstream_clean.parquet", index=False)

    print("✅ Transformations done")

def validate(**context):
    """Validate transformed datasets and push status to XCom"""
    with open("/tmp/rates.json", "r") as f:
        rates = json.load(f).get("conversion_rates", {})

    df_txn = pd.read_parquet("/tmp/transactions_clean.parquet")
    df_click = pd.read_parquet("/tmp/clickstream_clean.parquet")

    status = "PASS"
    if df_txn["amount"].isnull().any():
        status = "FAIL: NULL values in amount"
    elif (df_txn["amount"] < 0).any():
        status = "FAIL: Negative amounts found"
    elif not df_txn["currency"].isin(rates.keys()).all():
        status = "FAIL: Invalid currency codes"
    elif df_click["user_id"].isnull().any():
        status = "FAIL: NULL user_id in clickstream"

    ti = context["ti"]
    ti.xcom_push(key="validation_status", value=status)

    if status != "PASS":
        raise ValueError(f"❌ Validation failed: {status}")

    print("✅ Validation passed")

def track_metadata(**context):
    """Tracks metadata and writes run_log.csv"""
    run_date = context['ds']

    # Row counts
    rows_ingested_txn = len(pd.read_csv("/tmp/transactions.csv"))
    rows_ingested_click = len(pd.read_csv("/tmp/clickstream.csv"))
    rows_transformed_txn = len(pd.read_parquet("/tmp/transactions_clean.parquet"))
    rows_transformed_click = len(pd.read_parquet("/tmp/clickstream_clean.parquet"))

    rows_loaded_txn = rows_transformed_txn
    rows_loaded_click = rows_transformed_click

    ti = context["ti"]
    validation_status = ti.xcom_pull(task_ids="validate_data", key="validation_status")

    metadata_row = [
        run_date,
        rows_ingested_txn + rows_ingested_click,
        rows_transformed_txn + rows_transformed_click,
        rows_loaded_txn + rows_loaded_click,
        validation_status
    ]

    log_file = "/tmp/run_log.csv"
    header = ["run_date", "rows_ingested", "rows_transformed", "rows_loaded", "validation_status"]

    file_exists = os.path.isfile(log_file)
    with open(log_file, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)
        writer.writerow(metadata_row)

    print(f"✅ Metadata tracked: {metadata_row}")

# -------------------------------
# DAG DEFINITION
# -------------------------------
default_args = {
    "owner": "vishnu",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="week2_pipeline_with_metadata",
    default_args=default_args,
    start_date=datetime(2025, 9, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["internship", "week2"],
) as dag:

    task_ingest_clickstream = PythonOperator(
        task_id="ingest_clickstream",
        python_callable=ingest_clickstream
    )

    task_ingest_transactions = PythonOperator(
        task_id="ingest_transactions",
        python_callable=ingest_transactions
    )

    task_ingest_currency = PythonOperator(
        task_id="ingest_currency_api",
        python_callable=ingest_currency_api
    )

    task_transform = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    task_validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate
    )

    task_upload_clickstream = LocalFilesystemToGCSOperator(
        task_id="upload_clickstream",
        src="/tmp/clickstream_clean.parquet",
        dst="processed/clickstream/clickstream_clean.parquet",
        bucket=BUCKET_NAME,
        gcp_conn_id="google_cloud_default"
    )

    task_upload_transactions = LocalFilesystemToGCSOperator(
        task_id="upload_transactions",
        src="/tmp/transactions_clean.parquet",
        dst="processed/transactions/transactions_clean.parquet",
        bucket=BUCKET_NAME,
        gcp_conn_id="google_cloud_default"
    )

    task_track_metadata = PythonOperator(
        task_id="track_metadata",
        python_callable=track_metadata
    )

    task_upload_metadata = LocalFilesystemToGCSOperator(
        task_id="upload_metadata",
        src="/tmp/run_log.csv",
        dst="metadata/run_log.csv",
        bucket=BUCKET_NAME,
        gcp_conn_id="google_cloud_default"
    )

    # Orchestration
    [task_ingest_clickstream, task_ingest_transactions, task_ingest_currency] >> task_transform
    task_transform >> task_validate
    task_validate >> [task_upload_clickstream, task_upload_transactions]
    [task_upload_clickstream, task_upload_transactions] >> task_track_metadata
    task_track_metadata >> task_upload_metadata
