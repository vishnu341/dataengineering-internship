import pandas as pd
import json

def validate_transactions(txn_file, rates_file, **context):
    """Validate transactions parquet file"""
    with open(rates_file, "r") as f:
        rates = json.load(f).get("conversion_rates", {})

    df_txn = pd.read_parquet(txn_file)

    status = "PASS"

    if df_txn["amount"].isnull().any():
        status = "FAIL: NULL values in amount"
    elif (df_txn["amount"] < 0).any():
        status = "FAIL: Negative amounts found"
    elif not df_txn["currency"].isin(rates.keys()).all():
        status = "FAIL: Invalid currency codes"

    ti = context["ti"]
    ti.xcom_push(key="validation_transactions", value=status)

    if status != "PASS":
        raise ValueError(f"❌ Validation failed: {status}")

    print("✅ Transactions validation passed")
    return status


def validate_clickstream(click_file, **context):
    """Validate clickstream parquet file"""
    df_click = pd.read_parquet(click_file)

    status = "PASS"
    if df_click["user_id"].isnull().any():
        status = "FAIL: NULL user_id found"

    ti = context["ti"]
    ti.xcom_push(key="validation_clickstream", value=status)

    if status != "PASS":
        raise ValueError(f"❌ Validation failed: {status}")

    print("✅ Clickstream validation passed")
    return status
