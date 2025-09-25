from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
import json
import time
from google.cloud import storage

def write_metadata_to_gcs(metadata: dict, gcs_path: str):
    """Write metadata dictionary as JSON to GCS."""
    client = storage.Client()
    bucket_name, *path_parts = gcs_path.replace("gs://", "").split("/")
    blob_path = "/".join(path_parts).lstrip("/")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(json.dumps(metadata, indent=2), content_type="application/json")
    print(f"✅ Metadata written to {gcs_path}")

def safe_write(df, path, max_retries=3, sleep_sec=5):
    """Write DataFrame to GCS with retry logic and coalesce to reduce small files."""
    df_to_write = df.coalesce(10)  # Adjust number of partitions based on dataset size
    for attempt in range(max_retries):
        try:
            df_to_write.write.mode("overwrite").parquet(path)
            print(f"✅ Data successfully written to {path}")
            return
        except Exception as e:
            print(f"⚠️ Write attempt {attempt+1} failed: {e}")
            if attempt < max_retries - 1:
                print(f"⏳ Retrying in {sleep_sec} seconds...")
                time.sleep(sleep_sec)
            else:
                raise

def run_clickstream_job(input_path, output_path, run_date):
    spark = SparkSession.builder \
        .appName("ClickstreamJob") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    try:
        # -------------------------
        # 1. Read Raw Data
        # -------------------------
        print(f"📥 Reading clickstream data from {input_path}")
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(input_path)
        )

        # -------------------------
        # 2. Schema Validation
        # -------------------------
        required_columns = {"user_id", "session_id", "page_url", "click_time", "device", "location"}
        missing_cols = required_columns - set(df.columns)
        if missing_cols:
            raise ValueError(f"❌ Schema validation failed! Missing columns: {missing_cols}")
        print(f"✅ Required columns detected: {required_columns}")

        # Ensure consistent column order
        df = df.select(*required_columns)

        # -------------------------
        # 3. Data Quality
        # -------------------------
        df_clean = df.dropna(subset=list(required_columns)).dropDuplicates()
        df_clean = df_clean.withColumn("user_id", col("user_id").cast("string"))
        print("✅ user_id cast to STRING")
        print("✅ Null checks and deduplication applied")

        # -------------------------
        # 4. Compute Metadata
        # -------------------------
        metadata = {
            "row_count": df_clean.count(),
            "columns": df_clean.columns,
            "run_date": run_date
        }

        # -------------------------
        # 5. Write Processed Data to GCS
        # -------------------------
        output_partition = f"{output_path}/run_date={run_date}"
        try:
            safe_write(df_clean, output_partition)
        except Exception as e:
            print(f"❌ Failed to write processed data: {e}")
            raise

        # -------------------------
        # 6. Write Metadata JSON to GCS
        # -------------------------
        metadata_path = f"{output_path}/metadata/{run_date}.json"
        try:
            write_metadata_to_gcs(metadata, metadata_path)
        except Exception as e:
            print(f"❌ Failed to write metadata: {e}")

        print("🏁 Job finished successfully")

    except Exception as e:
        print(f"❌ Job failed: {e}")

    finally:
        spark.stop()
        print("🏁 Spark session stopped")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: clickstream.py <input_path> <output_path> <run_date>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    run_date = sys.argv[3]

    run_clickstream_job(input_path, output_path, run_date)
