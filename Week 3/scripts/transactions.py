from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import sys

def validate_schema(df, required_columns):
    """
    Validates if DataFrame contains all required columns.
    Returns (is_valid, missing_columns).
    """
    df_columns = set(df.columns)
    missing = required_columns - df_columns
    return len(missing) == 0, missing

def main(input_path, output_path, run_date):
    spark = SparkSession.builder.appName("TransactionsETLJob").getOrCreate()

    print(f"📥 Reading transaction data from {input_path}")
    df = spark.read.option("header", True).csv(input_path)

    # ✅ Required schema based on actual CSV
    required_columns = {"txn_id", "user_id", "amount", "currency", "txn_time"}

    is_valid, missing = validate_schema(df, required_columns)
    if not is_valid:
        print(f"❌ Schema validation failed! Missing columns: {missing}")
        spark.stop()
        sys.exit(1)

    print("✅ Schema validation passed!")

    # 🔍 Data quality checks
    df_valid = (
        df.filter(col("txn_id").isNotNull())
          .filter(col("user_id").isNotNull())
          .filter(col("amount").cast("double") > 0)
          .filter(col("currency").isNotNull())
          .filter(col("txn_time").isNotNull())
    )

    # Add run_date column for partitioning
    df_valid = df_valid.withColumn("run_date", lit(run_date))

    print(f"💾 Writing processed data to {output_path}")
    (
        df_valid.write.mode("overwrite")
        .partitionBy("run_date")
        .parquet(output_path)
    )

    print("🎉 ETL job completed successfully")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: transactions_job.py <input_path> <output_path> <run_date>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    run_date = sys.argv[3]

    main(input_path, output_path, run_date)
