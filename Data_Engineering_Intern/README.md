# Week 2 – Data Engineering Internship  

 Objective  
The goal of Week 2 was to orchestrate the pipeline using Apache Airflow. The pipeline ingests raw data, transforms and validates it, uploads processed data to Google Cloud Storage (GCS), and logs metadata.  

---

##  Pipeline Overview  

ingest_clickstream → ingest_transactions → ingest_currency_api → transform → validate → load_to_gcs → track_metadata  

- Ingestion: Reads raw CSV files (`transactions.csv`, `clickstream.csv`) and API data (exchange rates).  
- **Transformation**:  
  - Standardizes column names.  
  - Converts timestamps to UTC.  
  - Adds `amount_in_usd` to transactions.  
- Validation:  
  - Transactions: no NULL amounts, no negative values, valid currency codes.  
  - Clickstream: no NULL user IDs.  
- Load to GCS: Saves cleaned parquet files into partitioned GCS paths.  
- Metadata Tracking: Logs run date, rows ingested, transformed, loaded, and validation status into `run_log.csv`.
---


The DAG shows task dependencies:  
- Parallel ingestion → transform → validate → upload → metadata.  

---

##  Monitoring & Alerts  
- Failures in validation raise errors and stop the DAG.  
- Composer logs can be monitored in **Cloud Logging**.  
---


