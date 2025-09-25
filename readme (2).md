# Internship Data Engineering – BigQuery Project

## Project Overview

This project demonstrates loading, processing, and analyzing **clickstream** and **transaction** data using **Google BigQuery**. The objective is to practice **ETL workflows**, data schema management, and data analysis in a cloud environment.

## Prerequisites

* **Google Cloud Account** with access to BigQuery and Cloud Storage.
* Basic understanding of **data formats** (Parquet, JSON).
* Knowledge of **SQL queries** for data analysis.

## Project Structure

* **Datasets**: Separate datasets for clickstream and transaction data.
* **Schema files**: JSON files defining table columns, data types, and constraints.
* **Parquet files**: Raw and processed data stored in Cloud Storage.

## Workflow

1. **Data Storage**: Upload raw data to Cloud Storage and process into Parquet files.
2. **Dataset Creation**: Create datasets for clickstream and transaction data in BigQuery.
3. **Schema Definition**: Define table schemas via JSON files to match data types.
4. **Data Loading**: Load Parquet files into BigQuery using the schema files.

## Key Points

* Ensure table **schemas match the data types** in Parquet files.
* Delete old tables before reloading data to avoid conflicts.
* Use **partitioning and clustering** for efficient queries.
* Analyze data for insights on **user behavior, revenue trends, and transaction patterns**.

## Outcome

* Successfully loaded **clickstream** and **transaction** data into BigQuery.
* Gained practical understanding of **cloud ETL workflows**, schema management, and query performance optimization.
