# ETL Project — Orders & Users Analytics

## Project Overview
This project demonstrates a full ETL (Extract, Transform, Load) workflow for e-commerce order and user data. The goal is to clean, enrich, and aggregate raw CSV data into a structured analytics table suitable for analysis.

## Raw Data
Two CSV files are used as input:
- `orders.csv` — contains individual order records (order ID, user ID, amount, quantity, status, timestamps, etc.)
- `users.csv` — contains user information (user ID, country, signup date)

## ETL Pipeline
The pipeline is implemented in three main stages:

### 1. Load & Convert (`run_day1_load.py`)
- Reads raw CSVs from the `data/raw/` directory.
- Converts data to Parquet format for faster processing.
- Ensures basic schema enforcement on the orders dataset.
- Saves intermediate outputs:
  - `orders.parquet`
  - `users.parquet`
- Writes a `_run_meta_day1.json` metadata file containing row counts and timestamps.

### 2. Data Cleaning (`run_day2_clean.py`)
- Performs validations and quality checks:
  - Required columns are present.
  - No empty datasets.
  - Unique keys for users and orders.
  - Values within expected ranges (e.g., amount ≥ 0, quantity ≥ 1).
- Cleans and normalizes categorical fields (`status` column mapped to standard categories: `paid` and `refund`).
- Deduplicates orders, keeping the latest record per `order_id`.
- Generates a missingness report for further inspection.
- Saves cleaned outputs:
  - `orders_clean.parquet`
  - `users.parquet`
- Updates `_run_meta.json` with processed row counts.

### 3. Analytics Table (`run_day3_build_analytics.py`)
- Parses timestamps and adds derived time features (year, month, day of week, hour).
- Joins orders with user information to enrich analytics data.
- Computes winsorized amounts to reduce outlier effects.
- Flags extreme values for further analysis.
- Produces the final analytics table:
  - `analytics_table.parquet`

## Output Files
All processed data and metadata are stored in the `data/processed/` directory:
- `orders_clean.parquet`
- `users.parquet`
- `analytics_table.parquet`
- `_run_meta.json` — contains pipeline timestamps, row counts, and join metrics.

## How to Run
1. Ensure your Python environment is set up:
   ```bash
   python -m venv .venv
   # activate the environment
   pip install -r requirements.txt
