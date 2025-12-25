#ETL Pipeline & EDA

This project implements a complete ETL (Extract, Transform, Load) pipeline using Python and pandas.
Raw orders and users data are cleaned, validated, transformed, and joined into a final analytics table.

---

## Project Structure

.
├── src/
│   └── bootcamp_data/
│       ├── __init__.py
│       ├── etl.py
│       ├── io.py
│       ├── transforms.py
│       ├── joins.py
│       └── quality.py
│
├── scripts/
│   └── run_etl.py
│
├── data/
│   ├── raw/
│   │   ├── orders.csv
│   │   └── users.csv
│   └── processed/
│       ├── orders_clean.parquet
│       ├── users.parquet
│       ├── analytics_table.parquet
│       └── _run_meta.json
│
├── notebooks/
│   └── eda.ipynb
│
├── reports/
│   ├── figures/
│   └── summary.md
│
├── README.md
└── requirements.txt

---

## ETL Overview

### Extract
- Read raw CSV files from `data/raw/`

### Transform
- Validate required columns
- Enforce schema and data types
- Normalize categorical values
- Parse datetime columns
- Add time-based features
- Add missing-value flags
- Validate user uniqueness
- Safely join orders with users
- Winsorize amount outliers and flag them

### Load
- Write processed datasets as Parquet files
- Generate run metadata JSON

---

## Setup

Create virtual environment:
python -m venv .venv

Activate (Windows):
.venv\Scripts\Activate.ps1

Install dependencies:
pip install -r requirements.txt
pip install -e .

---

## Run ETL

From project root:
python scripts/run_etl.py

If no error appears, the ETL ran successfully.

---

## Outputs

Generated files:
- data/processed/orders_clean.parquet
- data/processed/users.parquet
- data/processed/analytics_table.parquet
- data/processed/_run_meta.json

---

## EDA

Open:
notebooks/eda.ipynb

Figures are saved to:
reports/figures/

---

## Notes

- Timezone warnings during monthly aggregation are expected.
- Pipeline is deterministic and idempotent.
- All transformations are pure and testable functions.
