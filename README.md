# S&P 500 Stock Market — Big Data Analysis Project

**Student:** Atzin Eduardo Cruz Briones  
**Course:** Big Data — Universidad Panamericana  
**Professor:** Alfredo Marquez Martinez  
**Dataset:** S&P 500 Historical Stock Data (Kaggle) — 1,238,080 records  

---

## Project Overview

End-to-end big data pipeline that ingests, processes, stores, and analyzes 5 years of S&P 500 historical stock data (2013–2018) across 505 tickers. The pipeline covers batch processing with PySpark, streaming simulation with Spark Structured Streaming, a star schema relational database, sector-level analytics, and 30-day price forecasting with Facebook Prophet.

---

## Repository Structure

```
finance/
├── data/
│   ├── all_stocks_5yr.csv               # Raw data — 619,040 rows
│   └── individual_stocks_5yr/           # Raw data — 505 CSV files (619,040 rows)
├── notebooks/
│   └── stonks.ipynb                     # Main pipeline notebook
├── processed/
│   ├── stocks_clean.parquet             # Cleaned Spark output
│   ├── monthly_aggregated.parquet       # Monthly aggregations
│   ├── stocks_db.sqlite                 # Star schema database (5 tables)
│   └── charts/                          # Generated visualizations
│       ├── dashboard.png
│       ├── forecast_chart.png
│       ├── volatility_chart.png
│       └── pipeline_architecture.png
├── streaming/
│   ├── stream_input/                    # 5 micro-batch CSV files
│   └── stream_output/                   # Spark Structured Streaming output
├── evidence_run.py                      # Evidence/execution script
├── screenshots/                         # Execution evidence (required by rubric)
└── README.md
```

---

## Requirements

- Python 3.14+
- Java 11+ (required by PySpark — check with `java -version`)

Python packages:

| Package | Version |
|---|---|
| pyspark | 4.1.1 |
| pandas | 3.0.2 |
| prophet | 1.3.0 |
| pyarrow | 23.0.1 |
| matplotlib | 3.10.8 |

---

## Step-by-Step Execution

### 1. Clone the repository

```bash
git clone <repo-url>
cd finance
```

### 2. Create and activate virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate        # Mac/Linux
# .venv\Scripts\activate         # Windows
```

### 3. Install dependencies

```bash
pip install pyspark==4.1.1 pandas==3.0.2 prophet==1.3.0 pyarrow==23.0.1 matplotlib==3.10.8
```

### 4. Verify raw data is present

```bash
wc -l data/all_stocks_5yr.csv
# Expected: 619041 (619040 rows + header)

ls data/individual_stocks_5yr/individual_stocks_5yr/ | wc -l
# Expected: 505
```

### 5. Run the main pipeline (Jupyter Notebook)

```bash
pip install jupyter
jupyter notebook notebooks/stonks.ipynb
```

Run all cells from top to bottom. The notebook executes:
- Data ingestion and validation
- PySpark batch processing (clean, transform, feature engineering)
- JOIN with sector metadata
- Monthly aggregations
- Spark Structured Streaming simulation
- SQLite star schema population
- Prophet ML forecasting (AAPL, MSFT, GOOGL, BAC, GE)
- Dashboard visualization generation

### 6. Run the evidence script (execution proof)

```bash
python evidence_run.py
```

This script re-executes each pipeline stage with labeled output and pauses for screenshots at 6 checkpoints:

| Checkpoint | Evidence captured |
|---|---|
| Screenshot 1 | Data ingestion — row counts per source |
| Screenshot 2 | Spark session startup — version and config |
| Screenshot 3 | Batch load + cleaning — schema and row counts |
| Screenshot 4 | Transformations — daily_return, price_range features |
| Screenshot 5 | Aggregations — monthly stats per ticker |
| Screenshot 6 | Database load — final row counts for all 5 tables |

> **Note:** `processed/stocks_db.sqlite` is excluded from the repository (107 MB exceeds GitHub's 100 MB limit). It is automatically generated when the notebook is run from Step 5 onward. Screenshots in `screenshots/` provide execution evidence of all table row counts.

### 7. Verify database tables

```bash
python3 -c "
import sqlite3
conn = sqlite3.connect('processed/stocks_db.sqlite')
cur = conn.cursor()
for t in ['fact_daily_prices','fact_aggregated','streaming_results','ml_predictions','sector_analysis']:
    cur.execute(f'SELECT COUNT(*) FROM {t}')
    print(f'{t}: {cur.fetchone()[0]:,}')
"
```

Expected output:
```
fact_daily_prices:  619,029
fact_aggregated:     30,012
streaming_results:  619,025
ml_predictions:       6,445
sector_analysis:          9
```

---

## Pipeline Architecture

```
Kaggle CSV (raw)
      │
      ▼
OCI Object Storage (raw/)
      │
      ▼
PySpark Batch Processing
  ├── Clean (dropna, filter negatives)
  ├── Transform (daily_return, price_range, date parts)
  ├── JOIN (sector metadata enrichment)
  └── Aggregate (monthly stats per ticker)
      │
      ├──▶ Parquet (processed/)
      ├──▶ SQLite star schema (5 tables)
      └──▶ Charts (Matplotlib)
      
Spark Structured Streaming
  ├── Input: stream_input/ (5 batches × ~123k rows)
  └── Output: stream_output/ (8 part-files)
  
Facebook Prophet
  └── 30-day forecasts for AAPL, MSFT, GOOGL, BAC, GE
```

---

## Database Schema (Star Schema)

| Table | Type | Rows | Description |
|---|---|---|---|
| `fact_daily_prices` | Fact | 619,029 | Daily OHLCV + daily_return + price_range |
| `fact_aggregated` | Fact | 30,012 | Monthly avg_close, volatility, volume per ticker |
| `sector_analysis` | Dimension | 9 | Sector-level JOIN results |
| `ml_predictions` | Fact | 6,445 | Prophet 30-day forecasts with confidence bounds |
| `streaming_results` | Fact | ~619,025 | Spark Structured Streaming output |

---

## Data Sources

- **Dataset:** [S&P 500 Stock Data — Kaggle](https://www.kaggle.com/datasets/camnugent/sandp500)
- **Coverage:** February 2013 – February 2018
- **Tickers:** 505 S&P 500 companies
- **Total records:** 1,238,080 rows (both files combined)
