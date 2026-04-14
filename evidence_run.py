"""
Evidence script — S&P 500 Big Data Project
Atzin Eduardo Cruz Briones
Run: .venv/bin/python evidence_run.py
Take a screenshot at each SCREENSHOT marker.
"""

import os, time, sqlite3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, stddev, avg, sum as spark_sum, year, month, concat_ws

DB = "processed/stocks_db.sqlite"
RAW_CSV = "data/all_stocks_5yr.csv"
INDIV_DIR = "data/individual_stocks_5yr/individual_stocks_5yr"
PARQUET = "processed/stocks_clean.parquet"
MONTHLY_PARQUET = "processed/monthly_aggregated.parquet"

os.chdir(os.path.dirname(os.path.abspath(__file__)))

# ─────────────────────────────────────────────────────────────
# STEP 1 — DATA INGESTION
# ─────────────────────────────────────────────────────────────
print("=" * 60)
print("STEP 1 — DATA INGESTION")
print("=" * 60)

indiv_files = [f for f in os.listdir(INDIV_DIR) if f.endswith(".csv")]
indiv_rows = sum(
    sum(1 for _ in open(os.path.join(INDIV_DIR, f))) - 1
    for f in indiv_files
)
main_rows = sum(1 for _ in open(RAW_CSV)) - 1

print(f"  all_stocks_5yr.csv       : {main_rows:>10,} rows")
print(f"  individual_stocks_5yr/   : {len(indiv_files):>10,} files  |  {indiv_rows:>10,} rows")
print(f"  TOTAL INGESTED           : {main_rows + indiv_rows:>10,} rows")
print()
print("  >>> SCREENSHOT 1: Data Ingestion — row counts <<<")
input("  Press Enter to continue...")

# ─────────────────────────────────────────────────────────────
# STEP 2 — SPARK SESSION STARTUP
# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("STEP 2 — SPARK SESSION STARTUP")
print("=" * 60)

spark = SparkSession.builder \
    .appName("SP500_BigData_Evidence") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print(f"  Spark version  : {spark.version}")
print(f"  App name       : {spark.sparkContext.appName}")
print(f"  Master         : {spark.sparkContext.master}")
print(f"  Default parallelism : {spark.sparkContext.defaultParallelism}")
print()
print("  >>> SCREENSHOT 2: Spark Session — startup info <<<")
input("  Press Enter to continue...")

# ─────────────────────────────────────────────────────────────
# STEP 3 — BATCH PROCESSING: LOAD + CLEAN
# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("STEP 3 — SPARK BATCH: LOAD + CLEAN")
print("=" * 60)

df_raw = spark.read.csv(RAW_CSV, header=True, inferSchema=True)
print(f"  Raw rows loaded    : {df_raw.count():>10,}")

df_clean = df_raw.dropna(subset=["close", "volume", "open"]) \
                 .filter((col("close") > 0) & (col("volume") > 0))
print(f"  After cleaning     : {df_clean.count():>10,}")
print(f"  Columns            : {df_clean.columns}")
print()
print("  Schema:")
df_clean.printSchema()
print()
print("  >>> SCREENSHOT 3: Spark Batch — load and clean <<<")
input("  Press Enter to continue...")

# ─────────────────────────────────────────────────────────────
# STEP 4 — TRANSFORMATIONS (feature engineering)
# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("STEP 4 — SPARK TRANSFORMATIONS (Feature Engineering)")
print("=" * 60)

from pyspark.sql.functions import (
    col, round as spark_round, to_date,
    year as yr, month as mo, date_format
)

df_transformed = df_clean \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
    .withColumn("daily_return", spark_round((col("close") - col("open")) / col("open") * 100, 4)) \
    .withColumn("price_range",  spark_round(col("high") - col("low"), 4)) \
    .withColumn("year",  yr(col("date"))) \
    .withColumn("month", mo(col("date"))) \
    .withColumn("month_year", date_format(col("date"), "yyyy-MM"))

df_transformed.select("date", "Name", "open", "close", "daily_return", "price_range", "month_year").show(10, truncate=False)
print(f"  Total rows after transformation : {df_transformed.count():>10,}")
print()
print("  >>> SCREENSHOT 4: Spark Transformations — features <<<")
input("  Press Enter to continue...")

# ─────────────────────────────────────────────────────────────
# STEP 5 — AGGREGATIONS
# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("STEP 5 — SPARK AGGREGATIONS (Monthly per Ticker)")
print("=" * 60)

from pyspark.sql.functions import stddev, max as spark_max, min as spark_min

monthly_agg = df_transformed.groupBy("Name", "month_year") \
    .agg(
        spark_round(avg("close"), 4).alias("avg_close"),
        spark_round(stddev("close"), 4).alias("volatility"),
        spark_round(avg("daily_return"), 6).alias("avg_daily_return"),
        spark_sum("volume").alias("total_volume"),
        spark_round(spark_max("high"), 4).alias("max_high"),
        spark_round(spark_min("low"), 4).alias("min_low")
    )

print(f"  Monthly aggregation rows : {monthly_agg.count():>10,}")
monthly_agg.orderBy("Name", "month_year").show(10, truncate=False)
print()
print("  >>> SCREENSHOT 5: Spark Aggregations — monthly stats <<<")
input("  Press Enter to continue...")

# ─────────────────────────────────────────────────────────────
# STEP 6 — LOAD INTO DATABASE
# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("STEP 6 — LOAD INTO SQLITE DATABASE")
print("=" * 60)

conn = sqlite3.connect(DB)

# fact_daily_prices
df_pd = df_transformed.select(
    "date", col("Name").alias("ticker"),
    "open", "high", "low", "close", "volume",
    "daily_return", "price_range"
).toPandas()
df_pd["date"] = df_pd["date"].astype(str)

df_pd.to_sql("fact_daily_prices", conn, if_exists="replace", index=False, chunksize=10000)
print(f"  fact_daily_prices loaded : {len(df_pd):>10,} rows")

# fact_aggregated
agg_pd = monthly_agg.toPandas()
agg_pd.rename(columns={"Name": "ticker"}, inplace=True)
agg_pd.to_sql("fact_aggregated", conn, if_exists="replace", index=False, chunksize=10000)
print(f"  fact_aggregated loaded   : {len(agg_pd):>10,} rows")

conn.close()

# Verify
conn = sqlite3.connect(DB)
cur = conn.cursor()
print()
print("  Final row counts in SQLite:")
for t in ["fact_daily_prices", "fact_aggregated", "streaming_results", "ml_predictions", "sector_analysis"]:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    print(f"    {t:<25}: {cur.fetchone()[0]:>10,}")
conn.close()
print()
print("  >>> SCREENSHOT 6: Database Load — all table counts <<<")
input("  Press Enter to continue...")

# ─────────────────────────────────────────────────────────────
# DONE
# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("ALL STEPS COMPLETE — pipeline evidence captured")
print("=" * 60)
spark.stop()
