# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

from datetime import datetime
start_time = datetime.now()
notebook_name = "02_silver_transformation"

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_coinbase_candles (
  product_id STRING,
  granularity INT,
  ts TIMESTAMP,
  unix_ts BIGINT,
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  close DOUBLE,
  volume DOUBLE,
  ingestion_batch_id STRING,
  ingestion_ts TIMESTAMP,
  source STRING,
  _source_window_start TIMESTAMP,
  _source_window_end TIMESTAMP
)
USING DELTA
""")

print(" Silver table ready: silver_coinbase_candles")


# COMMAND ----------

from pyspark.sql.functions import col, from_json, explode, to_timestamp, lit, row_number, desc
from pyspark.sql.types import ArrayType, DoubleType, LongType
from pyspark.sql.window import Window

try:
    # --- STEP 1: PARSING LOGIC ---
    # Load raw data from Bronze table
    bronze = spark.table("bronze_coinbase_candles_raw")
    
    # Define the Coinbase API response schema (nested arrays of doubles)
    candle_schema = ArrayType(ArrayType(DoubleType()))

    # Parse JSON payload and explode the array into individual rows
    parsed = (bronze
        .withColumn("candles_array", from_json(col("raw_payload"), candle_schema))
        .withColumn("candle", explode(col("candles_array")))
    )

    # Map array elements to meaningful column names and cast data types
    silver_df = (parsed
        .select(
            col("product_id"), 
            col("granularity"), 
            col("ingestion_batch_id"),
            col("ingestion_ts"), 
            col("source"),
            col("start_ts").alias("_source_window_start"),
            col("end_ts").alias("_source_window_end"),
            col("candle")[0].cast("long").alias("unix_ts"),
            col("candle")[3].cast("double").alias("open"),
            col("candle")[2].cast("double").alias("high"),
            col("candle")[1].cast("double").alias("low"),
            col("candle")[4].cast("double").alias("close"),
            col("candle")[5].cast("double").alias("volume"),
        )
        .withColumn("ts", to_timestamp(col("unix_ts")))
    )

    # --- STEP 2: DEDUPLICATION LOGIC ---
    # Define a window to identify the most recent ingestion for the same timestamp
    w = Window.partitionBy("product_id", "granularity", "unix_ts").orderBy(desc("ingestion_ts"))
    
    # Keep only the latest record (rn=1) per candle
    silver_dedup = (silver_df
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # --- STEP 3: WRITE LOGIC ---
    # Append the processed data to the Silver Delta table
    silver_dedup.write.format("delta").mode("append").saveAsTable("silver_coinbase_candles")
    
    # --- SUCCESS LOGGING ---
    # Capture the row count for monitoring and audit purposes
    rows_final = silver_dedup.count()
    log_pipeline_step(
        notebook_name=notebook_name, 
        status="SUCCESS", 
        start_ts=start_time, 
        rows=rows_final, 
        message="Silver transformation successful"
    )
    print(f"Success: {rows_final} rows processed")

except Exception as e:
    # --- ERROR LOGGING ---
    # Record the failure and the error message in the centralized logs
    log_pipeline_step(
        notebook_name=notebook_name, 
        status="ERROR", 
        start_ts=start_time, 
        rows=0, 
        message=str(e)[:250]
    )
    raise e

# COMMAND ----------

spark.sql("""
SELECT product_id, COUNT(*) as n_rows, MIN(ts) as min_ts, MAX(ts) as max_ts
FROM silver_coinbase_candles
GROUP BY product_id
ORDER BY product_id
""").show(truncate=False)


# COMMAND ----------

# Reset and clean the Silver table to ensure data integrity
# This ensures we have exactly 730 rows per asset (2 years of daily data) without duplicates
silver_dedup.write.format("delta").mode("overwrite").saveAsTable("silver_coinbase_candles")

print("Silver table successfully reset and cleaned.")

# COMMAND ----------

# Check the final count after overwrite
spark.sql("""
SELECT product_id, COUNT(*) as n_rows, MIN(ts) as min_ts, MAX(ts) as max_ts
FROM silver_coinbase_candles
GROUP BY product_id
ORDER BY product_id
""").show()