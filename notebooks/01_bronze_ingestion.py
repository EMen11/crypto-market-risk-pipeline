# Databricks notebook source
# MAGIC %run ./00_config
# MAGIC

# COMMAND ----------

from datetime import datetime
start_time = datetime.now()
notebook_name = "01_bronze_ingestion"

# COMMAND ----------

# Create Bronze table (Delta)
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_coinbase_candles_raw (
  ingestion_ts TIMESTAMP,
  ingestion_batch_id STRING,
  product_id STRING,
  granularity INT,
  start_ts TIMESTAMP,
  end_ts TIMESTAMP,
  http_status INT,
  records_count INT,
  raw_payload STRING,
  source STRING
)
USING DELTA
""")

print(" Bronze table ready: bronze_coinbase_candles_raw")



# COMMAND ----------

from datetime import timedelta

# --- DO NOT DELETE THIS FUNCTION ---
def generate_windows(start_dt: datetime, end_dt: datetime, window_days: int = 250):
    windows = []
    cur = start_dt
    while cur < end_dt:
        nxt = min(cur + timedelta(days=window_days), end_dt)
        windows.append((cur, nxt))
        cur = nxt
    return windows

# --- NEW INCREMENTAL LOGIC ---
# 1. Get the starting point (Watermark)
# We don't need %run here because it was already executed in Cell 1
INCREMENTAL_START_DT = get_bronze_watermark("bronze_coinbase_candles_raw", START_DT)

# 2. Define the end point as 'now'
CURRENT_END_DT = datetime.now()

# 3. Generate the windows
windows = generate_windows(INCREMENTAL_START_DT, CURRENT_END_DT, window_days=250)

print(f"--- Ingestion Plan ---")
print(f"Start: {iso_utc(INCREMENTAL_START_DT)}")
print(f"End  : {iso_utc(CURRENT_END_DT)}")
print(f"Total windows to fetch: {len(windows)}")

# COMMAND ----------

import json
from pyspark.sql.types import *

try:
    ingestion_batch_id = make_batch_id("bronze")
    rows_data = []

    for product_id in PRODUCTS:
        for (w_start, w_end) in windows:
            url = build_candles_url(product_id, w_start, w_end, GRANULARITY)
            res = http_get_json(url)
            payload = res["payload"] if res["payload"] is not None else []
            
            # Create a simple tuple with EXACTLY 10 elements
            row_tuple = (
                utc_now(),                          
                str(ingestion_batch_id),                 
                str(product_id),                         
                int(GRANULARITY),                   
                w_start,                            
                w_end,                              
                int(res["status"]) if res["status"] is not None else None, 
                int(len(payload)),                  
                str(json.dumps(payload)),                
                str(SOURCE_NAME)                         
            )
            rows_data.append(row_tuple)

    print(f"Total rows to insert: {len(rows_data)}")

    # Define schema explicitly
    schema = StructType([
        StructField("ingestion_ts", TimestampType(), True),
        StructField("ingestion_batch_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("granularity", IntegerType(), True),
        StructField("start_ts", TimestampType(), True),
        StructField("end_ts", TimestampType(), True),
        StructField("http_status", IntegerType(), True),
        StructField("records_count", IntegerType(), True),
        StructField("raw_payload", StringType(), True),
        StructField("source", StringType(), True)
    ])

    # Create DataFrame WITHOUT using sparkContext
    bronze_df = spark.createDataFrame(rows_data, schema=schema)
    
    # Write to Delta
    bronze_df.write.format("delta").mode("append").saveAsTable("bronze_coinbase_candles_raw")
    
    # LOG SUCCESS
    log_pipeline_step(
        notebook_name=notebook_name, 
        status="SUCCESS", 
        start_ts=start_time, 
        rows=len(rows_data), 
        message="Data ingested successfully on Serverless compute"
    )
    print(f"FINAL SUCCESS: Data ingested and Logged.")

except Exception as e:
    # LOG ERROR
    error_str = str(e)[:250]
    # We still try to log the error, but if log_pipeline_step fails too, we print it
    try:
        log_pipeline_step(notebook_name, "ERROR", start_time, rows=0, message=error_str)
    except:
        print(f"Logging failed as well. Original error: {error_str}")
    raise e

# COMMAND ----------

spark.sql("""
SELECT
  ingestion_ts,
  ingestion_batch_id,
  product_id,
  granularity,
  http_status,
  records_count,
  start_ts,
  end_ts
FROM bronze_coinbase_candles_raw
ORDER BY ingestion_ts DESC
LIMIT 20
""").show(truncate=False)

