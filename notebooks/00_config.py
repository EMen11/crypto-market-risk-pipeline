# Databricks notebook source
# 00_config — Global config + helpers for Coinbase candles ingestion

from datetime import datetime, timedelta, timezone
import json
import time
import uuid

# --- Project config (MVP) ---
PRODUCTS = ["BTC-USD", "ETH-USD", "SOL-USD"]
GRANULARITY = 86400  # daily candles
DAYS_BACK = 730      # 2 years

BASE_URL = "https://api.exchange.coinbase.com"
SOURCE_NAME = "coinbase_exchange"

# --- Helpers ---
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def iso_utc(dt: datetime) -> str:
    """Format datetime as ISO-8601 with Z suffix."""
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def make_batch_id(prefix: str = "ingest") -> str:
    # Example: ingest_20260121T225500Z_ab12cd34
    ts = utc_now().strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}_{ts}_{uuid.uuid4().hex[:8]}"

def build_candles_url(product_id: str, start_dt: datetime, end_dt: datetime, granularity: int) -> str:
    start_s = iso_utc(start_dt)
    end_s = iso_utc(end_dt)
    return f"{BASE_URL}/products/{product_id}/candles?start={start_s}&end={end_s}&granularity={granularity}"

# Simple retry wrapper (keeps it production-ish)
def http_get_json(url: str, retries: int = 3, backoff_seconds: float = 1.5, timeout_seconds: int = 30):
    import requests  # usually available in Databricks
    
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=timeout_seconds, headers={"User-Agent": "databricks-free-edition"})
            status = resp.status_code
            text = resp.text
            # Coinbase returns JSON arrays; try to parse
            try:
                payload = resp.json()
            except Exception:
                payload = None
            
            return {
                "ok": 200 <= status < 300 and payload is not None,
                "status": status,
                "payload": payload,
                "text": text[:5000],  # cap for logs
                "url": url
            }
        except Exception as e:
            last_err = str(e)
            if attempt < retries:
                time.sleep(backoff_seconds ** attempt)
            else:
                break

    return {"ok": False, "status": None, "payload": None, "text": last_err, "url": url}

# Window used for historical pull (we will chunk later if needed)
END_DT = utc_now()
START_DT = END_DT - timedelta(days=DAYS_BACK)

print("Config loaded:")
print("PRODUCTS:", PRODUCTS)
print("GRANULARITY:", GRANULARITY)
print("DAYS_BACK:", DAYS_BACK)
print("Start:", iso_utc(START_DT))
print("End:  ", iso_utc(END_DT))
# 00_config — Global config + helpers for Coinbase candles ingestion

from datetime import datetime, timedelta, timezone
import json
import time
import uuid

# --- Project config (MVP) ---
PRODUCTS = ["BTC-USD", "ETH-USD", "SOL-USD"]
GRANULARITY = 86400  # daily candles
DAYS_BACK = 730      # 2 years

BASE_URL = "https://api.exchange.coinbase.com"
SOURCE_NAME = "coinbase_exchange"

# --- Helpers ---
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def iso_utc(dt: datetime) -> str:
    """Format datetime as ISO-8601 with Z suffix."""
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def make_batch_id(prefix: str = "ingest") -> str:
    # Example: ingest_20260121T225500Z_ab12cd34
    ts = utc_now().strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}_{ts}_{uuid.uuid4().hex[:8]}"

def build_candles_url(product_id: str, start_dt: datetime, end_dt: datetime, granularity: int) -> str:
    start_s = iso_utc(start_dt)
    end_s = iso_utc(end_dt)
    return f"{BASE_URL}/products/{product_id}/candles?start={start_s}&end={end_s}&granularity={granularity}"

# Simple retry wrapper (keeps it production-ish)
def http_get_json(url: str, retries: int = 3, backoff_seconds: float = 1.5, timeout_seconds: int = 30):
    import requests  # usually available in Databricks
    
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=timeout_seconds, headers={"User-Agent": "databricks-free-edition"})
            status = resp.status_code
            text = resp.text
            # Coinbase returns JSON arrays; try to parse
            try:
                payload = resp.json()
            except Exception:
                payload = None
            
            return {
                "ok": 200 <= status < 300 and payload is not None,
                "status": status,
                "payload": payload,
                "text": text[:5000],  # cap for logs
                "url": url
            }
        except Exception as e:
            last_err = str(e)
            if attempt < retries:
                time.sleep(backoff_seconds ** attempt)
            else:
                break

    return {"ok": False, "status": None, "payload": None, "text": last_err, "url": url}

# Window used for historical pull (we will chunk later if needed)
END_DT = utc_now()
START_DT = END_DT - timedelta(days=DAYS_BACK)

print("Config loaded:")
print("PRODUCTS:", PRODUCTS)
print("GRANULARITY:", GRANULARITY)
print("DAYS_BACK:", DAYS_BACK)
print("Start:", iso_utc(START_DT))
print("End:  ", iso_utc(END_DT))


# COMMAND ----------

# Quick API connectivity test (Plan A)
test_batch_id = make_batch_id("test")
test_end = utc_now()
test_start = test_end - timedelta(days=10)

url = build_candles_url("BTC-USD", test_start, test_end, GRANULARITY)
result = http_get_json(url)

print("batch_id:", test_batch_id)
print("ok:", result["ok"])
print("status:", result["status"])
print("url:", result["url"])
if result["ok"]:
    print("candles returned:", len(result["payload"]))
    print("first candle sample:", result["payload"][0])
else:
    print("error (first 500 chars):", (result["text"] or "")[:500])


# COMMAND ----------

from datetime import datetime
import uuid
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# 1. Drop the old table if it exists to fix the schema mismatch
spark.sql("DROP TABLE IF EXISTS pipeline_logs")

# 2. Define the exact schema (7 columns)
log_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("notebook_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("start_ts", TimestampType(), True),
    StructField("end_ts", TimestampType(), True),
    StructField("rows_processed", IntegerType(), True),
    StructField("message", StringType(), True)
])

# 3. Create the empty table with the correct schema
spark.createDataFrame([], log_schema).write.format("delta").mode("overwrite").saveAsTable("pipeline_logs")

# 4. Updated logging function (using Tuple for strict ordering)
def log_pipeline_step(notebook_name, status, start_ts, rows=0, message=""):
    run_id = str(uuid.uuid4())[:8]
    
    # We create a list containing one TUPLE with exactly 7 elements
    log_data = [(
        run_id,
        str(notebook_name),
        str(status),
        start_ts,
        datetime.now(),
        int(rows),
        str(message)
    )]
    
    # Create DataFrame using the strict schema
    log_df = spark.createDataFrame(log_data, schema=log_schema)
    
    # Append to the Delta table
    log_df.write.format("delta").mode("append").saveAsTable("pipeline_logs")
    print(f" Log recorded: {notebook_name} | {status}")

# COMMAND ----------

log_pipeline_step("test_setup", "SUCCESS", datetime.now(), rows=10, message="Logging system fixed")

# COMMAND ----------

def get_bronze_watermark(table_name, default_start_dt):
    """
    Finds the latest timestamp in the Bronze table to allow incremental loading.
    If the table is empty, it falls back to the default start date.
    """
    try:
        # We fetch the max end_ts from the Bronze table
        max_dt = spark.sql(f"SELECT MAX(end_ts) FROM {table_name}").collect()[0][0]
        
        if max_dt:
            print(f"✅ Watermark found: Last ingestion ended at {max_dt}")
            return max_dt
        else:
            print(" Table is empty. Using default start date.")
            return default_start_dt
    except Exception as e:
        print(f" Table not found or error: {str(e)[:50]}. Using default start date.")
        return default_start_dt