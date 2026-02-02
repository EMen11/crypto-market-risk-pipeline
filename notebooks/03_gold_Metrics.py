# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

from datetime import datetime
start_time = datetime.now()
notebook_name = "03_gold_risk_metrics"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

try:
    # --- STEP 1: LOAD SILVER DATA ---
    silver_df = spark.table("silver_coinbase_candles")
    
    # --- STEP 2: CALCULATE RETURNS ---
    # Create the time-series window grouped by asset
    win_spec = Window.partitionBy("product_id").orderBy("ts")

    gold_returns = (silver_df
        .withColumn("prev_close", F.lag("close").over(win_spec))
        .withColumn("return_simple", (F.col("close") / F.col("prev_close")) - 1)
        .withColumn("return_log", F.log(F.col("close") / F.col("prev_close")))
    )

    # --- STEP 3: CALCULATE RISK METRICS (VOLATILITY) ---
    # 30-day rolling window: 29 days look-back + current day
    vol_win = Window.partitionBy("product_id").orderBy("ts").rowsBetween(-29, 0)

    gold_final = (gold_returns
        .withColumn("vol_30d", F.stddev("return_log").over(vol_win))
        # Annualized Volatility calculation (daily_vol * sqrt(365))
        .withColumn("vol_30d_ann", F.col("vol_30d") * F.sqrt(F.lit(365)))
        # Filter out rows with NULL returns (first record of each asset)
        .filter(F.col("prev_close").isNotNull())
    )

    # --- STEP 4: WRITE TO GOLD TABLE ---
    (gold_final.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true") 
      .saveAsTable("gold_crypto_risk_metrics_daily"))

    # --- SUCCESS LOGGING ---
    rows_gold = gold_final.count()
    log_pipeline_step(
        notebook_name=notebook_name, 
        status="SUCCESS", 
        start_ts=start_time, 
        rows=rows_gold, 
        message="Gold risk metrics calculated and saved successfully"
    )
    print(f" Success: Gold table updated with {rows_gold} rows.")

except Exception as e:
    # --- ERROR LOGGING ---
    error_msg = str(e)[:250]
    log_pipeline_step(
        notebook_name=notebook_name, 
        status="ERROR", 
        start_ts=start_time, 
        rows=0, 
        message=error_msg
    )
    print(f" Gold layer failed: {error_msg}")
    raise e