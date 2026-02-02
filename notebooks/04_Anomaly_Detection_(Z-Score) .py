# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

from datetime import datetime
start_time = datetime.now()
notebook_name = "04_gold_anomalies"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

try:
    # --- STEP 1: LOAD GOLD RISK DATA ---
    gold_df = spark.table("gold_crypto_risk_metrics_daily")
    
    # --- STEP 2: ESTABLISH STATISTICAL BASELINES ---
    win_global = Window.partitionBy("product_id")
    stats_df = (gold_df
        .withColumn("median_return", F.percentile_approx("return_log", 0.5).over(win_global))
        .withColumn("abs_deviation", F.abs(F.col("return_log") - F.col("median_return")))
        .withColumn("mad", F.percentile_approx("abs_deviation", 0.5).over(win_global))
    )

    # --- STEP 3: ANOMALY SCORING (Robust Z-Score with Zero Check) ---
    anomaly_df = (stats_df
        .withColumn("z_score", 
            F.when(F.col("mad") == 0, 0)
             .otherwise((F.col("return_log") - F.col("median_return")) / (F.col("mad") * 1.4826))
        )
        .withColumn("is_anomaly", F.abs(F.col("z_score")) > 3.5)
    )

    # --- STEP 4: WRITE TO GOLD ANOMALY TABLE ---
    (anomaly_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("gold_crypto_anomalies"))

    # --- SUCCESS LOGGING ---
    rows_processed = anomaly_df.count()
    log_pipeline_step(
        notebook_name=notebook_name, 
        status="SUCCESS", 
        start_ts=start_time, 
        rows=rows_processed, 
        message="Anomaly detection pipeline completed (Zero-division check applied)"
    )
    print(f" Success: Anomaly detection updated with {rows_processed} rows.")

except Exception as e:
    error_msg = str(e)[:250]
    log_pipeline_step(notebook_name, "ERROR", start_time, rows=0, message=error_msg)
    print(f" Anomaly detection failed: {error_msg}")
    raise e

# COMMAND ----------

display(spark.table("gold_crypto_anomalies"))