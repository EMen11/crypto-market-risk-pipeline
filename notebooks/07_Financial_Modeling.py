# Databricks notebook source
from pyspark.sql import Window
import pyspark.sql.functions as F

# Load the gold data from the previous step
df = spark.table("gold_crypto_anomalies")

# COMMAND ----------

# Setup window for cumulative calculations (Running Max for Drawdown)
# Partition by product to ensure independent calculations for BTC, ETH, SOL
window_spec = Window.partitionBy("product_id").orderBy("ts").rowsBetween(Window.unboundedPreceding, 0)

# Calculate the running peak price for each asset
df_mdd = df.withColumn("rolling_peak", F.max("close").over(window_spec))

# Calculate current drawdown from the peak (Current Price / Peak - 1)
df_mdd = df_mdd.withColumn("drawdown", (F.col("close") - F.col("rolling_peak")) / F.col("rolling_peak"))

# Capture the historical Maximum Drawdown per asset
df_mdd = df_mdd.withColumn("max_drawdown", F.min("drawdown").over(Window.partitionBy("product_id")))

# COMMAND ----------

# Approximate the 5th percentile of log returns (Historical VaR)
# Represents the threshold where we are 95% confident that losses won't exceed this value
df_final = df_mdd.withColumn("VaR_95", F.percentile_approx("return_log", 0.05).over(Window.partitionBy("product_id")))

# COMMAND ----------

# Final view to check results and the new financial columns
display(df_final)

# COMMAND ----------

# Overwrite the gold table and allow schema evolution to include new columns
df_final.write.mode("overwrite") \
  .option("mergeSchema", "true") \
  .saveAsTable("gold_crypto_anomalies")