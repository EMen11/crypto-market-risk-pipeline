# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 1: Most extreme market shocks detected
# MAGIC SELECT 
# MAGIC     product_id, 
# MAGIC     ts as date, 
# MAGIC     round(return_log * 100, 2) as return_pct, 
# MAGIC     round(z_score, 2) as z_score
# MAGIC FROM gold_crypto_anomalies
# MAGIC WHERE is_anomaly = true
# MAGIC ORDER BY abs(z_score) DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 2: Top 10 worst performing days per asset
# MAGIC SELECT 
# MAGIC     product_id, 
# MAGIC     ts as date, 
# MAGIC     round(return_log * 100, 2) as daily_loss_pct
# MAGIC FROM gold_crypto_risk_metrics_daily
# MAGIC ORDER BY return_log ASC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 3: 30-Day Rolling Correlation between BTC and ETH
# MAGIC -- This shows how synchronized the market is
# MAGIC WITH btc_returns AS (
# MAGIC     SELECT ts, return_log FROM gold_crypto_risk_metrics_daily WHERE product_id = 'BTC-USD'
# MAGIC ),
# MAGIC eth_returns AS (
# MAGIC     SELECT ts, return_log FROM gold_crypto_risk_metrics_daily WHERE product_id = 'ETH-USD'
# MAGIC )
# MAGIC SELECT 
# MAGIC     b.ts,
# MAGIC     round(corr(b.return_log, e.return_log) OVER (ORDER BY b.ts ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 3) as btc_eth_correlation_30d
# MAGIC FROM btc_returns b
# MAGIC JOIN eth_returns e ON b.ts = e.ts
# MAGIC ORDER BY b.ts DESC
# MAGIC LIMIT 20;