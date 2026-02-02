# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View the 20 most recent executions to catch errors
# MAGIC SELECT 
# MAGIC     notebook_name, 
# MAGIC     status, 
# MAGIC     start_ts, 
# MAGIC     end_ts, 
# MAGIC     round(cast(end_ts as double) - cast(start_ts as double), 2) as duration_seconds,
# MAGIC     rows_processed, 
# MAGIC     message 
# MAGIC FROM pipeline_logs 
# MAGIC ORDER BY start_ts DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monitor the average duration of each step to detect bottlenecks
# MAGIC SELECT 
# MAGIC     notebook_name, 
# MAGIC     AVG(cast(end_ts as double) - cast(start_ts as double)) as avg_duration_sec,
# MAGIC     SUM(rows_processed) as total_rows_processed,
# MAGIC     COUNT(*) as total_runs
# MAGIC FROM pipeline_logs 
# MAGIC GROUP BY notebook_name
# MAGIC ORDER BY avg_duration_sec DESC;

# COMMAND ----------

display(spark.table("pipeline_logs"))