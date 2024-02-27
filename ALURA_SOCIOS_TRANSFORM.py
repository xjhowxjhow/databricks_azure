# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM DATA_LAKE_SOCIOS;
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
spark:SparkSession = spark

# COMMAND ----------


df = spark.read.table("DATA_LAKE_SOCIOS")
display(df.limit(5))

