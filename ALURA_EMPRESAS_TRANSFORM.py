# Databricks notebook source
# MAGIC %md
# MAGIC #### Pegando Variavel global da Task Inicial

# COMMAND ----------

var_global = dbutils.jobs.taskValues.get(taskKey = "Carrega_dados_p_DataLake", key = "DICT_VAR_NOME_COLUNAS", default = {}, debugValue = {"TEST":"VALUE"})
print(var_global)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM DATA_LAKE_EMPRESAS;
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
spark:SparkSession = spark

# COMMAND ----------


df = spark.read.table("DATA_LAKE_EMPRESAS")
display(df.limit(20))

