# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) FROM LOADERZ.DATA_LAKE_ESTABELECIOMENTOS;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE  LOADERZ.DATA_LAKE_ESTABELECIOMENTOS SET cnpj_dv =99 WHERE cnpj_basico ='90461'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM   LOADERZ.DATA_LAKE_ESTABELECIOMENTOS WHERE cnpj_basico ='90461'

# COMMAND ----------

from pyspark.sql import SparkSession
spark:SparkSession = spark

# COMMAND ----------

from pyspark.sql import functions as F
df = spark.read.table("LOADERZ.DATA_LAKE_ESTABELECIOMENTOS")

df = df.withColumn(colName='data_situacao_cadastral',
                   col= F.date_format(F.to_date(F.col('data_situacao_cadastral'),'yyyymmdd')
                                     ,'dd/mm/yyyy'))

display(df.limit(20).orderBy([df.cnpj_ordem, df.cnpj_basico, df.cnpj_dv],
                             ascending=[True, True,True]))
