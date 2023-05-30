# Databricks notebook source
# MAGIC %md
# MAGIC #####ACCESS AZURE DATA LAKE USING Shared Access Signature
# MAGIC 1.Set the spark config for SAS Token
# MAGIC
# MAGIC 2.List files from demo container
# MAGIC
# MAGIC 3.Read the data from circuits.csv

# COMMAND ----------

formula1_sas_token=dbutils.secrets.get(scope='formula1-scope',key='formula1-demoSas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dllatest.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dllatest.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dllatest.dfs.core.windows.net", formula1_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dllatest.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dllatest.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

