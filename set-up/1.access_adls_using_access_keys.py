# Databricks notebook source
# MAGIC %md
# MAGIC #####ACCESS AZURE DATA LAKE USING ACCESS KEYS
# MAGIC 1.Set the spark config fs.azure.config.key
# MAGIC
# MAGIC 2.List files from demo container
# MAGIC
# MAGIC 3.Read the data from circuits.csv

# COMMAND ----------

formula1_access_key=dbutils.secrets.get(scope='formula1-scope',key='formula1-access-key')

# COMMAND ----------

spark.conf.set("fs.azure.account.key.formula1dllatest.dfs.core.windows.net",formula1_access_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dllatest.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dllatest.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

