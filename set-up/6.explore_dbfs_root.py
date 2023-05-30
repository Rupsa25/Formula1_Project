# Databricks notebook source
# MAGIC %md
# MAGIC ####Explore DBFS Root
# MAGIC 1. List all the folders in DBFS Root
# MAGIC 2. Interact DBFS File Browser
# MAGIC 3. Upload File to DBFS Root

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(spark.read.csv('dbfs:/FileStore/circuits.csv'))

# COMMAND ----------

display(spark.read.csv('/FileStore/circuits.csv'))

# COMMAND ----------

