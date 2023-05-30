# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Constructors.json

# COMMAND ----------

#imports
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1- Read the JSON file using the spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df=spark.read \
    .schema(constructors_schema) \
    .json(f"/mnt/formula1dllatest/raw/{v_file_date}/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 -Drop the unwanted columns

# COMMAND ----------

constructor_dropped_df=constructor_df.drop('url')

# COMMAND ----------

display(constructor_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns and add ingestion date 

# COMMAND ----------

constructor_final_df=constructor_dropped_df.withColumnRenamed("constructorId","constructor_id") \
.withColumnRenamed("constructorRef","constructor_ref") \
.withColumn("ingestion_date",current_timestamp()) \
.withColumn("data_source",lit(v_data_source)) \
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dllatest/processed/constructors

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

