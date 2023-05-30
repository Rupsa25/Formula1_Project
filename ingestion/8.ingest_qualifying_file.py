# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying json files

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

#imports
from pyspark.sql.functions import current_timestamp, col, concat, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the json file using spark dataframe reader

# COMMAND ----------

qualifying_schema = StructType(fields = [StructField("qualifyId", IntegerType(),False),
                            StructField("raceId", IntegerType(),True),
                            StructField("driverId", IntegerType(),True),
                            StructField("constructorId", IntegerType(),True),
                            StructField("number", StringType(),True),
                            StructField("position", IntegerType(),True),
                            StructField("q1", StringType(),True),
                            StructField("q2", StringType(),True),
                            StructField("q3", StringType(),True)])       

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"/mnt/formula1dllatest/raw/{v_file_date}/qualifying")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

final_df=qualifying_df.withColumnRenamed("qualifyId","qualify_id")\
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumn("ingestion_date",current_timestamp()) \
    .withColumn("data_source",lit(v_data_source)) \
    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to processed folder in parquet format

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to processed folder in delta format

# COMMAND ----------

merge_condition="tgt.qualify_id=src.qualify_id AND tgt.race_id=src.race_id"
merge_delta_data(final_df,"f1_processed","qualifying",processed_folder_path,"race_id",merge_condition)

# COMMAND ----------

display(spark.read.format("delta").load("/mnt/formula1dllatest/processed/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

