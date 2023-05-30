# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Pit_Stops.json

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

#imports
from pyspark.sql.functions import current_timestamp, col, concat, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the json file using spark dataframe reader

# COMMAND ----------

pit_stops_schema = StructType(fields = [StructField("raceId", IntegerType(),False),
                            StructField("driverId", IntegerType(),True),
                            StructField("stop", StringType(),True),
                            StructField("lap", IntegerType(),True),
                            StructField("time", StringType(),True),
                            StructField("duration", StringType(),True),
                            StructField("milliseconds", IntegerType(),True)])       

# COMMAND ----------

pit_stops_df=spark.read \
    .schema(pit_stops_schema) \
    .option("multiLine",True) \
    .json(f"/mnt/formula1dllatest/raw/{v_file_date}/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

pitstops_with_columns_df=pit_stops_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumn("ingestion_date",current_timestamp()) \
    .withColumn("data_source",lit(v_data_source)) \
    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(pitstops_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to processed folder in parquet format

# COMMAND ----------

# pitstops_with_columns_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")
# incremental_load(pitstops_with_columns_df,'f1_processed','pit_stops','race_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to processed folder in delta format

# COMMAND ----------

merge_condition="tgt.driver_id=src.driver_id AND tgt.race_id=src.race_id AND tgt.stop=src.stop"
merge_delta_data(pitstops_with_columns_df,"f1_processed","pit_stops",processed_folder_path,"race_id",merge_condition)

# COMMAND ----------

display(spark.read.format("delta").load("/mnt/formula1dllatest/processed/pit_stops"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

