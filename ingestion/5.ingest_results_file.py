# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Results.json

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date=dbutils.widgets.get("p_file_date")

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

results_schema = StructType(fields = [StructField("resultId", IntegerType(),False),
                               StructField("raceId", IntegerType(),True),
                            StructField("driverId", IntegerType(),True),
                            StructField("constructorId", IntegerType(),True),
                            StructField("number", IntegerType(),True),
                            StructField("grid", IntegerType(),True),
                            StructField("position", IntegerType(),True),
                            StructField("positionText", StringType(),True),
                            StructField("positionOrder", IntegerType(),True),
                            StructField("points", FloatType(),True),
                            StructField("laps", IntegerType(),True),
                            StructField("time", StringType(),True),
                            StructField("milliseconds", IntegerType(),True),
                            StructField("fastestLap", IntegerType(),True),
                            StructField("rank", IntegerType(),True),
                            StructField("fastestLapTime", StringType(),True),
                            StructField("FastestLapSpeed", FloatType(),True),
                            StructField("statusId", StringType(),True)])       

# COMMAND ----------

results_df=spark.read \
    .schema(results_schema) \
    .json(f"/mnt/formula1dllatest/raw/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

results_with_columns_df=results_df.withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("positionText", "postion_text") \
    .withColumnRenamed("positionOrder", "position_order") \
    .withColumnRenamed("fastestLap", "fastest_lap") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
    .withColumn("ingestion_date",current_timestamp()) \
    .withColumn("data_source",lit(v_data_source)) \
    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(results_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop unwanted columns

# COMMAND ----------

results_final_df=results_with_columns_df.drop("statusId")

# COMMAND ----------

#dedup
results_ddup_df=results_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write output to processed container in parquet format

# COMMAND ----------

# MAGIC %md
# MAGIC ### METHOD 1

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###METHOD 2

# COMMAND ----------

# #dynamic overwrite to make sure if table exists find and replace only the partitions that exist not the entire table
# spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

# #for insert into last column is the one considered to be partioned on
# results_final_df=results_final_df.select("result_id","driver_id","constructor_id","number","grid","position","postion_text","position_order","points","laps","time","milliseconds","fastest_lap","rank","fastest_lap_time","fastest_lap_speed","data_source","file_date","ingestion_date","race_id")

# COMMAND ----------

# if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     results_final_df.write.mode("overwrite").insertInto("f1_processed.results")
# else:
#     results_final_df.write.mode("overwrite").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# incremental_load(results_final_df,'f1_processed','results','race_id')

# COMMAND ----------


# spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
# from delta.tables import DeltaTable
# if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     deltaTable=DeltaTable.forPath(spark,"/mnt/formula1dllatest/processed/results")
#     deltaTable.alias("tgt").merge(
#         results_final_df.alias("src"),
#         "tgt.result_id=src.result_id AND tgt.race_id=src.race_id")\
#     .whenMatchedUpdateAll() \
#     .whenNotMatchedInsertAll() \
#     .execute()
# else:
#     results_final_df.write.mode("overwrite").partitionBy("race_id").format("delta").saveAsTable("f1_processed.results")

# COMMAND ----------

merge_condition="tgt.result_id=src.result_id AND tgt.race_id=src.race_id"
merge_delta_data(results_ddup_df,"f1_processed","results",processed_folder_path,"race_id",merge_condition)

# COMMAND ----------

display(spark.read.format("delta").load("/mnt/formula1dllatest/processed/results"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(*) from f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY RACE_id DESC;

# COMMAND ----------
