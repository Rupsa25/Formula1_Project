# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Races.csv

# COMMAND ----------

#imports
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions  import current_timestamp,lit,to_timestamp, concat,col

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

races_df=spark.read \
.option("header",True) \
.option("inferSchema",True) \
.csv(f"dbfs:/mnt/formula1dllatest/raw/{v_file_date}/races.csv")

# COMMAND ----------

#Select required columns
races_selected_df=races_df.select("raceId","year","round","circuitId","name","date","time")

# COMMAND ----------

#Rename the columns and add the new columns
races_renamed_df=races_selected_df.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("year","race_year") \
.withColumnRenamed("circuitId","circuit_id") \
.withColumn("ingestion_date",current_timestamp()) \
.withColumn('race_timestamp',to_timestamp(concat(col('date'), lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss')) \
.withColumn("data_source",lit(v_data_source)) \
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

new_l=[i for i in races_renamed_df.columns if i not in ('date','time')]

# COMMAND ----------

new_l

# COMMAND ----------

#dropping the time and date columns
races_final_df=races_renamed_df.select(new_l)

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

