# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingest Circuits.csv

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the CSV file using Spark Dataframes

# COMMAND ----------

#imports
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema=StructType(fields=[StructField("circuitId",IntegerType(),False),
                                   StructField("circuitRef",StringType(),True),
                                   StructField("name",StringType(),True),
                                   StructField("location",StringType(),True),
                                   StructField("country",StringType(),True),
                                   StructField("lat",DoubleType(),True),
                                   StructField("long",DoubleType(),True),
                                   StructField("alt",IntegerType(),True),
                                   StructField("url",StringType(),True)])

# COMMAND ----------

circuits_df=spark.read \
.option("header",True) \
.option("inferSchema",True) \
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

temp=spark.read \
    .option("header",True) \
    .schema(circuits_schema) \
    .csv(f"dbfs:/mnt/formula1dllatest/raw/{v_file_date}/circuits.csv")

# COMMAND ----------

temp.printSchema()

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 -Select only the required columns

# COMMAND ----------

circuits_selected_df=circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

circuits_selected_df.describe().show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_temp_df=circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country").alias("race_country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

circuits_temp_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 -Rename the columns as required

# COMMAND ----------

circuits_renamed_df=circuits_selected_df.withColumnRenamed("circuitId","circuit_id") \
    .withColumnRenamed("circuitRef","circuit_ref") \
    .withColumnRenamed("lat","latitude") \
    .withColumnRenamed("lng","longitude") \
    .withColumnRenamed("alt","altitude") \
    .withColumn("data_source",lit(v_data_source)) \
    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 4 - Add the ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions  import current_timestamp,lit

# COMMAND ----------

# #to add a literal value to column, has to be wrapped by the lit function
# circuits_final_df=circuits_renamed_df.withColumn("ingestion_date",current_timestamp()) \
#     .withColumn("env",lit("Production"))

# COMMAND ----------

circuits_final_df=add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data to datalake as parquet

# COMMAND ----------

# circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write data as delta

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dllatest/processed/circuits

# COMMAND ----------

df=spark.read.format("delta").load("/mnt/formula1dllatest/processed/circuits")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

