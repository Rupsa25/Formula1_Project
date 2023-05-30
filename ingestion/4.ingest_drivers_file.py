# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Drivers.json

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

#imports
from pyspark.sql.functions import current_timestamp, col, concat, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the json file using spark dataframe reader

# COMMAND ----------

#defining schemas - plural since json file has nested schemas
name_schema=StructType(fields=[StructField("forename", StringType(),True),
                               StructField("surname", StringType(),True)])
drivers_schema = StructType(fields = [StructField("driverId", IntegerType(),True),
                               StructField("driverRef", StringType(),True),
                            StructField("number", IntegerType(),True),
                            StructField("code", StringType(),True),
                            StructField("name", name_schema),
                            StructField("dob", DateType(),True),
                            StructField("nationality", StringType(),True),
                            StructField("url", StringType(),True)])                              

# COMMAND ----------

drivers_df=spark.read \
    .schema(drivers_schema) \
    .json(f"/mnt/formula1dllatest/raw/{v_file_date}/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns(ingestion_date and concatenated name field)

# COMMAND ----------

drivers_with_columns_df= drivers_df.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("driverRef","driver_ref") \
.withColumn("ingestion_date",current_timestamp()) \
.withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname"))) \
.withColumn("data_source",lit(v_data_source)) \
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(drivers_with_columns_df)

# COMMAND ----------

drivers_with_columns_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop unwanted columns

# COMMAND ----------

drivers_dropped_df=drivers_with_columns_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write output to Parquet file

# COMMAND ----------

drivers_dropped_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

display(spark.read.format("delta").load("/mnt/formula1dllatest/processed/drivers"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

