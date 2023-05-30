# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df=input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def incremental_load(df,db,table,partition_column):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    li=[i for i in df.schema.names if i!=partition_column]
    li.append(partition_column)
    df=df.select(li)
    if (spark._jsparkSession.catalog().tableExists(f"{db}.{table}")):
        df.write.mode("overwrite").insertInto(f"{db}.{table}")
    else:
        df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db}.{table}")

# COMMAND ----------

def merge_delta_data(df,db,table,folder_path,partition_column,merge_condition):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    from delta.tables import DeltaTable
    if (spark._jsparkSession.catalog().tableExists(f"{db}.{table}")):
        deltaTable=DeltaTable.forPath(spark,f"{folder_path}/{table}")
        deltaTable.alias("tgt").merge(
            df.alias("src"),
           merge_condition)\
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    else:
        df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db}.{table}")

# COMMAND ----------

