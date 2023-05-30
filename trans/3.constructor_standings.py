# Databricks notebook source
from pyspark.sql.functions import col, sum, when, count

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "/Formula1/includes/common_functions"

# COMMAND ----------

# MAGIC %run "/Formula1/includes/configuration"

# COMMAND ----------

race_results_list=spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(f"file_date='{v_file_date}'") \
    .select("race_year") \
    .distinct() \
    .collect()
race_year_list=[i.race_year for i in race_results_list]

# COMMAND ----------

race_results_df=spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum,when,count,col

# COMMAND ----------

constructor_standings_df=race_results_df.groupBy("race_year","team") \
    .agg(sum("points").alias("total_points"),count(when(col("position")==1,True)).alias("wins"))

# COMMAND ----------

display(constructor_standings_df.filter("race_year==2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank,asc

# COMMAND ----------

constructor_rank_spec=Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

# COMMAND ----------

final_df=constructor_standings_df.withColumn("rank",rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_df.filter('race_year = 2020'))

# COMMAND ----------

# final_df.write.mode('overwrite').format("parquet").saveAsTable("f1_presentation.constructor_standings")
# incremental_load(final_df,"f1_presentation","constructor_standings","race_year")
merge_condition="tgt.team=src.team AND tgt.race_year=src.race_year"
merge_delta_data(final_df,"f1_presentation","constructor_standings",presentation_folder_path,"race_year",merge_condition)

# COMMAND ----------

