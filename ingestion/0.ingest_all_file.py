# Databricks notebook source
a_result=dbutils.notebook.run("1.ingest_circuits_file",0,{"p_data_source":"Ergast API","p_file_date":"2021-03-21"})

# COMMAND ----------

a_result

# COMMAND ----------

notebook_names=["2.ingest_race_file","3.ingest_constructors_file","4.ingest_drivers_file","5.ingest_results_file","6.ingest_pit_stops_file","7.ingest_lap_times_file","8.ingest_qualifying_file"]

# COMMAND ----------

for i in notebook_names:
    print(i)
    val1=dbutils.notebook.run(i,0,{"p_data_source":"Ergast API","p_file_date":"2021-03-21"})
    val2=dbutils.notebook.run(i,0,{"p_data_source":"Ergast API","p_file_date":"2021-03-28"})
    val3=dbutils.notebook.run(i,0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})
    print(val1,val2,val3)

# COMMAND ----------

val=dbutils.notebook.run("5.ingest_results_file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(*)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id
# MAGIC DESC

# COMMAND ----------

