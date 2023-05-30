-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### DROP ALL TABLES

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed 
LOCATION "/mnt/formula1dllatest/processed"

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1dllatest/presentation"

-- COMMAND ----------

