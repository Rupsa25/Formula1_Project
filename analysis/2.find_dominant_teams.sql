-- Databricks notebook source
SELECT team_name,
COUNT(*) as total_races,
SUM(calculated_points) as total_points,
AVG(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year BETWEEN 2011 and 2020
GROUP BY team_name
HAVING total_races >= 100
ORDER BY avg_points DESC;

-- COMMAND ----------

