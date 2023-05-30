# Databricks notebook source
# MAGIC %md
# MAGIC #####ACCESS AZURE DATA LAKE USING SERVICE PRINCIPAL
# MAGIC Steps to follow
# MAGIC
# MAGIC 1.Register Azure AD Application/Service Principal
# MAGIC
# MAGIC 2.Generate a secret password for the Application
# MAGIC
# MAGIC 3.Set the Spark Config with App/Client Id, Directory/Tenant Id & Secret
# MAGIC
# MAGIC 4.Assign role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

client_id=dbutils.secrets.get(scope='formula1-scope',key='formula1-app-client-id')
tenant_id=dbutils.secrets.get(scope='formula1-scope',key='formula1-tenant-id-app')
client_secret=dbutils.secrets.get(scope='formula1-scope',key='formula1-app-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dllatest.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dllatest.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dllatest.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dllatest.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dllatest.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dllatest.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dllatest.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

