# Databricks notebook source
# MAGIC %md
# MAGIC #####ACCESS AZURE DATA LAKE USING SERVICE PRINCIPAL
# MAGIC ##Steps to follow
# MAGIC
# MAGIC 1. Get client_id,tenant_id and client_secret from key_vault
# MAGIC 2. Set the Spark Config with App/Client Id, Directory/Tenant Id & Secret
# MAGIC 3. Call the file system utility mount to mount the storage 
# MAGIC 4. Exp;ore other file system utilities related to the mount (list all mounts,unmount)

# COMMAND ----------

client_id=dbutils.secrets.get(scope='formula1-scope',key='formula1-app-client-id')
tenant_id=dbutils.secrets.get(scope='formula1-scope',key='formula1-tenant-id-app')
client_secret=dbutils.secrets.get(scope='formula1-scope',key='formula1-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dllatest.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dllatest/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dllatest/demo"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

#function to mount a adls
def mount_adls(storage_account_name,container_name):
    client_id=dbutils.secrets.get(scope='formula1-scope',key='formula1-app-client-id')
    tenant_id=dbutils.secrets.get(scope='formula1-scope',key='formula1-tenant-id-app')
    client_secret=dbutils.secrets.get(scope='formula1-scope',key='formula1-app-client-secret')
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('formula1dllatest','raw')

# COMMAND ----------

mount_adls('formula1dllatest','processed')

# COMMAND ----------

mount_adls('formula1dllatest','presentation')

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dllatest/demo/circuits.csv"))

# COMMAND ----------

  display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dllatest/presentation")

# COMMAND ----------

