# Databricks notebook source
# DBTITLE 1,Keys
app id: -----------------------------
tenant id : -----------------------------
secret : -----------------------------
vault-URL: DNS Name: https://databricks-secrets-------.vault.azure.net/
Resource-ID = /subscriptions/-----------------------------/resourceGroups/rg-unitycatalog-useast/providers/Microsoft.KeyVault/vaults/databricks-secrets-63911


# COMMAND ----------

# DBTITLE 1,Mount using Keys
# syntax for configs and mount methods
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "-----------------------------",
          "fs.azure.account.oauth2.client.secret": "-----------------------------",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/-----------------------------/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://delta-live@datalake63977.dfs.core.windows.net/",
  mount_point = "/mnt/delta-live",
  extra_configs = configs)

# COMMAND ----------

# DBTITLE 1,Add key in variable
app_id=dbutils.secrets.get(scope="databricks-secrets-63911", key="app-id")
secret=dbutils.secrets.get(scope="databricks-secrets-63911", key="secret")
tenant=dbutils.secrets.get(scope="databricks-secrets-63911", key="tenant")

# COMMAND ----------

# DBTITLE 1,Mount using Keys variable
# syntax for configs and mount methods
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": app_id,
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://streaming-to-table@datalake63977.dfs.core.windows.net/",
  mount_point = "/mnt/streaming_table",
  extra_configs = configs)

# COMMAND ----------

# DBTITLE 1,Delete File
dbutils.fs.rm('dbfs:/mnt/Customers.csv', recurse=False)

# COMMAND ----------

# DBTITLE 1,List all mount
# List all mounted file systems
mounts = dbutils.fs.mounts()

# Create a list of dictionaries with mount point and source
mounts_list = [{"Mount Point": mount.mountPoint, "Source": mount.source} for mount in mounts]

# Create a DataFrame from the list of dictionaries
mounts_df = spark.createDataFrame(mounts_list)

# Display the DataFrame
display(mounts_df)

# COMMAND ----------

# DBTITLE 1,Unmount
# Unmount the specified mount point
dbutils.fs.unmount("/mnt/streaming_table")

