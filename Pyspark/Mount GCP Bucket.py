# Databricks notebook source
# DBTITLE 1,Download this file fom GCP - IAM & Admin - Service Account
dbfs:/FileStore/GCP_User_Key.json

# COMMAND ----------

# DBTITLE 1,Validate File
json = spark.read.options(multiLine=True).json('dbfs:/user/GCP_User_Key.json')
display(json)

# COMMAND ----------

# DBTITLE 1,Import OS
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/dbfs/user/GCP_User_Key.json'

# COMMAND ----------

pip install google-cloud-storage

# COMMAND ----------

# DBTITLE 1,Restart the kernel
# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,Access GCP bucket
import os

# Path to the service account JSON key stored in Databricks
json_key_path = "/dbfs/user/GCP_User_Key.json"

# Set the environment variable for Google Cloud authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_key_path

# Test authentication
from google.cloud import storage

# Initialize the client
client = storage.Client()

# Access the bucket
bucket_name = "suman_demo_bucket"
bucket = client.get_bucket(bucket_name)

# List files in the bucket
blobs = bucket.list_blobs()
for blob in blobs:
    print(blob.name)

# COMMAND ----------

# DBTITLE 1,Mount Bucket
# Path to the key file in DBFS
key_file_path = '/dbfs/user/GCP_User_Key.json'

# Mount the GCS bucket
dbutils.fs.mount(
    source = "gs://your-bucket-name",
    mount_point = "/mnt/gcs_bucket",
    extra_configs = {"google.cloud.auth.service.account.json.keyfile": key_file_path}
)

print("GCS bucket mounted successfully")

