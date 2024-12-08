# Databricks notebook source
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.getOrCreate()

# Access the parameter value
param_value = dbutils.widgets.get("env1")

# Use the parameter value in your PySpark code
df = spark.read.format("csv").option("header", "true").load(param_value)

# Do something with the DataFrame
#display(df)

# COMMAND ----------

#env = dbutils.jobs.taskValues.get(
#   taskKey='wf_Variables_use_case',  # Use taskKey (not task_key)
#    key='env',                   # The parameter you want to retrieve
#    default='DEV'                # Default value if the parameter is not found
#)

# Access the parameter value
param_value = dbutils.widgets.get("env2")

# Print the retrieved 'env2' value
print(f"Retrieved env parameter: {param_value}")

# Define paths based on the env parameter value
path_prd = "dbfs:/Suman/PRD"
path_pre = "dbfs:/Suman/PRE"
path_dev = "dbfs:/Suman/DEV"

# Use the 'env2' value to select the correct path
if param_value == "PRD":
    file_path = path_prd
elif param_value == "PRE":
    file_path = path_pre
elif param_value == "DEV":
    file_path = path_dev
else:
    raise ValueError("Invalid environment value. Expected 'PRD', 'PRE', or 'DEV'.")

# Output the selected file path
print(f"Selected file path for environment '{param_value}': {file_path}")
