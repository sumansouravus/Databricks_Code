# Databricks notebook source
# MAGIC %md
# MAGIC **Data Set**

# COMMAND ----------

# DBTITLE 1,Read Data
# Reading the full orders dataset into a DataFrame called orders_full
# Please update with your specific file path and assign it to the variable orders_full_path
orders_full_path = "dbfs:/mnt/streaming/Full_Data/orders.csv"
orders_full = spark.read.csv(orders_full_path, header=True)

# COMMAND ----------

# DBTITLE 1,Path Where new data need to be loaded
orders_streaming_path_For_New_Data = "dbfs:/mnt/streaming/Source"

# COMMAND ----------

# DBTITLE 1,Load Data
# Loading the 4th and 5th order into the streaming dataset
orders = orders_full.filter( (orders_full['ORDER_ID'] == 4) | (orders_full['ORDER_ID']==5))
orders.write.options(header=True).mode('append').csv(orders_streaming_path_For_New_Data)

# COMMAND ----------

# MAGIC %md
# MAGIC **Streaming**

# COMMAND ----------

# DBTITLE 1,Define Schema

# import the relevant data types
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType

# For a streaming source DataFrame we must define the schema
# Please insert your file path
orders_streaming_path = "dbfs:/mnt/streaming/Full_Data/"
 
orders_schema = StructType([
                    StructField("ORDER_ID", IntegerType(), False),
                    StructField("ORDER_DATETIME", StringType(), False),
                    StructField("CUSTOMER_ID", IntegerType(), False),
                    StructField("ORDER_STATUS", StringType(), False),
                    StructField("STORE_ID", IntegerType(), False)
                    ]
                    )

# COMMAND ----------

# DBTITLE 1,Read Data
orders_sdf = spark.readStream.csv(orders_streaming_path, orders_schema, header=True)

# COMMAND ----------

# DBTITLE 1,Write Data Stream
# Creating a new streaming query
# the checkpoint location should be different
# This is a managed table, you can also create external tables too
# Please update the filepaths accordingly
streamQuery = orders_sdf.writeStream.format("delta").\
option("checkpointLocation", "dbfs:/mnt/streaming/Checkpoint/1").\
toTable("dbw_lakehouse_useast.streaming_db.orders_streaming")

# COMMAND ----------

# DBTITLE 1,Data Count
# MAGIC %sql
# MAGIC
# MAGIC select count(*) from dbw_lakehouse_useast.streaming_db.orders_streaming;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dbw_lakehouse_useast.streaming_db.orders_streaming;
