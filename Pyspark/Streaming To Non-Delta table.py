# Databricks notebook source
# DBTITLE 1,Define DataFrame
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

# Creating a new streaming query
# the checkpoint location should be different
# This is a managed table, you can also create external tables too
# Please update the filepaths accordingly
streamQuery = orders_sdf.writeStream.format("parquet").\
option("checkpointLocation", "dbfs:/mnt/streaming/Checkpoint/2").\
toTable("hive_metastore.streaming_db.orders_streaming")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from hive_metastore.streaming_db.orders_streaming;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dbw_lakehouse_useast.streaming_db.orders_streaming;
