# Databricks notebook source
# DBTITLE 1,Import datatype
# import the relevant data types
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType

# COMMAND ----------

# DBTITLE 1,Define Schema
# For a streaming source DataFrame we must define the schema
# Please update the filepath accordingly
orders_streaming_path = "dbfs:/mnt/streaming/Source"
 
orders_schema = StructType([
                    StructField("ORDER_ID", IntegerType(), False),
                    StructField("ORDER_DATETIME", StringType(), False),
                    StructField("CUSTOMER_ID", IntegerType(), False),
                    StructField("ORDER_STATUS", StringType(), False),
                    StructField("STORE_ID", IntegerType(), False)
                    ]
                    )


# COMMAND ----------

# DBTITLE 1,Read Stream data using auto loader strategy
# Using Auto Loader to read the streaming data, notice the cloudFiles format that is required for Auto Loader
orders_sdf = spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").schema(orders_schema).load(orders_streaming_path, header=True)

# COMMAND ----------

#Initialising the stream
orders_sdf.display()
