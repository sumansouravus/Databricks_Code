# Databricks notebook source
# DBTITLE 1,Create Table First
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dbw_lakehouse_useast.streaming_db.orders_streaming_upsert (
# MAGIC     ORDER_ID INT,
# MAGIC     ORDER_DATETIME STRING,
# MAGIC     CUSTOMER_ID INT,
# MAGIC     ORDER_STATUS STRING,
# MAGIC     STORE_ID INT
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# DBTITLE 1,Upsert Data Stream
from delta.tables import DeltaTable
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# Define the schema for streaming data
orders_schema = StructType([
    StructField("ORDER_ID", IntegerType(), False),
    StructField("ORDER_DATETIME", StringType(), False),
    StructField("CUSTOMER_ID", IntegerType(), False),
    StructField("ORDER_STATUS", StringType(), False),
    StructField("STORE_ID", IntegerType(), False)
])

# Specify the path for streaming data
orders_streaming_path = "dbfs:/mnt/streaming/Full_Data/"

# Read the streaming data into a DataFrame with the defined schema
orders_sdf = spark.readStream.csv(
    orders_streaming_path,
    schema=orders_schema,
    header=True
)

# Define the upsert (merge) logic
def upsert_to_delta(df, epoch_id):
    # Specify the Delta table to upsert data into
    delta_table = "dbw_lakehouse_useast.streaming_db.orders_streaming_upsert"
    
    # Read the Delta table as a DeltaTable instance
    delta_table_instance = DeltaTable.forName(spark, delta_table)
    
    # Perform the merge operation: Upsert based on ORDER_ID
    delta_table_instance.alias('target') \
        .merge(
            df.alias('source'),
            'target.ORDER_ID = source.ORDER_ID'  # Merge condition based on ORDER_ID
        ) \
        .whenMatchedUpdate(set={
            'ORDER_DATETIME': 'source.ORDER_DATETIME',
            'CUSTOMER_ID': 'source.CUSTOMER_ID',
            'ORDER_STATUS': 'source.ORDER_STATUS',
            'STORE_ID': 'source.STORE_ID'
        }) \
        .whenNotMatchedInsert(values={
            'ORDER_ID': 'source.ORDER_ID',
            'ORDER_DATETIME': 'source.ORDER_DATETIME',
            'CUSTOMER_ID': 'source.CUSTOMER_ID',
            'ORDER_STATUS': 'source.ORDER_STATUS',
            'STORE_ID': 'source.STORE_ID'
        }) \
        .execute()

# Start the streaming query with foreachBatch for upsert
streamQuery = orders_sdf.writeStream \
    .foreachBatch(upsert_to_delta) \
    .option("checkpointLocation", "dbfs:/mnt/streaming/Checkpoint/3") \
    .start()

# Display the stream query
display(streamQuery)

# COMMAND ----------

# DBTITLE 1,Data Count
# MAGIC %sql
# MAGIC
# MAGIC select count(*) from dbw_lakehouse_useast.streaming_db.orders_streaming_upsert;
