# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Bronze Tables**

# COMMAND ----------

# Please update the filepath accordingly
orders_path = "dbfs:/mnt/delta-live/Source/orders_full.csv"

orders_schema = StructType([
                    StructField("ORDER_ID", IntegerType(), False),
                    StructField("ORDER_DATETIME", StringType(), False),
                    StructField("CUSTOMER_ID", IntegerType(), False),
                    StructField("ORDER_STATUS", StringType(), False),
                    StructField("STORE_ID", IntegerType(), False)
                    ]
                    )

# COMMAND ----------

@dlt.table
def orders_bronze():
    df = spark.read.format("csv").option("header",True).schema(orders_schema).load(orders_path)
    return df

# COMMAND ----------

# Please update the filepath accordingly
order_items_path = "dbfs:/mnt/delta-live/Source/order_items_full.csv"

order_items_schema = StructType([
                    StructField("ORDER_ID", IntegerType(), False),
                    StructField("LINE_ITEM_ID", IntegerType(), False),
                    StructField("PRODUCT_ID", IntegerType(), False),
                    StructField("UNIT_PRICE", DoubleType(), False),
                    StructField("QUANTITY", IntegerType(), False)
                    ]
                    )

# COMMAND ----------

@dlt.table
def order_items_bronze():
    return spark.read.format("csv").option("header",True).schema(order_items_schema).load(order_items_path)

# COMMAND ----------

# MAGIC %md
# MAGIC **Silver Table**

# COMMAND ----------

@dlt.table
def orders_silver():
    return (
        dlt.read("orders_bronze")
              .select(
              'ORDER_ID', \
              to_date('ORDER_DATETIME', "dd-MMM-yy kk.mm.ss.SS").alias('ORDER_DATE'), \
              'CUSTOMER_ID', \
              'ORDER_STATUS', \
              'STORE_ID',
              current_timestamp().alias("MODIFIED_DATE")
               )
          )

# COMMAND ----------

@dlt.table
def order_items_silver():
    return (
        dlt.read("order_items_bronze")
              .select(
              'ORDER_ID', \
              'PRODUCT_ID', \
              'UNIT_PRICE', \
              'QUANTITY',
              current_timestamp().alias("MODIFIED_DATE")
               )
          )
