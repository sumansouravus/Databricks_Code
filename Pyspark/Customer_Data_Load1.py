# Databricks notebook source
# Ensure df is properly initialized
df = spark.read.csv('dbfs:/Suman/Customers_1000.csv', header=True, inferSchema=True)

# Write the DataFrame to CSV
df.write.option("sep", ",").option("header", "true").mode("overwrite").csv('dbfs:/Suman/Files/Customer_Test')
