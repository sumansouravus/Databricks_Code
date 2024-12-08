# Databricks notebook source
# Setting the configuration
spark.conf.set("fs.azure.account.auth.type.datalake63977.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.datalake63977.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.datalake63977.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-11-27T07:03:52Z&st=2024-11-22T23:03:52Z&spr=https,http&sig=YEVByghFtiaFnU3P3BuQDRnfBf%2F1Kq910szIrDJCQPs%3D")

# COMMAND ----------

# Reading data from storage account
spark.read.csv("abfss://bronze@datalake63977.dfs.core.windows.net/country_regions.csv", header=True).display()
