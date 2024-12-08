# Databricks notebook source
# DBTITLE 1,Import Library
from pyspark.sql.functions import col
from delta.tables import DeltaTable


# COMMAND ----------

# DBTITLE 1,Read the source
#  Read the source CSV file
Employee = spark.read.csv("dbfs:/FileStore/Employee.csv", header=True, inferSchema=True)
display(Employee)

# COMMAND ----------

target_table = "dbw_lakehouse_useast.prd.employee"

# COMMAND ----------

delta_table = DeltaTable.forName(spark, target_table)

# COMMAND ----------

from pyspark.sql.functions import to_date

# Convert the date_of_birth column to DATE type
Employee = Employee.withColumn("date_of_birth", to_date(Employee["date_of_birth"], "dd-MMM-yy hh.mm.ss a"))

delta_table.alias("target") \
    .merge(
        Employee.alias("source"),  # source dataframe
        "target.indx = source.indx"  # matching condition on 'indx'
    ) \
    .whenMatchedUpdate(  # Update condition if the record already exists (based on 'indx')
        condition="target.indx = source.indx",
        set={
            "organization_id": "source.organization_id",
            "first_name": "source.first_name",
            "last_name": "source.last_name",
            "sex": "source.sex",
            "email": "source.email",
            "phone": "source.phone",
            "date_of_birth": "source.date_of_birth",
            "job_title": "source.job_title"
        }
    ) \
    .whenNotMatchedInsert(  # Insert condition if the record doesn't exist
        values={
            "indx": "source.indx",
            "organization_id": "source.organization_id",
            "first_name": "source.first_name",
            "last_name": "source.last_name",
            "sex": "source.sex",
            "email": "source.email",
            "phone": "source.phone",
            "date_of_birth": "source.date_of_birth",
            "job_title": "source.job_title"
        }
    ) \
    .execute()

print("Upsert operation completed successfully!")
