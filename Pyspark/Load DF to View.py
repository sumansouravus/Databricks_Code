# Databricks notebook source
# DBTITLE 1,import Function
from pyspark.sql.functions import col
from delta.tables import DeltaTable


# COMMAND ----------

# DBTITLE 1,Define Structure
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Define the schema
Employee_schema = StructType([
    StructField("INDX", IntegerType(), True),  # Integer type for INDX
    StructField("ORGANIZATION_ID", StringType(), True),  # String type for ORGANIZATION_ID
    StructField("FIRST_NAME", StringType(), True),  # String type for FIRST_NAME
    StructField("LAST_NAME", StringType(), True),  # String type for LAST_NAME
    StructField("SEX", StringType(), True),  # String type for SEX
    StructField("EMAIL", StringType(), True),  # String type for EMAIL
    StructField("PHONE", StringType(), True),  # String type for PHONE
    StructField("DATE_OF_BIRTH", StringType(), True),  # String type for DATE_OF_BIRTH
    StructField("JOB_TITLE", StringType(), True)  # String type for JOB_TITLE
])


# COMMAND ----------

# DBTITLE 1,Read DF
employee_df = spark.read.csv("dbfs:/FileStore/Employee.csv", schema=Employee_schema, header=True)
# Limit the DataFrame to the first 10 rows and display
display(employee_df.limit(10))


# COMMAND ----------

# DBTITLE 1,Create temp view if needed - not needed here
--employee_df.createOrReplaceTempView("Employee_VW1")


# COMMAND ----------

# DBTITLE 1,Create permanent view
# Step 1: Save the DataFrame as a permanent table in the 'dbw_lakehouse_useast.prd' schema
employee_df.write.saveAsTable("dbw_lakehouse_useast.prd.Employee_Table")

# Step 2: Create a permanent view based on the table
spark.sql("""
CREATE OR REPLACE VIEW dbw_lakehouse_useast.prd.Employee_View AS
SELECT * FROM dbw_lakehouse_useast.prd.Employee_Table
""")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dbw_lakehouse_useast.prd.Employee_View limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC **Loading for Testing**

# COMMAND ----------

# Step 1: Read data from the view dbw_lakehouse_useast.prd.Employee_View
employee_df = spark.sql("SELECT * FROM dbw_lakehouse_useast.prd.Employee_View")

# Step 2: Write the data into a new table dbw_lakehouse_useast.prd.Employee_All
employee_df.write.mode("overwrite").saveAsTable("dbw_lakehouse_useast.prd.Employee_All")

