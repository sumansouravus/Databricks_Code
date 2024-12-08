# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower

# Initialize Spark session
spark = SparkSession.builder.appName("AlphabetCount").getOrCreate()

# Function to count occurrences of alphabet characters in a text file
def count_alphabet_occurrences(file_path: str):
    # Read the file as a DataFrame (single column of text)
    text_df = spark.read.text(file_path)

    # Split each line into characters (ignoring non-alphabet characters)
    alphabet_df = text_df.select(explode(split(col("value"), "")).alias("char"))  # Split text into characters
    alphabet_df = alphabet_df.filter(col("char").rlike("[a-zA-Z]"))  # Filter only alphabet characters
    alphabet_df = alphabet_df.select(col("char").alias("alphabet"))  # Keep only alphabet characters

    # Convert all characters to lowercase to count both 'a' and 'A' as the same letter
    alphabet_df = alphabet_df.withColumn("alphabet", lower(col("alphabet")))

    # Count the occurrences of each alphabet
    alphabet_counts_df = alphabet_df.groupBy("alphabet").count().orderBy("alphabet")

    # Return the resulting DataFrame
    return alphabet_counts_df


# Databricks notebook source
# MAGIC
# MAGIC %run "/Workspace/Users/sumansouravus@outlook.com/Function/Define_Function"

# COMMAND ----------

# Specify the path to the text file
file_path = "dbfs:/FileStore/Text.txt"  # Replace with your actual file path

# Call the function and get the results
alphabet_counts = count_alphabet_occurrences(file_path)

# Show the results
display(alphabet_counts)


