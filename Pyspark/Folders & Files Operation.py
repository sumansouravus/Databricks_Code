# Databricks notebook source
# DBTITLE 1,Create a empty file
file_path = "/dbfs/Suman/Files/Test_File9txt"
with open(file_path, 'w') as file:
    pass
print(f"Empty file created at {file_path}")

# COMMAND ----------

# DBTITLE 1,Create a new file with data
# Path to where the file will be created
file_path = 'Suman/Files/Test_File3.txt'  # Replace with your desired path

# Content to write into the file
file_content = "Hello, Databricks! This is a test file."

# Use dbutils.fs.put to create and write content to the file
dbutils.fs.put(file_path, file_content, overwrite=True)

# COMMAND ----------

# DBTITLE 1,Insert Data in to file - Truncate
# Define the file path
file_path = "/dbfs/Suman/Files/Test_File6.txt"

# Define the text to insert
text_to_insert = "This is the new text to insert into the file.\n Adding some more Text"

# Use DBFS utilities to append text to the file
dbutils.fs.put(file_path, text_to_insert, True)

print(f"Text has been inserted into {file_path}")

# COMMAND ----------

# DBTITLE 1,Insert data in to file - Append
# Define the file path
file_path = "/dbfs/Suman/Files/Test_File6.txt"

# Define the text to append
text_to_insert = "This is the new text to insert into the file.\nAdding some more Text"

# Check if the file exists
from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)

if dbutils.fs.ls("/dbfs/Suman/Files/"):
    # Read the existing content
    existing_content = dbutils.fs.head(file_path, 1000000)
    # Append the new text
    new_content = existing_content + text_to_insert
    # Write the updated content back to the file
    dbutils.fs.put(file_path, new_content, overwrite=True)
else:
    # If the file doesn't exist, create it and write the text
    dbutils.fs.put(file_path, text_to_insert)

print(f"Text has been appended to {file_path}")

# COMMAND ----------

# DBTITLE 1,Count of files in Folder
# Importing necessary libraries
from pyspark.dbutils import DBUtils

# Accessing DBFS with dbutils
dbutils = DBUtils(spark)

# Function to recursively list files in all directories
def list_files_recursive(path):
    files = dbutils.fs.ls(path)
    file_list = []
    for file in files:
        if file.isDir():
            # If the item is a directory, call the function recursively
            file_list.extend(list_files_recursive(file.path))
        else:
            # If it's a file, add it to the list
            file_list.append(file)
    return file_list

# Get all files starting from a valid subdirectory of DBFS
all_files_list = list_files_recursive("dbfs:/Suman/Files/")

# Get the total number of files
file_count = len(all_files_list)

print(f"Total number of files in DBFS: {file_count}")

# COMMAND ----------

# DBTITLE 1,List all files
# Path to list files and folders (e.g., root directory)
path = 'dbfs:/Suman/Files'

# List all files and folders at the given path
files_and_folders = dbutils.fs.ls(path)

# Display the result
display(files_and_folders)

# COMMAND ----------

# DBTITLE 1,Read a text file
countries_txt = spark.read.options(header=True, sep='\t').text('/dbfs/Suman/Files/Test_File6.txt')
display(countries_txt)

# COMMAND ----------

# DBTITLE 1,Delete File
dbutils.fs.rm("dbfs:/Suman/Files", recurse=True)

# COMMAND ----------

# DBTITLE 1,Move File
dbutils.fs.mv("dbfs:/FileStore/people_1000.csv", "dbfs:/Suman/People.csv")

# Databricks notebook source
# DBTITLE 1,Create folders in DBFS
#Create Parent folder Suman and subfolder Files.
dbutils.fs.mkdirs("Suman/Files")

# COMMAND ----------

# DBTITLE 1,Create sub folder
#Create Subfolder inside Files

dbutils.fs.mkdirs("Suman/Files/Test")

# COMMAND ----------

# DBTITLE 1,Delete Subfolder
#Create test folder
dbutils.fs.rm('Suman/Files/Test', recurse=False)

# COMMAND ----------

# DBTITLE 1,Delete folder and subfolders recursively
#Delete folder and subfolders recursively (Files Folder and Subfolder)
dbutils.fs.rm('Suman/Files', recurse=True)

