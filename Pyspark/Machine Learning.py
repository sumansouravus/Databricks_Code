# Databricks notebook source
# Importing necessary libraries
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("TelecomChurnPrediction").getOrCreate()

# Load the dataset (assuming it's stored as a CSV file in DBFS or other accessible storage)
data_path = "dbfs:/FileStore/Telecom_new_data.csv"
customer_df = spark.read.csv(data_path, header=True, inferSchema=True)

# Display the first few rows of the dataset
display(customer_df)


# COMMAND ----------

from pyspark.sql.functions import col, when, sum as _sum

# Create a new DataFrame with 1 for nulls and 0 for non-nulls in each column
null_counts = customer_df.select([when(col(c).isNull(), 1).otherwise(0).alias(c) for c in df.columns])

# Sum the null values in each column
null_summary = null_counts.select([_sum(col(c)).alias(c) for c in null_counts.columns])

# Show the count of null values for each column
display(null_summary)

# COMMAND ----------

# Remove rows with any null values
customer_df_clean = customer_df.dropna()

# Show the cleaned data
display(customer_df_clean)


# COMMAND ----------

# Show the actual column names
print(customer_df_clean.columns)

# COMMAND ----------

#Import Required Libraries
#StringIndexer: This is used to convert categorical string values into numeric indices. For example, it will convert categories like "Male" and "Female" into numeric values.

#VectorAssembler: This combine multiple column into a single feature vector, which is required by machine learning algorithms in PySpark. It helps in preparing data for model training.
from pyspark.ml.feature import StringIndexer, VectorAssembler

#LogisticRegression: This is the machine learning algorithm that will be used for classification (predicting churn).
from pyspark.ml.classification import LogisticRegression

#Pipeline:This is sequence of steps that allow to automate workflow of transforming data and training models.It enables seamless chaining of data transformations and model training.
from pyspark.ml import Pipeline

# Define the correct feature columns (update this list based on your dataset)
feature_cols = [
    'Age', 
    'Tenure (months)',  # Corrected column name
    'Monthly Charges (USD)', 
    'Total Charges (USD)', 
    'Complaints (count)'
]

# StringIndexer for categorical columns
indexers = [StringIndexer(inputCol=col, outputCol=col + "_index") for col in ['Service Type', 'Payment Status', 'Gender']]

# StringIndexer for the 'Churn' column
churn_indexer = StringIndexer(inputCol='Churn', outputCol='Churn_index')

# VectorAssembler for feature transformation
assembler = VectorAssembler(
    inputCols=feature_cols + [col + "_index" for col in ['Service Type', 'Payment Status', 'Gender']], 
    outputCol="features"
)

# Split data into training and testing sets (80% training, 20% testing)
train_data, test_data = customer_df_clean.randomSplit([0.8, 0.2], seed=1234)

# Initialize the Logistic Regression model (Inbuilt model)
lr = LogisticRegression(labelCol='Churn_index', featuresCol='features')

# Set up the pipeline
pipeline = Pipeline(stages=indexers + [churn_indexer, assembler, lr])

# Train the model using the training data
model = pipeline.fit(train_data)

# Save the model using MLflow (to track versions)
import mlflow
import mlflow.spark

mlflow.start_run()
mlflow.spark.log_model(model, "churn_model")
mlflow.end_run()

# Model training complete message
print("Model Training Complete!")

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Make predictions on the test data
predictions = model.transform(test_data)

# Initialize evaluator for accuracy
evaluator = MulticlassClassificationEvaluator(labelCol="Churn_index", predictionCol="prediction", metricName="accuracy")

# Evaluate the model's accuracy
accuracy = evaluator.evaluate(predictions)
print(f"Model Accuracy: {accuracy * 100:.2f}%")

# Evaluate Precision, Recall, and F1 Score
precision_evaluator = MulticlassClassificationEvaluator(labelCol="Churn_index", predictionCol="prediction", metricName="weightedPrecision")
recall_evaluator = MulticlassClassificationEvaluator(labelCol="Churn_index", predictionCol="prediction", metricName="weightedRecall")
f1_evaluator = MulticlassClassificationEvaluator(labelCol="Churn_index", predictionCol="prediction", metricName="f1")

precision = precision_evaluator.evaluate(predictions)
recall = recall_evaluator.evaluate(predictions)
f1 = f1_evaluator.evaluate(predictions)

print(f"Precision: {precision * 100:.2f}%")
print(f"Recall: {recall * 100:.2f}%")
print(f"F1 Score: {f1 * 100:.2f}%")


# COMMAND ----------

# Save the trained model using MLflow
import mlflow
import mlflow.spark

# Start a new MLflow run
mlflow.start_run()

# Log the model
mlflow.spark.log_model(model, "churn_model")

# End the MLflow run
mlflow.end_run()

# Alternatively, save the model locally
model.save("/path/to/save/churn_model")


# COMMAND ----------

# DBTITLE 1,delete
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline

# Initialize Spark session
spark = SparkSession.builder.appName("ChurnPrediction").getOrCreate()

# Create the DataFrame
Sample_data = [
    (1, 32, 'Male', 24, 75, 1800, 'Prepaid', 'Yes', 'No', 'Month-to-month', 'Credit Card', 2, 'Paid', 'No'),
    (2, 45, 'Female', 12, 50, 600, 'Postpaid', 'Yes', 'Yes', 'One year', 'Bank Transfer', 1, 'Unpaid', 'Yes'),
    (3, 28, 'Male', 36, 80, 2880, 'Postpaid', 'No', 'Yes', 'Two years', 'Credit Card', 0, 'Paid', 'No'),
    (4, 50, 'Female', 48, 120, 5760, 'Prepaid', 'Yes', 'Yes', 'Month-to-month', 'Debit Card', 5, 'Paid', 'Yes')
]

columns = ['CustomerID', 'Age', 'Gender', 'Tenure (months)', 'Monthly Charges (USD)', 'Total Charges (USD)', 
           'Service Type', 'Internet Service', 'Tech Support', 'Contract Type', 'Payment Method', 'Complaints (count)', 
           'Payment Status', 'Churn']

# Create DataFrame
Sample_df = spark.createDataFrame(Sample_data, columns)

# Show DataFrame
display(Sample_df)


# COMMAND ----------

# Assuming new data is available (new customer data)
new_data = spark.createDataFrame([
    (1, 32, 'Male', 24, 75, 1800, 'Prepaid', 'Yes', 'No', 'Month-to-month', 'Credit Card', 2, 'Paid', 'No'),
    (2, 45, 'Female', 12, 50, 600, 'Postpaid', 'Yes', 'Yes', 'One year', 'Bank Transfer', 1, 'Unpaid', 'Yes'),
    (3, 28, 'Male', 36, 80, 2880, 'Postpaid', 'No', 'Yes', 'Two years', 'Credit Card', 0, 'Paid', 'No'),
    (4, 50, 'Female', 48, 120, 5760, 'Prepaid', 'Yes', 'Yes', 'Month-to-month', 'Debit Card', 5, 'Paid', 'Yes')
], ['CustomerID', 'Age', 'Gender', 'Tenure (months)', 'Monthly Charges (USD)', 'Total Charges (USD)', 
           'Service Type', 'Internet Service', 'Tech Support', 'Contract Type', 'Payment Method', 'Complaints (count)', 
           'Payment Status', 'Churn'])

# Apply the same transformations as before (indexing and assembler)
new_data_transformed = model.transform(new_data)

# Show the predictions
new_data_transformed.select('CustomerID', 'prediction', 'probability').show(5)


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Define the schema for the DataFrame
schema = StructType([
    StructField('CustomerID', StringType(), True),
    StructField('Age', IntegerType(), True),
    StructField('Gender', StringType(), True),
    StructField('Tenure (months)', IntegerType(), True),
    StructField('Monthly Charges (USD)', DoubleType(), True),
    StructField('Total Charges (USD)', DoubleType(), True),
    StructField('Service Type', StringType(), True),
    StructField('Internet Service', StringType(), True),
    StructField('Tech Support', StringType(), True),
    StructField('Contract Type', StringType(), True),
    StructField('Payment Method', StringType(), True),
    StructField('Complaints (count)', IntegerType(), True),
    StructField('Payment Status', StringType(), True),
    StructField('Churn', StringType(), True)
])

# Create an empty DataFrame with the defined schema
new_test_data = spark.createDataFrame([], schema)

# Make predictions on new data
new_predictions = model.transform(new_test_data)

# Evaluate and track performance again
new_accuracy = evaluator.evaluate(new_predictions)
new_precision = precision_evaluator.evaluate(new_predictions)
new_recall = recall_evaluator.evaluate(new_predictions)
new_f1 = f1_evaluator.evaluate(new_predictions)

# Log new performance metrics for monitoring
print(f"New Accuracy: {new_accuracy * 100:.2f}%")
print(f"New Precision: {new_precision * 100:.2f}%")
print(f"New Recall: {new_recall * 100:.2f}%")
print(f"New F1 Score: {new_f1 * 100:.2f}%")
