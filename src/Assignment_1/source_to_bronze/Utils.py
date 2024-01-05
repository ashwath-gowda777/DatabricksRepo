# Databricks notebook source
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, StringType, IntegerType
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Reading the csv files.
#Function to read the csv file

def read_csv(spark, file_path, mode='permissive', custom_schema=None):
    if custom_schema:
        return spark.read.option("mode", mode).schema(custom_schema).csv(file_path, header=True)
    else:
        return spark.read.option("mode", mode).csv(file_path, header=True)

# COMMAND ----------

# DBTITLE 1,Writing the csv files.
#write the csv to a specific path
def write_to_csv(df, file_path, header=True, mode="overwrite"):
    df.write.csv(file_path, header=header, mode=mode)

# COMMAND ----------

# DBTITLE 1,Write to delta table.
def write_delta(df,file_format,output_path,table_name):
    df.write.format(file_format).mode("overwrite").option("path", output_path).saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,Reading from delta table.
def read_delta(spark,file_format,delta_location):
    df = spark.read.format(file_format).load(delta_location)
    df.display()
    return df

# COMMAND ----------

# DBTITLE 1,Function to rename columns.
def rename_columns(df, column_mapping):
    for old_col, new_col in column_mapping.items():
        df = df.withColumnRenamed(old_col, new_col)
    return df

# COMMAND ----------

# DBTITLE 1,Function to convert column names to lower
 # Convert column names to lowercase
def convert_column_names_to_lowercase(df):
    df = df.toDF(*[col.lower() for col in df.columns])
    return df

# COMMAND ----------

# DBTITLE 1,Function to add date column
# Load_date column with current date
def add_load_date_column(df, colum_name):
    df = df.withColumn(colum_name, F.current_date())
    return df

# COMMAND ----------

# DBTITLE 1,Function to drop columns
def drop_columns(df, columns_to_drop):
    return df.drop(*columns_to_drop)

def add_at_load_date(df, new_column_name):
    return df.withColumn(new_column_name, F.current_date())

def order_salary_by_department(df, salary_column, department_column):
    return df.orderBy(department_column, F.desc(salary_column))

# COMMAND ----------

# DBTITLE 1,Function to order salary by department.
def order_salary_by_department(df, salary_column, department_column):
    return df.orderBy(department_column, F.desc(salary_column))

# COMMAND ----------

# DBTITLE 1,Unittest

import unittest
from pyspark.sql import SparkSession


class TestYourFunctions(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
        
    def test_read_csv(self):
        test_data = [("John", 30, "Engineer"), ("Alice", 25, "Data Scientist")]
        schema = ["Name", "Age", "Occupation"]

        df = self.spark.createDataFrame(test_data, schema=schema)
        file_path = "dbfs:/tmp/test_csv" 
        df.write.csv(file_path, header=True, mode="overwrite")
        read_df = read_csv(self.spark, temp_csv_path)
        self.assertEqual(read_df.count(), 2)
        self.assertEqual(read_df.columns, schema)

    def test_convert_column_names_to_lowercase(self):
        test_data = [("John", 30, "Engineer"), ("Alice", 25, "Data Scientist")]
        schema = ["Name", "Age", "Occupation"]
        df = self.spark.createDataFrame(test_data, schema=schema)
        result_df = convert_column_names_to_lowercase(df)
        self.assertTrue(all(col.lower() in result_df.columns for col in df.columns))
        self.assertEqual(result_df.columns, [col.lower() for col in df.columns])

    def test_rename_columns(self):
        test_data = [("John", 30, "Engineer"), ("Alice", 25, "Data Scientist")]
        schema = ["Name", "Age", "Occupation"]
        df = self.spark.createDataFrame(test_data, schema=schema)
        column_mapping = {"Name": "Full_Name", "Age": "Employee_Age"}
        result_df = rename_columns(df, column_mapping)
        self.assertEqual(result_df.columns, ["Full_Name", "Employee_Age", "Occupation"])

    def test_add_load_date_column(self):
        test_data = [("John", 30, "Engineer"), ("Alice", 25, "Data Scientist")]
        schema = ["Name", "Age", "Occupation"]
        df = self.spark.createDataFrame(test_data, schema=schema)
        column_name = "load_date"
        result_df = add_load_date_column(df, column_name)
        self.assertEqual(result_df.columns, schema + [column_name])
        self.assertEqual(result_df.select(column_name).distinct().count(), 1)

    def test_drop_columns(self):
        # Prepare test data
        test_data = [("John", 30, "Engineer"), ("Alice", 25, "Data Scientist")]
        schema = ["Name", "Age", "Occupation"]
        df = self.spark.createDataFrame(test_data, schema=schema)
        columns_to_drop = ["Age", "Occupation"]
        result_df = drop_columns(df, columns_to_drop)
        self.assertEqual(result_df.columns, ["Name"])

    def test_add_at_load_date(self):
        test_data = [("John", 30, "Engineer"), ("Alice", 25, "Data Scientist")]
        schema = ["Name", "Age", "Occupation"]
        df = self.spark.createDataFrame(test_data, schema=schema)
        new_column_name = "load_date"
        result_df = add_at_load_date(df, new_column_name)
        self.assertTrue(new_column_name in result_df.columns)
        self.assertEqual(result_df.select(new_column_name).distinct().count(), 1)

    def test_order_salary_by_department(self):
        test_data = [("John", 30, "Engineer", 5000), ("Alice", 25, "Data Scientist", 7000)]
        schema = ["Name", "Age", "Occupation", "Salary"]
        df = self.spark.createDataFrame(test_data, schema=schema)
        salary_column = "Salary"
        department_column = "Occupation"
        result_df = order_salary_by_department(df, salary_column, department_column)
        self.assertEqual(result_df.first()["Name"], "Alice")

if __name__ == '__main__':
    
    unittest.main(argv=[''], exit=False)
