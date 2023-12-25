# Databricks notebook source
# MAGIC %run /Users/ashwathgowda216@gmail.com/DatabricksRepo1/Assignment_1/source_to_bronze/utils

# COMMAND ----------

# DBTITLE 1,Reading the file Employees_table.csv
from pyspark.sql import SparkSession

df_employee = spark.read.csv("dbfs:/source_to_bronze/Employees_table.csv",mode="permissive",header=True)
df_employee.show()

# COMMAND ----------

# DBTITLE 1,Reading the file Departments_table.csv
df_department = spark.read.csv("dbfs:/source_to_bronze/Department_table.csv",mode="DROPMALFORMED",header=True)
df_department.show()

# COMMAND ----------

# DBTITLE 1,Reading the file Country_table.csv
df_country = spark.read.csv("dbfs:/source_to_bronze/Country_table.csv",mode="FAILFAST",header=True)
df_country.show()

# COMMAND ----------

file_format='csv'
path="dbfs:/source_to_bronze/Employees_table.csv"
write_csv(df_employee,file_format,path)

path="dbfs:/source_to_bronze/Departments_table.csv"
write_csv(df_department,file_format,path)

path="dbfs:/source_to_bronze/Country_table.csv"
write_csv(df.country,file_format,path)

# COMMAND ----------

