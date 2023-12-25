# Databricks notebook source
# MAGIC %run /Users/ashwathgowda216@gmail.com/DatabricksRepo1/Assignment_1/source_to_bronze/utils

# COMMAND ----------

# DBTITLE 1,Read the table stored in silver layer.
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("EmployeeInfo").getOrCreate()

# Read Delta tables from Silver layer
silver_location_employees = "/silver/employee_info/dim_employee/Employees_table"
silver_location_departments = "/silver/employee_info/dim_employee/Departments_table"
silver_location_country = "/silver/employee_info/dim_employee/Country_table"

df_employees_silver = spark.read.format("delta").load(silver_location_employees)
df_departments_silver = spark.read.format("delta").load(silver_location_departments)
df_country_silver = spark.read.format("delta").load(silver_location_country)

# Display the DataFrames from Silver layer
df_employees_silver.show()
df_departments_silver.show()
df_country_silver.show()

# COMMAND ----------

# DBTITLE 1,droping  load_date in table
# Drop the load_date column
df_employees_gold = df_employees_silver.drop("load_date")
df_departments_gold = df_departments_silver.drop("load_date")
df_country_gold = df_country_silver.drop("load_date")

# Show the DataFrames without load_date
df_employees_gold.show()
df_departments_gold.show()
df_country_gold.show()

# Stop the Spark session
# spark.stop()

# COMMAND ----------

# DBTITLE 1,salary of each department in desc order
from pyspark.sql import functions as F

# Join employees and departments DataFrames on the 'department' column
joined_df = df_employees_silver.join(df_departments_silver, df_employees_silver.department == df_departments_silver.dept_id, "inner")

# Group by department and calculate the total salary for each department
result_df = joined_df.groupBy("dept_name").agg(F.sum("salary").alias("total_salary"))

# Order the result in descending order
result_df = result_df.orderBy(F.desc("total_salary"))

# Show the result
result_df.show()

# Stop the Spark session
# spark.stop()


# COMMAND ----------

# DBTITLE 1,Adding at_load_date
delta_path_employees = "/silver/employee_info/dim_employee/Employees_table"
delta_path_departments = "/silver/employee_info/dim_employee/Departments_table"


# Read Delta tables
df_employees_silver = spark.read.format("delta").load(delta_path_employees)
df_departments_silver = spark.read.format("delta").load(delta_path_departments)

# Add at_load_date column with current date
current_date = F.current_date()
df_employees_silver = df_employees_silver.withColumn("at_load_date", current_date)
df_departments_silver = df_departments_silver.withColumn("at_load_date", current_date)

df_employees_silver.show()
df_departments_silver.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create a Spark session
spark = SparkSession.builder.appName("EmployeeInfo").getOrCreate()

# Define Delta table paths
delta_path_employees = "/silver/employee_info/dim_employee/Employees_table"

# Read Delta table
df_employees_silver = spark.read.format("delta").load(delta_path_employees)

# Add at_load_date column with current date
current_date = F.current_date()
df_employees_silver = df_employees_silver.withColumn("at_load_date", current_date)

# Define the DBFS path for the gold table
gold_path_employees = "/gold/employee/fact_employee"

# Write DataFrame to DBFS location with overwrite, replace condition, and schema migration
df_employees_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "at_load_date = current_date()") \
    .option("mergeSchema", "true") \
    .save(gold_path_employees)

# # Stop the Spark session
# spark.stop()
