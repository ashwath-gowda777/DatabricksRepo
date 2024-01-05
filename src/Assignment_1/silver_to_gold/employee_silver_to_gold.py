# Databricks notebook source
# MAGIC %run /Users/ashwathgowda216@gmail.com/DatabricksRepo_Final/source_to_bronze/Utils

# COMMAND ----------

# DBTITLE 1,Reading the delta table stored
filepath = "/silver/employee_info/dim_employee/Employees_table"
employee1_df = spark.read.format("delta").load(filepath)

# COMMAND ----------

employee1_df.show()

# COMMAND ----------

# DBTITLE 1,Finnding salary of each department.
from pyspark.sql import functions as F

employee1_df = drop_columns(employee1_df, ["load_date"])

employee1_df = add_at_load_date(employee1_df, new_column_name="at_load_date")

employee1_df=order_salary_by_department(employee1_df, salary_column="salary", department_column="department_name")

# COMMAND ----------

# DBTITLE 1,Salary of each department
employee1_df.display()

# COMMAND ----------

# DBTITLE 1,Writing the delta file
file_format='delta'
output_path="/gold/employee/fact_employee"
table_name="fact_employee"
write_delta(employee1_df,file_format,output_path,table_name)