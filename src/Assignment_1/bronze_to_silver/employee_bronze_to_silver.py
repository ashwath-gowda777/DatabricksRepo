# Databricks notebook source
# MAGIC %run /Users/ashwathgowda216@gmail.com/DatabricksRepo_Final/source_to_bronze/Utils

# COMMAND ----------

# DBTITLE 1,Reading the dataframes with custom schema, from dbfs location friom source_to_bronze.
country_schema=StructType([
    StructField("country_code",StringType(),True),
    StructField("country_name",StringType(),True)
])

employee_schema=StructType([
    StructField("employee_id",IntegerType(),True),
    StructField("employee_name",StringType(),True),
    StructField("department", StringType(), True),
    StructField("State",StringType(), True),
    StructField("Salary",IntegerType(), True),
    StructField("Age",IntegerType(), True)
])

department_schema=StructType([
    StructField("dept_id",StringType(),True),
    StructField("dept_name",StringType(),True),
])


country_path="dbfs:/source_to_bronze/country_df.csv"
country_df=read_csv(spark, file_path=country_path , mode='permissive', custom_schema=country_schema)

employee_path="dbfs:/source_to_bronze/employee_df.csv"
employee1_df=read_csv(spark, file_path=employee_path , mode='failfast', custom_schema=employee_schema)

department_path="dbfs:/source_to_bronze/employee_df.csv"
department1_df=read_csv(spark, file_path=department_path , mode='permissive', custom_schema=department_schema)

# COMMAND ----------

employee1_df= convert_column_names_to_lowercase(employee1_df)

column_mapping = {
    "employee id": "employee_id",
    "employee name": "employee_name",
    "department": "department_name",
    "state": "employee_state",
    "salary": "salary",
    "age": "age"
}
# Call the function to rename columns
employee1_df = rename_columns(employee1_df, column_mapping)

employee1_df=add_load_date_column(employee1_df, "load_date")



# COMMAND ----------

# DBTITLE 1,Write dataframe to delta table
file_format='delta'
output_path="/silver/employee_info/dim_employee/Employees_table"
table_name="Employees_table"
write_delta(employee1_df,file_format,output_path,table_name)

# COMMAND ----------

