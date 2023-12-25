# Databricks notebook source
# MAGIC %run /Users/ashwathgowda216@gmail.com/DatabricksRepo1/Assignment_1/source_to_bronze/utils

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# DBTITLE 1,Schema for Employees_table
emp_schema = StructType([
    StructField("employee id", StringType(), True),
    StructField("employee_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("State", StringType(), True),
    StructField("salary", StringType(), True),
    StructField("Age", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Schema for Departments_table
dep_schema = StructType([
    StructField("dept_id", StringType(), True),
    StructField("dept_name", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Schema for Country_table
cou_schema = StructType([
    StructField("country_code", StringType(), True),
    StructField("country_name", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Read methods using custom schema 
file_format='csv'
path="dbfs:/source_to_bronze/Employees_table.csv"
df1 = spark.read.format(file_format).option("header" ,True).option("delimiter",',').option("schema",emp_schema).load(path)

path="dbfs:/source_to_bronze/Department_table.csv"
df2 = spark.read.format(file_format).option("header" ,True).option("delimiter",',').option("schema",dep_schema).load(path)

path="dbfs:/source_to_bronze/Country_table.csv"
df3 = spark.read.format(file_format).option("header" ,True).option("delimiter",',').option("schema",cou_schema).load(path)

# Display the DataFrames
df1.display()
df2.display()
df3.display()

# COMMAND ----------

# DBTITLE 1,Converting all the column names into lower case.
# Converting column names to lowercase for the Employees_table DataFrame
df1 = df1.toDF(*[col.lower() for col in df1.columns])

# Converting column names to lowercase for the Department_table DataFrame
df2 = df2.toDF(*[col.lower() for col in df2.columns])

# Converting column names to lowercase for the Country_table DataFrame
df3 = df3.toDF(*[col.lower() for col in df3.columns])

# Display the DataFrames
df1.display()
df2.display()
df3.display()

# COMMAND ----------

# DBTITLE 1,Renameing the column names

 # Rename columns for the Employees_table DataFrame
df1 = df1.withColumnRenamed("employee id", "employee_id") \
         .withColumnRenamed("employee_name", "employee_name") \
         .withColumnRenamed("department", "department_name") \
         .withColumnRenamed("state", "employee_state") \
         .withColumnRenamed("salary", "salary") \
         .withColumnRenamed("age", "age")
 

 
#Display the DataFrames
df1.display()



# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("EmployeeInfo").getOrCreate()

# Specify the Delta table locations
delta_location_employees = "dbfs:/silver/employee_info/dim_employee/Employees_table"
delta_location_departments = "dbfs:/silver/employee_info/dim_employee/Departments_table"
delta_location_country = "dbfs:/silver/employee_info/dim_employee/Country_table"

# Read the Delta tables
file_format = "delta"
df_employees = spark.read.format(file_format).load(delta_location_employees)
df_departments = spark.read.format(file_format).load(delta_location_departments)
df_country = spark.read.format(file_format).load(delta_location_country)

# Show the DataFrames
df_employees.show()
df_departments.show()
df_country.show()

# Stop the Spark session
# spark.stop()


# COMMAND ----------

# DBTITLE 1,Writing Delta table with specified paths and name
file_format='delta'
output_path="/silver/employee_info/dim_employee/Employees_table"
table_name="dim_employee.Employees_table"
write_delta(df_employees,file_format,output_path,table_name)

output_path="/silver/employee_info/dim_employee/Departments_table"
table_name="dim_employee.Departments_table"
write_delta(df_departments,file_format,output_path,table_name)

output_path="/silver/employee_info/dim_employee/Country_table"
table_name="dim_employee.Country_table"
write_delta(df_country,file_format,output_path,table_name)