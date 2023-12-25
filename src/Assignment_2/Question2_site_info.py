# Databricks notebook source
import requests
 
# getting the url from source
 
url = 'https://reqres.in/api/users?page=2'
 
response=requests.get('https://reqres.in/api/users?page=2')
 
df=spark.sparkContext.parallelize([response.text])
 
site_info_df=spark.read.option("multiline",True).json(df)
 
display(site_info_df)

# COMMAND ----------

columns_to_drop = ["page", "per_page", "total", "total_pages","support"]
site_info_df = site_info_df.drop(*columns_to_drop)
display(df)

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

# Define the schema for the element struct
element_schema = StructType([
    StructField("avatar", StringType(), True),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("id", LongType(), True),
    StructField("last_name", StringType(), True)
])
 
# Create the schema for the array of structs within the 'data' column
data_schema = StructType([
    StructField("data", ArrayType(StructType(element_schema)), True)
])

# COMMAND ----------

from pyspark.sql.functions import explode_outer, col
# converting the array type to struct type for data
 
df_explode = site_info_df.withColumn("data",explode_outer(col("data")))

# COMMAND ----------

# flatten the dataframe
site_info_df = df_explode.select('data.*')
display(site_info_df)

# COMMAND ----------

from pyspark.sql import functions as F

# Derive a new column from email as site_address with values (reqres.in)
site_info_df = site_info_df.withColumn("site_address", F.lit("reqres.in"))

# Display the DataFrame with the new column
display(site_info_df.select("avatar", "email", "first_name", "id", "last_name", "site_address"))


# COMMAND ----------

# Add load_date with the current date
site_info_df = site_info_df.withColumn("load_date", F.current_date())

# COMMAND ----------

# Write DataFrame to DBFS location in Delta format with overwrite mode
site_info_df.write.format("delta").option('mergeschema',True).mode("overwrite").save("/site_info/persons")
 
# Create or replace a Delta table in the specified database
spark.sql("CREATE DATABASE IF NOT EXISTS site_info")
site_info_df.write.format("delta").option('mergeschema',True).mode("overwrite").saveAsTable("site_info.persons")