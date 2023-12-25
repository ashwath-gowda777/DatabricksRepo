# Databricks notebook source
# DBTITLE 1,writing the Function for csv.
def write_csv(df,file_format,path):
    df.write.format(file_format).save(path, header=True).options(header='True', delimiter=',').csv(path)

# COMMAND ----------

# DBTITLE 1,Reading the Function.
def read_csv(spark,file_format,path,schema):
    df = spark.read.format(file_format).option("header" ,True).option("schema",schema)
    df.display()
    return df

# COMMAND ----------

# DBTITLE 1,Writing Delta table with SCD Type 1
def write_delta(df,file_format,output_path,table_name):
    df.write.format(file_format).mode("overwrite").option("path", output_path).saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,Reading the delta table
def read_delta(spark,file_format,delta_location):
    df = spark.read.format(file_format).load(delta_location)
    df.display()
    return df