# Databricks notebook source
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode_outer, col, split, lit, current_date

# COMMAND ----------

def get_api_data(url):
    return [requests.get(url).text]


# COMMAND ----------

def read_api_data(spark, url):
    api_data = get_api_data(url)
    return spark.read.option("multiline", True).json(spark.sparkContext.parallelize(api_data))

# COMMAND ----------

def drop_columns(site_info_df, columns_to_drop):
    return site_info_df.drop(*columns_to_drop)

# COMMAND ----------

def explode_columns(site_info_df, columns_to_explode):
    for column in columns_to_explode:
        site_info_df = site_info_df.withColumn(column, explode_outer(col(f"data.{column}")))
    return site_info_df.drop("data")

# COMMAND ----------

def derive_site_address(site_info_df):
    return site_info_df.withColumn('site_address', split(col('email'), '@').getItem(1))


# COMMAND ----------

def add_load_date(site_info_df):
    return site_info_df.withColumn('load_date', lit(current_date()))

# COMMAND ----------

def write_to_delta(site_info_df, delta_path):
    site_info_df.write.format("delta").mode("overwrite").option("mergeschema", True).save(delta_path)