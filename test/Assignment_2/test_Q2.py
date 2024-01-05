# Databricks notebook source
# MAGIC %run /Users/ashwathgowda216@gmail.com/DatabricksRepo_Final/Assignment_2/Utils_siteinfo

# COMMAND ----------


import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class TestSite(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("unittest").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


    def test_read_api_data(self):
        url = 'https://reqres.in/api/users?page=2'
        api_data_df = read_api_data(self.spark, url)
        self.assertIsNotNone(api_data_df)
        self.assertIn("data", api_data_df.columns)
        self.assertGreaterEqual(api_data_df.count(), 0)

    def test_drop_columns(self):
        data = [("John", 30, "Engineer", 5000), ("Alice", 25, "Data Scientist", 7000)]
        schema = ["Name", "Age", "Occupation", "Salary"]
        df = self.spark.createDataFrame(data, schema=schema)
        columns_to_drop = ["Age", "Salary"]
        result_df = drop_columns(df, columns_to_drop)
        self.assertEqual(result_df.columns, ["Name", "Occupation"])
        self.assertEqual(result_df.count(), 2)

    def test_explode_columns(self):
        data = [("John", ["Engineer", "Data Scientist"]), ("Alice", ["Manager", "Analyst"])]
        schema = ["Name", "Occupations"]
        df = self.spark.createDataFrame(data, schema=schema)
        columns_to_explode = ["Occupations"]
        result_df = explode_columns(df, columns_to_explode)
        self.assertEqual(result_df.columns, ["Name", "Occupations"])
        self.assertEqual(result_df.select("Name").distinct().count(), 2)
        self.assertEqual(result_df.select("Occupations").distinct().count(), 4)

    def test_derive_site_address(self):
        data = [("John", "john@example.com"), ("Alice", "alice@example.com")]
        schema = ["Name", "email"]
        df = self.spark.createDataFrame(data, schema=schema)
        result_df = derive_site_address(df)
        self.assertIn("site_address", result_df.columns)
        self.assertEqual(result_df.count(), 2)

    def test_add_load_date(self):
        data = [("John", "Engineer"), ("Alice", "Data Scientist")]
        schema = ["Name", "Occupation"]
        df = self.spark.createDataFrame(data, schema=schema)
        result_df = add_load_date(df)
        self.assertIn("load_date", result_df.columns)
        self.assertEqual(result_df.count(), 2)



if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)