# Databricks notebook source
# In this Video We will Cover
# PySpark Dataframe
# Reading The Dataset
# Checking the Datatypes of the Column(Schema)
# Selecting Columns And Indexing
# Check Describe option similar to Pandas
# Adding Columns
# Dropping columns
# Renaming Columns



from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('Dataframe').getOrCreate()

spark



