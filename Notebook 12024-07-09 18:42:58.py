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
import os

spark=SparkSession.builder.appName('Dataframe').getOrCreate()

spark


## read the dataset
df_pyspark=spark.read.option('header','true').csv('/FileStore/tables/test1-1.csv',inferSchema=True)

### Check the schema
df_pyspark.printSchema()



# COMMAND ----------

display(df_pyspark)

# COMMAND ----------

df_pyspark.select(['Name','Experience']).show()


# COMMAND ----------

### Adding Columns in data frame
df_pyspark=df_pyspark.withColumn('Experience After 2 year',df_pyspark['Experience']+2)
df_pyspark.show()

# COMMAND ----------

df_pyspark=df_pyspark.drop('Experience After 2 year')
df_pyspark.show()
