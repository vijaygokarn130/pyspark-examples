# Databricks notebook source
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
df_pyspark1=spark.read.option('header','true').csv('/FileStore/tables/test3-1.csv',inferSchema=True)
### Check the schema
df_pyspark1.printSchema()
df_pyspark1.head(0)


# COMMAND ----------

display(df_pyspark1.groupBy('Department'))

# COMMAND ----------

df_pyspark.select(['Name','Experience']).show()


# COMMAND ----------

### Adding Columns in data frame
df_pyspark=df_pyspark.withColumn('Experience After 2 year',df_pyspark['Experience']+2)
df_pyspark.show()

# COMMAND ----------

df_pyspark=df_pyspark.drop('Experience After 2 year')
df_pyspark1.show()

# COMMAND ----------

df_pyspark1.groupBy('Departments').avg().show()


# COMMAND ----------

df_pyspark1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

df_pyspark1.withColumnRenamed('salary','compensation').show()

# COMMAND ----------

df_pyspark1.agg({'Salary':'sum'}).show()

# COMMAND ----------

display(df_pyspark1.agg({'Salary':'max'}))

# COMMAND ----------

df_pyspark1.groupBy('Name').max().show()

