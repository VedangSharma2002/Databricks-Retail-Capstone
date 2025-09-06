# Databricks notebook source
# MAGIC %md
# MAGIC Bronze Layer: Getting the raw data file and storing it in the bronze layer. 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze_layer;
# MAGIC

# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/raw_source_data/retail_data/retail_sales_canada.csv", header=True, inferSchema=True)


# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC We are now adding metadata columns

# COMMAND ----------

from pyspark.sql import functions as F

bronze_df = df.withColumn("ingested_timestamp", F.current_timestamp()) \
              .withColumn("ingested_date", F.current_date())


# COMMAND ----------

# MAGIC %md
# MAGIC One of the columns in the dataset has special characters (NAICS). We fix it by renaming it to just NAICS

# COMMAND ----------

bronze_df = bronze_df.withColumnRenamed("North American Industry Classification System (NAICS)", "NAICS")


# COMMAND ----------

# MAGIC %md
# MAGIC We now write and save our data to the Bonze Layer.

# COMMAND ----------

bronze_df.write.format("delta").mode("overwrite").saveAsTable("bronze_layer.retail_sales_bronze")


# COMMAND ----------

