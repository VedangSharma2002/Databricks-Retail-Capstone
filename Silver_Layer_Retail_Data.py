# Databricks notebook source
# MAGIC %md
# MAGIC ### Silver Layer  
# MAGIC In this layer, we begin cleaning our raw data file. This includes removing redundant data, ensuring it is in an acceptable format, and—most importantly—getting rid of junk, such as empty values and fields that serve no purpose to our end-users.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC We first begin by creating a silver layer schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver_layer;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Read the bronze table into a Spark DataFrame

# COMMAND ----------

bronze_df = spark.table("bronze_layer.retail_sales_bronze")
display(bronze_df)


# COMMAND ----------

# MAGIC %md
# MAGIC In the cell below, I'm trying to get a count of NULL values in the dataset, to check for Data Quality

# COMMAND ----------

from pyspark.sql.functions import col, sum

null_counts = bronze_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in bronze_df.columns])
display(null_counts)


# COMMAND ----------

# MAGIC %md
# MAGIC In the next step, we filter out the null values and drop junk columns and fields which serve no purpose to our actual analysis, and also display it, just so that we can confirm that our code worked

# COMMAND ----------

silver_df = bronze_df.filter("VALUE IS NOT NULL") \
    .drop("STATUS", "SYMBOL", "TERMINATED", "DECIMALS", "DGUID", "UOM_ID", "SCALAR_ID", "COORDINATE")

display(silver_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Upon further inspection, we get rid of some additional junk columns, namely UOM and Vector

# COMMAND ----------

silver_df = silver_df.drop("VECTOR", "UOM")
display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC In the next step, we change the field name of REF_DATE to report_month, for the purpose of making it more readable for our end-users

# COMMAND ----------

silver_df = silver_df.withColumnRenamed("REF_DATE", "Report_Month")
display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC We now format our NAICS column and get rid of unnecessary noise in form of IDS such [44-45] from our dataset, so that we only have clean titles such "User car dealers" and "food and beverage retailers" for our sectors

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

silver_df = silver_df.withColumn("NAICS", regexp_replace("NAICS", r"\s*\[.*\]", ""))
display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Although NAICS might stand for North American Industry Classification System, our end-users may or may not be aware of what it stands for. Therefore, we rename the field "NAICS" to something easily identifiable for our end-users.

# COMMAND ----------

silver_df = silver_df.withColumnRenamed("NAICS", "Retail_Category")
display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC We now rename the field named "GEO" to region, for the purpose of making it more readable

# COMMAND ----------

silver_df = silver_df.withColumnRenamed("GEO", "Region")
display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC We are now gauging the usability of scalar_factor field in our daataset. If it only returns one value, then that means the entire field is filled with duplicated values which serve no purpose. Also, we will be able to make a final conclusion, if the only value our code below returns is "Thousands". That would help us calculate the real world value of the numbers found in the VALUE field

# COMMAND ----------

silver_df.select("SCALAR_FACTOR").distinct().show()


# COMMAND ----------

# MAGIC %md
# MAGIC As expected, SCALAR_FACTOR field only returns a singular value, when asked for DISTINCT values. Therefore, we can safely drop it.

# COMMAND ----------

silver_df = silver_df.drop("SCALAR_FACTOR")
display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC We now check Unique entries for our Region column

# COMMAND ----------

silver_df.select("Region").distinct().orderBy("Region").show(50)


# COMMAND ----------

# MAGIC %md
# MAGIC The Dataset contains messy values for Region column. For instance, it contains different levels of granularity. It contains contains Nation wide data, Provincial data, and even city data all at once. This may give us messy results when we make our Gold Layer visualizations, or even our dashboard. As a result, I've decided to split the dataset into three parts. National, Provincial, and City data
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Creating the NATIONAL dataframe

# COMMAND ----------

national_df = silver_df.filter(silver_df.Region == "Canada")
display(national_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Creating PROVINCIAL dataframe

# COMMAND ----------

provincial_df = silver_df.filter(
    (silver_df.Region != "Canada") & (~silver_df.Region.contains(","))
)
display(provincial_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Checking if the code worked and ensuring that REGION column only contains Canadian provinces

# COMMAND ----------

provincial_df.select("Region").distinct().orderBy("Region").show(50)


# COMMAND ----------

# MAGIC %md
# MAGIC Creating the CITY dataframe

# COMMAND ----------

city_df = silver_df.filter(silver_df.Region.contains(","))
display(city_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Checking if the code worked and ensuring that the newly created DataFrame only contains Canadian cities

# COMMAND ----------

city_df.select("Region").distinct().orderBy("Region").show(50)


# COMMAND ----------

# MAGIC %md
# MAGIC Checking for unique values in the total sales column. If the discinct count only returns a singular value, that means that the data is just noise. However, if it returns multiple values, then that would mean that the particular field may help derive business information for our end-users.

# COMMAND ----------

national_df.select("Sales").distinct().show()
provincial_df.select("Sales").distinct().show()
city_df.select("Sales").distinct().show()


# COMMAND ----------

# MAGIC %md
# MAGIC DataFrames other than national_df have indeed returned only 1 value. As a result, we get rid of Total Sales field, only for provincial and city dataframe

# COMMAND ----------

provincial_df = provincial_df.drop("Sales")
city_df = city_df.drop("Sales")


# COMMAND ----------

# MAGIC %md
# MAGIC Since most of the transformations are new done, we now save our source of truth table to the silver layer.

# COMMAND ----------

silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_layer.retail_sales_silver")


# COMMAND ----------

# MAGIC %md
# MAGIC Saving the national DataFrame. We then display the DataFrame as well to confirm that it is still in-tact.

# COMMAND ----------

national_df.write.format("delta").mode("overwrite").saveAsTable("silver_layer.retail_sales_national")

# COMMAND ----------

display(national_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Saving the Provincial DataFrame. We then display the DataFrame to confirm that it is still in-tact.

# COMMAND ----------

provincial_df.write.format("delta").mode("overwrite").saveAsTable("silver_layer.retail_sales_provincial")

display(provincial_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Saving the City DataFrame. We then display the City DataFrame and confirm that it is still in-tact.

# COMMAND ----------

city_df.write.format("delta").mode("overwrite").saveAsTable("silver_layer.retail_sales_city")

display(city_df)


# COMMAND ----------

