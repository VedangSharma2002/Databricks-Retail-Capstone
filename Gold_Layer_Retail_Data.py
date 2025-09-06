# Databricks notebook source
# MAGIC %md
# MAGIC Gold Layer – Retail Sales Analysis
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold_layer;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC We have now succesfully created our Gold Layer Schema. We now proceed to load our tables, and even display them, to ensure that they're still in-tact

# COMMAND ----------

# MAGIC %md
# MAGIC Load and display National DataFrame

# COMMAND ----------

national_df = spark.table("silver_layer.retail_sales_national")
display(national_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Load and Display Provincial DataFrame

# COMMAND ----------

provincial_df = spark.table("silver_layer.retail_sales_provincial")
display(provincial_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Load and display City DataFrame

# COMMAND ----------

city_df = spark.table("silver_layer.retail_sales_city")
display(city_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Getting and visualizing the results for National DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ### National Retail Sales Trend (2017–2025)
# MAGIC
# MAGIC The line chart below displays the monthly trend in **total retail sales across Canada** from January 2017 to March 2025. It highlights overall national growth, key seasonal fluctuations, and the sharp decline during the onset of COVID-19 in 2020. This visual helps investors and analysts quickly identify long-term retail trends and economic recovery patterns.  

# COMMAND ----------

from pyspark.sql.functions import col

national_monthly_df = national_df.groupBy("Report_Month") \
    .sum("VALUE") \
    .withColumnRenamed("sum(VALUE)", "Total_Sales")

display(national_monthly_df.orderBy("Report_Month"))


# COMMAND ----------

# MAGIC %md
# MAGIC Saving our Nationwide data to the gold-layer

# COMMAND ----------

national_monthly_df.write.format("delta").mode("overwrite").saveAsTable("gold_layer.national_monthly_sales")


# COMMAND ----------

# MAGIC %md
# MAGIC Getting and visualizing the results for Provincial DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total Retail Sales by Province (2017–2025)
# MAGIC
# MAGIC This bar chart below shows total retail sales for each Canadian province and territory from 2017 to 2025. This horizontal bar chart compares total retail sales across Canadian provinces from 2017 to 2025. Ontario and Quebec clearly lead in retail activity, while smaller regions like Yukon and Nunavut show significantly lower volumes. This view helps stakeholders identify the strongest provincial markets and evaluate regional investment opportunities.

# COMMAND ----------

provincial_total_df = provincial_df.groupBy("Region") \
    .sum("VALUE") \
    .withColumnRenamed("sum(VALUE)", "Total_Sales")

display(provincial_total_df.orderBy(col("Total_Sales").desc()))


# COMMAND ----------

# MAGIC %md
# MAGIC Saving it to our gold layer as well

# COMMAND ----------

provincial_total_df.write.format("delta").mode("overwrite").saveAsTable("gold_layer.provincial_monthly_sales")


# COMMAND ----------

# MAGIC %md
# MAGIC Getting and visualizing results for City DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total Retail Sales by City (2017–2025)
# MAGIC
# MAGIC This chart below displays total retail sales across major Canadian metropolitan areas. This bar chart shows cumulative retail sales at the city level. Toronto, Montréal, and Vancouver clearly lead, signaling them as key urban markets. This view is helpful for identifying top-performing metropolitan areas and supports localized investment strategies.
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

city_total_df = city_df.groupBy("Region") \
    .sum("VALUE") \
    .withColumnRenamed("sum(VALUE)", "Total_Sales")

display(city_total_df.orderBy(col("Total_Sales").desc()))


# COMMAND ----------

# MAGIC %md
# MAGIC Saving it to our gold layer as well

# COMMAND ----------

city_total_df.write.format("delta").mode("overwrite").saveAsTable("gold_layer.city_monthly_sales")


# COMMAND ----------

