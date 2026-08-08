# Databricks notebook source
spark.sql("SHOW CATALOGS").show()

# COMMAND ----------

spark.sql("SHOW SCHEMAS IN flight_delay").show()

# COMMAND ----------

spark.sql("SHOW TABLES IN flight_delay.bronze").show(truncate=False)

# COMMAND ----------

bronze_df = spark.table("flight_delay.bronze.flights_bronze")

display(bronze_df)

# COMMAND ----------

print(bronze_df.count())
bronze_df.printSchema()

# COMMAND ----------

""" Created a Volume for the Bronze Layer to be read """
spark.sql("CREATE VOLUME IF NOT EXISTS `flight_delay_databricks`.flight_delay.transtats_files")