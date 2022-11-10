# Databricks notebook source
df = spark.sql("select * from hive_metastore.default.india_covid_vaccination_delta_table")
dbutils.data.summarize(df)


# COMMAND ----------


