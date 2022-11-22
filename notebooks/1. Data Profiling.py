# Databricks notebook source
df = spark.sql("select * from hive_metastore.default.india_covid_vaccination_data_transform")
dbutils.data.summarize(df)


# COMMAND ----------


