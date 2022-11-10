# Databricks notebook source
file_location = "/stark/India_Covid_Vaccination.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# COMMAND ----------

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)
display(df)

# COMMAND ----------

# create delta table
df.write.format("delta").saveAsTable('india_covid_delta')


# COMMAND ----------


