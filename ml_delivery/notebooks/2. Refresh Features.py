# Databricks notebook source
from databricks import feature_store

# Scheduled job to update feature sets
fs = feature_store.FeatureStoreClient()

# Create a Spark Dataframe
df = spark.sql("select * from hive_metastore.default.india_covid_vaccination_data_transform")

# Create Dataframes of each feature set
vax_features_df = df.select("date","total_vaccinations", "people_vaccinated", "people_fully_vaccinated", "new_vaccinations")
display(vax_features_df)

pop_df = df.select("date","population", "population_density", "aged_65_older", "median_age")


# COMMAND ----------

# Merge features into Feature Store
fs.write_table(
 name="feature_store_india_covid.vaccination_features",
 df=vax_features_df,
 mode="merge",
)

fs.write_table(
 name="feature_store_india_covid.population_features",
 df=pop_df,
 mode="merge",
)

# COMMAND ----------


