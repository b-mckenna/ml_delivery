# Databricks notebook source
#from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from databricks import feature_store

# COMMAND ----------

# Create a Spark Dataframe
df = spark.sql("select * from hive_metastore.default.india_covid_vaccination_delta_table")

# Create Dataframes of each feature set
vax_features_df = df.select("date","total_vaccinations", "people_vaccinated", "people_fully_vaccinated", "new_vaccinations")
display(vax_features_df)

pop_df = df.select("date","population", "population_density", "aged_65_older", "median_age")


# COMMAND ----------

# Create the Feature Store Database
%sql
CREATE DATABASE IF NOT EXISTS feature_store_india_covid;


# COMMAND ----------

fs = feature_store.FeatureStoreClient()


# COMMAND ----------

# Create tables for each feature set and load the dataframes
fs.create_table(
    name="feature_store_india_covid.vaccination_features",
    primary_keys=["date"],
    df=vax_features_df,
    partition_columns="date",
    description="Covid cases in India. Vaccination features"
)

fs.create_table(
    name="feature_store_india_covid.population_features",
    primary_keys=["date"],
    df=pop_df,
    partition_columns="date",
    description="Covid cases in India. Population features"
)


# COMMAND ----------

# Load dataframe with lookup keys
df1 = spark.sql("select * from hive_metastore.default.india_covid_vaccination_delta_table")
training_df = df1.select("date", "new_cases", "new_deaths", "new_tests", "population", "population_density", "median_age", "aged_65_older")

# COMMAND ----------

from databricks.feature_store import FeatureLookup
from databricks import feature_store

# The model training uses two features from the 'customer_features' feature table and
# a single feature from 'product_features'
feature_lookups = [
    FeatureLookup(
      table_name = 'feature_store_india_covid.vaccination_features',
      feature_names = ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated", "new_vaccinations"],
      lookup_key = 'date'
    )
  ]

fs = feature_store.FeatureStoreClient()

# Create a training set using training DataFrame and features from Feature Store
# The training DataFrame must contain all lookup keys from the set of feature lookups,
# in this case 'customer_id' and 'product_id'. It must also contain all labels used
# for training, in this case 'rating'.
training_set = fs.create_training_set(
  df=training_df,
  feature_lookups = feature_lookups,
  label = 'new_cases'
)

training_df = training_set.load_df()

# COMMAND ----------

train_df, test_df = training_df.randomSplit([0.99, 0.01], seed=42)

# COMMAND ----------

myprofile = dbutils.data.summarize(train_df)

# COMMAND ----------

# MAGIC %pip install deepchecks

# COMMAND ----------

from deepchecks.tabular.suites import full_suite
from deepchecks.tabular import Dataset
df = spark.sql("select * from hive_metastore.default.india_covid_vaccination_delta_table").toPandas()
dataset = Dataset(df, label="new_cases", features=['date', 'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated', 'new_vaccinations', 'population', 'population_density', 'median_age', 'aged_65_older', ], cat_features=[])

suite = full_suite()
suite.run(dataset)


# COMMAND ----------


