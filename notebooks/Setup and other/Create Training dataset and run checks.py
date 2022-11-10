# Databricks notebook source
from databricks.feature_store import FeatureLookup
from databricks import feature_store

df = spark.sql('select date, new_cases, new_deaths, new_tests from hive_metastore.default.india_covid_vaccination_delta_table')

feature_lookups = [
    FeatureLookup(
        table_name = 'feature_store_india_covid.vaccination_features',
        feature_names = ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated", "new_vaccinations"],
        lookup_key = 'date'
    ),
    FeatureLookup(
        table_name = "feature_store_india_covid.population_features",
        feature_names=["population", "population_density", "aged_65_older", "median_age"],
        lookup_key = 'date'
    )
]

fs = feature_store.FeatureStoreClient()

training_set = fs.create_training_set(
  df=df,
  feature_lookups = feature_lookups,
  label = 'new_cases'
)

training_df = training_set.load_df()

train_df, test_df = training_df.randomSplit([0.90, 0.10], seed=42)

# COMMAND ----------

dbutils.data.summarize(training_df)

# COMMAND ----------

from deepchecks.tabular.suites import full_suite
from deepchecks.tabular import Dataset

train_dataset = Dataset(train_df.toPandas(), label="new_cases", features=['date', 'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated', 'new_vaccinations', 'population', 'population_density', 'median_age', 'aged_65_older'], cat_features=['date'])

test_dataset = Dataset(test_df.toPandas(), label="new_cases", features=['date', 'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated', 'new_vaccinations', 'population', 'population_density', 'median_age', 'aged_65_older'], cat_features=['date'])

suite = full_suite()
suite.run(train_dataset=train_dataset, test_dataset=test_dataset)


# COMMAND ----------

from deepchecks.tabular.checks import TrainTestFeatureDrift
import pandas as pd

train_dataset = Dataset(train_df.toPandas(), label="new_cases", features=['date', 'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated', 'new_vaccinations', 'population', 'population_density', 'median_age', 'aged_65_older'], cat_features=[])

test_dataset = Dataset(test_df.toPandas(), label="new_cases", features=['date', 'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated', 'new_vaccinations', 'population', 'population_density', 'median_age', 'aged_65_older'], cat_features=[])
# Initialize and run desired check
TrainTestFeatureDrift().run(train_dataset, test_dataset)

# COMMAND ----------


