# Databricks notebook source
# MAGIC %md # AutoML Experiment

# COMMAND ----------

from databricks import automl
import mlflow
import pandas
import sklearn.metrics
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
import time
from mlflow.tracking.client import MlflowClient
from mlflow.entities.model_registry.model_version_status import ModelVersionStatus

# COMMAND ----------

df = spark.sql('select date, new_cases, new_deaths, new_tests from hive_metastore.default.india_covid_vaccination_delta_table')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build dataset from features

# COMMAND ----------

from databricks.feature_store import FeatureLookup
from databricks import feature_store

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

# COMMAND ----------

# Split dataset into train and test
train_df, test_df = training_df.randomSplit([0.90, 0.10], seed=42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profile data

# COMMAND ----------

dbutils.data.summarize(training_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run data checks

# COMMAND ----------

from deepchecks.tabular.suites import full_suite
from deepchecks.tabular import Dataset

train_dataset = Dataset(train_df.toPandas(), label="new_cases", features=['date', 'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated', 'new_vaccinations', 'population', 'population_density', 'median_age', 'aged_65_older'], cat_features=['date'])

test_dataset = Dataset(test_df.toPandas(), label="new_cases", features=['date', 'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated', 'new_vaccinations', 'population', 'population_density', 'median_age', 'aged_65_older'], cat_features=['date'])

suite = full_suite()
suite.run(train_dataset=train_dataset, test_dataset=test_dataset)


# COMMAND ----------

# MAGIC %md
# MAGIC # Training
# MAGIC The following command starts an AutoML run. When the run completes, you can follow the link to the best trial notebook and training code as well as a feature importance plot.

# COMMAND ----------

summary = automl.regress(train_df, target_col='new_cases', data_dir='dbfs:/automl/india_covid_automl', timeout_minutes=30)

# COMMAND ----------

# MAGIC %md
# MAGIC # Inference
# MAGIC 
# MAGIC The model trained by AutoML can be used to make predictions on new data. The examples below demonstrate how to make predictions on data in pandas DataFrames, or register the model as a Spark UDF for prediction on Spark DataFrames.

# COMMAND ----------

#run_id = summary.best_trial.mlflow_run_id
#model_uri = "runs:/"+run_id+"/model"
#a456f1238dff4869a92ea530ed2768e0/
model_uri = "runs:/a456f1238dff4869a92ea530ed2768e0/model"
#run_id

# COMMAND ----------

# Prepare test dataset
test_pdf = test_df.toPandas()
y_test = test_pdf['new_cases']
X_test = test_pdf.drop('new_cases', axis=1)

# Run inference using the best model
model = mlflow.pyfunc.load_model(model_uri)
predictions = model.predict(X_test)
test_pdf['new_cases_predicted'] = predictions
spark_df = spark.createDataFrame(test_pdf)

spark_df.write.mode("overwrite").saveAsTable("predictions_india_covid_vaccination_delta_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register best performing model

# COMMAND ----------

model_name="predicting_new_cases"
model_details = mlflow.register_model(model_uri=model_uri, name=model_name)

# Wait until the model is ready
def wait_until_ready(model_name, model_version):
  client = MlflowClient()
  for _ in range(10):
    model_version_details = client.get_model_version(
      name=model_name,
      version=model_version,
    )
    status = ModelVersionStatus.from_string(model_version_details.status)
    print("Model status: %s" % ModelVersionStatus.to_string(status))
    if status == ModelVersionStatus.READY:
      break
    time.sleep(1)

wait_until_ready(model_details.name, model_details.version)
