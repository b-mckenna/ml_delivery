# Databricks notebook source
# MAGIC %md # AutoML Experiment

# COMMAND ----------

from databricks import automl
import mlflow
import pandas
import sklearn.metrics
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# COMMAND ----------

#df = spark.read.format("delta").load('hive_metastore.default.india_covid_vaccination_delta_table')
df = spark.sql("select * from hive_metastore.default.india_covid_vaccination_delta_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train / Test Split

# COMMAND ----------

train_df, test_df = df.randomSplit([0.99, 0.01], seed=42)
display(train_df)

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

# MAGIC 
# MAGIC %md
# MAGIC ## pandas DataFrame

# COMMAND ----------

#model_uri = summary.best_trial.model_path
model_uri = "runs:/110dfd89ed9343968730214b76a807d5/model"
# model_uri = "<model-uri-from-generated-notebook>"

# COMMAND ----------

import mlflow
# Prepare test dataset
test_pdf = test_df.toPandas()
y_test = test_pdf['new_cases']
X_test = test_pdf.drop('new_cases', axis=1)

# Run inference using the best model
model = mlflow.pyfunc.load_model(model_uri)
predictions = model.predict(X_test)
test_pdf['new_cases_predicted'] = predictions
spark_df = spark.createDataFrame(test_pdf)

spark_df.write.mode("overwrite").saveAsTable("test_india_covid_new_cases")

# COMMAND ----------

mlflow_run.info.run_id

# COMMAND ----------

import mlflow
run_id = summary.best_trial.mlflow_run_id
# Select best performing model
model_name = "india_covid_new_cases"
artifact_path = "model"

model_uri = "runs:/{run_id}/{artifact_path}".format(run_id=run_id, artifact_path=artifact_path)
model_details = mlflow.register_model(model_uri=model_uri, name=model_name)

import time
from mlflow.tracking.client import MlflowClient
from mlflow.entities.model_registry.model_version_status import ModelVersionStatus

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


# COMMAND ----------

spark.sql("INSERT INTO TABLE hive_metastore.default.india_covid_vaccination_delta_table VALUES" + predictions)

# COMMAND ----------

predictions

# COMMAND ----------



# COMMAND ----------


