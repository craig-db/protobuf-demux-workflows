# Databricks notebook source
# MAGIC %pip install databricks_cli

# COMMAND ----------

# MAGIC %run "./Secrets"

# COMMAND ----------

# DBTITLE 1,Delta Live Pipeline name will be "Protobuf Example_<your user>"
my_name = spark.sql("select current_user()").collect()[0][0]
my_name = my_name[:my_name.rfind('@')].replace(".", "_")

PIPELINE_NAME = f"Game_Protobufs_{my_name}"

# COMMAND ----------

TARGET_SCHEMA = f"{my_name}_demux_example"

# COMMAND ----------

from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.pipelines.api import PipelinesApi

# COMMAND ----------

nb_context = dbutils.entry_point.getDbutils().notebook().getContext()

# COMMAND ----------

#Intitialize Client
api_client = ApiClient(token = nb_context.apiToken().get(), host = nb_context.apiUrl().get())
pipelines_api = PipelinesApi(api_client)

# COMMAND ----------

# DBTITLE 1,Build the path to the DLT notebook by using the path of this notebook.
dlt_nb_path = nb_context.notebookPath().getOrElse(None).replace("Install_DLT_Pipeline", "DLT-wrapper")

# COMMAND ----------

# DBTITLE 1,Use Databricks API to register and start the DLT Pipeline
retval = pipelines_api.create(
  settings = {
    "name": PIPELINE_NAME, 
    "target": TARGET_SCHEMA,
    "development": True, 
    "continuous": False, 
    "configuration": {
        "SR_URL": SR_URL,
        "SR_API_KEY": SR_API_KEY,
        "SR_API_SECRET": SR_API_SECRET,
        "KAFKA_KEY": KAFKA_KEY,
        "KAFKA_SECRET": KAFKA_SECRET,
        "KAFKA_SERVER": KAFKA_SERVER,
        "KAFKA_TOPIC": KAFKA_TOPIC,
        "WRAPPER_TOPIC": WRAPPER_TOPIC
    },
    "libraries": [
      {
        "notebook": {
          "path": dlt_nb_path
        }
      }
    ],
    "clusters": [
          {
              "label": "default",
              "autoscale": {
                  "min_workers": 1,
                  "max_workers": 2,
                  "mode": "LEGACY"
              }
          }
      ]
    },
    settings_dir=None, 
    allow_duplicate_names=False
)

# COMMAND ----------

print(retval)

# COMMAND ----------


