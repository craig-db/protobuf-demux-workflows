# Databricks notebook source
# MAGIC %pip install databricks_cli

# COMMAND ----------

# DBTITLE 1,Delta Live Pipeline name will be "Protobuf Example_<your user>"
my_name = spark.sql("select current_user()").collect()[0][0]
my_name = my_name[:my_name.rfind('@')].replace(".", "_")

PIPELINE_NAME = f"Protobuf Example_{my_name}"

# COMMAND ----------

TARGET_SCHEMA = dbutils.secrets.get(scope = "protobuf-prototype", key = "TARGET_SCHEMA")

# COMMAND ----------

# DBTITLE 1,Prepare an 'init script' that will manage installing protoc
init_script_contents = """
#!/bin/sh
PC=`which protoc`
if [ $? -eq 1 ] 
then
  cd /
  PB_REL="https://github.com/protocolbuffers/protobuf/releases"
  curl -LO $PB_REL/download/v21.5/protoc-21.5-linux-x86_64.zip
  unzip -o /protoc-21.5-linux-x86_64.zip -d /usr/local/
fi
"""

dbutils.fs.put("dbfs:/FileStore/install_proto.sh", init_script_contents, True)

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
dlt_nb_path = nb_context.notebookPath().getOrElse(None).replace("Install_DLT_Pipeline", "DLT")

# COMMAND ----------

# DBTITLE 1,Use Databricks API to register and start the DLT Pipeline
retval = pipelines_api.create(
  settings = {
    "name": PIPELINE_NAME, 
    "target": TARGET_SCHEMA,
    "development": True, 
    "continuous": True, 
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
              "init_scripts": [
                  {
                      "dbfs": {
                          "destination": "dbfs:/FileStore/install_protoc.sh"
                      }
                  }
              ],
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


