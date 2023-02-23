# Databricks notebook source
# DBTITLE 1,Library for interacting with the Databricks API
# MAGIC %pip install databricks_cli

# COMMAND ----------

# MAGIC %md
# MAGIC # Important
# MAGIC 1. Run the Producer notebook before running this notebook. The Producer will set up the game names. Those are needed so that the destination tables can be deteremined and the workflow tasks can be created.
# MAGIC 2. Run this notebook cell-by-cell. Widgets will appear that will allow you to set the source for the stream (Delta or Kafka) as well as the number of streams per cluster. The job consuming the main stream will have its own cluster.
# MAGIC 
# MAGIC ### Cluster
# MAGIC This is a prototype. The clusters being spun up are single-node. For production or workloads with scale, edit the JSON to allocate more cluster resources.

# COMMAND ----------

# MAGIC %run "./Common"

# COMMAND ----------

# DBTITLE 1,Create widgets for the Workflow composition
dbutils.widgets.dropdown(name="destination", label="Stream Source", defaultValue="Kafka", choices=["Delta", "Kafka"])
dbutils.widgets.dropdown(name="reset_checkpoint", label="reset_checkpoint", defaultValue="No", choices=["Yes", "No"])

# COMMAND ----------

reset_checkpoint = dbutils.widgets.get("reset_checkpoint")
MODE = dbutils.widgets.get("destination")

# COMMAND ----------

# DBTITLE 1,The Workflow name will be "Game_Protobufs_wrapper_<your username>". Note: you will get an email if the workflow fails or is killed.
my_name = spark.sql("select current_user()").collect()[0][0]
FAILURE_EMAIL = my_name
my_name = my_name[:my_name.rfind('@')].replace(".", "_")

WRAPPER_WORKFLOW_NAME = f"Game_Protobufs_wrapper_{my_name}"

# COMMAND ----------

games = [x[0] for x in spark.sql(f"select distinct game_name from {catalog}.{schema}.wrapper").collect()]

# COMMAND ----------

from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.jobs.api import JobsApi

# COMMAND ----------

nb_context = dbutils.entry_point.getDbutils().notebook().getContext()

# COMMAND ----------

api_client = ApiClient(token = nb_context.apiToken().get(), host = nb_context.apiUrl().get())
jobs_api = JobsApi(api_client)

# COMMAND ----------

# DBTITLE 1,Build the path to the Workflow's notebook by using the path of this notebook
wrapper_nb_path = nb_context.notebookPath().getOrElse(None).replace("Install_Workflow", "Workflow-wrapper")

# COMMAND ----------

# DBTITLE 1,The Workflow settings
job_settings = {
    "run_as_owner": True,
    "settings": {
        "name": f"{WRAPPER_WORKFLOW_NAME}",
        "email_notifications": {
            "no_alert_for_skipped_runs": False
        },
        "tasks": [
            {
                "task_key": "kafka_consumer",
                "notebook_task": {
                    "notebook_path": f"{wrapper_nb_path}",
                    "source": "WORKSPACE",
                    "base_parameters": {
                      "source": f"{MODE}",
                      "reset_checkpoint": f"{reset_checkpoint}",
                      "games": ",".join(games)
                    },
                },
                "job_cluster_key": "KafkaConsumer_job_cluster_0",
                "max_retries": 2,
                "min_retry_interval_millis": 30000,
                "retry_on_timeout": False,
                "timeout_seconds": 0,
                "email_notifications": {
                    "on_failure": [
                        f"{FAILURE_EMAIL}"
                    ]
                }
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "KafkaConsumer_job_cluster_0",
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "12.1.x-scala2.12",
                    "spark_conf": {
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode"
                    },
                    "aws_attributes": {
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "zone_id": "us-west-2a",
                        "spot_bid_price_percent": 100,
                        "ebs_volume_count": 0
                    },
                    "node_type_id": "c6gd.2xlarge",
                    "driver_node_type_id": "c6gd.2xlarge",
                    "custom_tags": {
                        "ResourceClass": "SingleNode"
                    },
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": True,
                    "data_security_mode": "SINGLE_USER",
                    "runtime_engine": "STANDARD",
                    "num_workers": 0
                }
            }            
        ],
        "format": "MULTI_TASK"
    }
}  

# COMMAND ----------

# DBTITLE 1,Review the workflow JSON configuration
import json
print(json.dumps(job_settings, indent=4))

# COMMAND ----------

# DBTITLE 1,Use Databricks API to register the workflow
retval = jobs_api.create_job(json=job_settings["settings"])

# COMMAND ----------

print(retval)

# COMMAND ----------

# DBTITLE 1,Run the workflow
print(jobs_api.run_now(retval["job_id"], jar_params=None, notebook_params=None, python_params=None, spark_submit_params=None))
