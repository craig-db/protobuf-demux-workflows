# Databricks notebook source
# DBTITLE 1,Common settings
# MAGIC %run "./Common"

# COMMAND ----------

# DBTITLE 1,Set up widgets (parameters) for the notebook
dbutils.widgets.text(name="game_name", label="game_name", defaultValue="kimberly_game")
dbutils.widgets.dropdown(name="reset_checkpoint", label="reset_checkpoint_drop_destination", defaultValue="No", choices=["Yes", "No"])

# COMMAND ----------

reset_checkpoint = dbutils.widgets.get("reset_checkpoint")
game_name = dbutils.widgets.get("game_name")

# COMMAND ----------

# DBTITLE 1,Checkpoint setting is used for the sink to ensure the stream can be restarted
checkpoint_location = f"{CHECKPOINT_LOCATION}/{game_name}_checkpoint"

# COMMAND ----------

# DBTITLE 1,Game-specific protobuf registry topic
schema_registry_options["schema.registry.subject"] = f"{game_name}-value"

# COMMAND ----------

if reset_checkpoint == "Yes":
  print("cleaning checkpoints and destination table")
  spark.sql(f"drop table if exists {catalog}.{schema}.silver_{game_name}_wf")
  dbutils.fs.rm(checkpoint_location, True)

# COMMAND ----------

from pyspark.sql.protobuf.functions import from_protobuf
from pyspark.sql.functions import current_timestamp, lit, col
import time

# Sleep aims to allow the main Kafka consumer job to start first. 
# That job sets up the source table for this notebook.
time.sleep(60)

# COMMAND ----------

# DBTITLE 1,Read the bronze table that consists of the inner protobuf payload
silver_df = (
  spark
  .readStream
  .format("delta")
  .table(f"{catalog}.{schema}.bronze_protobufs_wf")
  .filter(col("game_name") == game_name)
  .withColumn("silver_deser_timestamp", lit(current_timestamp()))
  .select("silver_deser_timestamp", from_protobuf("payload", options = schema_registry_options).alias("payload"))
)
  
silver_df = silver_df.select("silver_deser_timestamp", "payload.*")

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Save as Delta (after deserializing the protobuf payload)
(silver_df
   .writeStream
   .format("delta")
   .option("checkpointLocation", checkpoint_location)
   .outputMode("append")
   .queryName(f"from_protobuf silver_df into {catalog}.{schema}.{game_name}")
   .toTable(f"{catalog}.{schema}.silver_{game_name}_wf")
)
