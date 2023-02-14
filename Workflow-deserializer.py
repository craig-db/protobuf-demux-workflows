# Databricks notebook source
# MAGIC %run "./Secrets"

# COMMAND ----------

# MAGIC %run "./Common"

# COMMAND ----------

dbutils.widgets.text(name="game_name", label="game_name", defaultValue="kimberly_game")
dbutils.widgets.dropdown(name="reset_checkpoint", label="reset_checkpoint_drop_destination", defaultValue="No", choices=["Yes", "No"])

# COMMAND ----------

reset_checkpoint = dbutils.widgets.get("reset_checkpoint")
game_name = dbutils.widgets.get("game_name")

# COMMAND ----------

checkpoint_location = f"{CHECKPOINT_LOCATION}/{game_name}_checkpoint"

# COMMAND ----------

schema_registry_options = {
  "schema.registry.subject" : f"{game_name}-value",
  "schema.registry.address" : f"{SR_URL}",
  "confluent.schema.registry.basic.auth.credentials.source" : "USER_INFO",
  "confluent.schema.registry.basic.auth.user.info" : f"{SR_API_KEY}:{SR_API_SECRET}"
}

schema_registry_conf = {
    'url': SR_URL,
    'basic.auth.user.info': '{}:{}'.format(SR_API_KEY, SR_API_SECRET)
}

kafka_config = {
  "bootstrap.servers": f"{KAFKA_SERVER}",
  "security.protocol": "SASL_SSL",
  "sasl.mechanisms": "PLAIN",
  "sasl.username": f"{KAFKA_KEY}",
  "sasl.password": f"{KAFKA_SECRET}",
  "session.timeout.ms": "45000"
} 

# COMMAND ----------

if reset_checkpoint == "Yes":
  print("cleaning checkpoints and destination table")
  spark.sql(f"drop table if exists {catalog}.{schema}.silver_{game_name}_wf")
  dbutils.fs.rm(checkpoint_location, True)

# COMMAND ----------

from pyspark.sql.protobuf.functions import from_protobuf
from pyspark.sql.functions import current_timestamp, lit, col

# COMMAND ----------

# DBTITLE 1,Gold table with the transformed protobuf messages, surfaced in a Delta table
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

(silver_df
   .writeStream
   .format("delta")
   .option("checkpointLocation", checkpoint_location)
   .outputMode("append")
   .queryName(f"from_protobuf silver_df into {catalog}.{schema}.{game_name}")
   .toTable(f"{catalog}.{schema}.silver_{game_name}_wf")
)
