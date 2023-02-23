# Databricks notebook source
# MAGIC %run "./Common"

# COMMAND ----------

# OK so this is a bit of laziness on my part. This cell is getting the names of the games that comprise the inner payload.
# The simulator saves records to delta (in addition to publishing them to Kafka, if that is desired).
# I needed a way for this consumer to understand the child payload ("game names"). I could have gone to the schema registry to get that list,
# but this was quicker/easier:
all_games = [x[0] for x in spark.sql(f"select distinct game_name from {catalog}.{schema}.wrapper").collect()]
all_games_str = ",".join(all_games)

# COMMAND ----------

# DBTITLE 1,Create widgets for the Kafka consumer variables
dbutils.widgets.dropdown(name="reset_checkpoint", label="reset_checkpoint_drop_destination", defaultValue="No", choices=["Yes", "No"])
dbutils.widgets.dropdown(name="starting_offset", label="starting_offset", defaultValue="earliest", choices=["earliest", "latest"])
dbutils.widgets.dropdown(name="source", label="Source", defaultValue="Kafka", choices=["Delta", "Kafka"])
dbutils.widgets.dropdown(name="mode", label="Mode", defaultValue="SingleDestination", choices=["SingleDestination", "foreachBatch"])
dbutils.widgets.text(name="games", label="Games(comma-sep)", defaultValue=all_games_str)

# COMMAND ----------

# DBTITLE 1,Retrieve widget values
reset_checkpoint = dbutils.widgets.get("reset_checkpoint")
starting_offset = dbutils.widgets.get("starting_offset")
source = dbutils.widgets.get("source")
games_str = dbutils.widgets.get("games")
mode = dbutils.widgets.get("mode")

games = games_str.split(",")

# COMMAND ----------

# DBTITLE 1,Checkpoint setting is used for the sink to ensure the stream can be restarted
checkpoint_location = f"{CHECKPOINT_LOCATION}/wrapper_checkpoint"
deep_checkpoint_location = f"{CHECKPOINT_LOCATION}/deep_wrapper_checkpoint"

# COMMAND ----------

if reset_checkpoint == "Yes":
  print("cleaning checkpoints and destination table")
  spark.sql(f"drop table if exists {catalog}.{schema}.bronze_protobufs_wf")
  spark.sql(f"drop table if exists {catalog}.{schema}.deep_bronze_protobufs_wf")
  for game_name in games:
    spark.sql(f"drop table if exists {catalog}.{schema}.silver_{game_name}_onehop_wf")
    spark.sql(f"drop view if exists {catalog}.{schema}.silver_view_{game_name}")
  dbutils.fs.rm(checkpoint_location, True)
  dbutils.fs.rm(deep_checkpoint_location, True)

# COMMAND ----------

# DBTITLE 1,Wrapper protobuf registry topic
schema_registry_options["schema.registry.subject"] = f"{WRAPPER_TOPIC}-value"

# COMMAND ----------

from pyspark.sql.protobuf.functions import from_protobuf
from pyspark.sql.functions import current_timestamp, lit, col

# COMMAND ----------

# DBTITLE 1,Bronze source consists of the wrapper protobuf
if source == "Kafka":
  bronze_df = (
    spark
      .readStream
      .format("kafka")
      .option("startingOffsets", starting_offset)
      .option("kafka.bootstrap.servers", KAFKA_SERVER)
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.jaas.config", 
              "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(
                KAFKA_KEY, KAFKA_SECRET))
      .option("kafka.ssl.endpoint.identification.algorithm", "https")
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("subscribe", WRAPPER_TOPIC)
      .option("mergeSchema", "true")
      .load()
      .withColumn("wrapper_deser_timestamp", lit(current_timestamp()))
      .select("wrapper_deser_timestamp", 
              from_protobuf("value", options = schema_registry_options).alias("wrapper"))
  )
else:
  bronze_df = (
    spark
    .readStream
    .format("delta")
    .table(f"{catalog}.{schema}.wrapper")
    .withColumn("wrapper_deser_timestamp", lit(current_timestamp()))
    .select("wrapper_deser_timestamp", from_protobuf("wrapper", options = schema_registry_options).alias("wrapper"))
  )
  
bronze_df = bronze_df.select("wrapper_deser_timestamp", "wrapper.*")

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

# Used in foreachBatch "Mode" (not recommended; see comments)
def fan_out(bronze_df, batchId):
  bronze_df.persist()  
  
  # note this loop: within foreachBatch the code has to be single-threaded! Contrast 
  # this with the other fan-out approach (multiple tasks) where you get parallelism 
  # at the task & cluster level.
  for game_name in games:
    game_conf = schema_registry_options.copy()
    game_conf["schema.registry.subject"] = f"{game_name}-value"
    game_conf["schema.registry.schema.evolution.mode"] = "none"

    (
      # TODO: add filter
      bronze_df
       .select("wrapper_deser_timestamp", from_protobuf("payload", options = game_conf).alias("payload"))
       .select("wrapper_deser_timestamp", "payload.*")
       .write
       .format("delta")
       .mode("append")
       # Next two options help ensure idempotent writes
       .option("txnVersion", batchId)
       .option("txnAppId", "GAME_FAN_OUT")
       .option("mode", "append")
       .option("mergeSchema", "true")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{catalog}.{schema}.silver_{game_name}_onehop_wf")
    )

  bronze_df.unpersist()

# COMMAND ----------

# DBTITLE 1,Save the inner protobuf payload into a bronze table
if "mode" == "foreachBatch":
  (
    bronze_df
      .writeStream
      .option("checkpointLocation", checkpoint_location)
      .foreachBatch(fan_out)
      .start()
  )
else:
  deep_bronze_df = None
  for game_name in games:
    game_conf = schema_registry_options.copy()
    game_conf["schema.registry.subject"] = f"{game_name}-value"
    inner_df = bronze_df.filter(col("game_name") == game_name).withColumn(game_name, from_protobuf(bronze_df["payload"], game_conf))
    if deep_bronze_df != None:
      deep_bronze_df = deep_bronze_df.unionByName(inner_df, True)
    else:
      deep_bronze_df = inner_df
  
  (deep_bronze_df
     .writeStream
     .format("delta")
     .partitionBy("game_name")
     .option("checkpointLocation", deep_checkpoint_location)
     .outputMode("append")
     .queryName(f"from_protobuf bronze_df into {schema}")
     .toTable(f"{catalog}.{schema}.deep_bronze_protobufs_wf")
  )                                                                               

# COMMAND ----------

if mode == "SingleDestination":
  for game_name in games:
    spark.sql(f"create view if not exists {catalog}.{schema}.silver_view_{game_name} as select {game_name}.* from {catalog}.{schema}.deep_bronze_protobufs_wf where game_name = '{game_name}'")

# COMMAND ----------


