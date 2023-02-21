# Databricks notebook source
# MAGIC %run "./Common"

# COMMAND ----------

# DBTITLE 1,Create widgets for the Kafka consumer variables
dbutils.widgets.dropdown(name="reset_checkpoint", label="reset_checkpoint_drop_destination", defaultValue="No", choices=["Yes", "No"])
dbutils.widgets.dropdown(name="starting_offset", label="starting_offset", defaultValue="earliest", choices=["earliest", "latest"])
dbutils.widgets.dropdown(name="source", label="Source", defaultValue="Delta", choices=["Delta", "Kafka"])

# COMMAND ----------

# DBTITLE 1,Retrieve widget values
reset_checkpoint = dbutils.widgets.get("reset_checkpoint")
starting_offset = dbutils.widgets.get("starting_offset")
source = dbutils.widgets.get("source")

# COMMAND ----------

# DBTITLE 1,Checkpoint setting is used for the sink to ensure the stream can be restarted
checkpoint_location = f"{CHECKPOINT_LOCATION}/wrapper_checkpoint"

# COMMAND ----------

if reset_checkpoint == "Yes":
  print("cleaning checkpoints and destination table")
  spark.sql(f"drop table if exists {catalog}.{schema}.bronze_protobufs_wf")
  dbutils.fs.rm(checkpoint_location, True)

# COMMAND ----------

# DBTITLE 1,Wrapper protobuf registry topic
schema_registry_options["schema.registry.subject"] = f"{WRAPPER_TOPIC}-value"

# COMMAND ----------

from pyspark.sql.protobuf.functions import from_protobuf
from pyspark.sql.functions import current_timestamp, lit

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

# DBTITLE 1,Save the inner protobuf payload into a bronze table
(bronze_df
   .writeStream
   .format("delta")
   .partitionBy("game_name")
   .option("checkpointLocation", checkpoint_location)
   .outputMode("append")
   .queryName(f"from_protobuf bronze_df into {schema}")
   .toTable(f"{catalog}.{schema}.bronze_protobufs_wf")
)
