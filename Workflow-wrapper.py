# Databricks notebook source
# MAGIC %run "./Secrets"

# COMMAND ----------

my_name = spark.sql("select current_user()").collect()[0][0]
my_name = my_name[:my_name.rfind('@')].replace(".", "_")
default_schema = f"{my_name}_demux_example"

# COMMAND ----------

dbutils.widgets.text(name="target_schema", label="target_schema", defaultValue=default_schema)
dbutils.widgets.dropdown(name="reset_checkpoint", label="reset_checkpoint_drop_destination", defaultValue="No", choices=["Yes", "No"])
dbutils.widgets.dropdown(name="starting_offset", label="starting_offset", defaultValue="earliest", choices=["earliest", "latest"])

# COMMAND ----------

target_schema = dbutils.widgets.get("target_schema")
reset_checkpoint = dbutils.widgets.get("reset_checkpoint")
starting_offset = dbutils.widgets.get("starting_offset")

# COMMAND ----------

checkpoint_location = f"{CHECKPOINT_LOCATION}/wrapper_checkpoint"

# COMMAND ----------

if reset_checkpoint == "Yes":
  print("cleaning checkpoints and destination table")
  spark.sql(f"drop table if exists {target_schema}.bronze_protobufs_wf")
  dbutils.fs.rm(checkpoint_location, True)

# COMMAND ----------

schema_registry_options = {
  "schema.registry.subject" : f"{WRAPPER_TOPIC}-value",
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

from pyspark.sql.protobuf.functions import from_protobuf
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# DBTITLE 1,Gold table with the transformed protobuf messages, surfaced in a Delta table
bronze_df = (
  spark
    .readStream
    .format("kafka")
    .option("startingOffsets", starting_offset)
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(KAFKA_KEY, KAFKA_SECRET))
    .option("kafka.ssl.endpoint.identification.algorithm", "https")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("subscribe", WRAPPER_TOPIC)
    .option("mergeSchema", "true")
    .load()
    .withColumn("wrapper_deser_timestamp", lit(current_timestamp()))
    .select("wrapper_deser_timestamp", from_protobuf("value", options = schema_registry_options).alias("wrapper"))
)

bronze_df = bronze_df.select("wrapper_deser_timestamp", "wrapper.*")

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

(bronze_df
   .writeStream
   .format("delta")
   .partitionBy("game_name")
   .option("checkpointLocation", checkpoint_location)
   .outputMode("append")
   .queryName(f"from_protobuf bronze_df into {target_schema}")
   .toTable(f"{target_schema}.bronze_protobufs_wf")
)