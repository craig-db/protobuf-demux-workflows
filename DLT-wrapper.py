# Databricks notebook source
import dlt

# COMMAND ----------

# IMPORTANT: for production-quality code, use Databricks Secrets (instead of pipeline settings)

# Kafka-related settings
KAFKA_KEY = spark.conf.get("KAFKA_KEY")
KAFKA_SECRET = spark.conf.get("KAFKA_SECRET")
KAFKA_SERVER = spark.conf.get("KAFKA_SERVER")
KAFKA_TOPIC = spark.conf.get("KAFKA_TOPIC")

# Schema Registry-related settings
SR_URL = spark.conf.get("SR_URL")
SR_API_KEY = spark.conf.get("SR_API_KEY")
SR_API_SECRET = spark.conf.get("SR_API_SECRET")

# COMMAND ----------

schema_registry_options = {
  "schema.registry.subject" : f"{KAFKA_TOPIC}-value",
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
import dlt

# COMMAND ----------

# DBTITLE 1,Gold table with the transformed protobuf messages, surfaced in a Delta table
@dlt.table
def bronze_protobufs_dlt (
  partition_cols = "game_name",
  comment = "game-specific protobufs"
):
  bronze_df = (
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_SERVER)
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(KAFKA_KEY, KAFKA_SECRET))
      .option("kafka.ssl.endpoint.identification.algorithm", "https")
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("subscribe", KAFKA_TOPIC)
      .option("mergeSchema", "true")
      .load()
      .select(from_protobuf("value", options = schema_registry_options)).alias("wrapper")
    )
  return bronze_df.select("wrapper.*")
