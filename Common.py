# Databricks notebook source
# MAGIC %run "./Secrets"

# COMMAND ----------

my_name = spark.sql("select current_user()").collect()[0][0]
my_name = my_name[:my_name.rfind('@')].replace(".", "_")

schema = f"{my_name}_demux_db"
catalog = f"{my_name}_demux_catalog"

print(f"Table location: {catalog}.{schema}")

# COMMAND ----------

schema_registry_options = {
  "schema.registry.subject" : f"{KAFKA_TOPIC}-value",
  "schema.registry.address" : f"{SR_URL}",
  "confluent.schema.registry.basic.auth.credentials.source" : "USER_INFO",
  "confluent.schema.registry.basic.auth.user.info" : f"{SR_API_KEY}:{SR_API_SECRET}"
}

schema_registry_conf = {
  "url": SR_URL,
  "basic.auth.user.info": '{}:{}'.format(SR_API_KEY, SR_API_SECRET)
}

kafka_config = {
  "bootstrap.servers": f"{KAFKA_SERVER}",
  "security.protocol": "SASL_SSL",
  "sasl.mechanisms": "PLAIN",
  "sasl.username": f"{KAFKA_KEY}",
  "sasl.password": f"{KAFKA_SECRET}",
  "session.timeout.ms": "45000"
} 
