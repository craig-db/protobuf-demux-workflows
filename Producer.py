# Databricks notebook source
# MAGIC %pip install --upgrade  confluent_kafka

# COMMAND ----------

# MAGIC %pip install Faker

# COMMAND ----------

# MAGIC %run "./Common"

# COMMAND ----------

# DBTITLE 1,Create widgets for the producer (simulator) variables
vdd = ["1", "5", "10"]
ndd = [str(i) for i in range(5, 250, 5)]
nrec = [str(i) for i in range(10, 100000, 1000)]
dbutils.widgets.dropdown(name="num_destinations", label="Number of Target Delta tables", defaultValue="5", choices=ndd)
dbutils.widgets.dropdown(name="num_versions", label="Number of Versions to Produce", defaultValue="5", choices=vdd)
dbutils.widgets.dropdown(name="num_records", label="Number of Records to Produce per Version", defaultValue="10", choices=nrec)
dbutils.widgets.dropdown(name="destination", label="Destination", defaultValue="Kafka", choices=["Delta", "Kafka"])
dbutils.widgets.dropdown(name="clean_up", label="clean_up", defaultValue="No", choices=["Yes", "No"])

# COMMAND ----------

# DBTITLE 1,Retrieve widget values
NUM_VERSIONS=int(dbutils.widgets.get("num_versions"))
NUM_RECORDS_PER_VERSION=int(dbutils.widgets.get("num_records"))
NUM_TARGET_TABLES=int(dbutils.widgets.get("num_destinations"))
MODE = dbutils.widgets.get("destination")
CLEAN_UP = dbutils.widgets.get("clean_up")
print(f"This run will produce {NUM_RECORDS_PER_VERSION} messages for each of the {NUM_VERSIONS} versions, for {NUM_TARGET_TABLES} tables")

# COMMAND ----------

# DBTITLE 1,Set up target Catalog and Schema (clean up, if needed)
if CLEAN_UP == "Yes":
  print("Dropping catalog")
  spark.sql(f"drop catalog if exists {catalog} cascade")
  
spark.sql(f"create catalog if not exists {catalog}")
spark.sql(f"create database if not exists {catalog}.{schema}")

# COMMAND ----------

GAMES_ARRAY = []
REGISTERED_SCHEMAS = {}
REGISTERED_TOPICS = {}

# COMMAND ----------

from faker import Faker
from pyspark.sql.functions import col, lit, udf, current_timestamp, concat_ws
from pyspark.sql.protobuf.functions import to_protobuf
from pyspark.sql.types import BinaryType
from datetime import datetime
import random 

Faker.seed(999)
fake = Faker()

# COMMAND ----------

# DBTITLE 1,Each game name will be generated using Faker's first_name() function
GAMES_ARRAY = [f"{str(fake.first_name()).lower()}_game" for i in range(0, NUM_TARGET_TABLES)] if len(GAMES_ARRAY) == 0 else GAMES_ARRAY

# COMMAND ----------

# DBTITLE 1,Checkpoint setting is used for the sink to ensure the stream can be restarted
print(f"CHECKPOINT_LOCATION: {CHECKPOINT_LOCATION}")

# COMMAND ----------

if CLEAN_UP == "Yes":
  dbutils.fs.rm(CHECKPOINT_LOCATION, True)

# COMMAND ----------

from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.admin import AdminClient, NewTopic

admin_client = AdminClient(kafka_config)

# COMMAND ----------

# DBTITLE 1,Clean up topics and schemas (if needed)
if CLEAN_UP == "Yes":
  schema_registry_client = SchemaRegistryClient(schema_registry_conf)
  subjects = schema_registry_client.get_subjects()
  for subject in subjects:
    schema_registry_client.delete_subject(subject, True)
    
  t_dict = admin_client.list_topics()
  t_topics = t_dict.topics
  t_list = [key for key in t_topics]
  if len(t_list) > 0:
    admin_client.delete_topics(t_list)

# COMMAND ----------

# MAGIC %md
# MAGIC # Proceed if you wish to create topics, schemas and publish messages...

# COMMAND ----------

def register_topic(topic):
  fs = admin_client.create_topics([NewTopic(
     topic,
     num_partitions=1,
     replication_factor=3
  )])

# COMMAND ----------

def register_schema(topic, schema):
  schema_registry_client = SchemaRegistryClient(schema_registry_conf)
  k_schema = Schema(schema, "PROTOBUF", list())
  schema_id = int(schema_registry_client.register_schema(f"{topic}-value", k_schema))
  schema_registry_client.set_compatibility(subject_name=f"{topic}-value", level="FULL")

  return schema_id

# COMMAND ----------

wrapper_schema = """

message wrapper {
  optional string game_name = 1;
  optional int32 schema_id = 2;
  optional bytes payload = 3;
} 

"""

# COMMAND ----------

register_topic(WRAPPER_TOPIC)
wrapper_schema_id = register_schema(WRAPPER_TOPIC, wrapper_schema)

# COMMAND ----------

print(wrapper_schema_id)

# COMMAND ----------

"""
Build the inner payload
"""
def get_inner_records(game_name, num_records, num_versions):
  print(f"Generating DF for {game_name}")

  # Protobuf schema
  proto_schema_arr = ["string game_name =1;", "string user_name =2;", 
                      "google.protobuf.Timestamp event_timestamp =3;", 
                      "string is_connection_stable =4;"]

  # To simulate schema evolution, newer versions get an additional column added
  for v in range(0, num_versions):
    proto_schema_arr.append(f"optional string col_{game_name}_{v} ={int(v + 5)};") 

  # Construct the final protobuf schema definition
  proto_schema_str = str("\n".join(proto_schema_arr))
  proto_schema_str = f"""syntax = "proto3";
     import 'google/protobuf/timestamp.proto';
     
     message event {{
       {proto_schema_str}
     }}
  """
  
  # Register the topic and schema with Confluent
  if proto_schema_str not in REGISTERED_SCHEMAS:
    if game_name not in REGISTERED_TOPICS:
      register_topic(game_name)
      REGISTERED_TOPICS[game_name] = True
    schema_id = register_schema(game_name, proto_schema_str)
    REGISTERED_SCHEMAS[proto_schema_str] = schema_id  

  df = spark.range(0, num_records)
  fake_username = udf(fake.user_name)
  stable = udf(lambda:random.choices(["Y", "N"], weights = [95, 5])[0])
  df = df.withColumn("game_name", lit(game_name))
  df = df.withColumn("user_name", fake_username())
  df = df.withColumn("event_timestamp", current_timestamp())
  df = df.withColumn("is_connection_stable", stable())
  for v in range(0, num_versions):
    df = df.withColumn(f"col_{game_name}_{v}", concat_ws("_", lit(f"custom_{game_name}"), col("id")))
  df = df.withColumn("schema_id", lit(REGISTERED_SCHEMAS[proto_schema_str]))

  df = df.selectExpr("schema_id", "struct(*) as event")
  df = df.withColumn("event", df["event"].dropFields("schema_id"))
  df = df.withColumn("event", df["event"].dropFields("id"))

  return df

# COMMAND ----------

latest_version = 0

# COMMAND ----------

NUM_VERSIONS

# COMMAND ----------

# DBTITLE 1,Send simulated payload messages to Kafka
for version in range(latest_version, NUM_VERSIONS + 1):
  # Starting at 1 because Confluent is free for 10 partitions; one partition is needed for the wrapper topic/schema
  for target in range(1, NUM_TARGET_TABLES): 
    latest_version = max(version, latest_version)
    print(f"Schema Version: {latest_version}")
    sr_conf = schema_registry_options.copy()

    # Construct inner payload
    sr_conf["schema.registry.subject"] = f"{GAMES_ARRAY[target]}-value"
    df = get_inner_records(GAMES_ARRAY[target], NUM_RECORDS_PER_VERSION, latest_version)
    df = df.withColumn("game_name", col("event.game_name"))
    df = df.withColumn("payload", to_protobuf("event", options = sr_conf))
    df = df.selectExpr("game_name", "struct(game_name, schema_id, payload) as inner_payload")
    # df.printSchema()
    
    # Construct wrapper payload
    sr_conf["schema.registry.subject"] = f"{WRAPPER_TOPIC}-value"
    df = df.withColumn("wrapper", to_protobuf("inner_payload", options = sr_conf))
    df = df.select(["game_name", "wrapper"])
    
    # df.printSchema()
    # This simulator writes to Delta first and then writes that content to Kafka. This is because
    # the simulator can run in "Destination" MODE of Kafka or Delta. 
    (df
       .write
       .format("delta")
       .mode("append")
       .partitionBy("game_name")
       .saveAsTable(f"{catalog}.{schema}.wrapper")
    )
    if MODE == "Kafka":
      # print("publishing to Kafka")
      df = spark.readStream.table(f"{catalog}.{schema}.wrapper")
      (df
         .selectExpr("game_name as key", "CAST(wrapper AS STRING) as value")
         .writeStream
         .format("kafka")
         .queryName(f"publish version {latest_version} for topic {target}")
         .option("checkpointLocation", CHECKPOINT_LOCATION)
         .option("topic", WRAPPER_TOPIC)
         .option("kafka.bootstrap.servers", KAFKA_SERVER)
         .option("kafka.security.protocol", "SASL_SSL")
         .option("kafka.sasl.jaas.config", 
                 "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(
                   KAFKA_KEY, KAFKA_SECRET))
         .option("kafka.ssl.endpoint.identification.algorithm", "https")
         .option("kafka.sasl.mechanism", "PLAIN")
         .trigger(availableNow=True)
         .start()
      )

# COMMAND ----------

# Inspect the wrapper payload
display(spark.sql(f"select * from {catalog}.{schema}.wrapper"))

# COMMAND ----------

# Check how many messages per game have been constructed and sent
display(spark.sql(f"select game_name, count(*) from {catalog}.{schema}.wrapper group by game_name"))
