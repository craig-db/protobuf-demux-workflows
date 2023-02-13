# Databricks notebook source
# MAGIC %pip install --upgrade  confluent_kafka

# COMMAND ----------

# MAGIC %pip install Faker

# COMMAND ----------

my_name = spark.sql("select current_user()").collect()[0][0]
my_name = my_name[:my_name.rfind('@')].replace(".", "_")
schema = f"{my_name}_demux_example"
print(f"Schema used (if in Delta mode): {schema}")

# COMMAND ----------

vdd = [str(i) for i in range(5, 100, 5)]
ndd = [str(i) for i in range(5, 250, 5)]
nrec = [str(i) for i in range(10, 110, 10)]
dbutils.widgets.dropdown(name="num_destinations", label="Number of Target Delta tables", defaultValue="5", choices=ndd)
dbutils.widgets.dropdown(name="num_versions", label="Number of Versions to Produce", defaultValue="5", choices=vdd)
dbutils.widgets.dropdown(name="num_records", label="Number of Records to Produce per Version", defaultValue="10", choices=nrec)
dbutils.widgets.dropdown(name="destination", label="Destination", defaultValue="Delta", choices=["Delta", "Kafka"])
dbutils.widgets.dropdown(name="clean_up", label="clean_up", defaultValue="No", choices=["Yes", "No"])

# COMMAND ----------

# DBTITLE 1,Variables for the producer
NUM_VERSIONS=int(dbutils.widgets.get("num_versions"))
NUM_RECORDS_PER_VERSION=int(dbutils.widgets.get("num_records"))
NUM_TARGET_TABLES=int(dbutils.widgets.get("num_destinations"))
MODE = dbutils.widgets.get("destination")
CLEAN_UP = dbutils.widgets.get("clean_up")
print(f"This run will produce {NUM_RECORDS_PER_VERSION} messages for each of the {NUM_VERSIONS} versions, for {NUM_TARGET_TABLES} tables")

# COMMAND ----------

if CLEAN_UP == "Yes":
  spark.sql(f"drop database if exists {schema} cascade")
  spark.sql(f"create database {schema}")

# COMMAND ----------

GAMES_ARRAY = []
REGISTERED_SCHEMAS = {}
REGISTERED_TOPICS = {}

# COMMAND ----------

from faker import Faker
from pyspark.sql.functions import *
from pyspark.sql.protobuf.functions import to_protobuf
from pyspark.sql.types import BinaryType
from datetime import datetime

Faker.seed(999)
fake = Faker()

# COMMAND ----------

GAMES_ARRAY = [f"{str(fake.first_name()).lower()}_game" for i in range(0, NUM_TARGET_TABLES)] if len(GAMES_ARRAY) == 0 else GAMES_ARRAY

# COMMAND ----------

# MAGIC %md
# MAGIC Create a notebook "Secrets" and set these variables:
# MAGIC * SR_URL="https://setme.confluent.cloud"
# MAGIC * SR_API_KEY="setme"
# MAGIC * SR_API_SECRET="setme"
# MAGIC * KAFKA_KEY="setme"
# MAGIC * KAFKA_SECRET="setme"
# MAGIC * KAFKA_SERVER="setme.confluent.cloud:9092"
# MAGIC * KAFKA_TOPIC = "app-events"
# MAGIC * WRAPPER_TOPIC = "wrapper"

# COMMAND ----------

# DBTITLE 1,Get Confluent Registry and Kafka related secrets
# MAGIC %run "./Secrets"

# COMMAND ----------

print(f"CHECKPOINT_LOCATION: {CHECKPOINT_LOCATION}")

# COMMAND ----------

if CLEAN_UP == "Yes":
  dbutils.fs.rm(CHECKPOINT_LOCATION, True)

# COMMAND ----------

# DBTITLE 1,Prepare config dictionaries, as expected by the confluent library
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

from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.admin import AdminClient, NewTopic

admin_client = AdminClient(kafka_config)

# COMMAND ----------

if CLEAN_UP == "Yes":
  schema_registry_client = SchemaRegistryClient(schema_registry_conf)
  subjects = schema_registry_client.get_subjects()
  for subject in subjects:
    schema_registry_client.delete_subject(subject, True)

# COMMAND ----------

if CLEAN_UP == "Yes":
  t_dict = admin_client.list_topics()
  t_topics = t_dict.topics
  t_list = [key for key in t_topics]
  if len(t_list) > 0:
    admin_client.delete_topics(t_list)

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
  return int(schema_registry_client.register_schema(f"{topic}-value", k_schema))

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

def get_inner_records(game_name, num_records, num_versions):
  print(f"Generating DF for {game_name}")
  records = []
  schema_arr = [f"game_name: STRING", "user_name: STRING", 
                "event_timestamp: TIMESTAMP", "is_connection_stable: BOOLEAN"]
  proto_schema_arr = ["string game_name =1;", "string user_name =2;", 
                      "google.protobuf.Timestamp event_timestamp =3;", 
                      "bool is_connection_stable =4;"]
  
  for v in range(0, num_versions):
    schema_arr.append(f"col_{game_name}_{v}: STRING") 
    proto_schema_arr.append(f"string col_{game_name}_{v} ={int(v + 5)};") 

  proto_schema_str = str("\n".join(proto_schema_arr))
  proto_schema_str = f"""syntax = "proto3";
     import 'google/protobuf/timestamp.proto';
     
     message event {{
       {proto_schema_str}
     }}
  """
  if proto_schema_str not in REGISTERED_SCHEMAS:
    if game_name not in REGISTERED_TOPICS:
      register_topic(game_name)
      REGISTERED_TOPICS[game_name] = True
    schema_id = register_schema(game_name, proto_schema_str)
    REGISTERED_SCHEMAS[proto_schema_str] = schema_id  
  
  for r in range(0, num_records):
    user_name = fake.user_name()
    record = {
      "event" : {
        "game_name": game_name,
        "user_name": user_name,
        "event_timestamp": datetime.now(),
        "is_connection_stable" : False if user_name[0] == "c" else True
      }
    }
    for v in range(0, num_versions):
      record["event"][f"col_{game_name}_{v}"] = f"custom_{game_name}_{v}_{r}"
      
    record["schema_id"] = REGISTERED_SCHEMAS[proto_schema_str]

    records.append(record)
    
  schema_str = ", ".join(schema_arr)
  return spark.createDataFrame(records, f"schema_id INTEGER, event STRUCT<{schema_str}>")

# COMMAND ----------

# DBTITLE 1,Send simulated payload messages (with evolving schema) to Kafka
for version in range(1, NUM_VERSIONS):
  for target in range(0, NUM_TARGET_TABLES):
    print(version)
    sr_conf = schema_registry_options.copy()

    sr_conf["schema.registry.subject"] = f"{GAMES_ARRAY[target]}-value"
    df = get_inner_records(GAMES_ARRAY[target], NUM_RECORDS_PER_VERSION, version)
    df = df.withColumn("game_name", col("event.game_name"))
    df = df.withColumn("payload", to_protobuf("event", options = sr_conf))
    df = df.selectExpr("game_name", "struct(game_name, schema_id, payload) as inner_payload")
    
    sr_conf["schema.registry.subject"] = f"{WRAPPER_TOPIC}-value"
    df = df.withColumn("wrapper", to_protobuf("inner_payload", options = sr_conf))
    df = df.select(["game_name", "wrapper"])
    df.printSchema()
    (df
       .write
       .format("delta")
       .mode("append")
       .partitionBy("game_name")
       .saveAsTable(f"{schema}.wrapper")
    )
    if MODE == "Kafka":
      print("publishing to Kafka")
      df = spark.readStream.table(f"{schema}.wrapper")
      (df
         .selectExpr("game_name as key", "CAST(wrapper AS STRING) as value")
         .writeStream
         .format("kafka")
         .queryName(f"publish version {version} for topic {target}")
         .option("checkpointLocation", CHECKPOINT_LOCATION)
         .option("topic", WRAPPER_TOPIC)
         .option("kafka.bootstrap.servers", KAFKA_SERVER)
         .option("kafka.security.protocol", "SASL_SSL")
         .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(KAFKA_KEY, KAFKA_SECRET))
         .option("kafka.ssl.endpoint.identification.algorithm", "https")
         .option("kafka.sasl.mechanism", "PLAIN")
         .trigger(availableNow=True)
         .start()
      )

# COMMAND ----------

display(spark.sql(f"select * from {schema}.wrapper"))

# COMMAND ----------

display(spark.sql(f"select game_name, count(*) from {schema}.wrapper group by game_name"))
