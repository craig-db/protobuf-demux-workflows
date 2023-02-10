# Databricks notebook source
# MAGIC %pip install --upgrade protobuf

# COMMAND ----------

# MAGIC %pip install pbspark

# COMMAND ----------

# MAGIC %pip install confluent_kafka

# COMMAND ----------

import dlt

# COMMAND ----------

#
# We avoid hard-coding URLs, keys, secrets, etc. by using Databricks Secrets. 
# Read about it here: https://docs.databricks.com/security/secrets/index.html
#

# Kafka-related settings
KAFKA_KEY = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_KEY")
KAFKA_SECRET = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_SECRET")
KAFKA_SERVER = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_SERVER")
KAFKA_TOPIC = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_TOPIC")

# Schema Registry-related settings
SR_URL = dbutils.secrets.get(scope = "protobuf-prototype", key = "SR_URL")
SR_API_KEY = dbutils.secrets.get(scope = "protobuf-prototype", key = "SR_API_KEY")
SR_API_SECRET = dbutils.secrets.get(scope = "protobuf-prototype", key = "SR_API_SECRET")

# COMMAND ----------

#
# Kafka configuration
#
# Confluent recommends SASL/GSSAPI or SASL/SCRAM for production deployments. Here, instead, we use
# SASL/PLAIN. Read more on Confluent security here: 
# https://docs.confluent.io/platform/current/security/security_tutorial.html#security-tutorial
config = {
  "bootstrap.servers": f"{KAFKA_SERVER}",
  "security.protocol": "SASL_SSL",
  "sasl.mechanisms": "PLAIN",
  "sasl.username": f"{KAFKA_KEY}",
  "sasl.password": f"{KAFKA_SECRET}",  
  "session.timeout.ms": "45000"
}  

#
# Schema Registry configuration
#
# For exploring more security options related to the Schema Registry, go here:
# https://docs.confluent.io/platform/current/schema-registry/security/index.html
schema_registry_conf = {
    'url': SR_URL,
    'basic.auth.user.info': '{}:{}'.format(SR_API_KEY, SR_API_SECRET)}

# COMMAND ----------

# DBTITLE 1,Necessary Imports
# Source for these libraries can be found here: https://github.com/confluentinc/confluent-kafka-python
from confluent_kafka import DeserializingConsumer, SerializingProducer

# https://github.com/confluentinc/confluent-kafka-python/tree/master/src/confluent_kafka/admin
from confluent_kafka.admin import AdminClient, NewTopic

# https://github.com/confluentinc/confluent-kafka-python/tree/master/src/confluent_kafka/schema_registry
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer, ProtobufDeserializer

# https://googleapis.dev/python/protobuf/latest/google/protobuf/message_factory.html
from google.protobuf.message_factory import MessageFactory

import pyspark.sql.functions as fn
from pyspark.sql.types import StringType, BinaryType

# https://github.com/crflynn/pbspark
import pbspark

import os, sys, inspect, importlib

# COMMAND ----------

# DBTITLE 1,PySpark UDF: transform the binary payload into a StringType
binary_to_string = fn.udf(lambda x: str(int.from_bytes(x, byteorder='big')), StringType())

# COMMAND ----------

# DBTITLE 1,Function: return the Python module associated with a version of the protobuf schema
#
# This function returns a Python class for a given version of a schema.
# If the Python class is not detected on the local machine, it is compiled
# using the "protoc" compiler. The protoc binary will exist on the cluster 
# because our "init script" installed it during cluster setup.
#
def get_message_type(version_id, schema_str):
  mod_name = f'destination_{version_id}_pb2'

  tmpdir = "/tmp"
  fname = f"destination_{version_id}.proto"
  fpath = f"{tmpdir}/{fname}"
  mname = f'{fname.replace(".proto", "_pb2")}'

  if (not os.path.exists(f"{mname}.py")):
    f = open(fpath, "w")
    f.write(schema_str)
    f.close()
    retval = os.system(f"protoc -I={tmpdir} --python_out={tmpdir} {fpath}")
    if (retval != 0):
      raise Exception(f"FATAL ERROR: protoc failed with a return value of {retval}")
  if (tmpdir not in sys.path):
    sys.path.insert(0, tmpdir)
  
  pkg = importlib.import_module(mname)  

  return pkg

# COMMAND ----------

# DBTITLE 1,Function: return the Python class for the associated protobuf schema from within the Python module
#
# This function assumes that the protobuf schema is compiled into a 
# Python module with a single class. You will need to adjust this method
# if you have a more complex structure (e.g. nested classes) so that the
# appropriate Python class is returned from this function.
#
def get_proto_class(pkg):
    for name, obj in inspect.getmembers(pkg):
        if inspect.isclass(obj):
            return obj
    return None

# COMMAND ----------

# DBTITLE 1,Function: decode the incoming binary payload and return a Spark-friendly dictionary
#
# The pbspark library has a function to help translate the protobuf DESCRIPTOR into a 
# Python dictionary.
#
def decode_proto(version_id, schema_str, binary_value):
  
  if schema_str == None:
    # We want to fail the stream if an unknown (new) version of the schema is detected. 
    # Delta Live Tables should restart the stream and automatically update the schema.
    raise Exception(f"The stream has encountered a schema change during execution (version {version_id} is new to the stream). A new update using the new schema will be automatically started.")

  mc = pbspark.MessageConverter()
  pkg = get_message_type(version_id, schema_str)
  p = get_proto_class(pkg)
  
  clazz = MessageFactory().GetPrototype(p.DESCRIPTOR)
  deser = ProtobufDeserializer(clazz, {'use.deprecated.format': False})
  if isinstance(binary_value, bytearray):
    binary_value = bytes(binary_value)

  retval = mc.message_to_dict(deser.__call__(binary_value, None))

  return retval

# COMMAND ----------

# DBTITLE 1,Find starting and ending versions for the schema from the Schema Registry
# Establish a connection to the Schema Registry
src = SchemaRegistryClient(schema_registry_conf)

# We assume in this example pipeline that only the value is relevant to the stream, 
# so we use the Schema Registry Client to find the versions of the schema related 
# to the value. This is done by appending "-value" to the function call that looks
# up the schema versions from the Schema Registry. If you are versioning your Kafka
# keys and need to handle schema evolution of the keys, you will need a similar call 
# to the Schema Registry, using "key" instead of "value" 
#
raw_versions = src.get_versions(f"{KAFKA_TOPIC}-value")

# Keep in mind that the Schema Id is not the same as the concept of "version". However,
# we can starting and ending the Schema Ids for the various versions by subtracting.
latest_id = src.get_latest_version(f"{KAFKA_TOPIC}-value").schema_id
starting_id = latest_id - len(raw_versions) + 1
print(f"starting_id: {starting_id}, latest_id: {latest_id}")

# COMMAND ----------

# DBTITLE 1,Register a temp Spark view to help convey the protobuf schema details to the workers consuming the stream
# 
# We assume that the schema of an incoming message may be one of a variety of versions of the schema. 
# This DataFrame (`df_schemas`) is registed as a temp view and is used in a later cell to help
# handle message with various schemas. It will also help with schema evolution.
#
schemas = list()
for version_id in range(starting_id, latest_id + 1):
  schema_str = src.get_schema(version_id).schema_str
  row = {
    "valueSchemaId": version_id,
    "schema_str": schema_str
  }
  schemas.append(row)

df_schemas = spark.createDataFrame(data=schemas, schema="valueSchemaId BIGINT, schema_str STRING")
df_schemas.createOrReplaceTempView("proto_schemas")

# COMMAND ----------

# DBTITLE 1,Register the UDF
# Get the Python module for the newest protobuf schema
pkg = get_message_type((schemas[-1])['valueSchemaId'], (schemas[-1])['schema_str'])

# Extract the Python class related to the protobuf class
p = get_proto_class(pkg)

# Use pbspark to convert the protobuf DESCRIPTOR to a Spark schema
mc = pbspark.MessageConverter()
spark_schema = mc.get_spark_schema(p.DESCRIPTOR)

# Use the Spark schema to register the `decode_proto` function as a Spark UDF. By
# using the latest schema, the target Delta Live Table will evolve to use the latest
# schema accordingly.
decode_proto_udf = fn.udf(lambda x,y,z : decode_proto(x, y, z), spark_schema)
spark.udf.register("decode_proto_udf", f=decode_proto_udf)

# COMMAND ----------

# DBTITLE 1,The source view, consuming the Kafka messages and decoding the Schema Id of the payload
@dlt.view
def kafka_source_table():
  return (
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
    # The `binary_to_string` UDF helps to extract the Schema Id of each payload:
    .withColumn('valueSchemaId', binary_to_string(fn.expr("substring(value, 2, 4)")))
  )

# COMMAND ----------

# DBTITLE 1,Gold table with the transformed protobuf messages, surfaced in a Delta table
@dlt.table
def gold_unified():
  gold_df = spark.sql("""
  
    select a.valueSchemaId,
           b.schema_str,
           -- A new version will result in schema_str being NULL, thanks to the OUTER JOIN
           decode_proto_udf(a.valueSchemaId, b.schema_str, a.value) as decoded
      from STREAM(LIVE.kafka_source_table) a
      LEFT OUTER JOIN proto_schemas b on a.valueSchemaId = b.valueSchemaId
      
  """)
  
  return gold_df.select("decoded.*")
