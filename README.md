# Consuming Protobuf with an Evolving Schema 

<img src="https://github.com/craig-db/protobuf-demux-workflows/raw/main/resources/overview.png" width=1200 alt="Overview Databricks Lakehouse Architecture depicting Kafka, Schema Registry, Delta Lake, and Workflows" /><br>
_Descriptions for the numbered, yellow circles are found in the [appendix](https://github.com/craig-db/protobuf-demux-workflows/blob/main/README.md#appendix)_

## Overview
This [example repo](https://github.com/craig-db/protobuf-demux-workflows) demonstrates a demux pattern where the incoming payload (either from Kafka or Delta) has the following structure:
1. A "wrapper" protobuf message consisting of the following fields: game_name, schema_id, payload
2. Within the wrapper's payload, a game-specific payload

### Wrapper Protobuf
```
message wrapper {
  optional string game_name = 1;
  optional int32 schema_id = 2;
  optional bytes payload = 3;
} 
```

### Inner Payload Protobuf
```
syntax = "proto3";

import "google/protobuf/timestamp.proto";

message event {
  string game_name = 1;
  string user_name = 2;
  google.protobuf.Timestamp event_timestamp = 3;
  string is_connection_stable = 4;
  string col_<game_name>_game_0 = 5;
  string col_<game_name>_game_1 = 6;
  < more fields as the schema evolves >
}
```
The payload protobufs have their own schema with an evolving set of columns, e.g. "col_craig_game_0". The purpose of this is to demonstrate that the payload may:
1. be of different schemas
2. be comprised of varying versions of the schema


## Instructions
1. Set the Kafka and Schema Registry parameters in the "Secrets" notebook. See the "Important" note below in order to follow best practices.
2. Run the <a target=_blank href="https://github.com/craig-db/protobuf-demux-workflows/blob/main/Producer.py">Producer</a> notebook. You can choose if you want to write to Delta or to Kafka. Remember this for step 3. There are other parameters that you can set too (e.g. how many "games" to simulate; each game will get its own schema). The Producer notebook produces the simulated payload that will stream.
3. Run the <a target=_blank href="https://github.com/craig-db/protobuf-demux-workflows/blob/main/Install_Workflow.py">Install_Workflow</a> notebook after the Producer notebook has completed its first run. This will install and start a workflow named `Game_Protobufs_wrapper_<your name>`. Note: the initial run may result in failures (if reading from Kafka). This is because the bronze table may not yet exist. The Workflow's tasks will retry, however, and then the tasks should proceed.
5. In the Producer notebook, you can re-run the cell with the title "Send simulated payload messages to Kafka" to send more messages. Do not re-run the entire notebook.
6. Optional: explore the "Unity Catalog Grants" notebook. You will need another user to which you can explore granting SELECT permission to one of the game tables.

### Clean up
1. Stop and delete the Workflow
2. In the Producer notebook, choose "Yes" in the widget labeled "clean_up". Run each cell, one-by-one until you get to the cell with the message "Proceed if you wish to create topics, schemas and publish messages...". Do not proceed (unless you want the simulation to start over).

### Important: Secrets in this example are not using "best practices"!
This prototype has hard-coded "secrets" in a notebook. Please do not do this! You should, instead, follow the best practice of using Databricks secrets:
1. Set secrets using the [Databricks Secrets approach](https://docs.databricks.com/security/secrets/index.html)
2. Update the Secrets notebook in this example to retrieve the secrets using the recommended method

# Appendix
1. A Schema Registry, such as [Confluent's Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html), provides a metadata service for streaming message services, such as Confluent's implementation of [Apache Kafka](https://kafka.apache.org/). For protobuf messages, where the payload that streams does not include a schema definition, a Schema Registry provides a way for message producers and consumers to be aware of the message fields. Confluent's Schema Registry supports schema evolution; each schema version has its own Schema Id.
2. The program(s) that publish messages to the message bus leverage the Registry Schema, along with a Serializer, to prepare messages for the message bus. For instance, Confluent's [ProtobufSerializer](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-protobuf.html) can be leveraged on the producer side. In this demo, take a look at the <a target=_blank href="https://github.com/craig-db/protobuf-demux-workflows/blob/main/Producer.py">Producer</a> notebook to see how simulated messages are generated and published to Kafka (we use Databrick's impementation of `to_protobuf`). For this prototype, we assume that a wrapper protobuf encapsulates one of many different "game-specific" payloads that are serialized protobuf messages too. In other words, protobuf wrapped in an outer protobuf.
3. In Apache Spark version 3.4.0, "Protobuf Support" was added (see [SPARK-40653](https://issues.apache.org/jira/browse/SPARK-40653)). Databricks Runtime augments the Open Source capabilities by adding Schema Registry support, as documented [here](https://docs.databricks.com/structured-streaming/protocol-buffers.html). 
4. Apache Kafka is a messaging service that can be run on your own hardware. However, cloud-based, fully managed options exist for running Kafka, such as Confluent Cloud and [Amazon MSK](https://aws.amazon.com/msk/). This demo uses Confluent's fully managed Kafka implementation.
5. Databricks [Workflows](https://docs.databricks.com/workflows/index.html) makes it easy to build and manage reliable data pipelines that deliver high-quality data on Delta Lake. Databricks manages the task orchestration, cluster management, monitoring, and error reporting for all of your jobs. You can run your jobs immediately or periodically through an easy-to-use scheduling system. You can implement job tasks using notebooks, JARS, Delta Live Tables pipelines, or Python, Scala, Spark submit, and Java applications. Workflows can be "installed" (a) manually through the [Databricks platflorm "Jobs UI"](https://docs.databricks.com/workflows/jobs/jobs.html), (b) via the [Databricks Jobs API](https://docs.databricks.com/dev-tools/api/latest/jobs.html), (c) via the [Databricks Terraform provider](https://registry.terraform.io/providers/databricks/databricks/latest/docs/data-sources/jobs), or (d) via the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/jobs-cli.html).
6. [Delta Lake](https://delta.io/) is an open-source storage framework that enables building a [Lakehouse architecture](https://www.databricks.com/product/data-lakehouse). Databricks recommends organizing tables using the [Medallion Lakehouse Architecture](https://docs.databricks.com/lakehouse/medallion.html). Think of the "gold" layer tables as the "high quality", consumable tables. In this prototype, the initial Kafka stream is processed by the [Workflow-wrapper](./Workflow-wrapper.py) notebook. In step 7 (below), we will describe how this notebook is leveraged by the Databricks job and how that job is "installed" using the Databricks API. Workflow-wrapper is responsible for demux'ing a single stream/topic into a number of target tables. To maximize parallelism, we minimally process this initial stream (i.e. we extract the inner payload protobuf and land the payload into a partitioned "bronze" table). By partitioning the bronze table, we can then have subsequent workloads processing game-specific payloads in a scalable fashion. Note: this prototype works in two modes: Kafka source or Delta source. Databricks has built-in support for treating [Delta as a streaming source (and sink)](https://docs.databricks.com/structured-streaming/delta-lake.html), and this capability can be handy for testing purposes or for helping decompose development tasks, allowing developers to focus on the business logic (as opposed to worrying about Kafka setup and permissions as they develop stream-based workflows).
7. Take a look at the [Install_Workflow](./Install_Workflow.py) code now. You will see that, in the JSON that will be used with the Databricks API to install the workflow, the game-specific notebook (<a target=_blank href="https://github.com/craig-db/protobuf-demux-workflows/blob/main/Workflow-wrapper.py">Workflow-wrapper</a>) is invoked in a parameterized manner (see the "base_parameters" field). Look at <a target=_blank href="https://github.com/craig-db/protobuf-demux-workflows/blob/main/Workflow-deserializer.py">Workflow-deserializer</a> and you can see how [Databricks widgets](https://docs.databricks.com/notebooks/widgets.html) help the developer decompose the development and testing while also providing a mechanism to parameterize the notebook. You will also see how each parameterized instance of the workflow is assigned to one of a set of clusters. The Install_Workflow notebook currently uses single-node clusters; for production, you would likely want to change the instance-type details (in the JSON). Install_Workflow leverages the Databricks API to install the workflow after it composes the JSON payload that assigns each game-specific instance of the Workflow-deserializer to one of a small set of clusters. In practice, you would likely want to experiment to find the best number of streams to run on one cluster (typically, 10-30); the reason being that each stream exerts some pressure on the Spark Driver process.
8. A common misconception with Spark Structured Streaming is that it requires an always-on, continuously streaming flow. For some workloads, this makes sense. For other workloads, Structured Streaming can be used to process incremental data periodically (e.g. via scheduled job). For instance, the [AvailableNow trigger](https://docs.databricks.com/structured-streaming/triggers.html#configuring-incremental-batch-processing) allows a streaming job to run, process newly arrived data, and then stop. This flexibility helps minimize compute costs and is perfect for workloads where near real-time latency is not a key requirement.
9. Tasks within a Databricks Workflow can be structed into a DAG by setting dependencies within the job tasks. That is not done in this prototype. Other Workflow features that are demonstrated in this prototype are: automatic retries for tasks and task failure email notifications. 
10. You can repair and re-run a failed or canceled job using the UI or API. You can monitor job run results using the UI, CLI, API, and notifications (for example, [email, webhook destination, or Slack notifications](https://docs.databricks.com/administration-guide/workspace/notification-destinations.html)). 
11. As hinted earlier, the tasks within a Workflow can be configured to share a compute cluster or the tasks can be configured to run on their own cluster. Beyond that, the cluster configurations can also leveage cluster [pools](https://docs.databricks.com/clusters/instance-pools/index.html) and [policies](https://docs.databricks.com/administration-guide/clusters/policies.html) (not showcased in this prototype).
12. A lot of the code in this repo revolves around the Producer (that simulates generating the messages published to Kafka). The actual business logic for processing the incoming payload (<a target=_blank href="https://github.com/craig-db/protobuf-demux-workflows/blob/main/Workflow-wrapper.py">Workflow-wrapper</a>) demonstrates the elegance and cleanliness of the Spark API. Likewise, the source code that helps demux the main stream into the target Delta tables (<a target=_blank href="https://github.com/craig-db/protobuf-demux-workflows/blob/main/Workflow-deserializer.py">Workflow-deserializer</a>) exhibits how the separation of architectural concerns (compute, workflow, retries, alerting, etc.) within the Databricks platform allows developers to focus on business logic.
13. As discussed earlier, cluster configuration for the workflow tasks is configured so as to maximize compute and network utilization.
14. In addition to Databricks SQL capabilities (mentioned in #13), [Databricks SQL Warehouses](https://docs.databricks.com/sql/admin/sql-endpoints.html) allow other client applications (such as Tableau or Looker) to leverage Delta tables to drive analytics. You can use the <a target=_blank href="https://github.com/craig-db/protobuf-demux-workflows/blob/main/Unity Catalog Grants.py">Unity Catalog Grants</a> notebook to start to explore how [Unity Catalog provides governance](https://docs.databricks.com/data-governance/unity-catalog/index.html) across all analtyics personas (from Data Engineer to Data Scientist to Business Analyst). With Unity Catalog, Databricks also provides visibility into the [lineage of tables and columns](https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html) and how those assets are related to notebooks, etc. 
15. [Data Scientists have full MLOps and model serving capabilities within the Databricks platform](https://www.databricks.com/solutions/data-science). For example, [models can be updated and/or deployed](https://docs.databricks.com/machine-learning/model-inference/index.html) within a Workflow. And Unity Catalog's [search](https://www.databricks.com/blog/2022/06/28/whats-new-with-databricks-unity-catalog-at-the-data-ai-summit-2022.html) and lineage capabilities help ease the data discovery process for data scientists as well as business analysts and data engineers.
16. Workflows can be used in conjunction with other Databricks features to power applications. For example, a Workflow can populate a table that has alerts set on it, and those alerts can [trigger webhooks or PagerDuty alerts](https://docs.databricks.com/sql/admin/alert-destinations.html).