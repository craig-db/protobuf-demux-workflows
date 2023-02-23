# Consuming Protobuf with an Evolving Schema - Single Hop
In this version of the prototype, the protobuf deseriazation (of both the wrapper and inner payload) is done in one notebook/step.

## Overview
This [example repo](https://github.com/craig-db/protobuf-demux-workflows/tree/single-hop) demonstrates a demux pattern where the incoming payload (either from Kafka or Delta) has the following structure:
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
