# Databricks notebook source
SR_URL="https://psrc-68gz8.us-east-2.aws.confluent.cloud"
SR_API_KEY="HGXZS5EZYSDN5VVK"
SR_API_SECRET="YXOdcTzlFqNE68Nl5wt6RnmzFSumvzmvCdLSVbVwQ5dSzdZqul9X3/7uVrmspbyL"

KAFKA_KEY="BKN2DJIHJUCVONGE"
KAFKA_SECRET="qCrbJa2kdD+sF3SKtOi1rLy8moybg/vnzX7zd4br9SYe7O2S3MTx5WuPXExGpQnY"
KAFKA_SERVER="pkc-pgq85.us-west-2.aws.confluent.cloud:9092"
KAFKA_TOPIC = "app-events"
WRAPPER_TOPIC = "wrapper"

CHECKPOINT_LOCATION = "dbfs:/tmp/craig_lukasik/demux_cp_location"
