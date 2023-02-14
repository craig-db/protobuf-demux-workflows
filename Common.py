# Databricks notebook source
my_name = spark.sql("select current_user()").collect()[0][0]
my_name = my_name[:my_name.rfind('@')].replace(".", "_")

schema = f"{my_name}_demux_db"
catalog = f"{my_name}_demux_catalog"

print(f"Table location: {catalog}.{schema}")
