# Databricks notebook source
# MAGIC %run "./Common"

# COMMAND ----------

# DBTITLE 1,Unity Catalog restricted user (edit, as needed)
restricted_user = spark.sql("select current_user()").collect()[0][0].replace("@", "+ucc-user@")
print(f"Login with this user to see restricted data: {restricted_user}")

# COMMAND ----------

# DBTITLE 1,Remove all
spark.sql(f"REVOKE ALL PRIVILEGES ON SCHEMA {catalog}.{schema} FROM `{restricted_user}`")

# COMMAND ----------

# DBTITLE 1,Allow Usage
spark.sql(f"GRANT USAGE ON CATALOG {catalog} TO `{restricted_user}`")

# COMMAND ----------

spark.sql(f"GRANT USAGE ON SCHEMA {catalog}.{schema} TO `{restricted_user}`")

# COMMAND ----------

spark.sql(f"GRANT SELECT ON TABLE {catalog}.{schema}.silver_christopher_game_wf TO `{restricted_user}`")

# COMMAND ----------


