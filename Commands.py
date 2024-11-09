# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS develop;
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS stage;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS develop.demo_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS stage.defualt;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG develop TO `my-databrick-project-group`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT ALL PRIVILEGES ON DATABASE develop.demo_db TO `my-databrick-project-group`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME develop.demo_db.files;

# COMMAND ----------

# MAGIC %fs ls /Volumes/develop/demo_db/files

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTERNAL LOCATION `external-data`

# COMMAND ----------

# MAGIC %fs ls abfss://databricks-container@akhileshsa2.dfs.core.windows.net/sample_data/

# COMMAND ----------

# MAGIC %fs ls abfss://databricks-container@akhileshsa2.dfs.core.windows.net/sample_data/people.json

# COMMAND ----------

spark.read.json("abfss://databricks-container@akhileshsa2.dfs.core.windows.net/sample_data/people.json").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT READ FILES ON EXTERNAL LOCATION `external-data` TO `my-databrick-project-group`;
