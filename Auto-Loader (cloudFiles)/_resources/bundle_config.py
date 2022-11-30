# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "auto-loader",
  "title": "Databricks Autoloader (cloudfile)",
  "description": "Incremental ingestion on your cloud storage folder.",
  "notebooks": [
    {"path": "01-Auto-loader-schema-evolution-Ingestion", "pre_run": True, "publish_on_website": True, "title":  "Databricks Autoloader", "description": "Simplify incremental ingestion with Databricks Autoloader (cloud_file)."},
    {"path": "01-Auto-loader-schema-evolution-Ingestion", "pre_run": True, "publish_on_website": True, "title":  "02 Databricks Autoloader test", "description": "Simplify incremental ingestion with Databricks Autoloader (cloud_file)."}
  ]
}