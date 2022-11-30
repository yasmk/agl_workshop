# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data");

# COMMAND ----------

from time import sleep
from _resources.global_setup_bundle import DBDemos
dbName, cloud_storage_path = DBDemos.init(spark, db_prefix="demo_product", reset_all=dbutils.widgets.get("reset_all_data"))
raw_data_location = DBDemos.load_data(spark, cloud_storage_path+"/auto_loader", dbutils.widgets.get("reset_all_data"))

#cleanup schema in all cases
dbutils.fs.rm(raw_data_location+'/inferred_schema', True)

# COMMAND ----------

# DBTITLE 1,Helper functions to wait between streams to have a nice execution & results clicking on "run all"
#Wait to have data to be available in the _rescued_data column.
def wait_for_rescued_data():
  i = 0
  while DBDemos.is_folder_empty(dbutils, raw_data_location+'/_wait_rescued/data/_delta_log/') or spark.read.load(raw_data_location+'/_wait_rescued/data').count() == 0:
    get_stream().filter("_rescued_data is not null") \
               .writeStream.option("checkpointLocation", raw_data_location+'/_wait_rescued/ckpt') \
               .trigger(once=True).start(raw_data_location+'/_wait_rescued/data').awaitTermination()
    i+=1
    sleep(1)
    if i > 30:
      raise Exception("Can't capture the new column. Please make sure the stream on the previous cell is running.")


# COMMAND ----------

