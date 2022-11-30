# Databricks notebook source
# MAGIC %md
# MAGIC ## I Suggest to have the autoloader section as a task (in databricks workflow) that's retried if it fails. If the schema of the files is changed autolader fails. When the task is retied it will pick up the new schema. You can also use the exception rasied to identify that there's been change in the schema to discuss with the Dynamics team (but you should ingest the data regardless)

# COMMAND ----------

spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "json") \
  .option("cloudFiles.schemaLocation", parent_dir) \
  .option("inferColumnTypes", True) \
  .load(data_dir) \
  .writeStream \
  .option("mergeSchema", True) \
  .option("checkpointLocation", checkpoint_dir) \
  .start(delta_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create read stream on the bronze table
# MAGIC Stream only picks up new rows added to the bronze table 
# MAGIC this is assuming
# MAGIC * bronze is append only and rows are not updated (if there are updates extra options are needed or need to use CDF)
# MAGIC *  bronze doesn't have duplicates

# COMMAND ----------

bronze_readings_stream = spark \
                   .readStream \
                   .format("delta") \
                   .table("mydatabase.mytable_bronze")

# COMMAND ----------

bronze_readings_stream.createOrReplaceTempView("bronze_readings_streaming")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Perform ETL 

# COMMAND ----------

# MAGIC %md
# MAGIC Sample backfill data

# COMMAND ----------

dataPath = f"{dbfs_data_path}backfill_sensor_data_final.csv"

back_fill_df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(dataPath)


df.createOrReplaceTempView("backfill_view")

# COMMAND ----------

# MAGIC %md
# MAGIC sample merge operation
# MAGIC you can run other SQL statements as well, like filtering or adding new columns, ... you can laso use dataframe operations instead to generate silver data

# COMMAND ----------

silver_out_stream = spark.sql("""MERGE INTO bronze_readings_streaming AS SL
USING backfill_view AS BF
ON 
  SL.id = BF.id
WHEN MATCHED THEN 
UPDATE SET *
WHEN NOT MATCHED THEN 
INSERT *""")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## write results to the silver table
# MAGIC 
# MAGIC ## make sure you specify the location for the table

# COMMAND ----------

silver_table_path = "s3://s3_bucket/silver/mytable"

silver_out_stream.writeStream \
          .outputMode("append") \
          .option('location', silver_table_path)  # this is important as if you don't specify this data is stored in root s3 bucket (manged table) and you should avoid this
          .option("checkpointLocation", silver_checkpoint_path) \
          .table("mydatabase.mytable_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create read stream on the silver table
# MAGIC Stream only picks up new rows added to the silver table 
# MAGIC this is assuming
# MAGIC * Silver is append only and rows are not updated (if there are updates extra options are needed or need to use CDF)
# MAGIC *  Silver doesn't have duplicates

# COMMAND ----------

silver_readings_stream = spark \
                   .readStream \
                   .format("delta") \
                   .table("mydatabase.mytable_silver")

silver_readings_stream.createOrReplaceTempView("readings_streaming")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Perform aggregates

# COMMAND ----------

gold_out_stream = spark.sql("""SELECT 
  window,
  device_type, 
  avg(reading_1) AS reading_1, 
  count(reading_1) AS count_reading_1 
FROM readings_streaming 
GROUP BY 
  window(reading_time, '2 minutes', '1 minute'), 
  device_type 
ORDER BY 
  window DESC, 
  device_type ASC""")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## write results to the gold table
# MAGIC 
# MAGIC ## make sure you specify the location for the table

# COMMAND ----------

gold_table_path = "s3://s3_bucket/gold/mytable"

gold_out_stream.writeStream \
          .outputMode("append") \
          .option('location', gold_table_path)  # this is important as if you don't specify this data is stored in root s3 bucket (manged table) and you should avoid this
          .option("checkpointLocation", gold_checkpoint_path) \
          .table("mydatabase.mytable_gold")
