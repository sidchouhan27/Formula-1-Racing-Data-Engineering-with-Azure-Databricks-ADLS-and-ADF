# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read.option("header", True).option("multiLine", True).schema(pit_stops_schema).json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import lit #, current_timestamp

# COMMAND ----------

pit_stops_final_df_1 = pit_stops_df.withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))
pit_stops_final_df = add_ingestion_date(pit_stops_final_df_1)

# COMMAND ----------

# pit_stops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")
# overwrite_partition(pit_stops_final_df, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(pit_stops_final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")