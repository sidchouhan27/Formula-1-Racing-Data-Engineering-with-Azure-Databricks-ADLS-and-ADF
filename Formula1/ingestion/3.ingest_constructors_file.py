# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read the JSON file using spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Drop unwanted columns from the dataframe using drop command

# COMMAND ----------

constructors_dropped_df = constructors_df.drop(constructors_df["url"])

# COMMAND ----------

from pyspark.sql.functions import lit #, current_timestamp

# COMMAND ----------

constructors_final_df_1 = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                               .withColumnRenamed("constructorRef", "constructor_ref") \
                                               .withColumn("data_source", lit(v_data_source))\
                                               .withColumn("file_date", lit(v_file_date))
constructors_final_df = add_ingestion_date(constructors_final_df_1)

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write output to parquet file

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")