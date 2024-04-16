# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.formula1dl627.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dl627.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dl627.dfs.core.windows.net", "sv=2023-01-03&st=2024-03-01T21%3A16%3A47Z&se=2024-03-02T21%3A16%3A47Z&sr=c&sp=rl&sig=Dka64GYHdY51oxA2RJF7XRJjXABUfWCCctAKTMCHbh8%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl627.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl627.dfs.core.windows.net/circuits.csv"))