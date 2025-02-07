# Databricks notebook source
account_key = dbutils.secrets.get(scope="formala1-scope", key="account-key")
spark.conf.set("fs.azure.account.key.formala1datalake.dfs.core.windows.net", account_key)

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")
print(v_file_date)

# COMMAND ----------

races_df = spark.read.csv(f"abfss://raw@formala1datalake.dfs.core.windows.net/{v_file_date}/races.csv", header=True,inferSchema=True)
display(races_df)

# COMMAND ----------

from pyspark.sql.functions import lit
races_renamed_df = races_df.withColumnRenamed("raceId", "race_id").withColumnRenamed("year", "race_year").withColumnRenamed("round", "round").withColumnRenamed("circuitId", "circuit_id").withColumn("file_date",lit(v_file_date))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
races_renamed_df = races_renamed_df.withColumn("ingestion_date",current_timestamp())
display(races_renamed_df)

# COMMAND ----------

races_renamed_df = races_renamed_df.drop("url")
display(races_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,concat,col
races_renamed_df = races_renamed_df.withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")),"yyyy-MM-dd HH:mm:ss"))
display(races_renamed_df)

# COMMAND ----------

races_final_df = races_renamed_df.drop("date").drop("time")

# COMMAND ----------

races_final_df.write.mode("overwrite").format("delta").partitionBy("race_year").saveAsTable("f1_processed.races")

# COMMAND ----------

#display(spark.read.parquet("abfss://processed@formala1datalake.dfs.core.windows.net/races"))

# COMMAND ----------


