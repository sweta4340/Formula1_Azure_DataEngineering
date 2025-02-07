# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read the csv file with dataframe reader API.

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks provides widgets to create interactive controls in notebooks. Widgets are a way to dynamically parameterize and control the behavior of notebooks, making them highly customizable and interactive for data workflows.
# MAGIC
# MAGIC Types of Widgets
# MAGIC 1. Text Widget: Allows the user to input arbitrary text.
# MAGIC 2. Dropdown Widget: Provides a list of options for selection.
# MAGIC 3. Combobox Widget: A combination of a text widget and a dropdown widget.
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")
print(v_data_source)

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")
print(v_file_date)

# COMMAND ----------

# MAGIC %md
# MAGIC The %run magic command in Databricks is used to include or run another notebook within your current notebook. This is particularly useful for modularizing your code, such as including configuration settings, utility functions, or reusable code blocks.

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

print(raw_path)

# COMMAND ----------

account_key = dbutils.secrets.get(scope = "formala1-scope",key = "account-key") 
spark.conf.set("fs.azure.account.key.formala1datalake.dfs.core.windows.net", account_key)

# COMMAND ----------

circuits_df = spark.read.csv(f"{raw_path}/{v_file_date}/circuits.csv", header=True,inferSchema=True)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.show(5)

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# COMMAND ----------

circuits_schema = StructType([StructField("circuitId", IntegerType(), False), StructField("circuitRef", StringType(), True), StructField("name", StringType(), True), StructField("location", StringType(), True), StructField("country", StringType(), True), StructField("lat", DoubleType(), True), StructField("lng", DoubleType(), True), StructField("alt", IntegerType(), False), StructField("url", StringType(), True)])

# COMMAND ----------

circuits_df = spark.read.csv("abfss://demo@formala1datalake.dfs.core.windows.net/circuits.csv", header=True, schema=circuits_schema)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Selecting the specific columns
# MAGIC

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt") # this will only select the columns. Rest of the three methods will aalow us to functions on columns.

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df['circuitId'], circuits_df['circuitRef'], circuits_df['name'], circuits_df['location'], circuits_df['country'].alias("race_country"), circuits_df['lat'], circuits_df['lng'], circuits_df['alt'])

# COMMAND ----------

from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Renaming of the columns

# COMMAND ----------

# MAGIC %md
# MAGIC Passing the parameter to the DF using widget.

# COMMAND ----------

from pyspark.sql.functions import lit
circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id",).withColumnRenamed("circuitRef", "circuit_ref").withColumnRenamed("lat", "latitude").withColumnRenamed("lng", "longitude").withColumnRenamed("alt", "altitude").withColumnRenamed("race_country", "country").withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# Adding the new column
#from pyspark.sql.functions import current_timestamp, lit
#circuits_final_df = circuits_renamed_df.withColumn("ingestion_date",current_timestamp()) # column has to be object in order to be added.
circuits_final_df = add_ingestion_date(circuits_renamed_df).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC 4. Writing the data

# COMMAND ----------

circuits_final_df.write.mode("append").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

df = spark.read.format("delta").load(f"{processed_path}/circuits")

# COMMAND ----------

display(df)

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
