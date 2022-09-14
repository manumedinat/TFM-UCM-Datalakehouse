# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Using Auto Loader to simplify ingest as the first step in your ETL
# MAGIC 
# MAGIC ### Overview
# MAGIC [Auto Loader](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-gen2.html) is an ingest feature of Databricks that makes it simple to incrementally ingest only new data from Azure Data Lake. In this notebook we will use Auto Loader for a basic ingest use case but there are many features of Auto Loader, like [schema inference and evolution](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-gen2.html#schema-inference-and-evolution), that make it possible to ingest very complex and dynymically changing data.
# MAGIC 
# MAGIC The following example ingests financial data. Estimated Earnings Per Share (EPS) is financial data from analysts predicting what a company’s quarterly earnings per share will be. The raw data can come from many different sources and from multiple analysts for multiple stocks. In this notebook, the data is simply ingested into the bronze table using Auto Loader.
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/jodb/DatabricksAndAzureMapsWorkshop/master/episode4/azureDbIngestEtlArch.png">

# COMMAND ----------

###Establish account key G2
spark.conf.set("fs.azure.account.key.tfmgendos.dfs.core.windows.net","QT/ULR9KopMmUZmz55avGjsJf/cuoAdFd/HUT3qDxs5pLllRhvXwyQEsqn8b1RKn91hv7lTuhb9K+AStoUFp+A==")

basepath="abfss://tfm@tfmgendos.dfs.core.windows.net/"

dbutils.fs.ls(basepath)


# COMMAND ----------

# DBTITLE 1,Set up storage paths
# autoloader table and checkpoint paths
filesBronze=['GDP_Constant_Price_Annual', 'GDP_Current_Price_Annual', 'GDP_PerCapita_Constant_Prices_Annual', 'GDP_PerCapita_Current_Prices_Annual']
#Generar carpetas de autoloader y checkpoints para cada tipo de fichero
for name in filesBronze:      
    exec(f'bronzeTable_{name} = basepath + "bronze/ocde/" + name')
    exec(f'bronzeCheckpoint_{name} = basepath + "bronze/ocde/" + name + "/checkpoint/"')
    exec(f'bronzeSchema_{name} = basepath + "bronze/ocde/" + name + "/schema/"')
    exec(f'landing_{name} = basepath + "landing/ocde/"+ name')



# COMMAND ----------

#dbutils.fs.ls(landing_GDP_Constant_Price_Annual)


# COMMAND ----------

# DBTITLE 1,Remove data from the base path, comment out if you would like to run this
# dbutils.fs.rm(basepath, recurse = True)

# COMMAND ----------

# MAGIC %md 
# MAGIC In the code below we use [Auto Loader](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-gen2.html) to ingest the data into the bronze table and well as [schemaLocation](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html#schema-inference) which allows us to infer the schema instead of having to define it ahead of time.
# MAGIC 
# MAGIC In the writing of the data, we use [trigger.once](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers) and [checkpointing](https://docs.databricks.com/spark/latest/structured-streaming/production.html?_ga=2.205141207.63998281.1636395861-1297538452.1628713993#enable-checkpointing) both of which make Incremental ETL easy which you can read more about in [this blog](https://databricks.com/blog/2021/08/30/how-incremental-etl-makes-life-simpler-with-data-lakes.html).

# COMMAND ----------

# DBTITLE 1,Trigger once stream from autoloader to bronze table
for name in filesBronze:
    exec(f'dfBronze_{name} = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("delimiter",",") \
    .option("header", "true") \
    .option("cloudFiles.schemaLocation",'f'bronzeSchema_{name}) \
    .option("cloudFiles.inferColumnTypes","true") \
    .load('f'landing_{name})')

# COMMAND ----------

from pyspark.sql import functions as F
for name in filesBronze:
   exec(f'dfBronze_{name} = 'f'dfBronze_{name}.select([F.col(col).alias(col.replace(" ", "_")) for col in 'f'dfBronze_{name}.columns])')

# COMMAND ----------

dfBronze_GDP_Constant_Price_Annual.printSchema()

# COMMAND ----------

# The stream will shut itself off when it is finished based on the trigger once feature
# The checkpoint location saves the state of the ingest when it is shut off so we know where to pick up next time
for name in filesBronze:
  exec(f'dfBronze_{name}.writeStream \
    .format("delta") \
    .trigger(once=True) \
    .option("checkpointLocation",'f'bronzeCheckpoint_{name}) \
    .start('f'bronzeTable_{name})')

# COMMAND ----------

for name in filesBronze:
  exec(f'dfBronze_{name}= spark.read.format("delta").load('f'bronzeTable_{name})')

#dfBronze = spark.read.format("delta").load(bronzeTable)

#dfBronze.select('Frequency').distinct().collect()

# COMMAND ----------

display(dfBronze_GDP_Constant_Price_Annual)
