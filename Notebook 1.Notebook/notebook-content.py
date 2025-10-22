# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

Rohan = "abfss://0a50491e-5d61-4438-ad2e-a66f93ec6299@onelake.dfs.fabric.microsoft.com/854e83d0-ef52-4121-ad8e-ea5cc0a6ac6e"
MistyMountainGoblins = "abfss://0a50491e-5d61-4438-ad2e-a66f93ec6299@onelake.dfs.fabric.microsoft.com/e38b57ef-9f97-4fd6-bfff-5f7abcf1ace1"
Dwarves = "abfss://0a50491e-5d61-4438-ad2e-a66f93ec6299@onelake.dfs.fabric.microsoft.com/ba5451ba-9b2e-46a2-b4c4-e7cf8fdfa72b"
Elves = "abfss://0a50491e-5d61-4438-ad2e-a66f93ec6299@onelake.dfs.fabric.microsoft.com/f0e4b9cd-52b1-4757-b291-b0eb4f2316b2"
Isengard = "abfss://0a50491e-5d61-4438-ad2e-a66f93ec6299@onelake.dfs.fabric.microsoft.com/5180c8cf-6d2c-4a45-8275-ddbee558c392"
Gondor = "abfss://0a50491e-5d61-4438-ad2e-a66f93ec6299@onelake.dfs.fabric.microsoft.com/59a64123-fe1b-4d56-a8b0-d71fdea2b76b"

lakehouse = MistyMountainGoblins
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import time

spark = SparkSession.builder.getOrCreate()

# Access Hadoop FileSystem through Spark
hadoop_conf = spark._jsc.hadoopConfiguration()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
path = spark._jvm.org.apache.hadoop.fs.Path(f"{lakehouse}/Files")
print(path)
# List files in the top-level folder
status = fs.listStatus(path)

for fileStatus in status:
    filepath = (fileStatus.getPath().toString())
    #print(filepath)
    relative_path = filepath.split("/Files/")[1]
    print(relative_path)
    table = f"{lakehouse}/Tables/{relative_path}"
    #print(table)
    inhoud = spark.read.parquet(filepath)
    #display(inhoud)
    

    # Define the wait time in seconds
    wait_time_seconds = 5

    inhoud.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
