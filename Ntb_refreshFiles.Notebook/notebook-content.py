# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c39886bf-6f06-4e24-9b4f-40f4514d1b51",
# META       "default_lakehouse_name": "LH_MiddleEarth",
# META       "default_lakehouse_workspace_id": "0a50491e-5d61-4438-ad2e-a66f93ec6299",
# META       "known_lakehouses": [
# META         {
# META           "id": "c39886bf-6f06-4e24-9b4f-40f4514d1b51"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

def refreshFromCsv(csv_path, table_path, delimiter = ';', kingdom = ''):
    
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType
    import time

    # Define the wait time in seconds
    wait_time_seconds = 5

    # Read the CSV
    df = (spark.read.format("csv")
        .option("header", "true")  # first row is header
        .option("inferSchema", "true")   # automatically detect data types
        .option("delimiter", delimiter)
        .load(csv_path))
                       

    if(kingdom!=''):        
        # Replace the TradeValue column with its integer-casted version
        df = df.withColumn("TradeValue", F.col("TradeValue").cast(IntegerType()))

        # Handle potential non-numeric values by replacing them with null
        df = df.withColumn("TradeValue",
                        F.when(F.col("TradeValue").isNotNull(), F.col("TradeValue"))
                        .otherwise(None))
        df = df.withColumn('MiddleEarthGroup', F.lit(kingdom))
        
    # Write the DataFrame to a Delta table only if it is not empty
    print(df.count())
    if df.count() > 0:
        df.write.format("delta").option("mergeSchema", "true").mode("append").save(table_path)
    else:
        print(f"No data to write for kingdom: {kingdom}")
    
    time.sleep(wait_time_seconds)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # refresh garrison places

# CELL ********************

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sql(f"Truncate table LH_MiddleEarth.garrisonplaces")
    
garrisonplaces_path = "abfss://0a50491e-5d61-4438-ad2e-a66f93ec6299@onelake.dfs.fabric.microsoft.com/c39886bf-6f06-4e24-9b4f-40f4514d1b51/Files/GarrisonPlaces.csv"
table = "abfss://0a50491e-5d61-4438-ad2e-a66f93ec6299@onelake.dfs.fabric.microsoft.com/c39886bf-6f06-4e24-9b4f-40f4514d1b51/Tables/garrisonplaces"

(
    refreshFromCsv
    (
        csv_path= garrisonplaces_path,
        table_path= table
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # refresh names

# CELL ********************


spark = SparkSession.builder.getOrCreate()
spark.sql(f"Truncate table LH_MiddleEarth.middleearthnames")
    
garrisonplaces_path = "abfss://0a50491e-5d61-4438-ad2e-a66f93ec6299@onelake.dfs.fabric.microsoft.com/c39886bf-6f06-4e24-9b4f-40f4514d1b51/Files/MiddleEarthNames.csv"
table = "abfss://0a50491e-5d61-4438-ad2e-a66f93ec6299@onelake.dfs.fabric.microsoft.com/c39886bf-6f06-4e24-9b4f-40f4514d1b51/Tables/middleearthnames"

(
    refreshFromCsv
    (
        csv_path= garrisonplaces_path,
        table_path= table
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # refresh raiding targets

# CELL ********************


spark = SparkSession.builder.getOrCreate()
spark.sql(f"Truncate table LH_MiddleEarth.raidingtargets")
    
garrisonplaces_path = "abfss://0a50491e-5d61-4438-ad2e-a66f93ec6299@onelake.dfs.fabric.microsoft.com/c39886bf-6f06-4e24-9b4f-40f4514d1b51/Files/RaidingTargets.csv"
table = "abfss://0a50491e-5d61-4438-ad2e-a66f93ec6299@onelake.dfs.fabric.microsoft.com/c39886bf-6f06-4e24-9b4f-40f4514d1b51/Tables/raidingtargets"

(
    refreshFromCsv
    (
        csv_path= garrisonplaces_path,
        table_path= table
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # refresh items

# CELL ********************

def getpath(kingdom):
    if(kingdom=='Dwarves'):
        return "abfss://0a50491e-5d61-4438-ad2e-a66f93ec6299@onelake.dfs.fabric.microsoft.com/c39886bf-6f06-4e24-9b4f-40f4514d1b51/Files/Dwarf_Items_By_Category.csv"
    elif(kingdom=='Elves'):
        return "abfss://0a50491e-5d61-4438-ad2e-a66f93ec6299@onelake.dfs.fabric.microsoft.com/c39886bf-6f06-4e24-9b4f-40f4514d1b51/Files/Elf_Items_By_Category.csv"
    elif(kingdom=='Humans'):
        return "abfss://0a50491e-5d61-4438-ad2e-a66f93ec6299@onelake.dfs.fabric.microsoft.com/c39886bf-6f06-4e24-9b4f-40f4514d1b51/Files/HumanItems.csv"
    elif(kingdom=='Isengard'):
        return "abfss://0a50491e-5d61-4438-ad2e-a66f93ec6299@onelake.dfs.fabric.microsoft.com/c39886bf-6f06-4e24-9b4f-40f4514d1b51/Files/Isengard_Uruk_Items_By_Category.csv"
    elif(kingdom=='Mordor'):
        return "abfss://0a50491e-5d61-4438-ad2e-a66f93ec6299@onelake.dfs.fabric.microsoft.com/c39886bf-6f06-4e24-9b4f-40f4514d1b51/Files/Mordor_Orc_Items_By_Category.csv"
    elif(kingdom=='Rohan'):
        return "abfss://0a50491e-5d61-4438-ad2e-a66f93ec6299@onelake.dfs.fabric.microsoft.com/c39886bf-6f06-4e24-9b4f-40f4514d1b51/Files/Rohan_Items_By_Category.csv"
    elif(kingdom=='Gondor'):
        return "abfss://0a50491e-5d61-4438-ad2e-a66f93ec6299@onelake.dfs.fabric.microsoft.com/c39886bf-6f06-4e24-9b4f-40f4514d1b51/Files/Gondor_Items_By_Category.csv"
    elif(kingdom=='Hobbits'):
        return "abfss://0a50491e-5d61-4438-ad2e-a66f93ec6299@onelake.dfs.fabric.microsoft.com/c39886bf-6f06-4e24-9b4f-40f4514d1b51/Files/Hobbit_Items_By_Category.csv"
    elif(kingdom=='Misty Mountain Goblins'):
        return "abfss://0a50491e-5d61-4438-ad2e-a66f93ec6299@onelake.dfs.fabric.microsoft.com/c39886bf-6f06-4e24-9b4f-40f4514d1b51/Files/MistyMountainGoblins_Items_By_Category.csv"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark = SparkSession.builder.getOrCreate()
spark.sql(f"Truncate table LH_MiddleEarth.me_itemslist")
table = "abfss://0a50491e-5d61-4438-ad2e-a66f93ec6299@onelake.dfs.fabric.microsoft.com/c39886bf-6f06-4e24-9b4f-40f4514d1b51/Tables/me_itemslist"

Kingdoms = ['Dwarves', 'Elves', 'Humans', 'Isengard', 'Mordor', 'Rohan', 'Gondor', 'Hobbits', 'Misty Mountain Goblins']

for kingdom in Kingdoms:
    csv_path = getpath(kingdom)
    try:
        (
            refreshFromCsv
            (
                csv_path= csv_path,
                table_path= table,
                kingdom=kingdom
            )
        )
        print(f"{kingdom} done")
    except Exception as e:
        print(f"{kingdom} failed witch exception: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
