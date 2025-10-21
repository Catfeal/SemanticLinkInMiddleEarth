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

import random
from enum import Enum
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # enums

# CELL ********************

class RaidSuccesRate(Enum):
    TotalLoss = (0, 100)
    ClearDefeat = (0, 50)
    Defeat = (1, 60)
    Draw = (2, 40)
    Win = (5, 20)
    clearVictory = (8, 10)
    TotalWin = (12, 0)


    def __init__(self, loot_items, death_rate):
        self.loot_items = loot_items
        self.death_rate = death_rate

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class ItemCategory(Enum):
    Weapons = 'Weapons'
    Armour = 'Armour'
    FoodAndDrinks = 'Food and Drinks'
    Jewelry = 'Jewelry'
    Tools = 'Tools'
    RelicsAndHeirlooms = 'Relics & Heirlooms'
    
    def __init__(self, display_name):
        self.display_name = display_name

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class RacesAndKingdoms(Enum):
    Hobbits = 'Hobbits'
    Isengard = 'Isengard'
    Mordor = 'Mordor'
    GoblinsMistyMountains = 'Misty Mountain Goblins'
    Rohan = 'Rohan'
    Gondor = 'Gondor'
    Elves = 'Elves'
    Dwarves = 'Dwarves'
    
    def __init__(self, display_name):
        self.display_name = display_name

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class itemDistribution(Enum):
    Hobbits = (1, 2, 5, 3, 4, 1)
    Isengard =(3, 3, 2, 1, 1, 1)
    Mordor = (3, 2, 1, 0, 1, 1)
    GoblinsMistyMountains = (2, 1, 1, 3, 1, 1)
    Rohan = (4, 5, 4, 4, 3, 2)
    Gondor = (5, 6, 5, 3, 2, 3)
    Elves = (3, 3, 2, 3, 6, 6)
    Dwarves = (7, 6, 7, 7, 3, 4)
# This enum gives the distribution of items for each group.
# to know the order, you can always look at the enum ItemCategory
    # Weapons 
    # Armour 
    # FoodAndDrinks 
    # Jewelry 
    # Tools
    # RelicsAndHeirlooms
    def __init__(self, weapons, armour, foodanddrinks, jewelry, tools, relicsandheirlooms):
        self.weapons = weapons
        self.armour = armour
        self.foodanddrinks = foodanddrinks
        self.jewelry = jewelry
        self.tools = tools
        self.relicsandheirlooms = relicsandheirlooms

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Random functions

# CELL ********************

def randomTarget_Evil():
    n = randomiser(1,4)
    if(n == 1):
        return RacesAndKingdoms.Elves
    if(n == 2):
        return RacesAndKingdoms.Dwarves
    if(n == 3):
        return RacesAndKingdoms.Rohan
    if(n == 4):
        return RacesAndKingdoms.Gondor

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def randomTarget_Good():
    n = randomiser(1,3)
    if(n == 1):
        return RacesAndKingdoms.Mordor
    if(n == 2):
        return RacesAndKingdoms.Isengard
    if(n == 3):
        return RacesAndKingdoms.GoblinsMistyMountains

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def randomSucces():    
    n = randomiser(1,7)
    if(n == 1):
        return RaidSuccesRate.TotalLoss
    if(n == 2):
        return RaidSuccesRate.ClearDefeat
    if(n == 3):
        return RaidSuccesRate.Defeat
    if(n == 4):
        return RaidSuccesRate.Draw
    if(n == 5):
        return RaidSuccesRate.Win
    if(n == 6):
        return RaidSuccesRate.clearVictory
    if(n == 7):
        return RaidSuccesRate.TotalWin
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def randomiser(minnumber = 1, maxnumber=100):
    import random
    return random.randint(minnumber, maxnumber)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def randomitemvalue(raritylevel='Common'):
    if(raritylevel=='Common'):
        return randomiser(1,13)
    if(raritylevel=='Uncommon'):
        return randomiser(10,34)
    if(raritylevel=='Rare'):
        return randomiser(28,111)
    if(raritylevel=='Epic'):
        return randomiser(100,500)
    if(raritylevel=='Legendary'):
        return randomiser(450,5000)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # get things

# CELL ********************

def getName(raceorkingdom = 'Isengard'):
    enum_value = RacesAndKingdoms[raceorkingdom]
    chosenRaceorKingdom = enum_value.display_name
    name = None
    counter = 0
    maxamountoftries = 10
    while(name==None) and (counter<maxamountoftries):
        counter = counter + 1
        try:
            df = (MiddleEarthNames
                    .filter(F.col("MiddleEarthGroup")==chosenRaceorKingdom)
                    .orderBy(F.rand())
                    .limit(1)
                )
            name = df.collect()[0]
        except:
            print(Exception, enum_value)
    return name["name"]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def getLoot(target):
    chosenRaceorKingdom = target
    rarity = DecideRarity()
    df = []
    row = None
    counter = 0
    maxamountoftries = 10
    while(row==None) and (counter<maxamountoftries):
        counter = counter + 1
        try:
            df = (MiddleEarthItems
                    .filter(F.col("MiddleEarthGroup")==chosenRaceorKingdom)
                    .filter(F.col("Rarity")==rarity)
                    .orderBy(F.rand())
                    .limit(1)
                    .withColumn("IsLoot", F.lit(True))
                    
                )
            #df.withColumn("TradeValue", randomitemvalue(raritylevel))
            row = df.collect()[0]
        except Exception as e:
            print(e, chosenRaceorKingdom, rarity)
    return row


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def getItem(_Type = ItemCategory.Weapons, raceorkingdom = 'Isengard'):
    kingdom_enum = RacesAndKingdoms[raceorkingdom]
    item_Cat = _Type.display_name
    chosenRaceorKingdom = kingdom_enum.display_name    
    df = []
    row = None
    rarity = None
    counter = 0
    maxamountoftries = 10
    while(row==None) and (counter<maxamountoftries):
        counter = counter + 1
        try:
            rarity = DecideRarity()
            df = (MiddleEarthItems
                    .filter(F.col("Category")==item_Cat)
                    .filter(F.col("MiddleEarthGroup")==chosenRaceorKingdom)
                    .filter(F.col("Rarity")==rarity)
                    .orderBy(F.rand())
                    .limit(1)
                    .withColumn("IsLoot", F.lit(False))
                )
            #df.withColumn("TradeValue", randomitemvalue(raritylevel))
            row = df.collect()[0]
            #print(row)
        except Exception as e:
            print(e, item_Cat, chosenRaceorKingdom, rarity)
    return row

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def getRaidingTarget(kingdom):
    enum_value = RacesAndKingdoms[kingdom]
    row = None
    while(row==None):
        try:
            df = (RaidingTargets
                    .filter(F.col("Raider")==enum_value.display_name)
                    .orderBy(F.rand())
                    .limit(1)
                )
            row = df.collect()[0]
            print(row)
        except:
            print(Exception, _Type, raceorkingdom, isloot)
    return row


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def DecideRarity():
    r = randomiser()
    if(r<=50):
        return 'Common'
    elif (r<=80):
        return 'Uncommon'
    elif (r<=95):
        return 'Rare'
    elif (r<=99):
        return 'Epic'
    else:
        return "Legendary"
    


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # managing Lakehouses

# CELL ********************

def GetLakehouseabfs_path(kingdom = 'Mordor'):
    enum_value = RacesAndKingdoms[kingdom]
    LH_Name = 'LH_' + enum_value.name
    try:
        notebookutils.lakehouse.create(name = LH_Name)
    except Exception as ex:
        print('Lakehouse already exists')
    lakehouse = mssparkutils.lakehouse.get(LH_Name)
    lh_abfs_path = lakehouse.get("properties").get("abfsPath")
    return lh_abfs_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_overview_to_table(kingdom, partytype, target, ta_date, placename, result):
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    prm_maxnummer = 1
    try:
        df = spark.sql("SELECT max(EventID) FROM LH_MiddleEarth.Events LIMIT 1000")
        prm_maxnummer =df.collect()[0]["max(EventID)"]
        prm_maxnummer = prm_maxnummer +1
    except:
        print('failed to get max number')
    table = "abfss://0a50491e-5d61-4438-ad2e-a66f93ec6299@onelake.dfs.fabric.microsoft.com/c39886bf-6f06-4e24-9b4f-40f4514d1b51/Tables/Events"
    data = [(kingdom, partytype, target, ta_date, placename,prm_maxnummer, result)]
    schema = StructType([
        StructField("kingdom", StringType(), True),
        StructField("partytype", StringType(), True),
        StructField("target", StringType(), True),
        StructField("ta_date", IntegerType(), True),
        StructField("placename", StringType(), True),
        StructField("EventID", IntegerType(), True),
        StructField("Result", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    df.write.format("delta").option("mergeSchema", "true").mode("append").save(table)
    return prm_maxnummer

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def writeToTable(dataframe, partymember, TAdate, raceorkingdom, targettablename, partytype, partymember_id, event_id,succes,
                    Survived = True, placename = '' ):
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
    from pyspark.sql.functions import regexp_replace
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    prm_kingdom = RacesAndKingdoms[raceorkingdom]
    lh_abfs_path = GetLakehouseabfs_path(prm_kingdom.name)
    characters_to_remove = r"[,\;\{\}\(\)\n\t=]"
    
    schema = StructType([
        StructField("Name", StringType(), True),
        StructField("Rarity", StringType(), True),
        StructField("TradeValue", IntegerType(), True),
        StructField("Description", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("MiddleEarthGroup", StringType(), True),
        StructField("IsLoot", BooleanType(), True)

    ])
    try:
        sparkdf = spark.createDataFrame(dataframe, schema)
        sparkdf = sparkdf.withColumn('partymember', F.lit(partymember))
        sparkdf = sparkdf.withColumn('TAdate', F.lit(TAdate))
        sparkdf = sparkdf.withColumn('Survived', F.lit(Survived))
        sparkdf = sparkdf.withColumn('Place', F.lit(placename))
        sparkdf = sparkdf.withColumn("Name", regexp_replace("Name", characters_to_remove, ""))
        sparkdf = sparkdf.withColumn("Description", regexp_replace("Description", characters_to_remove, ""))
        sparkdf = sparkdf.withColumn('PartyType', F.lit(partytype))
        sparkdf = sparkdf.withColumn('PartyMemberID', F.lit(partymember_id))
        sparkdf = sparkdf.withColumn('EventID', F.lit(event_id))
        sparkdf = sparkdf.withColumn('Result', F.lit(succes))
        spark

        
        sparkdf.write.format("delta").option("mergeSchema", "true").mode("append").save(f"{lh_abfs_path}/Tables/{targettablename}")

    except Exception as e:
        print(f"failed to write {partymember}, {TAdate}, {raceorkingdom}, {targettablename}, {Survived} with exception: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# Mordor
# => 1 grote file
# 
# Gondor
# => duidelijk gestructureerd, uniforme naamgeving
# 
# Rohan
# => ander soort naam per regio, hoge variabiliteit, maar uniforme look and feel
# 
# Isengard
# => afhankelijk van oorsprong
# 
# Dwergen
# => compleet verschillend per oorsprong, basis gevoel nog hetzelfde, maar verder elk eigen richting
# 
# Misty mountains goblins
# => Free for all
# 
# Elves
# => redelijk gelijklopende structuur, met verschillen per groep
# 
# Hobbits
# elke plaats eigen naamgeving, ruwweg
# 
# Rangers 
# => iedereen doet zijn ding
# 
# Army of the dead
# => niks, geen documentatie, geen toegang, gewoon rapport

# CELL ********************

def CreateFilename(kingdom, typeOfParty, placename, TAdate):
    prm_kingdom = RacesAndKingdoms[kingdom]

    # 1 file to have everything neat for the dark lord
    if prm_kingdom == RacesAndKingdoms.Mordor:
        return 'Force'
    #seperate file per subject, clearly organised
    elif prm_kingdom == RacesAndKingdoms.Gondor:
        return typeOfParty + '_' + placename

    elif prm_kingdom == RacesAndKingdoms.Rohan:
        if(typeOfParty == 'Garrison'):
            return placename
        elif(typeOfParty=='Patrol'):
            return placename
        elif(typeOfParty=='Raid'):
            return 'Raiding' + placename
        elif(typeOfParty=='Army'):
            return 'GreatArmyOf' + str(TAdate) + 'Against' + placename

    #different system based on the origins of the forces
    elif prm_kingdom == RacesAndKingdoms.Isengard:
        alliesList = ['Uruk-hai', 'orcs', 'Dunlendings', 'Wargs']
        ally = random.choice(alliesList)
        if(ally == 'Uruk-hai'):
            return typeOfParty + '_' + placename + '_Uruk'
        if(ally == 'orcs'):
            return str(TAdate) + '_' + typeOfParty + '_' + placename + '_Orc'
        if(ally == 'Dunlendings'):
            return placename + str(TAdate) + '_Dunlending'
        if(ally == 'Wargs'):
            return 'Warg'

    elif prm_kingdom == RacesAndKingdoms.Dwarves:
        dwarvenkingdoms = ['Erebor', 'Blue mountains', 'The Iron hills']
        dwarvenkingdom = random.choice(dwarvenkingdoms)
        if(dwarvenkingdom == 'Erebor'):
            return 'erebor_' + placename
        if(dwarvenkingdom == 'Blue mountains'):
            return typeOfParty + '_' + placename + '_Blue'
        if(dwarvenkingdom == 'The Iron hills'):
            return 'Iron' + typeOfParty + str(TAdate) + '_' + placename
        return ''

    elif prm_kingdom == RacesAndKingdoms.GoblinsMistyMountains:
        orc_prefixes = ["gash", "kruk", "mog", "zurg", "thrakh", "ugluk", "snag", "grish"]
        orc_suffixes = ["g", "dur", "mog", "thrak", "snak", "ruk", "zulg", "burz"]
        subject_tags = {
                            "Raid": ["blood", "fire", "skull", "burn"],
                            "Patrol": ["watch", "sneak", "track", "prowl"],
                            "Garrison": ["hold", "gate", "fort", "keep"],
                            "Army": ["horde", "legion", "march", "warband"]
                        }
        prefix = random.choice(orc_prefixes)
        subjectlist = subject_tags[typeOfParty]
        tag = random.choice(subjectlist)
        suffix = random.choice(orc_suffixes)
        filename = f"{prefix}-{tag}{suffix}"
        return filename

    elif prm_kingdom == RacesAndKingdoms.Elves:
        elvenrealms = ['Rivendel', 'Lothlorien', 'Mirkwood', 'Lindon']
        elvenrealm = random.choice(elvenrealms)
        return elvenrealm + '_' + typeOfParty + '_' + placename

    elif prm_kingdom == RacesAndKingdoms.Hobbits:
        randomgetal = randomiser()
        if(randomgetal<=98):
            return placename
        else:
            return typeOfParty + '_' + placename


    #Army

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create party - Core part

# CELL ********************

def createParty(members = 5, raceorkingdom = 'Isengard', TaDate = 2900, raidtarget = ''
        , raidsucces = '',  placename='', partytype='Garrison'):  
    
    prm_kingdom = RacesAndKingdoms[raceorkingdom]
    prm_kingdom_name = prm_kingdom.name
    prm_itemdistributionlist = itemDistribution[prm_kingdom_name]

    result_itemlist = []

    prm_tablename = CreateFilename(
                                kingdom=prm_kingdom_name,
                                placename=placename,
                                TAdate=TaDate,
                                typeOfParty=partytype  
                            )

    partymembers = list(range(1, members+1))
    prm_death_rate = 0
    if(raidsucces != ''):
          prm_death_rate =   RaidSuccesRate[raidsucces].death_rate
    prm_event_id = write_overview_to_table(prm_kingdom_name, partytype, raidtarget, TaDate,placename,raidsucces )
    #loop through party members
    currentpartymemberid = 0
    for n in partymembers:
        currentpartymemberid = currentpartymemberid +1
        result_itemlist=[]
        partymembername = getName(prm_kingdom_name)

        # Weapons
        for num in range(0,prm_itemdistributionlist.weapons):
            result_itemlist.append(getItem(ItemCategory.Weapons, prm_kingdom_name))
        print('weapons done (' + str(n) + '/' + str(members))
        
        # Armour
        for num in range(0,prm_itemdistributionlist.weapons):
            result_itemlist.append(getItem(ItemCategory.Armour, prm_kingdom_name))
        print('Armour done (' + str(n) + '/' + str(members))

        # food and drinks
        for num in range(0,prm_itemdistributionlist.foodanddrinks):
            result_itemlist.append(getItem(ItemCategory.FoodAndDrinks, prm_kingdom_name))        
        print('food and drinks done (' + str(n) + '/' + str(members))

        # Jewelry
        for num in range(0,prm_itemdistributionlist.weapons):
            result_itemlist.append(getItem(ItemCategory.Jewelry, prm_kingdom_name))
        print('Jewelry done (' + str(n) + '/' + str(members))

        # RelicsAndHeirlooms
        for num in range(0,prm_itemdistributionlist.weapons):
            result_itemlist.append(getItem(ItemCategory.RelicsAndHeirlooms, prm_kingdom_name))
        print('RelicsAndHeirlooms done (' + str(n) + '/' + str(members))

        if(raidtarget != '' and raidsucces != ''):
            RaidSuccesRate[raidsucces].loot_items
            numberofloot = RaidSuccesRate[raidsucces].loot_items
            for n in range(numberofloot):
                result_itemlist.append(getLoot(raidtarget))
        membersurvived = True
        if(randomiser()<=prm_death_rate):
            membersurvived = False
        
        (writeToTable
            (
                result_itemlist, 
                partymember=partymembername, 
                TAdate=TaDate,
                raceorkingdom=prm_kingdom_name, 
                targettablename= prm_tablename, 
                partytype = partytype,
                Survived= membersurvived, 
                placename=placename,
                partymember_id=currentpartymemberid, 
                event_id = prm_event_id,
                succes= raidsucces
            )
        )
    return 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # prepared parties

# CELL ********************

def Army(kingdom = 'Mordor'):
    prm_kingdom = RacesAndKingdoms[kingdom]
    prm_partytype = 'Army'
    prm_ta_date = randomiser(2900,3000)

    if((prm_kingdom == RacesAndKingdoms.Mordor)
        or (prm_kingdom == RacesAndKingdoms.Isengard)
        or (prm_kingdom == RacesAndKingdoms.GoblinsMistyMountains)
    ):
        prm_target = randomTarget_Evil()
    else:
        prm_target = randomTarget_Good()
    raidsucces = randomSucces()  
    membersamount = randomiser(100,3000)
    kingdomname = prm_kingdom.name
    (   
        createParty
        (
            members = membersamount, 
            raceorkingdom = prm_kingdom.name,
            TaDate = prm_ta_date, 
            raidtarget = prm_target.name,
            raidsucces = raidsucces,
            partytype=prm_partytype
        )
    )  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def RaidingParty(kingdom = 'Mordor'):
    prm_kingdom = RacesAndKingdoms[kingdom]
    prm_partytype = 'Raid'
    prm_ta_date = randomiser(2900,3000)

    row_target = getRaidingTarget(prm_kingdom.name)
    prm_target = row_target["Owner"]
    
    prm_target_place=row_target["Place"]
    Prm_membersamount = randomiser(1,1)
    prm_raidsucces = randomSucces()    

    print(f"{prm_kingdom.display_name} is raiding {prm_target} and it is a {prm_raidsucces.name}")
    (   
        createParty
        (
            members = Prm_membersamount, 
            raceorkingdom = kingdom,
            TaDate = prm_ta_date, 
            raidtarget = prm_target,
            raidsucces= prm_raidsucces.name,
            placename=prm_target_place,
            partytype=prm_partytype
        )
    )   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def patrol(kingdom = 'Mordor',patroltarget = 'unknown'):
    prm_kingdom = RacesAndKingdoms[kingdom]
    prm_partytype = 'Patrol'
    prm_ta_date = randomiser(2900,3000)
    prm_membersamount = randomiser(2,6)
    prm_kingdomname = prm_kingdom.display_name
    print('not finished')
    return
    #filename = 'Patrol_' + patroltarget
    (   
        createParty
        (
            members = prm_membersamount, 
            raceorkingdom = prm_kingdomname,
            TaDate = prm_ta_date, 
            #targettablename = filename,
            partytype=prm_partytype
        )
    ) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def garrison(kingdom = 'Mordor', garrison_place = 'fort'):
    prm_kingdom = RacesAndKingdoms[kingdom]
    prm_partytype = 'Garrison'
    prm_ta_date = randomiser(2900,3000)
    prm_membersamount = randomiser(10,60)
    prm_kingdomname = prm_kingdom.display_name
    #filename = kingdomname + '_' + garrison_place
    (   
        createParty
        (
            members = prm_membersamount, 
            raceorkingdom = prm_kingdomname,
            TaDate = prm_ta_date, 
            #targettablename = filename,
            partytype=prm_partytype
        )
    ) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # running the code

# CELL ********************

LH_Name = 'LH_middleEarth'
lakehouse = mssparkutils.lakehouse.get(LH_Name)


#The TA date is a number between 2900 and 3000 as those are the years in Middle earth this dataset is about
TA_Date = ''

# we need a date, so we create one if none provided
if(TA_Date==''):
    TA_Date = randomiser(2900,3000)

# fill necessary lists
MiddleEarthItems = spark.sql("SELECT * FROM LH_MiddleEarth.me_itemslist")
MiddleEarthNames = spark.sql("SELECT * FROM LH_MiddleEarth.middleearthnames")
RaidingTargets = spark.sql("SELECT * FROM LH_MiddleEarth.raidingtargets")
garrisonplaces = spark.sql("SELECT * FROM LH_MiddleEarth.garrisonplaces")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

RaidingParty(RacesAndKingdoms.Dwarves.name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for k in RacesAndKingdoms:
    print(f"now doing {k.display_name}")
    totalraids = randomiser(1,1)
    for r in range(totalraids):
        try:
            RaidingParty(k)
        except Exception as e:
            print(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
