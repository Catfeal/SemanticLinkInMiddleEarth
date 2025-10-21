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
    Hobbit = (1, 2, 5, 3, 4, 1)
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
    enum_value = RacesAndKingdoms[kingdom]
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
    item_Cat = _Type.display_name
    chosenRaceorKingdom = raceorkingdom.display_name    
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
    LH_Name = 'LH_' + kingdom.display_name
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

def writeToTable(dataframe, partymember, TAdate, raceorkingdom, targettablename, partytype, partymember_id,
                    Survived = True, placename = ''):
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
    from pyspark.sql.functions import regexp_replace
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    enum_value = RacesAndKingdoms[raceorkingdom]
    lh_abfs_path = GetLakehouseabfs_path(enum_value)
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
    kingdom_enum = RacesAndKingdoms[kingdom]

    # 1 file to have everything neat for the dark lord
    if kingdom_enum == RacesAndKingdoms.Mordor:
        return 'Force'
    #seperate file per subject, clearly organised
    elif kingdom_enum == RacesAndKingdoms.Gondor:
        return typeOfParty + '_' + placename

    elif kingdom_enum == RacesAndKingdoms.Rohan:
        if(typeOfParty == 'Garrison'):
            return placename
        elif(typeOfParty=='Patrol'):
            return placename
        elif(typeOfParty=='Raid'):
            return 'Raiding' + placename
        elif(typeOfParty=='Army'):
            return 'GreatArmyOf' + str(TAdate) + 'Against' + placename

    #different system based on the origins of the forces
    elif kingdom_enum == RacesAndKingdoms.Isengard:
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

    elif kingdom_enum == RacesAndKingdoms.Dwarves:
        dwarvenkingdoms = ['Erebor', 'Blue mountains', 'The Iron hills']
        dwarvenkingdom = random.choice(dwarvenkingdoms)
        if(dwarvenkingdom == 'Erebor'):
            return 'erebor_' + placename
        if(dwarvenkingdom == 'Blue mountains'):
            return typeOfParty + '_' + placename + '_Blue'
        if(dwarvenkingdom == 'The Iron hills'):
            return 'Iron' + typeOfParty + str(TAdate) + '_' + placename
        return ''

    elif kingdom_enum == RacesAndKingdoms.GoblinsMistyMountains:
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

    elif kingdom_enum == RacesAndKingdoms.Elves:
        elvenrealms = ['Rivendel', 'Lothlorien', 'Mirkwood', 'Lindon']
        elvenrealm = random.choice(elvenrealms)
        return elvenrealm + '_' + typeOfParty + '_' + placename

    elif kingdom_enum == RacesAndKingdoms.Hobbit:
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
    RaceOrKingdomname = raceorkingdom.name
    item = itemDistribution[RaceOrKingdomname]
    itemlist = []

    filename = CreateFilename(
                                kingdom=RaceOrKingdomname,
                                placename=placename,
                                TAdate=TaDate,
                                typeOfParty=partytype  
                            )

    partymembers = list(range(1, members+1))

    deathrate = 0
    if(raidsucces != ''):
          deathrate =   RaidSuccesRate[raidsucces].death_rate
    #loop through party members
    currentpartymemberid = 0
    for n in partymembers:
        currentpartymemberid = currentpartymemberid +1
        itemlist=[]
        partymembername = getName(raceorkingdom)

        # Weapons
        for num in range(0,item.weapons):
            itemlist.append(getItem(ItemCategory.Weapons, raceorkingdom))
        print('weapons done (' + str(n) + '/' + str(members))
        
        # Armour
        for num in range(0,item.weapons):
            itemlist.append(getItem(ItemCategory.Armour, raceorkingdom))
        print('Armour done (' + str(n) + '/' + str(members))

        # food and drinks
        for num in range(0,item.foodanddrinks):
            itemlist.append(getItem(ItemCategory.FoodAndDrinks, raceorkingdom))        
        print('food and drinks done (' + str(n) + '/' + str(members))

        # Jewelry
        for num in range(0,item.weapons):
            itemlist.append(getItem(ItemCategory.Jewelry, raceorkingdom))
        print('Jewelry done (' + str(n) + '/' + str(members))

        # RelicsAndHeirlooms
        for num in range(0,item.weapons):
            itemlist.append(getItem(ItemCategory.RelicsAndHeirlooms, raceorkingdom))
        print('RelicsAndHeirlooms done (' + str(n) + '/' + str(members))

        if(raidtarget != '' and raidsucces != ''):
            RaidSuccesRate[raidsucces].loot_items
            numberofloot = RaidSuccesRate[raidsucces].loot_items
            for n in range(numberofloot):
                itemlist.append(getLoot(raidtarget))
        membersurvived = True
        if(randomiser()<=deathrate):
            membersurvived = False
        
        (writeToTable
            (
                itemlist, 
                partymember=partymembername, 
                TAdate=TaDate,
                raceorkingdom=RaceOrKingdomname, 
                targettablename= filename, 
                partytype = partytype,
                Survived= membersurvived, 
                placename=placename,
                partymember_id=currentpartymemberid
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
    kingdom_enum = RacesAndKingdoms[kingdom]
    typeOfParty = 'Army'
    ta_date = randomiser(2900,3000)

    if((kingdom_enum == RacesAndKingdoms.Mordor)
        or (kingdom_enum == RacesAndKingdoms.Isengard)
        or (kingdom_enum == RacesAndKingdoms.GoblinsMistyMountains)
    ):
        target = randomTarget_Evil()
    else:
        target = randomTarget_Good()
    raidsucces = randomSucces()  
    membersamount = randomiser(100,3000)
    kingdomname = kingdom_enum.name
    """
    filename = CreateFilename(kingdom.name, typeOfParty, target.name, ta_date)
    filename = CreateFilename(
                                kingdom=kingdomname,
                                placename=target.display_name,
                                TAdate=ta_date,
                                typeOfParty=typeOfParty  
                            )
    print(filename)
    """
    (   
        createParty
        (
            members = membersamount, 
            raceorkingdom = kingdom_enum.name,
            TaDate = ta_date, 
            raidtarget = target.name,
            raidsucces = raidsucces,
            #targettablename = filename,
            partytype=typeOfParty
        )
    )  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def RaidingParty(kingdom = 'Mordor'):
    kingdom_enum = RacesAndKingdoms[kingdom]
    typeOfParty = 'Raid'
    ta_date = randomiser(2900,3000)

    row_target = getRaidingTarget(kingdom_enum.display_name)
    target = row_target["Owner"]
    
    placename=row_target["Place"]

    membersamount = randomiser(1,1)

    kingdomname = kingdom_enum.display_name

    raidsucces = randomSucces()    
    print(f"{kingdom_enum.display_name} is raiding {target} and it is a {raidsucces.name}")
    """
    #filename = 'Raiding_' + target + '_members_' + str(membersamount) + '_' + str(ta_date)
    filename = CreateFilename(
                                kingdom=kingdomname,
                                placename=placename,
                                TAdate=ta_date,
                                typeOfParty=typeOfParty  
                            )
    print(f"filename is: {filename}")
    """
    (   
        createParty
        (
            members = membersamount, 
            raceorkingdom = kingdom,
            TaDate = ta_date, 
            raidtarget = target,
            raidsucces =raidsucces.name,
            #targettablename = filename,
            placename=placename,
            partytype=typeOfParty
        )
    )   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def patrol(kingdom = RacesAndKingdoms.Mordor,patroltarget = 'unknown'):
    kingdom_enum = RacesAndKingdoms[kingdom]
    typeOfParty = 'Patrol'
    ta_date = randomiser(2900,3000)
    membersamount = randomiser(2,6)
    kingdomname = kingdom_enum.display_name
    #filename = 'Patrol_' + patroltarget
    (   
        createParty
        (
            members = membersamount, 
            raceorkingdom = kingdomname,
            TaDate = ta_date, 
            #targettablename = filename,
            partytype=typeOfParty
        )
    ) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def garrison(kingdom = 'Mordor', garrison_place = 'fort'):
    kingdom_enum = RacesAndKingdoms[kingdom]
    typeOfParty = 'Garrison'
    ta_date = randomiser(2900,3000)
    membersamount = randomiser(10,60)
    kingdomname = kingdom_enum.display_name
    #filename = kingdomname + '_' + garrison_place
    (   
        createParty
        (
            members = membersamount, 
            raceorkingdom = kingdomname,
            TaDate = ta_date, 
            #targettablename = filename,
            partytype=typeOfParty
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

RaidingParty(RacesAndKingdoms.Hobbits.name)

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
