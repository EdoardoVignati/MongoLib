#!/usr/bin/python

import json
import pymongo
import time
import csv
import bson
import pandas
import pprint as pp

from GinfoMongoPython.EdoLib.BaseLib import mongoLib
#To import use :
# from BaseLib import *

m = mongoLib("gamesdb", "games")


"""


############################## EVERYTHING ON JSON ##############################
m.printCurrentJsonInfo()
print("\nCurrent json collections:", m.returnCurrentJsonCollections())


############################## WORK ON DB ##############################
m.saveJsonInDb()
m.printCurrentDbsInfo()
print("\nDb stored collections in", m.dbName, ":", m.returnDbCollections())


############################## PRINT DATA ##############################
print("\nBOARDGAMES --------------------------------------------------------------------")
for record in m.returnEverythingGivenColl("boardgames"):
	print(record)

print("\nVIDEOGAMES --------------------------------------------------------------------")
for record in m.returnEverythingGivenColl("videogames"):
	print(record)

############################## COUNT RECORDS ##############################
print("\nRecords per collection:")
for k,v in m.countRecordsPerColl().items():
	print(k , ":", v)

############################## GET TIMER ##############################
totalTime=0;
for line in m.getTimer():
	totalTime+=(line[1]*1000)
print("\nBaseLib total execution time:", totalTime, "ms")


############################## GET KEYS ##############################

for line in m.getKeysPerColl("videogames"):
	print(line)
for line in m.getKeysPerColl("boardgames"):
	print(line)


############################## QUERIES ##############################
p = {}
for record in m.findDbDataPerColl("videogames"):
		pp.pprint(record)

# With ID
p={}
q=m.buildProjectionToDisplay(["title","publisher"])
print(q)
for record in m.findDbDataPerColl("videogames",p,q):
		pp.pprint(record)


# Without ID
p = {}
q = m.buildProjectionToDisplay(["title","publisher"],0)
print(q)
for record in m.findDbDataPerColl("videogames", p, q):
		pp.pprint(record)

############################## CSV MANAGER ##############################
m.csvToDb("./stations.csv", "stations")
m.csvToJson()

############################## COLLECTIONS ##############################

# Create new collection and add data
data = m.findDbDataPerColl("videogames")
m.createCollection("NuoviVideogames")
m.saveDataIntoCollection("NuoviVideogames")

# Append new data to existing collection
data = m.findDbDataPerColl("altriVideogames")
m.appendDataIntoCollection("NuoviGiochi",data)


#Drop collection
m.dropCollection("NuoviVideogames")


############################## FILTERING DATA ##############################
sum=0
for record in m.getAllValuesOfField("platforms", "videogames"):
	sum+=1
print(sum)


sum=0
for record in m.getSingledOutValuesOfField("platforms", "videogames"):
	sum+=1
print(sum)



############################## USEFUL AGGREGATIONS ##############################


# Group by year and count total years
##########################################
p1 = {
	'$group': {
		'_id': '$year'
	}
}
p2 = {

	'$count': 'Anni totali'
}
pipeline=[p1,p2]

for record in m.execPipeline("videogames",pipeline):
	print(record)

##########################################


# Count total games per year
##########################################
p1 = {
	'$group': {
		'_id': '$year',
		'giochi_per_anno': {
			'$sum': 1
		}
	}
}
pipeline=[p1]
##########################################


# Round average per number of games per year
###########################################
p1 = {
	'$group': {
		'_id': '$year',
		'giochi_per_anno': {
			'$sum': 1
		}

	}
}
p2 = {
	'$group': {
		'_id': None,
		'media': {
			'$avg': '$giochi_per_anno'
		}
	}
}

p3 = {'$project':{"media" : {"$divide":[{"$subtract":[{"$multiply":["$media",10]},{"$mod":[{"$multiply":["$media",10]}, 1]}]},10]}} }

pipeline = [p1, p2, p3]
###########################################


# Unpack an array ( unwind() )
###########################################
p1 = {
	'$unwind': '$platforms'
}

pipeline = [p1]
###########################################


# Unpack, match only 'Ninentdo Wii' and count
###########################################
p1 = {
	'$unwind': '$platforms'
}

p2 = {
	'$match': {
		'platforms': 'Nintendo Wii'
	}
}

p3 = {'$count': 'Tot'}

pipeline = [p1, p2, p3]
###########################################



# Project only publisher name and sort (09-AZ-az)
###########################################
p1 = {
	'$project': {
		'publisher.name': 1,
		'_id': 0
	}
}

p2 = {
	'$sort':{
		'publisher.name':1
	}
}

pipeline=[p1, p2]
###########################################







"""



m.printCurrentDbsInfo()
print(m.getTimer())
#pipeline=[]
#for record in m.execPipeline("videogames",pipeline):
#	print(record)


