#!/usr/bin/python


"""
This HTML was generated for BaseLib with command:

    pydoc -w BaseLib

"""


import json
import pymongo
import time
import csv
import bson
import sys
import pandas
import inspect

class mongoLib():

	def __init__(self,indbname="", inputNameFile=""):
		try:
			self.client = pymongo.MongoClient('localhost', 27017);
			self.csvFile = inputNameFile + ".csv"
			self.jsonFile = inputNameFile + ".json"
			self.timer = []
			if (self.getAllDBs().__contains__(indbname) or indbname==""):
				self.dbName = indbname
				if(indbname==""):
					print("Remember to set with setDbName('dbname')")
			else:
				raise Exception()

		except:
			print("Db connection failed.")


	########################################## JSON ###########################################

	# Print collections of json file
	def printCurrentJsonInfo(self):
		"""Print dbs and collections of json file"""
		start = time.time()
		print("\nCollections of:", self.jsonFile)
		with open(self.jsonFile) as block:
			data=json.load(block)
			for coll in data:
				print("  ->", coll)
		self.timer.append(("printCurrentJsonInfo", time.time()-start))

	# Return collections of json file
	def getCurrentJsonCollections(self):
		"""Print colls of json file"""
		start = time.time()
		collList = []
		with open(self.jsonFile) as block:
			data = json.load(block)
			for coll in data:
				collList.append(coll)
		self.timer.append(("getCurrentJsonCollections", time.time()-start))
		return collList

	# Save json in db from scratch removing old version
	def saveJsonInDb(self):
		"""Import json into MongoDB"""
		start = time.time()
		self.client.drop_database(self.dbName)
		with open(self.jsonFile) as block:
			data = json.load(block)
		for singleColl in self.getCurrentJsonCollections():
			self.client[self.dbName].create_collection(singleColl).insert(data[singleColl])
		print("\nCollections succesfully stored in new db: ", self.dbName)
		self.timer.append(("saveJsonInDb", time.time()-start))




	########################################## DB MANAGEMENT ###########################################



	# Get all dbs in mongoDB
	def getAllDBs(self):
		"""Get all dbs"""
		start = time.time()
		listdbs=self.client.database_names()
		self.timer.append(("getAllDBs", time.time()-start))
		return listdbs

	# Print all databases and collections saved in mongoDB
	def printCurrentDbsInfo(self):
		"""Print dbs and collections for each db"""
		start = time.time()
		print("\nCurrent stored dbs and collections:")
		for select_db in self.client.database_names():
				print(select_db)
				colls = self.client[select_db].list_collection_names()
				for coll in colls:
					print("  ->", coll)
		self.timer.append(("printCurrentDbInfo", time.time()-start))

	# Return collections of selected db
	def getDbCollections(self):
		"""Get colls from database in use"""
		start = time.time()
		data=self.client[self.dbName].list_collection_names()
		self.timer.append(("getDbCollections", time.time()-start))
		return data


	def getEverythingGivenColl(self, coll=''):
		"""Get all data from given collection"""
		start = time.time()
		globalRecords = []
		data = self.client[self.dbName][coll].find()
		for record in data:
			globalRecords.append(record)
		self.timer.append(("getEverythingGivenColl", time.time()-start))
		return globalRecords

	def printEverythingGivenColl(self, coll=''):
		"""Print all data from given collection"""
		start = time.time()
		data = self.client[self.dbName][coll].find()
		for record in data:
			print(record)
		self.timer.append(("printEverythingGivenColl", time.time()-start))


	def countRecordsPerColl(self):
		"""Count all records from all collections"""
		start = time.time()
		counter = {}
		for col in self.getDbCollections():
			counter[col] = self.client[self.dbName][col].count()
		self.timer.append(("countRecordsPerColl", time.time()-start))
		return counter


	def getKeysPerColl(self, coll=''):
		"""Find different keys for given collection"""
		start = time.time()
		keys = []
		for record in self.findDbDataPerColl(coll):
			if not keys.__contains__(list(record.keys())):
				keys.append(list(record.keys()))
		self.timer.append(("getKeysPerColl", time.time() - start))
		return keys

	def dropCollection(self, coll=''):
		"""Drop a collection given its name"""
		start = time.time()
		self.client[self.dbName][coll].drop()
		self.timer.append(("dropCollection", time.time() - start))


	def createCollection(self, coll=''):
		"""Create a collection in db by given name"""
		start = time.time()
		newDb=self.client[self.dbName]
		newColl=newDb[coll]
		self.timer.append(("createCollection", time.time() - start))
		return newColl

	def getCollectionObj(self, coll=''):
		"""Get collection obj from its name"""
		start = time.time()
		collObj=self.client[self.dbName][coll]
		self.timer.append(("getCollectionObj", time.time() - start))
		return collObj



	########################################## CSV ###########################################


	def csvToDb(self, newCollName='',listFields=[]):
		"""Save directly a csv into db"""
		start = time.time()
		self.client.drop_database(self.dbName)
		dataframe = pandas.read_csv(self.csvFile, low_memory=False, skiprows=1, names=listFields)
		gettedColl = self.createCollection(newCollName)
		gettedColl.insert_many(dataframe.to_dict('records'))
		self.timer.append(("csvToDb", time.time() - start))

	def getFieldsFromCsv(self):
		"""Get all fields of csv file (column names)"""
		start = time.time()
		data = list(pandas.read_csv(self.csvFile).keys())
		self.timer.append(("getFieldsFromCsv", time.time() - start))
		return data


	# Read CSV File
	def csvToJson(self):
		"""Read a csv and convert it into json file"""
		start = time.time()
		self.client.drop_database(self.dbName)
		dataframe = pandas.read_csv(self.csvFile, low_memory=False, skiprows=1, names=self.getFieldsFromCsv(self.csvFile))
		data = dataframe.to_json(orient="table", index=False)
		with open(self.jsonFile, 'w') as file:
			file.write(data)
		self.timer.append(("csvToJson", time.time() - start))


	########################################## INDEXES ###########################################

	def createIndex(self,coll='', field=''):
		"""Create an index on a key"""
		start = time.time()
		try:
			if field not in self.client[self.dbName][coll].index_information():
				self.client[self.dbName][coll].create_index([(field, 1)])
				self.timer.append(("createIndex", time.time() - start))
		except:
			print("Creation of index failed")


	def deleteIndex(self, coll='', indexName=''):
		"""Remove an index from a key"""
		start = time.time()
		try:
			if indexName in self.client[self.dbName][coll].index_information():
				self.client[self.dbName][coll].drop_index(indexName)
				self.timer.append(("deleteIndex", time.time() - start))
			else:
				raise Exception()
		except:
			print("Cannot delete correctly index")

	def getIndexesFromCollections(self, coll=''):
		"""Get all indexes from a collection"""
		start = time.time()
		data = self.client[self.dbName][coll].index_information()
		self.timer.append(("getIndexesFromCollections", time.time() - start))
		return data

	def printIndexesFromCollections(self, coll=''):
		"""Print all indexes from a collection"""
		start = time.time()
		data = self.client[self.dbName][coll].index_information()
		for record in data:
			print(record)
		self.timer.append(("printIndexesFromCollections", time.time() - start))


	########################################## DB DATA MANAGEMENT ###########################################


	def appendDataIntoCollection(self, data=[{},{}] ,coll=''):
		"""Insert data in given collection"""
		start = time.time()
		try:
			self.client[self.dbName][coll].insert_many(data)
		except:
			print("Error appending. Probably ", coll, " already contains these data.")
		self.timer.append(("appendDataIntoCollection", time.time() - start))



	def getAllValuesOfField(self, field='', coll=''):
		"""Find all values for a given field from a given collection"""
		start = time.time()
		vals = []
		for record in self.findDbDataPerColl(coll, {}, {field: 1, '_id': 0}):
				vals.append(record[field])
		self.timer.append(("getAllValuesOfField", time.time() - start))
		return vals

	def getSingledOutValuesOfField(self, field='', coll=''):
		"""Find different values for a given field from a given collection"""
		start = time.time()
		vals = []
		for record in self.findDbDataPerColl(coll, {}, {field: 1, '_id': 0}):
				vals.append(record[field])
		unique_data = [list(x) for x in set(tuple(x) for x in vals)]
		self.timer.append(("getSingledOutValuesOfField", time.time() - start))
		return unique_data


	def buildProjectionToDisplay(self, q=[], keepId = 1):
		"""Build projection with elements to display. keepId=1|0"""
		start = time.time()
		data = {}
		for elem in q:
			data[elem] = 1
		if(keepId != 1):
			data["_id"] = 0
		self.timer.append(("buildProjectionToDisplay", time.time() - start))
		return data


	def execPipeline(self, coll='', pipeline=[{},{}]):
		"""Given a collection and a [pipeline], execute it"""
		start = time.time()
		data = self.client[self.dbName][coll].aggregate(pipeline)
		self.timer.append(("execPipeline", time.time() - start))
		return data


	def saveDataIntoCollection(self, data=[{},{}], coll=''):
		"""Drop collection, then insert data in given collection"""
		start = time.time()
		self.client[self.dbName][coll].drop()
		self.client[self.dbName][coll].insert_many(data)
		self.timer.append(("saveDataIntoCollection", time.time() - start))


	def findDbDataPerColl(self, coll='', match={}, project={}, lst=0):
		"""Get data with filters ( find() ), list=1|0"""
		start = time.time()
		if(project!={}):
			data = self.client[self.dbName][coll].find(match, project)
		else:
			data=self.client[self.dbName][coll].find(match)
		if(lst):
			self.timer.append(("findDbDataPerColl", time.time() - start))
			return list(data)
		else:
			self.timer.append(("findDbDataPerColl", time.time() - start))
			return data


	########################################## META ###########################################

	def getTimer(self):
		"""Get function time usage"""
		return self.timer

	def getTotalTimer(self):
		totalTime = 0
		for line in self.getTimer():
			#totalTime+=(line[1]*1000) # millis
			totalTime+=(line[1])
		return totalTime

	def setDbName(self, InputDbName=''):
		self.dbName=InputDbName



