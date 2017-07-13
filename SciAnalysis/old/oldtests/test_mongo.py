addr = 'localhost'
port = 27021

from pymongo import MongoClient
client = MongoClient(addr, port)
print(client.database_names())

dbmds = client.get_database('metadatastore-production-v1')
print(dbmds.collection_names())

coll = dbmds.get_collection("run_start")
res = coll.find()

