import pymongo
import csv

client = pymongo.MongoClient("mongodb+srv://elee353:Lov4lorn.@oss-cluster0-jca2c.gcp.mongodb.net/test?retryWrites=true")

names = client.list_database_names()

mozillaDB = client["mozilla"]

firefoxCol = mozillaDB["Firefox"]

# print(mozillaDB.list_collection_names())

# print(firefoxCol.find().count())

colSizes = {}

with open("mozillaSummary.csv", "w", newline='') as sumFile:
    csvHeader = ["Collection", "Num Documents"]
    writer = csv.DictWriter(sumFile, fieldnames=csvHeader)
    writer.writeheader()

    for colName in mozillaDB.list_collection_names():
        numDocs = mozillaDB[colName].count_documents({})
        writer.writerow({'Collection' : colName, 'Num Documents' : numDocs})



# print(colSizes)

