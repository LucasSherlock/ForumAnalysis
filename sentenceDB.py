# -*- coding:utf-8 -*-


import pymongo
import re
import csv

#---------------FUNCTIONS-------------------------------------

def get_qURL_by_id(list, id):
    for dict in list:
        if dict["_id"] == id:
            return dict["questionURL"]

def split_string(string):
    sentences = re.findall(r".+?(?:[\.\?!][\s\n]|\Z)", string, flags = re.DOTALL)
    return sentences


def get_col_name():
    cols = ["Firefox", "Firefox Focus", "Firefox Lite", "Firefox OS", "Firefox Reality", "Firefox for Amazon Devices", "Firefox for Android", "Firefox for Enterprise", "Firefox for Fire TV", "Firefox for iOS", "Thunderbird", "Webmaker"]

    print("Select a collection:\n")
    i = 0
    for string in cols:
        out = str(i) + " - " + string
        print(out)
        i += 1

    num = -1

    while num < 0 or num > i-1:
        num = int(input("Row Number: "))
        if num < 0 or num > i-1:
            print("Please enter a valid row number.")

    return cols[num]

def output():
    print(
    """Output to:
    1 - CSV file
    2 - Local Database
    3 - Both
    """
    )
    line = 0
    while line != 1 and line != 2 and line != 3:
       line = int(input("Select Format: "))

    return line



#-------------------------------SETUP---------------------------

num = output()
if num == 1:
        _csv = True
        _localDB = False
elif num == 2:
        _csv = False
        _localDB = True
elif num == 3:
        _csv = True
        _localDB = True

atlasClient = pymongo.MongoClient("mongodb+srv://elee353:Lov4lorn.@oss-cluster0-jca2c.gcp.mongodb.net/test?retryWrites=true")

mozDB = atlasClient["mozilla"]

colName = get_col_name()
collection = mozDB[colName]
replyCollection = mozDB[colName + " - Reply"]


#------------------------------------ENTRIES---------------------------------

# List to hold all entries
entries = []

qCursor = collection.find({})
id = 0


numQs = int(input("How many questions? "))

i = 0

for qDoc in qCursor:

    # Only take a specified number of questions
    if i >= numQs: 
        break
    

    # Merge all the text from the question into one string 
    mergedText = qDoc["questionText"][0]
    n = 1
    while n < len(qDoc["questionText"]):
        mergedText = mergedText + " " + qDoc["questionText"][n]
        n += 1

    # Get the Asker and URL of the question
    qAsker = qDoc["questionAsker"]
    qURL = qDoc["questionURL"]
    
    # Add the title of the question as a sentence
    entry = { "_id" : id, "postPosition" : "Title", "sentenceAuthor" : qAsker, "questionAsker" : qAsker, "questionURL" : qURL, "sentenceText" : qDoc["questionTitle"] }
    entries.append(entry)
    id += 1

    # Split question text based on either a period, question mark or exclamation mark followed by whitespace or a newline
    qSentences = []
    qSentences.extend(split_string(mergedText))
    

    for sentence in qSentences:
        entry = { "_id" : id, "postPosition" : "Original Post", "sentenceAuthor" : qAsker, "questionAsker" : qAsker, "questionURL" : qURL, "sentenceText" : sentence }
        entries.append(entry)
        id += 1
    
    # Query the replies collection for replies with the same question URL, sort replies by time
    query = { "questionURL" : qURL }
    rCursor = replyCollection.find(query).sort("replyTime")

    postPos = 1

    for rDoc in rCursor:
        rAuthor = rDoc["replyAuthor"]
        
        mergedText = rDoc["replyText"][0]
        n = 1
        while n < len(rDoc["replyText"]):
            mergedText = mergedText + " " + rDoc["replyText"][n]
            n += 1

        rSentences = split_string(mergedText)
        for sentence in rSentences:
            postPosStr = str(postPos)
            
            if qAsker == rAuthor:
                postPosStr = postPosStr + " - op"
            

            entry = { "_id" : id, "postPosition" : postPosStr, "sentenceAuthor" : rAuthor, "questionAsker" : qAsker, "questionURL" : qURL, "sentenceText" : sentence }
            entries.append(entry)
            id += 1
        postPos += 1

    i += 1
    
# --------------------CSV-----------------------------
if _csv:
    with open("sentences.csv", "w", newline='', encoding='utf-8') as sentFile:
        csvHeader = ["_id", "postPosition", "sentenceAuthor", "questionAsker", "questionURL", "sentenceText"]
        writer = csv.DictWriter(sentFile, fieldnames=csvHeader)
        writer.writeheader()
        writer.writerows(entries)
       
# --------------------------LOCAL_DB-------------------------------
if _localDB:
    localClient = pymongo.MongoClient("mongodb://localhost:27017/")
    db = localClient["sentenceDatabase"]
    sentencesCol = db["sentences"]
    sentencesCol.delete_many({})
    ins = sentencesCol.insert_many(entries)



