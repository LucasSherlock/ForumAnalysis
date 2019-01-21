# -*- coding:utf-8 -*-


import pymongo
import re
import csv

#---------------FUNCTIONS-------------------------------------

def get_qURL_by_id(list, id):
    for dict in list:
        if dict["_id"] == id:
            return dict["questionURL"]


def split_sentences(text):
    sentences = []
    for string in text:
        split = re.split(r"[\.\?!][\s\n]", string)
        sentences.extend(split)

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


#------------------------------------QUESTION---------------------------------

# List to hold all entries
entries = []

# List to identify questions
questions = []

qCursor = collection.find({})
id = 0


numQs = int(input("How many questions? "))

i = 0

for qDoc in qCursor:

    # Only take a specified number of questions
    if i >= numQs: 
        break
    

    # Get all text from the question. This includes both the title and the body 
    qText = []
    qText.append(qDoc["questionTitle"])
    qText.extend(qDoc["questionText"])


    # Split question text based on either a period, question mark or exclamation mark followed by whitespace or a newline
    qSentences = split_sentences(qText)
    
    
    # Get the Asker and URL of the question
    qAsker = qDoc["questionAsker"]
    qURL = qDoc["questionURL"]
    qTime = qDoc["questionTime"]

    for sentence in qSentences:
        qEntry = { "_id" : id, "time" : qTime, "isQuestion" : True, "sentenceAuthor" : qAsker, "questionAsker" : qAsker, "questionURL" : qURL, "sentenceText" : sentence }
        entries.append(qEntry)
        id += 1
    

    questions.append({"_id" : i, "questionURL" : qURL})

    i += 1
    
    
    



# -----------------------REPLIES---------------------------------------

qid = 0
while qid < numQs:
    qURL = get_qURL_by_id(questions, qid)

    # Query the replies collection for replies with the same question URL, sort replies by time
    query = { "questionURL" : qURL }
    rCursor = replyCollection.find(query).sort("replyTime")

    
    
    for rDoc in rCursor:
        rAuthor = rDoc["replyAuthor"]
        qAsker = rDoc["questionAsker"]
        rText = rDoc["replyText"]
        rSentences = split_sentences(rText)
        rTime = rDoc["replyTime"]
        for sentence in rSentences:
            rEntry = { "_id" : id, "time" : rTime, "isQuestion" : False, "sentenceAuthor" : rAuthor, "questionAsker" : qAsker, "questionURL" : qURL, "sentenceText" : sentence }
            entries.append(rEntry)
            id += 1
        
    
    qid += 1

# --------------------CSV-----------------------------
if _csv:
    with open("sentences.csv", "w", newline='', encoding='utf-8') as sentFile:
        csvHeader = ["_id", "time", "isQuestion", "sentenceAuthor", "questionAsker", "questionURL", "sentenceText"]
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



