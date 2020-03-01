from multiprocessing import Pool
from pathlib import Path
import os
import re
import json
from bs4 import BeautifulSoup, Comment
import string


# returns unique set of file urls from hasthtable.txt
def reduceResult(listOfSets, folderPath):
    listUrls = list()  # holds unique file paths of .json files

    hashSet = None
    hashTablePath = Path(folderPath) / "hashtable.txt"
    with open(hashTablePath, "r") as file:
        data = file.read()
        hashSet = json.loads(data)

    tempSet = None
    for setObjs in listOfSets:
        #we initialize our tempset if first iteration of loop to first set in list
        if tempSet == None:
            tempSet = setObjs
            continue

        tempSet = tempSet.intersection(setObjs)

    for docID in tempSet:
        fileUrl = hashSet[docID]
        listUrls.append(fileUrl)

    return listUrls


# Takes in query as str. Returns list of docs that match the AND query
def simpleBoolAnd(query, folderPath):
    listOfSets = list()
    queryList = set()   #set because if user types in duplicate term, we won't have to open file twice
    tempList = query.strip().lower().replace("'", "").split(" ")

    for word in tempList:
        if word not in stopWords:
            queryList.add(word)

    print("Cleaned query tokens:")
    print(queryList) # query tokens with stopwords removed and replacing apostrohe and lower()

    #convert set to list to enumerate
    queryList = list(queryList)

    for word in queryList:
        charPath = word[0] #Get 1st char of current word, use to find subdir

        # get the full file path of the indexed .json file
        jsonFilePath = str(Path(folderPath) / charPath / word) + ".json"

        try:
            with open(jsonFilePath, "r") as file:
                data = file.read()
                jsonObj = json.loads(data)

                listDocs = jsonObj["listDocIDs"]
                tempSet = set(listDocs)

                listOfSets.append(tempSet)
        except:
            pass

    return reduceResult(listOfSets, folderPath)

#Cleans up query by removing stopwords. Also changes query from str to list<str>
stopWords = {"a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't",
             "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by",
             "can't",
             "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down",
             "during",
             "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having",
             "he", "he'd",
             "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's",
             "i", "i'd", "i'll",
             "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more",
             "most", "mustn't", "my",
             "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours",
             "ourselves", "out", "over",
             "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some",
             "such", "than", "that", "that's",
             "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd",
             "they'll", "they're", "they've",
             "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd",
             "we'll", "we're", "we've", "were", "weren't",
             "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom",
             "why", "why's", "with", "won't", "would", "wouldn't",
             "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"}


if __name__ == '__main__':
    #####
    # Aljon
    #folderPath = "C:\\Users\\aljon\\Documents\\IndexFiles\\DEV"
    #foldePath = "C:\\Users\\aljon\\Documents\\CS_121\\Assignment_3\\DEV"
    # Aljon
    # subdir = f'C:\\Users\\aljon\\Documents\\CS121_InvertedIndex\\partial_indexes\\{charPath}'
    # subdir = f'C:\\Users\\aljon\\Documents\\CS_121\\Assignment_3\\CS121_InvertedIndex\\partial_indexes\\{charPath}'
    #####

    # William
    folderPath = "C:\\1_Repos\\developer\\partial_indexes"

    # Jerome
    #folderPath = "C:\\Users\\arkse\\Desktop\\CS121_InvertedIndex\\DEV"

    # Art
    # windows
    #folderPath = "C:\\Users\\aghar\\Downloads\\DEV"
    # linux
    #folderPath = "/home/anon/Downloads/DEV"
    #####
    #####
    # ???
    # subdir = f'C:\\Users\\aghar\\Documents\\121_web\\CS121_InvertedIndex\\partial_indexes\\{charPath}'

    listReport2 = [
        "cristina lopes",
        "machine learning",
        "ACM",
        "master of software engineering"
    ]

    # query = input("Enter a search query:")

    for query in listReport2:
        iCount = 1
        results = simpleBoolAnd(query, folderPath)
        print("\n------------ Full Doc List ------------\n")

        # PRINT top 5 urls for a query
        for fileUrl in sorted(results):
            if (iCount > 5):
                break
            print(fileUrl)
            iCount += 1

    print("\n------------ DONE! ------------\n")
