from multiprocessing import Pool
from pathlib import Path
import os
import re
import json
from bs4 import BeautifulSoup, Comment
import string
import math


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


# Main Functions (aka functions called in __main__)

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

    unrankedDocList = reduceResult(listOfSets, folderPath)
    return calculateTFIDF(queryList, unrankedDocList, folderPath)


# Helper Functions (aka functions called by other functions)

# Returns unique set of file urls from hasthtable.txt
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


# Calculate TF-IDF scores for each token in query, use for ranking documents
def calculateTFIDF(queryList, unrankedDocList, folderPath):
    indexFile = open(os.path.join(folderPath, "index.txt"), 'r')
    hashtableFile = open(os.path.join(folderPath, "hashtable.txt"), 'r')
    hashtable = json.load(hashtableFile)
    N = 13518180 # N = len(indexTxt.readlines()) 
    
    # Create dict of {key=docURL : value=TF-IDF score}
    tfidfDict = dict.fromkeys(unrankedDocList, 0)
    for token in queryList:
        with open(os.path.join(folderPath, queryList[0][0], f"{queryList[0]}.json"), 'r') as jsonFile:
            tokenInfo = json.load(jsonFile)
            # Get TF (Overall Token Frequency) from token's json file in index
            RawTF = tokenInfo["freq"]
            # Get N (Number of docs the token is in) from token's json file in index (not right??)
            #N = len(tokenInfo["listDocIDs"])

        #Create a "temporary" dict of {key=docURL : value=frequency of token in this doc}
        docFreqDict = dict.fromkeys(unrankedDocList, 0)
        # Return to top of file, if not already there
        indexFile.seek(0)
        for line in indexFile.readlines():
            # Get the frequency of this token for this specific document
            # Have to go line-by-line in index.txt to find them, but only once per token
            line = str(line)
            if line.startswith(token):
                # Parses Posting from index.txt -> [0] = DocID, [1] = freq for this doc
                indexItems = re.findall(r"\[.*\]", line)[0].strip("][").split(', ')
                currentDocURL = hashtable[indexItems[0]]
                # If current DocID is in our dictionary, add to its freq total
                if currentDocURL in docFreqDict:
                    docFreqDict[currentDocURL] += int(indexItems[1])
        
        #for key, value in docFreqDict.items():   
        #    print(f"freqInDoc '{key}' for {token} = {value}")

        # Calculate TF-IDF score for this document
        for key in tfidfDict:
            if docFreqDict[key] == 0:
                tfidfDict[key] = 0
            else:
                tfidfDict[key] = (1 + math.log(RawTF)) * math.log(N / docFreqDict[key])
    
    # Note: tfidfDict considers the entire query (Works similar to BoolAnd search)
    return tfidfDict



if __name__ == '__main__':
    #####
    # Aljon
    folderPath = "C:\\Users\\aljon\\Documents\\CS_121\\Assignment_3\\CS121_InvertedIndex\\partial_indexes"

    # William
    # folderPath = "C:\\1_Repos\\developer\\partial_indexes"
    #folderPath = "C:\\Anaconda3\\envs\\Projects\\developer\\partial_indexes"

    # Jerome
    #folderPath = "C:\\Users\\arkse\\Desktop\\CS121_InvertedIndex\\DEV"

    # Art
    # windows
    #folderPath = "C:\\Users\\aghar\\Downloads\\DEV"
    # linux
    #folderPath = "/home/anon/Downloads/DEV"
    #####

    listReport2 = [
        "cristina lopes",
        "machine learning",
        "ACM",
        "master of software engineering"
    ]

    #query = input("Enter a search query:")
    for i, query in enumerate(listReport2):
        iCount = 1
        # Sorts results by TF-IDF score before printing
        results = sorted(simpleBoolAnd(query, folderPath).items(), key=lambda x: x[1], reverse=True)
        print(f"\n------------ Top 5 Docs for '{listReport2[i]}' ------------\n")
        # Print top 5 ranked file-urls for given query
        for fileUrl in results:
            if (iCount > 5):
                break
            print(fileUrl)
            iCount += 1
        print(f"\n------------------------------------------------------------\n")

    print("\n------------ DONE! ------------\n")
