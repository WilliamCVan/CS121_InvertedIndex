from multiprocessing import Pool
from pathlib import Path
import os
import re
import json
from bs4 import BeautifulSoup, Comment
import string
import nltk
nltk.download('punkt')
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
import math
import utils as util
import hashlib


# Data Structures

# 'Posting' objects store the info we later put into each token.json file in our index
# Initially, each token and Posting we find in each document goes into index.txt
# Postings contain docID (where token was found), RAW frequency (in the document of docID), and tag (from HTML content)
class Posting:
    def __init__(self, docID, frequency, tag):
        self.docID = int(docID)
        self.frequency = int(frequency)
        self.tag = str(tag)

    def incFreq(self):
        self.frequency += 1

    def showData(self):
        return [self.docID, self.frequency, self.tag]



# Main Functions (aka functions called in __main__)

# Creates subdirectories for each integer and letter.
def createPartialIndexes() -> None:
    if not Path("index").exists():
        Path("index").mkdir()
    for char in list(string.ascii_lowercase):
        pathFull = Path("index") / char
        if not pathFull.exists():
            pathFull.mkdir()
    for num in list("0123456789"):
        pathFull = Path("index") / num
        if not pathFull.exists():
            pathFull.mkdir()


# Uses multithreading, tokenizes every document in the "DEV" corpus
def parseJSONFiles(directoryPath: str) -> None:
    filePathsList = getAllFilePaths(directoryPath) #55K+ json files to process

    for filePath in filePathsList:
        tokenize(filePath)
    
    '''
    # https://stackoverflow.com/questions/2846653/how-can-i-use-threading-in-python
    # Make the Pool of workers
    pool = Pool(processes=20)
    # Each worker get a directory from list above, and begin tokenizing all json files inside
    pool.map(tokenize, filePathsList)
    # Close the pool and wait for the work to finish
    pool.close()
    pool.join()
    '''

def urlHashTableBuilder(directoryPath) -> None:
    uniqueURLDict = dict()  #holds docID : url
    dupeURLDict = dict()
    hashSets = set()

    filePathsList = getAllFilePaths(directoryPath)  # 55K+ json files to process

    for fileObj in filePathsList:
        filePath = fileObj[1]
        docID = int(fileObj[0])

        with open(filePath, 'r') as content_file:
            textContent = content_file.read()
            jsonOBJ = json.loads(textContent)
            htmlContent = jsonOBJ["content"]
            urlContent = jsonOBJ["url"]

            # initialize BeautifulSoup object and pass in html content
            soup = BeautifulSoup(htmlContent, 'html.parser')

            # return if html text has identical hash
            # add all tokens found from html response with tags removed
            varTemp = soup.get_text()
            hashOut = hashlib.md5(varTemp.encode('utf-8')).hexdigest()
            if hashOut in hashSets:
                dupeURLDict[docID] = urlContent
                continue

            # add unique url to redis
            uniqueURLDict[docID] = urlContent

            #add hash to set
            hashSets.add(hashOut)

    with open(os.path.join("C:\\Anaconda3\\envs\\Projects\\developer", "hashurls.txt"), "w") as hash:
        hash.write(json.dumps(uniqueURLDict))
    with open(os.path.join("C:\\Anaconda3\\envs\\Projects\\developer", "sameurls.txt"), "w") as hash:
        hash.write(json.dumps(dupeURLDict))

# Reads index.txt line by line, then sums the frequencies of each token.
# Next, it collects a list of DocIDs into a list for each token.
# Finally, it adds the tag the token had originally
def mergeTokens():
    indexTxt = open(os.path.join("index", "index.txt"), 'r')

    for line in indexTxt:
        try:
            # Convert raw text in this line into Posting data
            posting = line.split(" : ")
            token = posting[0].replace("'", "")
            postingList = posting[1].replace("[", "").replace("]", "").replace("\n", "").split(", ")

            # Collect data about the token from this line
            newDocID = int(postingList[0])
            newFreq = int(postingList[1])
            newTag = str(postingList[2].strip("'"))

            # Create a new filename and filepath for this token
            firstLetter = token[0:1]
            filePathFull = Path("index") / firstLetter / (token + ".json")

            # If file already exists, then we read it and update it
            if filePathFull.is_file():
                with open(filePathFull, "r+") as posting:
                    # Add to the existing data and save updated values back to json
                    data = posting.read()
                    jsonObj = json.loads(data)
                    jsonObj["freq"] += newFreq
                    jsonObj["docIDList"].append([newDocID, newTag])
                    jsonObj["docIDList"] = sorted(jsonObj["docIDList"])
                    posting.seek(0)  # reset to beginning of file to overwrite
                    posting.write(json.dumps(jsonObj))
                    posting.truncate()

            else:
                # Otherwise, write it from scratch
                jsonObj = {"freq": newFreq, "docIDList": [[newDocID, newTag]]}
                with open(filePathFull, "w+") as posting:
                    posting.write(json.dumps(jsonObj))
        except Exception as e:
            #print(e)
            continue


# Calculate TF-IDF scores for a given (token:document) in index.txt and 'DEV' corpus
def calculateTFIDF(token, unrankedDocList, folderPath):
    indexFile = open(os.path.join(folderPath, "index.txt"), 'r')
    hashtableFile = open(os.path.join(folderPath, "hashtable.txt"), 'r')
    hashtable = json.load(hashtableFile)
    N = 13518180 # N = len(indexTxt.readlines()) 
    
    # Create dict of {key=docURL : value=TF-IDF score}
    tfidfDict = dict.fromkeys(unrankedDocList, 0)
    for token in queryList:
        with open(os.path.join(folderPath, queryList[0][0], f"{queryList[0]}.json"), 'r') as jsonFile:
            tokenInfo = json.load(jsonFile)
            # Get RAW TF (Token Frequency in all docs) from token's json file in index
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

        # Calculate TF-IDF score for this document
        for key in tfidfDict:
            if docFreqDict[key] == 0:
                tfidfDict[key] = 0
            else:
                tfidfDict[key] = (1 + math.log(RawTF)) * math.log(N / docFreqDict[key])
    
    # Note: tfidfDict considers the entire query (Works similar to BoolAnd search)
    return tfidfDict


### Helper Functions (aka functions called by other functions) ###

# Gets all filepaths
def getAllFilePaths(directoryPath: str) -> list:
    listFilePaths = list()
    hashTableIDToUrl = dict()

    # create list of all subdirectories that we need to process
    pathParent = Path(directoryPath)
    directory_list = [(pathParent / dI) for dI in os.listdir(directoryPath) if
                      Path(Path(directoryPath).joinpath(dI)).is_dir()]

    iDocID = 0
    # getting all the .json file paths and adding them to a list to be processed by threads calling tokenize()
    # also creates a hashtable that maps docID => filepath urls
    for directory in directory_list:
        # print(str(directory))
        for files in Path(directory).iterdir():
            if files.is_file():
                fullFilePath = directory / files.name
                listFilePaths.append([iDocID, str(fullFilePath)])
                hashTableIDToUrl[iDocID] = str(fullFilePath)
                iDocID += 1

    # Writes "hashtable" file that maps the docIDs to filepaths of those documents.
    with open(os.path.join("index", "hashtable.txt"), "w+") as hash:
        hash.write(json.dumps(hashTableIDToUrl))

    return listFilePaths


# Tokenizes and collects data from one json file from the "DEV" corpus
def tokenize(fileItem: list) -> None:
    ps = PorterStemmer()
    tokenDict = dict()
    filePath = fileItem[1]
    docID = int(fileItem[0])

    with open(filePath, 'r') as content_file:
        textContent = content_file.read()
        jsonOBJ = json.loads(textContent)
        htmlContent = jsonOBJ["content"]

        # initialize BeautifulSoup object and pass in html content
        soup = BeautifulSoup(htmlContent, 'html.parser')

        # Deletes HTML comments, javascript, and css from text
        for tag in soup(text=lambda text: isinstance(text, Comment)):
            tag.extract()
        for element in soup.findAll(['script', 'style']):
            element.extract()

        # Collect all words found from html response WITH TAGS IN A TUPLE WITH EACH WORD ('word', 'tag')
        # Tags below are in order of importance/weight
        tagNamesList = ['title', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong', 'b', 'a', 'p', 'span', 'div']
        tagsTextList = []
        for tag in tagNamesList:
            tagsTextList.append(soup.find_all(tag))


        ##### REDIS ONLY START #####
        urlContent = jsonOBJ["url"]

        # return if html text has identical hash
        # add all tokens found from html response with tags removed
        varTemp = soup.get_text()
        if util.isHashSame(varTemp):
            util.addDuplicateURL(docID, urlContent)
            return

        # add unique url to redis
        util.addUniqueURL(docID, urlContent)
        ##### REDIS ONLY END #####


        taggedTextDict = dict()
        for i, tagSubList in enumerate(tagsTextList):
            taggedTextDict[tagNamesList[i]] = list()
            for phrase in tagSubList:
                for word in re.split(r"[^a-z0-9']+", phrase.get_text().lower()):
                    taggedTextDict.get(tagNamesList[i]).append(word)

        # Store words as tokens in tokenDict, ignore words that are bad
        for tag, wordList in taggedTextDict.items():
            for word in wordList:
                if (len(word) == 0):  # ignore empty strings
                    continue
                if (len(word) > 30 and tag != 'a'):  # ignore words like ivborw0kggoaaaansuheugaaabaaaaaqcamaaaaolq9taaaaw1bmveuaaaacagiahb0bhb0bhr0ahb4chh8dhx8eicifisiukt4djzankywplcwhltkfpl8nn0clpvm9qumvvxu8wnvbrezesepkyxvwzxbpbnjqb3jtcxruc3vvdxhzdnhyehtefjvdf5xtjkv
                    continue # But accept any URLs that may be large
                if (word[0] == "'"):  # ignore words that start with '
                    continue
                if (len(word) == 1 and word.isalpha()): # ignore single characters
                    continue

                # Run Porter stemmer on token
                word = ps.stem(word)
                if word in tokenDict:
                    tokenDict.get(word).incFreq()
                else:
                    tokenDict[word] = Posting(docID, 1, tag)

        # Write tokens and their Postings to a text file ("store on disk")
        buildIndex(tokenDict)
        #buildMultipleIndexes(tokenDict)
        #return tokenDict
        

def buildIndex(tokenDict : dict) -> None:
    # Write index.txt to store indexing data for merging later
    with open(os.path.join("index", "index.txt"), "a") as partialIndex:
        for key in sorted(tokenDict):
            partialIndex.write(key + " : " + str(tokenDict.get(key).showData()) + '\n')


if __name__ == '__main__':
    # Aljon - Big laptop
    #folderPath = "C:\\Users\\aljon\\Documents\\IndexFiles\\DEV"
    # Aljon - Small laptop
    #folderPath = "C:\\Users\\aljon\\Documents\\CS_121\\Assignment_3\\DEV"

    # # William
    # folderPath = "C:\\Anaconda3\\envs\\Projects\\developer\\DEV"
    # urlHashTableBuilder(folderPath)

    # Jerome
    #folderPath = "C:\\Users\\arkse\\Desktop\\CS121_InvertedIndex\\DEV"

    # Art - windows
    #folderPath = "C:\\Users\\aghar\\Downloads\\DEV"
    # Art - linux
    #folderPath = "/home/anon/Downloads/DEV"


    #print("Creating partial index folders...")
    #createPartialIndexes()

    #print("Parsing 'DEV' JSON files, building index.txt...")
    #parseJSONFiles(folderPath)

    #print("Merging tokens from index.txt, storing token.JSON files into index...")
    #mergeTokens()

    # Note: Calculating TF-IDF has to be done AFTER mergeTokens()
    # Because it needs the full frequency for each token
    #print("Calculating TF-IDF scores for each token...")
    #calculateTFIDF()

    print("-----DONE!-----")
