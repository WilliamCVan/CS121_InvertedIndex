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
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet as wn
from nltk import pos_tag
import math
from collections import defaultdict
from functools import lru_cache


tag_map = defaultdict(lambda: wn.NOUN)
tag_map['J'] = wn.ADJ
tag_map['V'] = wn.VERB
tag_map['R'] = wn.ADV

# Data Structures
class Posting:
    def __init__(self, docID, freqCount):
        self.docID = docID
        self.freqCount = freqCount

    def incFreq(self):
        self.freqCount += 1

    def show(self):
        return [self.docID, self.freqCount]


# Main Functions (aka functions called in __main__)

# Creates subdirectories for each integer and letter.
def createPartialIndexes() -> None:
    if not Path("partial_indexes").exists():
        Path("partial_indexes").mkdir()

    for char in list(string.ascii_lowercase):
        pathFull = Path("partial_indexes") / char
        if not pathFull.exists():
            pathFull.mkdir()

    for num in list("0123456789"):
        pathFull = Path("partial_indexes") / num
        if not pathFull.exists():
            pathFull.mkdir()


def parseJSONFiles(directoryPath: str) -> int:
    listFilePaths = getAllFilePaths(directoryPath)

    # DOC_NUM = len(listFilePaths)    #number of files we will tokenize

    # https://stackoverflow.com/questions/2846653/how-can-i-use-threading-in-python
    # Make the Pool of workers
    pool = Pool(processes=20)
    # Each worker get a directory from list above, and begin tokenizing all json files inside
    pool.map(tokenize, listFilePaths)

    # Close the pool and wait for the work to finish
    pool.close()
    pool.join()


# Reads index.txt line by line, then merges the frequencies of each token.
# Finally, it collects a list of DocIDs into a list for each token.
def mergeTokens():
    indexTxt = open(os.path.join("partial_indexes", "index.txt"), 'r')

    for line in indexTxt:
        try:
            arrPosting = line.split(" : ")
            wordToken = arrPosting[0].replace("'", "")
            firstLetter = wordToken[0:1]

            templist = arrPosting[1].replace("[", "").replace("]", "").replace("\n", "").split(", ")
            docID = int(templist[0])
            frequency = templist[1]

            filePathFull = Path("partial_indexes") / firstLetter / (wordToken + ".json")

            # If file exists, then we read it out as json
            if filePathFull.is_file():
                with open(filePathFull, "r+") as posting:
                    data = posting.read()
                    jsonObj = json.loads(data)
                    jsonCount = jsonObj["freq"]
                    jsonListPostings = jsonObj["listDocIDs"]

                    newFreq = jsonCount + int(frequency)
                    jsonListPostings.append(docID)

                    # save updated values back to json
                    posting.seek(0)  # reset to beginning of file to overwrite
                    jsonObj["freq"] = newFreq
                    jsonObj["listDocIDs"] = sorted(jsonListPostings)
                    posting.write(json.dumps(jsonObj))
            else:
                jsonObj = {"freq": 0, "listDocIDs": []}
                newFreq = jsonObj["freq"] + int(frequency)
                newListID = [int(docID)]

                # save updated values back to json
                jsonObj["freq"] = newFreq
                jsonObj["listDocIDs"] = newListID
                with open(filePathFull, "w+") as posting:
                    posting.write(json.dumps(jsonObj))
        except:
            continue

    indexTxt.close()


### Helper Functions (aka functions called by other functions) ###

# Gets all filepaths
def getAllFilePaths(directoryPath: str) -> list:
    filePathsList = list()
    hashTable = dict()

    # create list of all subdirectories that we need to process
    pathParent = Path(directoryPath)
    directory_list = [(pathParent / dI) for dI in os.listdir(directoryPath) if
                      Path(Path(directoryPath).joinpath(dI)).is_dir()]

    # getting all the .json file paths and adding them to a list to be processed by threads calling tokenize()
    for index, directory in enumerate(directory_list):
        for file in Path(directory).iterdir():
            if file.is_file():
                fullFilePath = directory / file.name
                filePathsList.append([index, str(fullFilePath)])
                hashTable[index] = str(fullFilePath)

    # Writes "hashtable" file that maps the docIDs to filepaths of those documents.
    with open(os.path.join("partial_indexes", "hashtable.txt"), "w+") as hash:
        hash.write(json.dumps(hashTable))

    return filePathsList


def tokenize(fileItem: list) -> dict:
    filePath = fileItem[1]
    docID = int(fileItem[0])

    with open(filePath, 'r') as content_file:
        textContent = content_file.read()

        jsonOBJ = json.loads(textContent)
        htmlContent = jsonOBJ["content"]

        tokenDict = dict()

        # initialize object and pass in html
        soup = BeautifulSoup(htmlContent, 'html.parser')

        # removes comments from text
        for tag in soup(text=lambda text: isinstance(text, Comment)):
            tag.extract()

        # removes javascript and css from text
        for element in soup.findAll(['script', 'style']):
            element.extract()

        # add all tokens found from html response with tags removed
        varTemp = soup.get_text()

        listTemp = re.split(r"[^a-z0-9']+", varTemp.lower())
        #ps = PorterStemmer()
        wnl = WordNetLemmatizer()
        lem = lru_cache(maxsize=75000)(wnl.lemmatize)

        for word in listTemp:
            if (len(word) == 0):  # ignore empty strings
                continue
            if (len(word) > 30):  # ignore words like ivborw0kggoaaaansuheugaaabaaaaaqcamaaaaolq9taaaaw1bmveuaaaacagiahb0bhb0bhr0ahb4chh8dhx8eicifisiukt4djzankywplcwhltkfpl8nn0clpvm9qumvvxu8wnvbrezesepkyxvwzxbpbnjqb3jtcxruc3vvdxhzdnhyehtefjvdf5xtjkv
                continue
            if (word[0] == "'"):  # ignore words that start with '
                continue
            if (len(word) == 1 and word.isalpha()):  # ignore single characters
                continue
            try:
                word = str(int(word))  # get rid of numbers starting with 0 such as 00 or 01 and just stores 0 or 1

            except ValueError:
                pass

            pos = tag_map[pos_tag((word,))[0][1][0]]
            word = lem(word,pos)#ps.stem(word)  # run Porter stemmer on token

            if word in tokenDict:
                tokenDict.get(word).incFreq()
            else:
                tokenDict[word] = Posting(docID, 1)

        # write partial indexes to text files ("store on disk")
        content_file.close()#Got an error opening index.txt so I moved the below out of the with_open and closed content_file to be safe.
        buildIndex(tokenDict)
        # buildMultipleIndexes(tokenDict)
        # merge later
        # change the code here to save Postings (tdif, frequency count, linkedList of DocID's, etc)
        Sreturn tokenDict


def buildIndex(tokenDict):
    # write text files to store indexing data for merging later
    with open(os.path.join("partial_indexes", "index.txt"), "a") as partialIndex:
        for key in sorted(tokenDict):
            partialIndex.write(key + " : " + str(tokenDict.get(key).show()) + '\n')


def buildMultipleIndexes(tokenDict):
    # write separate text files to store data for merging later.
    cutOff = len(tokenDict) // 6  # divide token dict into 5 parts
    sortedTokens = sorted(tokenDict)

    partialIndex = open(os.path.join("partial_indexes", "index1.txt"), "a")
    for val in sortedTokens[:cutOff]:
        partialIndex.write(val + " : " + str(tokenDict.get(val).show()) + '\n')
    partialIndex.close()

    partialIndex = open(os.path.join("partial_indexes", "index2.txt"), "a")
    for val in sortedTokens[cutOff:cutOff * 2]:
        partialIndex.write(val + " : " + str(tokenDict.get(val).show()) + '\n')
    partialIndex.close()

    partialIndex = open(os.path.join("partial_indexes", "index3.txt"), "a")
    for val in sortedTokens[cutOff * 2:cutOff * 3]:
        partialIndex.write(val + " : " + str(tokenDict.get(val).show()) + '\n')
    partialIndex.close()

    partialIndex = open(os.path.join("partial_indexes", "index4.txt"), "a")
    for val in sortedTokens[cutOff * 4:cutOff * 5]:
        partialIndex.write(val + " : " + str(tokenDict.get(val).show()) + '\n')
    partialIndex.close()

    partialIndex = open(os.path.join("partial_indexes", "index5.txt"), "a")
    for val in sortedTokens[cutOff * 5:]:
        partialIndex.write(val + " : " + str(tokenDict.get(val).show()) + '\n')
    partialIndex.close()


def mergeIndexes():
    partialIndex = open(os.path.join("partial_indexes", f"index.txt"), "a")

    partialIndex1 = open(os.path.join("partial_indexes", f"index1.txt"), "a")
    for line in partialIndex1:
        partialIndex.write(line)
    partialIndex1.close()

    partialIndex2 = open(os.path.join("partial_indexes", f"index2.txt"), "a")
    for line in partialIndex2:
        partialIndex.write(line)
    partialIndex2.close()

    partialIndex3 = open(os.path.join("partial_indexes", f"index3.txt"), "a")
    for line in partialIndex3:
        partialIndex.write(line)
    partialIndex3.close()

    partialIndex4 = open(os.path.join("partial_indexes", f"index4.txt"), "a")
    for line in partialIndex4:
        partialIndex.write(line)
    partialIndex4.close()

    partialIndex5 = open(os.path.join("partial_indexes", f"index5.txt"), "a")
    for line in partialIndex5:
        partialIndex.write(line)
    partialIndex5.close()

    partialIndex.close()


if __name__ == '__main__':
    # Aljon - Big laptop
    # folderPath = "C:\\Users\\aljon\\Documents\\IndexFiles\\DEV"
    # Aljon - Small laptop
    # folderPath = "C:\\Users\\aljon\\Documents\\CS_121\\Assignment_3\\DEV"

    # William
    # folderPath = "C:\\Anaconda3\\envs\\Projects\\DEV"

    # Jerome
    folderPath = "C:\\Users\\arkse\\Desktop\\CS121_InvertedIndex\\DEV"

    # Art - windows
    # folderPath = "C:\\Users\\aghar\\Downloads\\DEV"
    # Art - linux
    # folderPath = "/home/anon/Downloads/DEV"

    print("Creating partial index folders...")
    createPartialIndexes()
    print("Parsing JSON files, creating index.txt...")
    parseJSONFiles(folderPath)
    # print("Merge partial indexes")
    # mergeIndexes()
    #print("Merging tokens, organizing files into partial index folders")
    #mergeTokens()
    # print("Calculating TF-IDF scores for each token...")
    # calculateTFIDF()
    print("-----DONE!-----")
