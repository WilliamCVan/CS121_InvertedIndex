from multiprocessing import Pool
from pathlib import Path
import os
import re
import json
from bs4 import BeautifulSoup, Comment
import string


#initialize datastructures
class Posting:
    def __init__(self, docID, freqCount):
        self.docID = docID
        self.freqCount = freqCount

    def incFreq(self):
        self.freqCount += 1

    def show(self):
        return [self.docID, self.freqCount]


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
        #print(str(directory))
        for files in Path(directory).iterdir():
            if files.is_file():
                fullFilePath = directory / files.name
                listFilePaths.append([iDocID, str(fullFilePath)])
                hashTableIDToUrl[iDocID] = str(fullFilePath)
                iDocID += 1
    #print(listFilePaths)
    #print(hashTableIDToUrl)
    # write json file that maps the docID's to file path urls
    writeHashTableToDisk(hashTableIDToUrl)

    return listFilePaths


def tokenize(fileItem: list) -> dict:
    filePath = fileItem[1]
    docID = fileItem[0]

    with open(filePath, 'r') as content_file:
        textContent = content_file.read()

        jsonOBJ = json.loads(textContent)
        #url = jsonOBJ["url"]
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
        for word in listTemp:
            if (len(word) == 0):  # ignore empty strings
                continue
            if (len(word) > 30):  # ignore words like ivborw0kggoaaaansuheugaaabaaaaaqcamaaaaolq9taaaaw1bmveuaaaacagiahb0bhb0bhr0ahb4chh8dhx8eicifisiukt4djzankywplcwhltkfpl8nn0clpvm9qumvvxu8wnvbrezesepkyxvwzxbpbnjqb3jtcxruc3vvdxhzdnhyehtefjvdf5xtjkv
                continue
            if (word.startswith("'")):  # ignore words that start with '
                continue
            if (len(word) == 1 and word.isalpha()):
                continue
            try:
                int(word)
                word = str(int(word))  # get rid of numbers starting with 0
            except ValueError:
                pass

            if word in tokenDict:
                tokenDict.get(word).incFreq()
            else:
                tokenDict[word] = Posting(docID, 1)

        # write partial indexes to text files ("store on disk")
        buildPartialIndex(tokenDict)

        #partialIndex = "C:\\Users\\arkse\\Desktop\\CS121_InvertedIndex\\partial_indexes"  # NO IDEA HOW TO MAKE THIS THE SAME FOR EVERYONE
        #art
        #partialIndex = "C:\\Users\\aghar\\Documents\\121_web\\CS121_InvertedIndex\\partial_indexes"
        #paths = [f.path for f in os.scandir(partialIndex) if f.is_dir()]

        # merge later

        # # change the code here to save Postings (tdif, frequency count, linkedList of DocID's, etc)
        # return tokenDict


# write json file that maps the docID's to file path urls
def createAlphabetFolders() -> None:
    listAlphabet = list(string.ascii_lowercase)

    if not Path("partial_indexes").exists():
        Path("partial_indexes").mkdir()
    for letter in listAlphabet:
        pathFull = Path("partial_indexes") / letter
        if not pathFull.exists():
            pathFull.mkdir()

    strNumbers = "0123456789"
    listNumbers = list(strNumbers)
    for number in listNumbers:
        pathFull = Path("partial_indexes") / number
        if not pathFull.exists():
            pathFull.mkdir()


# write json file that maps the docID's to file path urls
def writeHashTableToDisk(hashtable: dict) -> None:
    if not Path("partial_indexes").exists():
        Path("partial_indexes").mkdir()
    with open(os.path.join("partial_indexes", "hashtable.txt"), "w+") as hash:
        hash.write(json.dumps(hashtable))


def buildPartialIndex(tokenDict):
    # make subdirectory for partial indexes
    if not os.path.exists("partial_indexes"):
        Path("partial_indexes").mkdir()

    # write text files to store inndexing data for merging later
    with open(os.path.join("partial_indexes", "index.txt"), "a") as partialIndex:
        for key in sorted(tokenDict):
            partialIndex.write(key + " : " + str(tokenDict.get(key).show()) + '\n')


def parseJSONFiles(directoryPath: str) -> int:
    listFilePaths = getAllFilePaths(directoryPath)

    DOC_NUM = len(listFilePaths)    #number of files we will tokenize

    # https://stackoverflow.com/questions/2846653/how-can-i-use-threading-in-python
    # Make the Pool of workers
    pool = Pool(processes=20)
    # Each worker get a directory from list above, and begin tokenizing all json files inside
    results = pool.map(tokenize, listFilePaths)
    # for key in results[0]:
    # print(key, " : ", results[0].get(key).show())

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
                docID = templist[0]
                frequency = templist[1]

                filePathFull = Path("partial_indexes") / firstLetter / (wordToken + ".json")

                if filePathFull.is_file():
                    # If file exists, then we read it out as json
                    with open(filePathFull, "r+") as posting:
                        data = posting.read()
                        jsonObj = json.loads(data)
                        jsonCount = jsonObj["freq"]
                        jsonListPostings = jsonObj["listDocIDs"]

                        newFreq = jsonCount + int(frequency)
                        jsonListPostings.append(docID)

                        # save updated values back to json
                        posting.seek(0) #reset to beginning of file to overwrite
                        jsonObj["freq"] = newFreq
                        jsonObj["listDocIDs"] = sorted(jsonListPostings)
                        posting.write(json.dumps(jsonObj))
                else:
                    jsonObj = {"freq": 0, "listDocIDs": []}
                    newFreq = jsonObj["freq"] + int(frequency)
                    newListID = [docID]

                    # save updated values back to json
                    jsonObj["freq"] = newFreq
                    jsonObj["listDocIDs"] = newListID
                    with open(filePathFull, "w+") as posting:
                        posting.write(json.dumps(jsonObj))
            except:
                continue


#def calculateScores(index,hash):


#Creates subdirectories to store every token's json file.
def subDir(filePath):
    pathDict = dict()
    pathD = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k',
             'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
    for path in pathD:
        pathDict[path[-1]] = []
    # print(pathDict)
    indexTxt = open(os.path.join("partial_indexes", "index.txt"), 'r')

    for line in indexTxt:
        # print(line[0])
        if line[0] in pathDict:
            # print("hi")
            pathDict[line[0]].append(line)

    for path in pathDict.keys():
        # print(path)
        # file = open(Path(f"C:\\Users\\arkse\\Desktop\\CS121_InvertedIndex\\partial_indexes\\{path}\\{path}.txt"),"w+")

        # art windows
        file = open(Path(f"{filePath}\\{path}\\{path}.txt"),"w+")
        # art linux
        # file = open(Path(f"/home/anon/Documents/121/CS121_InvertedIndex/partial_indexes/{path}/{path}.txt"), "w+")

        for line in pathDict[path]:
            # print(f"Writing {line} into file {path}")
            file.write(line)
        file.close()


if __name__ == '__main__':
    # Aljon
    #folderPath = "C:\\Users\\aljon\\Documents\\IndexFiles\\DEV"
    folderPath = "C:\\Users\\aljon\\Documents\\CS_121\\Assignment_3\\DEV"

    # William
    #folderPath = "C:\\Anaconda3\\envs\\Projects\\DEV"

    # Jerome
    #folderPath = "C:\\Users\\arkse\\Desktop\\CS121_InvertedIndex\\DEV"

    # #Art
    # #windows
    # folderPath = "C:\\Users\\aghar\\Downloads\\DEV"
    # #linux
    # #folderPath = "/home/anon/Downloads/DEV"

    createAlphabetFolders()

    #iDocsCount = parseJSONFiles(folderPath)

    mergeTokens()
    # # split into subdirectories after index finishes
    #subDir(folder2)
    print("-----DONE!-----")

