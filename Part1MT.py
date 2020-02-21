from multiprocessing.dummy import Pool as ThreadPool
from pathlib import Path
import os
import re
import json
from bs4 import BeautifulSoup, Comment

def getAllFilePaths(directoryPath: str) -> list:
    listFilePaths = list()

    # create list of all subdirectories that we need to process
    pathParent = Path(directoryPath)
    directory_list = [(pathParent / dI) for dI in os.listdir(directoryPath) if
                      Path(Path(directoryPath).joinpath(dI)).is_dir()]

    for directory in directory_list:
        for files in Path(directory).iterdir():
            if files.is_file():
                fullFilePath = directory / files.name

                listFilePaths.append(str(fullFilePath))

        return listFilePaths

def tokenize(filePath: str)-> dict:
        with open(filePath, 'r') as content_file:
            textContent = content_file.read()

            jsonOBJ = json.loads(textContent)
            url = jsonOBJ["url"]
            htmlContent = jsonOBJ["content"]


            dictTEMP = dict()

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
                if(len(word) == 0): #ignore empty strings
                    continue

                if not word in dictTEMP:
                    dictTEMP[word] = 1
                else:
                    dictTEMP[word] += 1

            #change the code here to save Postings (tdif, frequency count, linkedList of DocID's, etc)
            return dictTEMP

def parseJSONFiles(directoryPath):
    listFilePaths = getAllFilePaths(directoryPath)

    # https://stackoverflow.com/questions/2846653/how-can-i-use-threading-in-python
    # Make the Pool of workers
    pool = ThreadPool(20)

    # Each worker get a directory from list above, and begin tokenizing all json files inside
    results = pool.map(tokenize, listFilePaths)

    # Close the pool and wait for the work to finish
    pool.close()
    pool.join()


if __name__ == '__main__':
    folderPath = "C:\\Anaconda3\\envs\\Projects\\DEV"
    parseJSONFiles(folderPath)