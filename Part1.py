from pathlib import Path
import os
import re
import json
from bs4 import BeautifulSoup, Comment

def tokenize(text)-> dict:
    dictTEMP = dict()

    # initialize object and pass in html
    soup = BeautifulSoup(text, 'html.parser')

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
        if not word in dictTEMP:
            dictTEMP[word] = 1
        else:
            dictTEMP[word] += 1

    return dictTEMP

def recursiveTokenize(directoryPath):
    """
    Recursively iterates all folders and foreach file, open file and tokenize the content
    """
    #try:
    for files in Path(directoryPath).iterdir():
        if files.is_file():
            directoryPath = Path(directoryPath)
            fullFilePath = directoryPath / files.name

            with open(fullFilePath, 'r') as content_file:
                content = content_file.read()
                jsonOBJ = json.loads(content)

                url = jsonOBJ["url"]
                htmlContent = jsonOBJ["content"]
                print(htmlContent)

                dictTokens = tokenize(htmlContent)


    dir_list = [dI for dI in os.listdir(directoryPath) if Path(Path(directoryPath).joinpath(dI)).is_dir()]
    for directories in dir_list:
        fullSubdirectory = Path(directoryPath) / directories
        recursiveTokenize(fullSubdirectory)
    # except:
    #     print("ERROR")
    #     return False;


if __name__ == '__main__':
    folderPath = "C:\\1_Repos\\CS121_InvertedIndex\\developer"
    recursiveTokenize(folderPath)