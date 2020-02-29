import json
import nltk
nltk.download('punkt')
from nltk.stem import PorterStemmer

def memoryParseIndex():
    indexpath = "C:\\1_Repos\\developer\\partial_indexes\\index.txt"
    dictMAIN = dict()

    with open(indexpath, 'r') as r:
        listLine = r.readlines()
        listLine.sort()

        iLineNumber = 1

        for line in listLine:
            arrPosting = line.split(" : ")
            wordToken = arrPosting[0].replace("'", "")

            ps = PorterStemmer()
            wordToken = ps.stem(wordToken)    # run Porter stemmer on token

            templist = arrPosting[1].replace("[", "").replace("]", "").replace("\n", "").split(", ")
            docID = int(templist[0])
            frequency = templist[1]

            if wordToken in dictMAIN:
                dictionTemp = dictMAIN.get(wordToken)

                newFreq = int(dictionTemp["freq"]) + int(frequency)
                listTemp = dictionTemp["listDocIDs"]
                listTemp.append(docID)

                dictionTemp["freq"] = newFreq
                dictionTemp["listDocIDs"] = listTemp

                dictMAIN[wordToken] = dictionTemp
            else:
                dictionTemp = dict()
                dictionTemp["freq"] = frequency
                dictionTemp["listDocIDs"] = [docID]

                dictMAIN[wordToken] = dictionTemp

            print(iLineNumber)
            iLineNumber += 1

    minipath = "C:\\1_Repos\\developer\\partial_indexes\\mini_index.txt"
    with open(minipath, 'w') as f:
        f.write(json.dumps(dictMAIN))

def readDictionaryIntoMemory():
    minipath = "C:\\1_Repos\\developer\\partial_indexes\\mini_index.txt"

    with open(minipath, 'r') as f:
        output = f.read()
        jsonObj = json.loads(output)
        return jsonObj

    # with open(minipath, 'r') as f:
    #     for line in f:
    #         print(line)

if __name__ == '__main__':
    memoryParseIndex()
    #readDictionaryIntoMemory()