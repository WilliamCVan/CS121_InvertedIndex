from multiprocessing import Pool
from pathlib import Path
import os
import re
import json
from bs4 import BeautifulSoup, Comment
import string


#DOC_COUNT = 55393

# Takes in query as str. Returns list of docs that match the AND query
def simpleBoolAnd(query):
    results = list()
    for q in queryList:
        q = q.lower()

    for i, word in enumerate(query):
        charPath = query[i][0] #Get 1st char of current word, use to find subdir

        #####
        # ???
        #subdir = f'C:\\Users\\aghar\\Documents\\121_web\\CS121_InvertedIndex\\partial_indexes\\{charPath}'

        # Aljon
        subdir = f'C:\\Users\\aljon\\Documents\\CS121_InvertedIndex\\partial_indexes\\{charPath}'
        #####

        # Open directory with correct first character that has our indexed json files.
        for filename in os.listdir(subdir):
            if filename.split('.')[0] == word:
                with open(os.path.join(subdir, filename), 'r') as jsonData:
                    results.append(json.load(jsonData))
    return results

    '''
    keys = dict()
    for k in file2:
        merge = dict()
        k.lower()
        path = k[0]

        for y in file:
            if k.rstrip() == y.split(" ", 1)[0]:
                temp = y.split("[", 1)[1]
                docID = temp.split(",", 1)[0]
                wordCount = y.split(",", 1)[1]
                wordCount = wordCount.split("]", 1)[0]
                # print("docID "+docID+" -> wordCount"+wordCount)
                merge[docID] = wordCount
                # print(merge)
                # print(docID.get(docID).show())
        file.close()
        if merge.__len__() != 0:
            keys[k.rstrip()] = merge

    output = dict()
    for d in keys.values():
        for key in d:
            if key in output:
                output[key] += 1
            else:
                output[key] = 1

    for k in sorted(output.keys()):
        if output[k] == keys.__len__():
            print(k)
    '''


#Cleans up query by removing stopwords.
def removeStopwords(query):
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
    queryList = list()
    for word in query:
        if word not in stopWords:
            queryList.append(word)
    return queryList


if __name__ == '__main__':
    #####
    # Aljon
    #folderPath = "C:\\Users\\aljon\\Documents\\IndexFiles\\DEV"

    # William
    #folderPath = "C:\\Anaconda3\\envs\\Projects\\DEV"

    # Jerome
    #folderPath = "C:\\Users\\arkse\\Desktop\\CS121_InvertedIndex\\DEV"

    # Art
    # windows
    #folderPath = "C:\\Users\\aghar\\Downloads\\DEV"
    # linux
    #folderPath = "/home/anon/Downloads/DEV"
    #####

    query = input("Enter a search query:").split()
    queryList = removeStopwords(query)
    print("Final query: " + str(queryList) + "\n")
    results = simpleBoolAnd(query)

    print("\n------------ Full Doc List ------------\n")
    for r in results:
        print(str(r) + "\n")
    print("------------ DONE! ------------\n")