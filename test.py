from pathlib import Path
import nltk
nltk.download('punkt')
import re
from nltk.stem import PorterStemmer


# # Takes in query as str. Returns list of docs that match the AND query
# def simpleBoolAnd(query):
#     results = set()
#     queryList = list()
#     tempList = query.lower().replace("'", "").split(" ")
#
#     for word in tempList:
#         if word not in stopWords:
#             queryList.append(word)
#
#     print("Cleaned query tokens:")
#     print(queryList) # query tokens with stopwords removed and replacing apostrohe and lower()
#
#     for word in enumerate(queryList):
#         charPath = word[0] #Get 1st char of current word, use to find subdir
#
#         #####
#         # ???
#         #subdir = f'C:\\Users\\aghar\\Documents\\121_web\\CS121_InvertedIndex\\partial_indexes\\{charPath}'
#
#         # Aljon
#         subdir = f'C:\\Users\\aljon\\Documents\\CS121_InvertedIndex\\partial_indexes\\{charPath}'
#         #subdir = f'C:\\Users\\aljon\\Documents\\CS_121\\Assignment_3\\CS121_InvertedIndex\\partial_indexes\\{charPath}'
#         #####
#
#         # Open directory with correct first character that has our indexed json files.
#         for filename in os.listdir(subdir):
#             if filename.split('.')[0] == word:
#
#                 #Add list of docIDs for each query term to a set
#                 with open(os.path.join(subdir, filename), 'r') as jsonData:
#                     data = json.load(jsonData)
#                     print(data)
#                     for docID in data['listDocIDs']:
#                         results.add(int(docID))
#     return results

# '''
#   keys = dict()
#   for k in file2:
#       merge = dict()
#       k.lower()
#       path = k[0]
#
#       for y in file:
#           if k.rstrip() == y.split(" ", 1)[0]:
#               temp = y.split("[", 1)[1]
#               docID = temp.split(",", 1)[0]
#               wordCount = y.split(",", 1)[1]
#               wordCount = wordCount.split("]", 1)[0]
#               # print("docID "+docID+" -> wordCount"+wordCount)
#               merge[docID] = wordCount
#               # print(merge)
#               # print(docID.get(docID).show())
#       file.close()
#       if merge.__len__() != 0:
#           keys[k.rstrip()] = merge
#
#   output = dict()
#   for d in keys.values():
#       for key in d:
#           if key in output:
#               output[key] += 1
#           else:
#               output[key] = 1
#
#   for k in sorted(output.keys()):
#       if output[k] == keys.__len__():
#           print(k)
#   '''

# #file = open("C:\\Users\\aghar\Documents\\121_web\\CS121_InvertedIndex\\partial_indexes\\index.txt")
# #file = open("t.txt")
# file2 = open("t2.txt")
#
# keys = dict()
# for k in file2:
#     merge = dict()
#     k.lower()
#     path=k[0]
#     file = open(f"C:\\Users\\aghar\\Documents\\121_web\\CS121_InvertedIndex\\partial_indexes\\{path}\\{path}.txt")
#     for y in file:
#         if k.rstrip() == y.split(" ",1)[0]:
#             temp= y.split("[",1)[1]
#             docID=temp.split(",",1)[0]
#             wordCount = y.split(",",1)[1]
#             wordCount = wordCount.split("]",1)[0]
#             #print("docID "+docID+" -> wordCount"+wordCount)
#             merge[docID] = wordCount
#             #print(merge)
#             #print(docID.get(docID).show())
#     file.close()
#     if merge.__len__()!=0:
#         keys[k.rstrip()]=merge
#
# output = dict()
# for w, d in keys.items():
#     for key in d:
#         if key in output:
#             output[key] +=1
#         else:
#             output[key] = 1
#
# for k in sorted(output.keys()):
#     if output[k]==keys.__len__():
#         print(k)

def PorterStemSentence():
    sampleText = """It is important to by very pythonly while you are pythoning with python. 
    All pythoners have pythoned poorly at least once. don't can't Carl's""".replace("'", "").lower()

    listTemp = re.split(r"[^a-z0-9']+", sampleText)

    ps = PorterStemmer()

    for token in listTemp:
        print(ps.stem(token))

def QueryParse(input):
    for word in input:
        print(word)

    oneSet = set()
    oneSet.add("a")
    oneSet.add("b")
    oneSet.add("c")
    oneSet.add("a")

    listSet = list(oneSet)
    print(listSet)

if __name__ == "__main__":
    #PorterStemSentence()
    QueryParse("multi word input")