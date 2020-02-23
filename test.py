from pathlib import Path

#file = open("C:\\Users\\aghar\Documents\\121_web\\CS121_InvertedIndex\\partial_indexes\\index.txt")
#file = open("t.txt")
file2 = open("t2.txt")

keys = dict()
for k in file2:
    merge = dict()
    k.lower()
    path=k[0]
    file = open(f"C:\\Users\\aghar\\Documents\\121_web\\CS121_InvertedIndex\\partial_indexes\\{path}\\{path}.txt")
    for y in file:
        if k.rstrip() == y.split(" ",1)[0]:
            temp= y.split("[",1)[1]
            docID=temp.split(",",1)[0]
            wordCount = y.split(",",1)[1]
            wordCount = wordCount.split("]",1)[0]
            #print("docID "+docID+" -> wordCount"+wordCount)
            merge[docID] = wordCount
            #print(merge)
            #print(docID.get(docID).show())
    file.close()
    if merge.__len__()!=0:
        keys[k.rstrip()]=merge

output = dict()
for w, d in keys.items():
    for key in d:
        if key in output:
            output[key] +=1
        else:
            output[key] = 1

for k in sorted(output.keys()):
    if output[k]==keys.__len__():
        print(k)