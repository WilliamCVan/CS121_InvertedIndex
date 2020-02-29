import sys
from importlib import reload
import memoryIndex
from nltk.stem import PorterStemmer
from time import perf_counter

# https://stackoverflow.com/questions/6687660/keep-persistent-variables-in-memory-between-runs-of-python-script

part1Cache = None
if __name__ == "__main__":
    while True:
        if not part1Cache:
            part1Cache = memoryIndex.readDictionaryIntoMemory() #loads json object (dictionary) into memory

        ps = PorterStemmer()
        query = input("Please enter a query: ")

        #start_time = time.time()
        t1_start = perf_counter()

        word = ps.stem(query) # 'machine'
        print(word)
        print(part1Cache[word])

        #print("--- %.8f seconds ---" % (time.time() - start_time))
        t1_stop = perf_counter()
        print("--- %.8f seconds ---" % (t1_stop - t1_start))

        print("Press enter to re-run the script, CTRL-C to exit")

        sys.stdin.readline()
        reload(memoryIndex)