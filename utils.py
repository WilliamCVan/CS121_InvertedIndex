import redis
import hashlib
import os
import json

## connect redis to server on a specific port
r = redis.Redis(host="localhost",port=6379,db=0, decode_responses=True)

HASH_SAME = "hashSame"
UNIQUE_URL = "uniqueURL"

def isHashSame(varTemp) -> bool:
    if r.sismember(HASH_SAME, varTemp):
        return True

    hashOut = hashlib.md5(varTemp.encode('utf-8')).hexdigest()
    r.sadd(HASH_SAME, hashOut)  # add hash of text output to redis set
    return False

def addUniqueURL(docID, url) -> None:
    url = _removeFragment(url)
    r.hset(UNIQUE_URL, docID, url)

def _removeFragment(str):
    str = str.split('#')[0]
    return str

def _writeUrlsToDisk() -> None:
    content = r.hgetall(UNIQUE_URL)
    with open(os.path.join("index", "hashurls.txt"), "w+") as hash:
        hash.write(json.dumps(content))

if __name__ == '__main__':
    _writeUrlsToDisk()