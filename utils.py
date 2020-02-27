import redis
import hashlib

## connect redis to server on a specific port
r = redis.Redis(host="localhost",port=6379,db=0, decode_responses=True)

HASH_SAME = "hashSame"
UNIQUE_URL = "uniqueURL"
REDIS_INSTALLED = False

def isHashSame(varTemp) -> bool:
    if r.sismember(HASH_SAME, varTemp):
        return True

    hashOut = hashlib.md5(varTemp.encode('utf-8')).hexdigest()
    r.sadd(HASH_SAME, hashOut)  # add hash of text output to redis set
    return False

def addUniqueURL(url) -> None:
    url = removeFragment(url)
    r.sadd(UNIQUE_URL, url)

def removeFragment(str):
    str = str.split('#')[0]
    return str