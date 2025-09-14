import os, hashlib, json
from fastapi import FastAPI, Body
from motor.motor_asyncio import AsyncIOMotorClient
from confluent_kafka import Producer
import redis

app = FastAPI(title="misinfo-gateway")

MONGO_URI = os.getenv("MONGO_URI","mongodb://mongo:27017")
DB_NAME = os.getenv("MONGO_DB","misinfo")
COLL_PAGES = os.getenv("COLL_PAGES","pages")
TOPIC_PAGES_RAW = os.getenv("TOPIC_PAGES_RAW","pages.raw")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP","kafka:9092")
REDIS_URL_SEEN = os.getenv("REDIS_URL_SEEN","redis://redis:6379/1")
SEEN_URLS_KEY = os.getenv("REDIS_SEEN_URLS_KEY","seen:urls")

db = AsyncIOMotorClient(MONGO_URI)[DB_NAME]
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
r_seen = redis.from_url(REDIS_URL_SEEN, decode_responses=True)

def url_key(u:str)->str: return hashlib.sha1(u.encode("utf-8")).hexdigest()

@app.post("/submit-url")
async def submit_url(url: str = Body(..., embed=True)):
    k = url_key(url)
    if r_seen.sismember(SEEN_URLS_KEY, k):
        return {"status":"skipped", "reason":"already-seen"}
    await db[COLL_PAGES].insert_one({"url": url, "status":"queued"})
    producer.produce(TOPIC_PAGES_RAW, json.dumps({"url":url}).encode("utf-8"), key=k)
    producer.poll(0)
    r_seen.sadd(SEEN_URLS_KEY, k)
    return {"status":"enqueued", "url": url}

@app.get("/pages")
async def list_pages(limit: int = 20):
    cursor = db[COLL_PAGES].find({}, {"_id":0}).sort([("_id",-1)]).limit(limit)
    return [doc async for doc in cursor]

@app.get("/health")
async def health():
    await db.command("ping")
    return {"ok": True}
