import os, hashlib, json
from fastapi import FastAPI, Body, Query
from motor.motor_asyncio import AsyncIOMotorClient
from confluent_kafka import Producer
import redis

app = FastAPI(title="misinfo-gateway")

MONGO_URI = os.getenv("MONGO_URI","mongodb://mongo:27017")
DB_NAME = os.getenv("MONGO_DB","misinfo")
COLL_PAGES = os.getenv("COLL_PAGES","pages")
COLL_CLAIMS = os.getenv("COLL_CLAIMS","claims")
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

@app.get("/claims")
async def list_claims(limit: int = 20, domain: str | None = Query(None)):
    q = {}
    if domain:
        q["domain"] = domain
    cursor = db[COLL_CLAIMS].find(q, {"_id":0}).sort([("_id",-1)]).limit(limit)
    return [doc async for doc in cursor]

@app.get("/health")
async def health():
    await db.command("ping")
    return {"ok": True}

@app.get("/verifications/{claim_id}")
async def verifications_for_claim(claim_id: str, limit: int = 10):
    cursor = db[os.getenv("COLL_VERIFICATIONS","verifications")].find(
        {"claim_id": claim_id},
        {"_id":0}
    ).sort("created_at", -1).limit(limit)
    return [doc async for doc in cursor]

@app.get("/claims/{claim_id}")
async def claim_detail(claim_id: str):
    # join claim + verdict for a nice demo payload
    claim = await db[os.getenv("COLL_CLAIMS","claims")].find_one({"claim_id": claim_id}, {"_id":0})
    verdict = await db[os.getenv("COLL_VERDICTS","verdicts")].find_one({"claim_id": claim_id}, {"_id":0})
    if not claim:
        return {"error":"claim_not_found"}
    return {"claim": claim, "verdict": verdict}

@app.get("/claims-with-verdicts")
async def claims_with_verdicts(limit: int = 20):
    pipeline = [
        {"$lookup": {
            "from": os.getenv("COLL_VERDICTS","verdicts"),
            "localField":"claim_id",
            "foreignField":"claim_id",
            "as":"v"
        }},
        {"$unwind": {"path":"$v", "preserveNullAndEmptyArrays": True}},
        {"$project": {"_id":0, "claim_id":1, "sentence":1, "url":1, "domain":1, "lang":1,
                      "verdict":"$v.verdict", "confidence":"$v.confidence"}},
        {"$sort": {"_id": -1}},
        {"$limit": limit}
    ]
    out = []
    async for doc in db[os.getenv("COLL_CLAIMS","claims")].aggregate(pipeline):
        out.append(doc)
    return out

