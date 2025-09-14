import os, json, re, hashlib, asyncio
from typing import List
from confluent_kafka import Consumer
from motor.motor_asyncio import AsyncIOMotorClient
from nltk.tokenize import sent_tokenize
import nltk

# download punkt data once at container start
nltk.download("punkt_tab", quiet=True) if hasattr(nltk, "download") else None
nltk.download("punkt", quiet=True)

KAFKA = os.getenv("KAFKA_BOOTSTRAP","kafka:9092")
TOPIC_IN = os.getenv("TOPIC_CLAIMS_EXTRACTED","claims.extracted")
MONGO_URI = os.getenv("MONGO_URI","mongodb://mongo:27017")
DB = os.getenv("MONGO_DB","misinfo")
COLL_PAGES = os.getenv("COLL_PAGES","pages")
COLL_CLAIMS = os.getenv("COLL_CLAIMS","claims")

def sent_is_checkable(s: str) -> bool:
    s = s.strip()
    if len(s) < 40 or len(s) > 400:  # avoid tiny or huge
        return False
    # heuristic: has digits or named-entity-like capitalization or assertive verbs
    if re.search(r"\d", s): return True
    if re.search(r"\b(is|are|was|were|has|have|claims|reports|states)\b", s, re.I): return True
    if re.search(r"[A-Z][a-z]+ [A-Z][a-z]+", s): return True  # looks like a name
    return False

def claim_id(url: str, sentence: str) -> str:
    return hashlib.sha256(f"{url}::{sentence}".encode("utf-8")).hexdigest()

async def run():
    db = AsyncIOMotorClient(MONGO_URI)[DB]
    consumer = Consumer({
        "bootstrap.servers": KAFKA,
        "group.id":"claims-group",
        "auto.offset.reset":"earliest",
        "enable.auto.commit": False
    })
    consumer.subscribe([TOPIC_IN])
    print("claim-extractor listening...")

    try:
        while True:
            msg = consumer.poll(0.5)
            if msg is None:
                await asyncio.sleep(0.05); continue
            if msg.error():
                print("Kafka error:", msg.error()); continue

            payload = json.loads(msg.value())
            url = payload["url"]
            page = await db[COLL_PAGES].find_one({"url": url})
            if not page or not page.get("text_norm"):
                consumer.commit(msg); continue

            sentences = sent_tokenize(page["text_norm"])
            claims = []
            for s in sentences:
                if sent_is_checkable(s):
                    cid = claim_id(url, s)
                    claims.append({
                        "claim_id": cid,
                        "url": url,
                        "domain": page.get("domain"),
                        "lang": page.get("lang"),
                        "sentence": s,
                    })

            # upsert claims (idempotent)
            for c in claims:
                await db[COLL_CLAIMS].update_one(
                    {"claim_id": c["claim_id"]},
                    {"$setOnInsert": c},
                    upsert=True
                )

            await db[COLL_PAGES].update_one({"url": url}, {"$set":{"status":"claims_extracted","claims_count": len(claims)}})
            consumer.commit(msg)
    finally:
        consumer.close()

if __name__ == "__main__":
    asyncio.run(run())
