import os, json, asyncio, hashlib
from typing import List
from confluent_kafka import Consumer, Producer
from motor.motor_asyncio import AsyncIOMotorClient
from .wiki import wiki_search, wiki_page_plain
from .vector import split_passages, upsert_passages, search_similar

KAFKA = os.getenv("KAFKA_BOOTSTRAP","kafka:9092")
TOPIC_IN = os.getenv("TOPIC_CLAIMS_EXTRACTED","claims.extracted")
TOPIC_OUT = os.getenv("TOPIC_EVIDENCE_CANDIDATES","evidence.candidates")
MONGO_URI = os.getenv("MONGO_URI","mongodb://mongo:27017")
DB = os.getenv("MONGO_DB","misinfo")
COLL_CLAIMS = os.getenv("COLL_CLAIMS","claims")
COLL_EVIDENCE = os.getenv("COLL_EVIDENCE","evidence")
TOPK = int(os.getenv("RETRIEVER_TOPK","5"))

def evid_id(claim_id: str, url: str, chunk_id: int) -> str:
    return hashlib.sha1(f"{claim_id}|{url}|{chunk_id}".encode()).hexdigest()

async def process_claim(db, producer, claim: dict):
    sentence = claim["sentence"]
    # 1) search Wikipedia for likely pages
    hits = wiki_search(sentence, limit=5)
    titles = [h["title"] for h in hits] if hits else []
    # 2) fetch + index passages for each title (idempotent: Qdrant upsert)
    total_chunks = 0
    for t in titles:
        txt = wiki_page_plain(t)
        if not txt: continue
        url = f"https://en.wikipedia.org/wiki/{t.replace(' ', '_')}"
        chunks = split_passages(txt)
        total_chunks += upsert_passages(t, url, chunks)
    # 3) semantic search top-K passages
    results = search_similar(sentence, topk=TOPK)
    # 4) persist candidates (idempotent upserts) + emit for verifier
    out = []
    for score, payload in results:
        e = {
            "evidence_id": evid_id(claim["claim_id"], payload.get("url", ""), payload.get("chunk_id", -1)),
            "claim_id": claim["claim_id"],
            "claim_sentence": sentence,
            "source_title": payload.get("title"),
            "source_url": payload.get("url"),
            "passage": payload.get("text"),
            "similarity": float(score)
        }
        await db[COLL_EVIDENCE].update_one(
            {"evidence_id": e["evidence_id"]},
            {"$set": e},
            upsert=True
        )
        out.append({"claim_id": e["claim_id"], "evidence_id": e["evidence_id"]})
    if out:
        producer.produce(TOPIC_OUT, json.dumps(out).encode("utf-8"), key=claim["claim_id"])
        producer.poll(0)

async def run():
    db = AsyncIOMotorClient(MONGO_URI)[DB]
    consumer = Consumer({
        "bootstrap.servers": KAFKA,
        "group.id":"retriever-group",
        "auto.offset.reset":"earliest",
        "enable.auto.commit": False
    })
    producer = Producer({"bootstrap.servers": KAFKA})
    consumer.subscribe([TOPIC_IN])
    print("retriever listening...")

    try:
        while True:
            msg = consumer.poll(0.5)
            if msg is None:
                await asyncio.sleep(0.05); continue
            if msg.error():
                print("Kafka error:", msg.error()); continue

            payload = json.loads(msg.value())
            url = payload.get("url")
            # read claims for this page
            async for claim in db[COLL_CLAIMS].find({"url": url}):
                try:
                    await process_claim(db, producer, claim)
                except Exception as e:
                    print("retriever-error:", e)
            consumer.commit(msg)
    finally:
        consumer.close()

if __name__ == "__main__":
    asyncio.run(run())
