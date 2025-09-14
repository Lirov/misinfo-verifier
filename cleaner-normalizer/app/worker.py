import os, json, re, asyncio
from confluent_kafka import Consumer, Producer
from motor.motor_asyncio import AsyncIOMotorClient
from langdetect import detect
import tldextract

KAFKA = os.getenv("KAFKA_BOOTSTRAP","kafka:9092")
TOPIC_IN = os.getenv("TOPIC_PAGES_CLEANED","pages.cleaned")
TOPIC_OUT = os.getenv("TOPIC_CLAIMS_EXTRACTED","claims.extracted")  # next stage
MONGO_URI = os.getenv("MONGO_URI","mongodb://mongo:27017")
DB = os.getenv("MONGO_DB","misinfo")
COLL_PAGES = os.getenv("COLL_PAGES","pages")

def normalize_text(txt: str) -> str:
    txt = re.sub(r"\s+", " ", txt or "").strip()
    return txt

async def run():
    db = AsyncIOMotorClient(MONGO_URI)[DB]
    consumer = Consumer({
        "bootstrap.servers": KAFKA,
        "group.id":"cleaner-group",
        "auto.offset.reset":"earliest",
        "enable.auto.commit": False
    })
    producer = Producer({"bootstrap.servers": KAFKA})
    consumer.subscribe([TOPIC_IN])
    print("cleaner-normalizer listening...")

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
            if not page or not page.get("text"):
                consumer.commit(msg); continue

            text = normalize_text(page["text"])
            try:
                lang = detect(text[:5000]) if text else "unknown"
            except Exception:
                lang = "unknown"

            domain = ".".join([p for p in tldextract.extract(url) if p])
            await db[COLL_PAGES].update_one(
                {"url": url},
                {"$set": {"text_norm": text, "lang": lang, "domain": domain, "status":"normalized"}}
            )
            # pass URL forward; claim-extractor will read from Mongo
            producer.produce(TOPIC_OUT, json.dumps({"url":url}).encode("utf-8"), key=msg.key())
            producer.poll(0)
            consumer.commit(msg)
    finally:
        consumer.close()

if __name__ == "__main__":
    asyncio.run(run())
