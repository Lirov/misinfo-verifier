import os, json, time
from confluent_kafka import Consumer, Producer
from motor.motor_asyncio import AsyncIOMotorClient
from extract import fetch_and_extract
import asyncio

KAFKA = os.getenv("KAFKA_BOOTSTRAP","kafka:9092")
TOPIC_IN = os.getenv("TOPIC_PAGES_RAW","pages.raw")
TOPIC_OUT = os.getenv("TOPIC_PAGES_CLEANED","pages.cleaned")
MONGO_URI = os.getenv("MONGO_URI","mongodb://mongo:27017")
DB = os.getenv("MONGO_DB","misinfo")
COLL_PAGES = os.getenv("COLL_PAGES","pages")

async def run():
    db = AsyncIOMotorClient(MONGO_URI)[DB]
    consumer = Consumer({
        "bootstrap.servers": KAFKA,
        "group.id":"crawler-group",
        "auto.offset.reset":"earliest",
        "enable.auto.commit": False
    })
    producer = Producer({"bootstrap.servers": KAFKA})
    consumer.subscribe([TOPIC_IN])
    print("crawler-service listening...")

    try:
        while True:
            msg = consumer.poll(0.5)
            if msg is None:
                await asyncio.sleep(0.05); continue
            if msg.error():
                print("Kafka error:", msg.error()); continue
            payload = json.loads(msg.value())
            url = payload["url"]
            try:
                result = fetch_and_extract(url)
                doc = {
                    "url": url,
                    "status": "fetched",
                    "status_code": result["status_code"],
                    "ok": result["ok"],
                    "text": result["text"],
                }
                await db[COLL_PAGES].update_one({"url": url}, {"$set": doc}, upsert=True)
                # forward for normalization
                producer.produce(TOPIC_OUT, json.dumps({"url":url}).encode("utf-8"), key=msg.key())
                producer.poll(0)
                consumer.commit(msg)
            except Exception as e:
                print("fetch-error", url, e)
                await db[COLL_PAGES].update_one({"url": url}, {"$set": {"status":"error","error":str(e)}})
                consumer.commit(msg)
    finally:
        consumer.close()

if __name__ == "__main__":
    asyncio.run(run())
