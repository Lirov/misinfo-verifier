import os, json, asyncio, hashlib, time
from typing import List, Dict
from confluent_kafka import Consumer, Producer
from motor.motor_asyncio import AsyncIOMotorClient
from .model import nli_batch

KAFKA = os.getenv("KAFKA_BOOTSTRAP","kafka:9092")
TOPIC_IN = os.getenv("TOPIC_EVIDENCE_CANDIDATES","evidence.candidates")
TOPIC_OUT = os.getenv("TOPIC_CLAIMS_SCORED","claims.scored")

MONGO_URI = os.getenv("MONGO_URI","mongodb://mongo:27017")
DB_NAME = os.getenv("MONGO_DB","misinfo")
COLL_EVIDENCE = os.getenv("COLL_EVIDENCE","evidence")
COLL_VERIF = os.getenv("COLL_VERIFICATIONS","verifications")
BATCH = int(os.getenv("NLI_BATCH","8"))

async def fetch_batch_for_claim(db, claim_id: str, evidence_ids: List[str]) -> List[Dict]:
    # pull the evidence passages from Mongo
    cursor = db[COLL_EVIDENCE].find({"evidence_id": {"$in": evidence_ids}}, {"_id":0})
    return [e async for e in cursor]

def verification_id(claim_id: str, evidence_id: str) -> str:
    return hashlib.sha1(f"{claim_id}|{evidence_id}".encode()).hexdigest()

async def process_chunk(db, producer, claim_id: str, evidences: List[Dict]):
    if not evidences:
        return

    # Prepare NLI inputs
    premises = [e["passage"] for e in evidences]            # evidence text
    hypotheses = [e["claim_sentence"] for e in evidences]   # claim text (hypothesis)

    results = nli_batch(premises, hypotheses)
    now = int(time.time())

    scored = []
    for e, r in zip(evidences, results):
        ver = {
            "verification_id": verification_id(e["claim_id"], e["evidence_id"]),
            "claim_id": e["claim_id"],
            "evidence_id": e["evidence_id"],
            "label": r["label"],                    # entailment / contradiction / neutral
            "scores": r["scores"],                  # probabilities
            "source_title": e.get("source_title"),
            "source_url": e.get("source_url"),
            "created_at": now
        }
        # idempotent upsert
        await db[COLL_VERIF].update_one(
            {"verification_id": ver["verification_id"]},
            {"$set": ver},
            upsert=True
        )
        scored.append({
            "claim_id": e["claim_id"],
            "evidence_id": e["evidence_id"],
            "label": ver["label"],
            "scores": ver["scores"]
        })

    # Emit scored results for the Scoring Aggregator (M5)
    out_msg = {
        "claim_id": claim_id,
        "verifications": scored
    }
    producer.produce(TOPIC_OUT, json.dumps(out_msg).encode("utf-8"), key=claim_id)
    producer.poll(0)

async def run():
    db = AsyncIOMotorClient(MONGO_URI)[DB_NAME]
    consumer = Consumer({
        "bootstrap.servers": KAFKA,
        "group.id":"verifier-group",
        "auto.offset.reset":"earliest",
        "enable.auto.commit": False
    })
    producer = Producer({"bootstrap.servers": KAFKA})
    consumer.subscribe([TOPIC_IN])
    print("verifier listening...")

    try:
        while True:
            msg = consumer.poll(0.5)
            if msg is None:
                await asyncio.sleep(0.05); continue
            if msg.error():
                print("Kafka error:", msg.error()); continue

            # evidence.candidates message format: list of {claim_id, evidence_id} OR a single object
            payload = json.loads(msg.value())
            if isinstance(payload, dict):
                pairs = [payload]
            else:
                pairs = payload

            # Group by claim_id and process in small batches
            by_claim = {}
            for p in pairs:
                by_claim.setdefault(p["claim_id"], []).append(p["evidence_id"])

            for cid, evid_ids in by_claim.items():
                # fetch from Mongo
                evidences = await fetch_batch_for_claim(db, cid, evid_ids)

                # process in chunks of BATCH
                for i in range(0, len(evidences), BATCH):
                    chunk = evidences[i:i+BATCH]
                    try:
                        await process_chunk(db, producer, cid, chunk)
                    except Exception as e:
                        print("verifier-chunk-error:", e)

            consumer.commit(msg)
    finally:
        consumer.close()

if __name__ == "__main__":
    asyncio.run(run())
