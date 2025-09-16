import os, json, asyncio, time, statistics, hashlib
from typing import List, Dict
from confluent_kafka import Consumer, Producer
from motor.motor_asyncio import AsyncIOMotorClient

KAFKA = os.getenv("KAFKA_BOOTSTRAP","kafka:9092")
TOPIC_IN = os.getenv("TOPIC_CLAIMS_SCORED","claims.scored")  # from verifier
# Optional: emit final verdict events if you want downstream consumers
TOPIC_OUT = os.getenv("TOPIC_VERDICTS","claims.verdicts")

MONGO_URI = os.getenv("MONGO_URI","mongodb://mongo:27017")
DB = os.getenv("MONGO_DB","misinfo")
COLL_VERIF = os.getenv("COLL_VERIFICATIONS","verifications")
COLL_VERDICTS = os.getenv("COLL_VERDICTS","verdicts")

SUPPORT_K = int(os.getenv("VERDICT_SUPPORT_K","2"))
THRESH_ENTAIL = float(os.getenv("THRESH_ENTAIL","0.70"))
THRESH_CONTRA = float(os.getenv("THRESH_CONTRA","0.70"))
NEUTRAL_BAND = float(os.getenv("NEUTRAL_BAND","0.10"))

def verdict_id(claim_id: str) -> str:
    return hashlib.sha1(f"verdict::{claim_id}".encode()).hexdigest()

def pick_top(items: List[Dict], key: str, k: int, reverse=True) -> List[Dict]:
    return sorted(items, key=lambda x: x.get(key, 0.0), reverse=reverse)[:k]

def compute_verdict(rows: List[Dict]) -> Dict:
    """
    rows: verifications for a claim, each row has scores: {entailment, contradiction, neutral}
    Strategy:
      - avg_entail = mean of entailment probs
      - avg_contra = mean of contradiction probs
      - If avg_entail >= THRESH_ENTAIL and (avg_entail - avg_contra) >= NEUTRAL_BAND -> TRUE
      - Else if avg_contra >= THRESH_CONTRA and (avg_contra - avg_entail) >= NEUTRAL_BAND -> FALSE
      - Else -> UNCERTAIN
      - confidence = max(avg_entail, avg_contra)
    """
    if not rows:
        return {"verdict":"UNCERTAIN","confidence":0.0,"avg_entail":0.0,"avg_contra":0.0}

    entails = [r["scores"].get("entailment",0.0) for r in rows]
    contras = [r["scores"].get("contradiction",0.0) for r in rows]
    avg_entail = statistics.mean(entails) if entails else 0.0
    avg_contra = statistics.mean(contras) if contras else 0.0

    if avg_entail >= THRESH_ENTAIL and (avg_entail - avg_contra) >= NEUTRAL_BAND:
        verdict = "TRUE"
        confidence = avg_entail
    elif avg_contra >= THRESH_CONTRA and (avg_contra - avg_entail) >= NEUTRAL_BAND:
        verdict = "FALSE"
        confidence = avg_contra
    else:
        verdict = "UNCERTAIN"
        confidence = max(avg_entail, avg_contra)

    return {
        "verdict": verdict,
        "confidence": round(float(confidence), 4),
        "avg_entail": round(float(avg_entail), 4),
        "avg_contra": round(float(avg_contra), 4)
    }

async def aggregate_and_store(db, producer, claim_id: str):
    # get all verifications for this claim
    cursor = db[COLL_VERIF].find({"claim_id": claim_id}, {"_id":0})
    rows = [r async for r in cursor]

    # compute verdict
    meta = compute_verdict(rows)

    # pick top supporting and contradicting evidence
    top_support = pick_top(
        [{"evidence_id": r["evidence_id"], "source_title": r.get("source_title"), "source_url": r.get("source_url"),
          "score": r["scores"].get("entailment",0.0)} for r in rows],
        key="score", k=SUPPORT_K, reverse=True
    )
    top_refute = pick_top(
        [{"evidence_id": r["evidence_id"], "source_title": r.get("source_title"), "source_url": r.get("source_url"),
          "score": r["scores"].get("contradiction",0.0)} for r in rows],
        key="score", k=SUPPORT_K, reverse=True
    )

    doc = {
        "verdict_id": verdict_id(claim_id),
        "claim_id": claim_id,
        "verdict": meta["verdict"],
        "confidence": meta["confidence"],
        "avg_entail": meta["avg_entail"],
        "avg_contra": meta["avg_contra"],
        "top_support": top_support,
        "top_refute": top_refute,
        "updated_at": int(time.time())
    }

    # idempotent upsert
    await db[COLL_VERDICTS].update_one(
        {"verdict_id": doc["verdict_id"]},
        {"$set": doc},
        upsert=True
    )

    # optional: publish verdict event
    try:
        producer.produce(TOPIC_OUT, json.dumps(doc).encode("utf-8"), key=claim_id)
        producer.poll(0)
    except Exception:
        pass

async def run():
    db = AsyncIOMotorClient(MONGO_URI)[DB]
    consumer = Consumer({
        "bootstrap.servers": KAFKA,
        "group.id":"scoring-group",
        "auto.offset.reset":"earliest",
        "enable.auto.commit": False
    })
    producer = Producer({"bootstrap.servers": KAFKA})
    consumer.subscribe([TOPIC_IN])
    print("scoring-aggregator listening...")

    try:
        while True:
            msg = consumer.poll(0.5)
            if msg is None:
                await asyncio.sleep(0.05); continue
            if msg.error():
                print("Kafka error:", msg.error()); continue

            payload = json.loads(msg.value())  # {"claim_id": "...", "verifications": [...]}
            claim_id = payload.get("claim_id")
            if claim_id:
                try:
                    await aggregate_and_store(db, producer, claim_id)
                except Exception as e:
                    print("scoring-error:", e)

            consumer.commit(msg)
    finally:
        consumer.close()

if __name__ == "__main__":
    asyncio.run(run())
