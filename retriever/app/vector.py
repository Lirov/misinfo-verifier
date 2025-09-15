import os, re
from typing import List, Tuple
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue
from fastembed import TextEmbedding

QDRANT_URL = os.getenv("QDRANT_URL", "http://qdrant:6333")
COLL = os.getenv("QDRANT_COLLECTION", "wiki_passages")

_model = None
_client = None

def get_model():
    global _model
    if _model is None:
        # Default model produces 384-dim embeddings similar to MiniLM
        _model = TextEmbedding()
    return _model

def get_client():
    global _client
    if _client is None:
        _client = QdrantClient(url=QDRANT_URL)
        # ensure collection exists
        if COLL not in [c.name for c in _client.get_collections().collections]:
            _client.recreate_collection(
                collection_name=COLL,
                vectors_config=VectorParams(size=384, distance=Distance.COSINE),
            )
    return _client

def split_passages(text: str, max_chars: int = 500) -> List[str]:
    # naive split to paragraphs, then chunk
    paras = [p.strip() for p in re.split(r"\n{2,}", text) if p.strip()]
    chunks = []
    for p in paras:
        if len(p) <= max_chars:
            chunks.append(p)
        else:
            # soft split
            for i in range(0, len(p), max_chars):
                chunks.append(p[i:i+max_chars])
    return chunks[:200]  # keep it bounded

def upsert_passages(title: str, url: str, passages: List[str]) -> int:
    client = get_client()
    model = get_model()
    if not passages: return 0
    vecs = [vec for vec in model.embed(passages)]
    points = []
    for i, (chunk, vec) in enumerate(zip(passages, vecs)):
        points.append(PointStruct(
            id=None,
            vector=vec,
            payload={
                "title": title,
                "url": url,
                "chunk_id": i,
                "text": chunk
            }
        ))
    client.upsert(collection_name=COLL, points=points)
    return len(points)

def search_similar(query: str, topk: int = 5, filter_by_title: str | None = None) -> List[Tuple[float, dict]]:
    client = get_client()
    model = get_model()
    qvec = [vec for vec in model.embed([query])][0]
    query_filter = None
    if filter_by_title:
        query_filter = Filter(must=[FieldCondition(key="title", match=MatchValue(value=filter_by_title))])
    res = client.search(collection_name=COLL, query_vector=qvec, limit=topk, query_filter=query_filter)
    return [(hit.score, hit.payload) for hit in res]
