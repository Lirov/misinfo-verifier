import os, requests, hashlib
import redis

WIKI_API = os.getenv("WIKI_API", "https://en.wikipedia.org/w/api.php")
WIKI_REST = os.getenv("WIKI_REST", "https://en.wikipedia.org/api/rest_v1/page/plain")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
CACHE_NS = os.getenv("REDIS_CACHE_NS", "cache:wiki:")

r = redis.from_url(REDIS_URL, decode_responses=True)

def _cache_key(kind: str, key: str) -> str:
    return f"{CACHE_NS}{kind}:{hashlib.sha1(key.encode()).hexdigest()}"

def wiki_search(query: str, limit: int = 5):
    ck = _cache_key("search", f"{query}|{limit}")
    cached = r.get(ck)
    if cached: 
        import json; return json.loads(cached)
    params = {
        "action": "query",
        "list": "search",
        "srsearch": query,
        "srlimit": limit,
        "format": "json"
    }
    res = requests.get(WIKI_API, params=params, timeout=10)
    res.raise_for_status()
    data = res.json().get("query", {}).get("search", [])
    r.setex(ck, 60*60, res.text)  # 1h cache
    return data

def wiki_page_plain(title: str) -> str:
    ck = _cache_key("plain", title)
    cached = r.get(ck)
    if cached: return cached
    # Use REST API plain text
    url = f"{WIKI_REST}/{requests.utils.quote(title)}"
    res = requests.get(url, timeout=10, headers={"User-Agent": "misinfo-bot/0.1"})
    if res.status_code == 404: return ""
    res.raise_for_status()
    text = res.text
    r.setex(ck, 60*60*6, text)  # 6h
    return text
