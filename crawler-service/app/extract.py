import trafilatura, requests

def fetch_and_extract(url: str) -> dict:
    # simple fetch with timeouts
    resp = requests.get(url, timeout=10, headers={"User-Agent":"misinfo-bot/0.1"})
    resp.raise_for_status()
    extracted = trafilatura.extract(resp.text, include_comments=False, include_tables=False)
    return {
        "url": url,
        "status_code": resp.status_code,
        "ok": bool(extracted),
        "text": extracted or "",
    }
