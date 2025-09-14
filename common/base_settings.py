from pydantic_settings import BaseSettings, SettingsConfigDict

class BaseConfig(BaseSettings):
    kafka_bootstrap: str = "kafka:9092"
    mongo_uri: str = "mongodb://mongo:27017"
    mongo_db: str = "misinfo"
    topic_pages_raw: str = "pages.raw"
    topic_pages_cleaned: str = "pages.cleaned"
    topic_claims_extracted: str = "claims.extracted"
    coll_pages: str = "pages"
    coll_claims: str = "claims"
    redis_url: str = "redis://redis:6379/0"
    redis_url_seen: str = "redis://redis:6379/1"
    redis_seen_urls_key: str = "seen:urls"
    redis_seen_claims_key: str = "seen:claims"
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
