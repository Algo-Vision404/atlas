from pathlib import Path
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict

# Resolve .env relative to the repository root so the same configuration is
# picked up no matter which directory a service or the CLI is started from.
ENV_FILE = Path(__file__).resolve().parents[2] / ".env"

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=str(ENV_FILE), extra="ignore", case_sensitive=False)

    PROJECT_NAME: str = "ATLAS"
    VERSION: str = "0.2.0"

    KAFKA_SERVERS: str = "localhost:9092"
    REDIS_URL: str = "redis://localhost:6379/0"
    DATABASE_URL: str = "postgresql://atlas:password@localhost:5432/atlas"

    OPENSEARCH_URL: str = "http://localhost:9200"
    QDRANT_URL: str = "http://localhost:6333"
    NEO4J_URI: str = "bolt://localhost:7687"
    NEO4J_USER: str = "neo4j"
    NEO4J_PASSWORD: str = "password"

    DEFAULT_USER_AGENT: str = "AtlasBot/0.2.0 (+https://atlas-engine.io)"
    CONCURRENT_REQUESTS_PER_DOMAIN: int = 5
    REQUEST_TIMEOUT: int = 30
    POLITENESS_DELAY: float = 0.5
    MAX_RETRIES: int = 3
    MAX_RESPONSE_BYTES: int = 10_000_000
    MAX_CRAWL_DEPTH: int = 3
    RESPECT_ROBOTS_TXT: bool = True
    ROBOTS_CACHE_TTL: int = 3600
    WORKER_LEASE_TTL: int = 120
    # Seconds before an already-crawled URL may be enqueued again (recrawl
    # freshness window). Set to 0 to keep the original permanent-dedupe behaviour.
    RECRAWL_INTERVAL: int = 86400

    # Indexer consumer behaviour: transient failures are retried in-process
    # before the message is routed to the dead-letter topic.
    INDEXER_MAX_ATTEMPTS: int = 3
    INDEXER_RETRY_BACKOFF: float = 2.0

    EMBEDDING_MODEL: str = "sentence-transformers/all-MiniLM-L6-v2"
    EMBEDDING_DIMENSION: int = 384
    OPENAI_API_KEY: Optional[str] = None
    ANTHROPIC_API_KEY: Optional[str] = None

    MAX_REDIRECTS: int = 5
    SEARCH_API_HOST: str = "0.0.0.0"
    SEARCH_API_PORT: int = 8000
    SEARCH_API_URL: str = "http://localhost:8000"
    CRAWLER_API_HOST: str = "0.0.0.0"
    CRAWLER_API_PORT: int = 8001
    CRAWLER_API_URL: str = "http://localhost:8001"

    ATLAS_THEME: str = "cyberpunk"
    LOG_LEVEL: str = "INFO"
    MOCK_MODE: bool = False

settings = Settings()
