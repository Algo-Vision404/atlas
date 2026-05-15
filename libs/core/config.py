import os
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", 
        extra="ignore", # Ignore extra fields from .env
        case_sensitive=False
    )
    
    # Project Info
    PROJECT_NAME: str = "ATLAS"
    VERSION: str = "0.1.0"
    
    # Infrastructure
    KAFKA_SERVERS: str = "localhost:9092"
    REDIS_URL: str = "redis://localhost:6379/0"
    DATABASE_URL: str = "postgresql://atlas:password@localhost:5432/atlas"
    
    # Search & Indexing
    OPENSEARCH_URL: str = "http://localhost:9200"
    QDRANT_URL: str = "http://localhost:6333"
    NEO4J_URI: str = "bolt://localhost:7687"
    NEO4J_USER: str = "neo4j"
    NEO4J_PASSWORD: str = "password"
    
    # Crawler Settings
    DEFAULT_USER_AGENT: str = "AtlasBot/0.1.0 (+https://atlas-engine.io)"
    CONCURRENT_REQUESTS_PER_DOMAIN: int = 5
    REQUEST_TIMEOUT: int = 30
    
    # NLP Settings
    EMBEDDING_MODEL: str = "sentence-transformers/all-MiniLM-L6-v2"
    OPENAI_API_KEY: Optional[str] = None
    ANTHROPIC_API_KEY: Optional[str] = None
    
    # CLI Settings
    ATLAS_THEME: str = "cyberpunk"
    LOG_LEVEL: str = "INFO"
    MOCK_MODE: bool = True # Enable simulation if infrastructure is offline

settings = Settings()
