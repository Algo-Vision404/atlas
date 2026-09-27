from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
from enum import Enum

class CrawlStatus(str, Enum):
    PENDING = "pending"
    QUEUED = "queued"
    CRAWLING = "crawling"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"

class URLMetadata(BaseModel):
    url: str
    depth: int = 0
    priority: float = 1.0
    discovered_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_crawled_at: Optional[datetime] = None
    status: CrawlStatus = CrawlStatus.PENDING
    retry_count: int = 0
    error: Optional[str] = None
    job_id: Optional[str] = None

class Document(BaseModel):
    id: str
    url: str
    title: Optional[str] = None
    content: str
    text_clean: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    links: List[str] = Field(default_factory=list)
    images: List[str] = Field(default_factory=list)
    language: Optional[str] = None
    mime_type: str = "text/html"
    crawled_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    checksum: str
    embedding: Optional[List[float]] = None

class CrawlJob(BaseModel):
    job_id: str
    seeds: List[str]
    max_depth: int = 3
    max_pages: Optional[int] = None
    domain_whitelist: List[str] = Field(default_factory=list)
    domain_blacklist: List[str] = Field(default_factory=list)
    status: str = "running"
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class EventType(str, Enum):
    URL_DISCOVERED = "url_discovered"
    PAGE_CRAWLED = "page_crawled"
    EXTRACTION_COMPLETED = "extraction_completed"
    INDEXING_COMPLETED = "indexing_completed"
    CRAWL_FAILED = "crawl_failed"

class AtlasEvent(BaseModel):
    event_id: str
    type: EventType
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: Dict[str, Any]
    source: str
