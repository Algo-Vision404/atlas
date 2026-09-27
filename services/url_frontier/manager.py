import hashlib
import logging
import time
from datetime import datetime, timezone
from typing import List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

try:
    import redis.asyncio as redis
except ImportError:
    redis = None

from libs.core.config import settings
from libs.schemas.models import URLMetadata, CrawlStatus

logger = logging.getLogger("atlas.frontier")

class URLFrontier:
    """Redis-backed URL frontier for distributed crawler workers."""

    def __init__(self, redis_url: str = settings.REDIS_URL):
        if redis is None:
            raise RuntimeError("redis package is required for the URL frontier")
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.queue_key = "atlas:frontier:queue"
        self.seen_key = "atlas:frontier:seen"
        self.completed_key = "atlas:frontier:completed"
        self.processing_key = "atlas:frontier:processing"
        self.metadata_prefix = "atlas:url:"
        self.politeness_prefix = "atlas:politeness:"
        self.lease_prefix = "atlas:lease:"

    @staticmethod
    def normalize_url(url: str) -> Optional[str]:
        try:
            parsed = urlparse(url.strip())
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                return None
            normalized = urlunparse(parsed._replace(fragment=""))
            if normalized.endswith("/") and parsed.path in {"", "/"}:
                normalized = normalized.rstrip("/")
            return normalized
        except ValueError:
            return None

    @staticmethod
    def _key(url: str) -> str:
        return hashlib.sha256(url.encode("utf-8")).hexdigest()

    async def add_urls(self, urls: List[str], depth: int = 0, priority: float = 1.0) -> int:
        if settings.MOCK_MODE:
            logger.info("MOCK Frontier: would add %d URLs", len(urls))
            return len(urls)

        added = 0
        for raw_url in urls:
            url = self.normalize_url(raw_url)
            if not url:
                continue
            key = self._key(url)
            if await self.redis.sadd(self.seen_key, key):
                meta = URLMetadata(url=url, depth=depth, priority=priority, status=CrawlStatus.QUEUED)
                await self.redis.set(f"{self.metadata_prefix}{key}", meta.model_dump_json())
                await self.redis.zadd(self.queue_key, {url: priority})
                added += 1
        return added

    async def get_next_url(self) -> Optional[Tuple[str, int]]:
        if settings.MOCK_MODE:
            return "https://example.com/simulated-url", 1

        candidates = await self.redis.zpopmax(self.queue_key, count=1)
        if not candidates:
            return None

        url, score = candidates[0]
        host = urlparse(url).netloc.lower()
        now = time.time()
        next_allowed = await self.redis.get(f"{self.politeness_prefix}{host}")
        if next_allowed and float(next_allowed) > now:
            await self.redis.zadd(self.queue_key, {url: float(score)})
            return None

        key = self._key(url)
        meta_json = await self.redis.get(f"{self.metadata_prefix}{key}")
        if not meta_json:
            return None

        meta = URLMetadata.model_validate_json(meta_json)
        meta.status = CrawlStatus.CRAWLING
        await self.redis.set(f"{self.metadata_prefix}{key}", meta.model_dump_json())
        await self.redis.sadd(self.processing_key, url)
        await self.redis.set(
            f"{self.lease_prefix}{key}",
            str(now),
            ex=max(1, settings.WORKER_LEASE_TTL),
        )
        await self.redis.set(
            f"{self.politeness_prefix}{host}",
            str(now + max(0.1, settings.POLITENESS_DELAY)),
            ex=max(1, int(settings.POLITENESS_DELAY) + 1),
        )
        return url, meta.depth

    async def mark_completed(self, url: str):
        key = self._key(url)
        await self.redis.srem(self.processing_key, url)
        await self.redis.delete(f"{self.lease_prefix}{key}")
        await self.redis.sadd(self.completed_key, key)
        meta_json = await self.redis.get(f"{self.metadata_prefix}{key}")
        if meta_json:
            meta = URLMetadata.model_validate_json(meta_json)
            meta.status = CrawlStatus.COMPLETED
            meta.last_crawled_at = datetime.now(timezone.utc)
            await self.redis.set(f"{self.metadata_prefix}{key}", meta.model_dump_json())

    async def mark_failed(self, url: str, error: str):
        key = self._key(url)
        await self.redis.srem(self.processing_key, url)
        await self.redis.delete(f"{self.lease_prefix}{key}")
        meta_json = await self.redis.get(f"{self.metadata_prefix}{key}")
        if not meta_json:
            return

        meta = URLMetadata.model_validate_json(meta_json)
        meta.error = error[:2000]
        meta.retry_count += 1
        if meta.retry_count < settings.MAX_RETRIES:
            meta.status = CrawlStatus.QUEUED
            await self.redis.zadd(self.queue_key, {url: max(0.01, meta.priority * (0.5 ** meta.retry_count))})
        else:
            meta.status = CrawlStatus.FAILED
        await self.redis.set(f"{self.metadata_prefix}{key}", meta.model_dump_json())

    async def recover_stale(self) -> int:
        """Requeue URLs whose worker lease expired before completion."""
        if settings.MOCK_MODE:
            return 0
        recovered = 0
        urls = await self.redis.smembers(self.processing_key)
        for url in urls:
            key = self._key(url)
            lease = await self.redis.get(f"{self.lease_prefix}{key}")
            if lease is not None:
                continue
            meta_json = await self.redis.get(f"{self.metadata_prefix}{key}")
            if not meta_json:
                await self.redis.srem(self.processing_key, url)
                continue
            meta = URLMetadata.model_validate_json(meta_json)
            meta.retry_count += 1
            if meta.retry_count >= settings.MAX_RETRIES:
                meta.status = CrawlStatus.FAILED
                meta.error = "worker lease expired"
            else:
                meta.status = CrawlStatus.QUEUED
                meta.error = "worker lease expired; requeued"
                await self.redis.zadd(self.queue_key, {url: max(0.01, meta.priority * (0.5 ** meta.retry_count))})
            await self.redis.set(f"{self.metadata_prefix}{key}", meta.model_dump_json())
            await self.redis.srem(self.processing_key, url)
            recovered += 1
        return recovered

    async def mark_skipped(self, url: str, reason: str):
        """Permanently skip a URL without consuming retry budget."""
        key = self._key(url)
        await self.redis.srem(self.processing_key, url)
        await self.redis.delete(f"{self.lease_prefix}{key}")
        meta_json = await self.redis.get(f"{self.metadata_prefix}{key}")
        if not meta_json:
            return
        meta = URLMetadata.model_validate_json(meta_json)
        meta.status = CrawlStatus.SKIPPED
        meta.error = reason[:2000]
        await self.redis.set(f"{self.metadata_prefix}{key}", meta.model_dump_json())

    async def close(self):
        await self.redis.aclose()
