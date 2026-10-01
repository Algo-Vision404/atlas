import hashlib
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

try:
    import redis.asyncio as redis
except ImportError:
    redis = None

from libs.core.config import settings
from libs.schemas.models import CrawlStatus, URLMetadata

logger = logging.getLogger("atlas.frontier")


class URLFrontier:
    """Redis-backed URL frontier for distributed crawler workers."""

    def __init__(self, redis_url: str = settings.REDIS_URL, scan_batch_size: int = 20):
        if redis is None:
            raise RuntimeError("redis package is required for the URL frontier")
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.scan_batch_size = max(1, scan_batch_size)
        self.queue_key = "atlas:frontier:queue"
        self.seen_key = "atlas:frontier:seen"
        self.completed_key = "atlas:frontier:completed"
        self.processing_key = "atlas:frontier:processing"
        self.metadata_prefix = "atlas:url:"
        self.politeness_prefix = "atlas:politeness:"
        self.lease_prefix = "atlas:lease:"
        self.job_active_prefix = "atlas:job:active:"

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

    async def add_urls(
        self,
        urls: List[str],
        depth: int = 0,
        priority: float = 1.0,
        job_id: Optional[str] = None,
    ) -> int:
        if settings.MOCK_MODE:
            valid = [u for u in (self.normalize_url(raw) for raw in urls) if u]
            logger.info("MOCK Frontier: would add %d URLs", len(valid))
            return len(valid)

        added = 0
        for raw_url in urls:
            url = self.normalize_url(raw_url)
            if not url:
                continue
            key = self._key(url)
            if await self.redis.sadd(self.seen_key, key):
                meta = URLMetadata(
                    url=url,
                    depth=depth,
                    priority=priority,
                    status=CrawlStatus.QUEUED,
                    job_id=job_id,
                )
                await self.redis.set(f"{self.metadata_prefix}{key}", meta.model_dump_json())
                if job_id:
                    await self.redis.sadd(f"{self.job_active_prefix}{job_id}", key)
                await self.redis.zadd(self.queue_key, {url: priority})
                added += 1
        return added

    async def get_next_url(self) -> Optional[Tuple[str, int]]:
        if settings.MOCK_MODE:
            return "https://example.com/simulated-url", 1

        candidates = await self.redis.zpopmax(self.queue_key, count=self.scan_batch_size)
        if not candidates:
            return None

        now = time.time()
        requeue: Dict[str, float] = {}
        selected: Optional[Tuple[str, float, str, str, str]] = None

        for url, score in candidates:
            if selected is not None:
                requeue[url] = float(score)
                continue

            host = urlparse(url).netloc.lower()
            next_allowed = await self.redis.get(f"{self.politeness_prefix}{host}")
            if next_allowed and float(next_allowed) > now:
                # Slightly decay score so rate-limited domains do not starve other ready hosts
                requeue[url] = max(0.001, float(score) * 0.99)
                continue

            key = self._key(url)
            meta_json = await self.redis.get(f"{self.metadata_prefix}{key}")
            if not meta_json:
                continue

            selected = (url, float(score), key, meta_json, host)

        if requeue:
            await self.redis.zadd(self.queue_key, requeue)

        if selected is None:
            return None

        url, score, key, meta_json, host = selected
        try:
            meta = URLMetadata.model_validate_json(meta_json)
            meta.status = CrawlStatus.CRAWLING
            await self.redis.sadd(self.processing_key, url)
            await self.redis.set(
                f"{self.lease_prefix}{key}",
                str(now),
                ex=max(1, settings.WORKER_LEASE_TTL),
            )
            await self.redis.set(f"{self.metadata_prefix}{key}", meta.model_dump_json())
            await self.redis.set(
                f"{self.politeness_prefix}{host}",
                str(now + max(0.1, settings.POLITENESS_DELAY)),
                ex=max(1, int(settings.POLITENESS_DELAY) + 1),
            )
            return url, meta.depth
        except Exception:
            await self.redis.zadd(self.queue_key, {url: score})
            raise

    async def get_metadata(self, url: str) -> Optional[URLMetadata]:
        if settings.MOCK_MODE:
            return URLMetadata(url=url, depth=1, status=CrawlStatus.CRAWLING)
        key = self._key(url)
        raw = await self.redis.get(f"{self.metadata_prefix}{key}")
        return URLMetadata.model_validate_json(raw) if raw else None

    async def refresh_lease(self, url: str) -> bool:
        """Extend the worker lease while a URL is still being processed."""
        if settings.MOCK_MODE:
            return True
        key = self._key(url)
        lease_key = f"{self.lease_prefix}{key}"
        if not await self.redis.exists(lease_key):
            return False
        await self.redis.expire(lease_key, max(1, settings.WORKER_LEASE_TTL))
        return True

    async def mark_completed(self, url: str):
        if settings.MOCK_MODE:
            return
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
            if meta.job_id:
                await self.redis.srem(f"{self.job_active_prefix}{meta.job_id}", key)

    async def mark_failed(self, url: str, error: str):
        if settings.MOCK_MODE:
            return
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
            await self.redis.zadd(
                self.queue_key,
                {url: max(0.01, meta.priority * (0.5 ** meta.retry_count))},
            )
        else:
            meta.status = CrawlStatus.FAILED
            if meta.job_id:
                await self.redis.srem(f"{self.job_active_prefix}{meta.job_id}", key)
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
                if meta.job_id:
                    await self.redis.srem(f"{self.job_active_prefix}{meta.job_id}", key)
            else:
                meta.status = CrawlStatus.QUEUED
                meta.error = "worker lease expired; requeued"
                await self.redis.zadd(
                    self.queue_key,
                    {url: max(0.01, meta.priority * (0.5 ** meta.retry_count))},
                )
            await self.redis.set(f"{self.metadata_prefix}{key}", meta.model_dump_json())
            await self.redis.srem(self.processing_key, url)
            recovered += 1
        return recovered

    async def mark_skipped(self, url: str, reason: str):
        """Permanently skip a URL without consuming retry budget."""
        if settings.MOCK_MODE:
            return
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
        if meta.job_id:
            await self.redis.srem(f"{self.job_active_prefix}{meta.job_id}", key)

    async def active_for_job(self, job_id: str) -> int:
        """Return number of queued or currently processing URLs for a crawl job."""
        if settings.MOCK_MODE:
            return 0
        return int(await self.redis.scard(f"{self.job_active_prefix}{job_id}") or 0)

    async def stats(self) -> Dict[str, int]:
        if settings.MOCK_MODE:
            return {"queued": 0, "processing": 0, "completed": 0, "seen": 0}
        queued, processing, completed, seen = (
            await self.redis.zcard(self.queue_key),
            await self.redis.scard(self.processing_key),
            await self.redis.scard(self.completed_key),
            await self.redis.scard(self.seen_key),
        )
        return {
            "queued": int(queued or 0),
            "processing": int(processing or 0),
            "completed": int(completed or 0),
            "seen": int(seen or 0),
        }

    async def close(self):
        if self.redis is not None:
            await self.redis.aclose()
