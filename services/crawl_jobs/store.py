from typing import Optional

import redis.asyncio as redis

from libs.core.config import settings
from libs.schemas.models import CrawlJob


class CrawlJobStore:
    """Persistent Redis state for crawl jobs and page counters."""

    def __init__(self, redis_url: str = settings.REDIS_URL):
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.prefix = "atlas:job:"
        self.pages_prefix = "atlas:job:pages:"

    def _key(self, job_id: str) -> str:
        return f"{self.prefix}{job_id}"

    async def create(self, job: CrawlJob) -> CrawlJob:
        await self.redis.set(self._key(job.job_id), job.model_dump_json())
        await self.redis.set(f"{self.pages_prefix}{job.job_id}", 0)
        return job

    async def get(self, job_id: str) -> Optional[CrawlJob]:
        raw = await self.redis.get(self._key(job_id))
        return CrawlJob.model_validate_json(raw) if raw else None

    async def increment_pages(self, job_id: str) -> int:
        return int(await self.redis.incr(f"{self.pages_prefix}{job_id}"))

    async def pages(self, job_id: str) -> int:
        return int(await self.redis.get(f"{self.pages_prefix}{job_id}") or 0)

    async def update(self, job: CrawlJob) -> None:
        await self.redis.set(self._key(job.job_id), job.model_dump_json())

    async def close(self):
        await self.redis.aclose()
