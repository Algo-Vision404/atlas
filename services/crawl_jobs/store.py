from typing import Dict, List, Optional

import redis.asyncio as redis

from libs.core.config import settings
from libs.schemas.models import CrawlJob


class CrawlJobStore:
    """Persistent Redis state for crawl jobs and page counters."""

    _mock_jobs: Dict[str, str] = {}
    _mock_pages: Dict[str, int] = {}

    def __init__(self, redis_url: str = settings.REDIS_URL):
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.prefix = "atlas:job:"
        self.pages_prefix = "atlas:job:pages:"
        self.index_key = "atlas:jobs:index"

    def _key(self, job_id: str) -> str:
        return f"{self.prefix}{job_id}"

    async def create(self, job: CrawlJob) -> CrawlJob:
        if settings.MOCK_MODE:
            self._mock_jobs[job.job_id] = job.model_dump_json()
            self._mock_pages[job.job_id] = 0
            return job
        await self.redis.set(self._key(job.job_id), job.model_dump_json())
        await self.redis.set(f"{self.pages_prefix}{job.job_id}", 0)
        await self.redis.sadd(self.index_key, job.job_id)
        return job

    async def get(self, job_id: str) -> Optional[CrawlJob]:
        if settings.MOCK_MODE:
            raw = self._mock_jobs.get(job_id)
            return CrawlJob.model_validate_json(raw) if raw else None
        raw = await self.redis.get(self._key(job_id))
        return CrawlJob.model_validate_json(raw) if raw else None

    async def increment_pages(self, job_id: str) -> int:
        if settings.MOCK_MODE:
            self._mock_pages[job_id] = self._mock_pages.get(job_id, 0) + 1
            return self._mock_pages[job_id]
        return int(await self.redis.incr(f"{self.pages_prefix}{job_id}"))

    async def pages(self, job_id: str) -> int:
        if settings.MOCK_MODE:
            return int(self._mock_pages.get(job_id, 0))
        return int(await self.redis.get(f"{self.pages_prefix}{job_id}") or 0)

    async def update(self, job: CrawlJob) -> None:
        if settings.MOCK_MODE:
            self._mock_jobs[job.job_id] = job.model_dump_json()
            return
        await self.redis.set(self._key(job.job_id), job.model_dump_json())

    async def list_jobs(self, limit: int = 50) -> List[CrawlJob]:
        if settings.MOCK_MODE:
            jobs = [CrawlJob.model_validate_json(raw) for raw in self._mock_jobs.values()]
            jobs.sort(key=lambda j: j.updated_at, reverse=True)
            return jobs[:limit]
        job_ids = await self.redis.smembers(self.index_key)
        jobs: List[CrawlJob] = []
        for job_id in job_ids:
            job = await self.get(job_id)
            if job:
                jobs.append(job)
        jobs.sort(key=lambda j: j.updated_at, reverse=True)
        return jobs[:limit]

    async def close(self):
        if self.redis is not None:
            await self.redis.aclose()
