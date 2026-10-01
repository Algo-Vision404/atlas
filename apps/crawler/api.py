import ipaddress
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from urllib.parse import urlparse

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from libs.core.config import settings
from libs.schemas.models import CrawlJob, CrawlJobStatus
from services.crawl_jobs.store import CrawlJobStore
from services.url_frontier.manager import URLFrontier


class CreateCrawlRequest(BaseModel):
    seeds: list[str] = Field(min_length=1)
    max_depth: int = Field(default=3, ge=0, le=20)
    max_pages: int | None = Field(default=None, ge=1)
    domain_whitelist: list[str] = Field(default_factory=list)
    domain_blacklist: list[str] = Field(default_factory=list)


jobs = CrawlJobStore()
frontier = URLFrontier()


@asynccontextmanager
async def lifespan(_: FastAPI):
    yield
    await jobs.close()
    await frontier.close()


app = FastAPI(title="ATLAS Crawl API", version=settings.VERSION, lifespan=lifespan)


def normalize_domains(domains: list[str]) -> list[str]:
    normalized = []
    for domain in domains:
        value = domain.strip().lower().rstrip(".")
        if value.startswith("*."):
            value = value[2:]
        elif value.startswith("."):
            value = value.lstrip(".")
        if value:
            normalized.append(value)
    return sorted(set(normalized))


def domain_allowed(hostname: str, whitelist: list[str], blacklist: list[str]) -> bool:
    host = hostname.lower().rstrip(".")
    norm_white = normalize_domains(whitelist)
    norm_black = normalize_domains(blacklist)
    if norm_white and not any(host == d or host.endswith("." + d) for d in norm_white):
        return False
    return not any(host == d or host.endswith("." + d) for d in norm_black)


def is_public_hostname_syntax(hostname: str) -> bool:
    host = hostname.lower().rstrip(".")
    if not host or host in {"localhost", "localhost.localdomain"} or host.endswith(".localhost"):
        return False
    try:
        ip = ipaddress.ip_address(host)
        return not (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_multicast
            or ip.is_reserved
            or ip.is_unspecified
        )
    except ValueError:
        return True


@app.post("/crawl", response_model=CrawlJob, status_code=201)
async def create_crawl(request: CreateCrawlRequest):
    whitelist = normalize_domains(request.domain_whitelist)
    blacklist = normalize_domains(request.domain_blacklist)

    normalized = [frontier.normalize_url(seed) for seed in request.seeds]
    seeds = []
    for url in normalized:
        if not url:
            continue
        hostname = urlparse(url).hostname
        if hostname and is_public_hostname_syntax(hostname) and domain_allowed(hostname, whitelist, blacklist):
            seeds.append(url)

    seeds = list(dict.fromkeys(seeds))
    if not seeds:
        raise HTTPException(
            status_code=400,
            detail="No valid public HTTP(S) seeds remain after domain policy validation",
        )

    job = CrawlJob(
        job_id=str(uuid.uuid4()),
        seeds=seeds,
        max_depth=request.max_depth,
        max_pages=request.max_pages,
        domain_whitelist=whitelist,
        domain_blacklist=blacklist,
        status=CrawlJobStatus.QUEUED,
        started_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    await jobs.create(job)
    added = await frontier.add_urls(seeds, depth=0, job_id=job.job_id)
    if added == 0:
        job.status = CrawlJobStatus.FAILED
        job.updated_at = datetime.now(timezone.utc)
        await jobs.update(job)
        raise HTTPException(status_code=503, detail="Unable to enqueue crawl seeds")

    return job


@app.get("/health")
async def health():
    return {"status": "ok", "version": settings.VERSION, "mock_mode": settings.MOCK_MODE}


@app.get("/crawl", response_model=list[CrawlJob])
async def list_crawls(limit: int = 50):
    return await jobs.list_jobs(limit=max(1, min(limit, 200)))


@app.get("/crawl/{job_id}", response_model=CrawlJob)
async def get_crawl(job_id: str):
    job = await jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Crawl job not found")
    return job


@app.get("/crawl/{job_id}/stats")
async def crawl_stats(job_id: str):
    job = await jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Crawl job not found")
    return {
        "job_id": job_id,
        "pages_crawled": await jobs.pages(job_id),
        "active_urls": await frontier.active_for_job(job_id),
        "status": job.status,
    }


if __name__ == "__main__":
    uvicorn.run(app, host=settings.CRAWLER_API_HOST, port=settings.CRAWLER_API_PORT)
