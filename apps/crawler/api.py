import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from libs.schemas.models import CrawlJob
from services.crawl_jobs.store import CrawlJobStore
from services.url_frontier.manager import URLFrontier


class CreateCrawlRequest(BaseModel):
    seeds: list[str] = Field(min_length=1)
    max_depth: int = Field(default=3, ge=0, le=20)
    max_pages: int | None = Field(default=None, ge=1)
    domain_whitelist: list[str] = Field(default_factory=list)
    domain_blacklist: list[str] = Field(default_factory=list)


app = FastAPI(title="ATLAS Crawl API", version="0.2.0")
jobs = CrawlJobStore()
frontier = URLFrontier()


@app.post("/crawl", response_model=CrawlJob, status_code=201)
async def create_crawl(request: CreateCrawlRequest):
    job = CrawlJob(
        job_id=str(uuid.uuid4()),
        seeds=request.seeds,
        max_depth=request.max_depth,
        max_pages=request.max_pages,
        domain_whitelist=request.domain_whitelist,
        domain_blacklist=request.domain_blacklist,
    )
    normalized = [frontier.normalize_url(seed) for seed in request.seeds]
    seeds = [url for url in normalized if url]
    if not seeds:
        raise HTTPException(status_code=400, detail="No valid HTTP(S) seeds supplied")

    job.seeds = seeds
    await jobs.create(job)
    await frontier.add_urls(seeds, depth=0, job_id=job.job_id)
    return job


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
    return {"job_id": job_id, "pages_crawled": await jobs.pages(job_id), "status": job.status}


@app.on_event("shutdown")
async def shutdown():
    await jobs.close()
    await frontier.close()
