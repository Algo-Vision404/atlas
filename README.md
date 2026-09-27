# ATLAS

ATLAS is a distributed web crawl, indexing, and hybrid-search engine. The 0.2 release focuses on turning the core retrieval pipeline into real, bounded infrastructure rather than a collection of simulated services.

## Architecture

Crawl API -> Redis Job Store + URL Frontier -> Crawler Workers -> Content Parser -> Embedding Engine -> OpenSearch + Qdrant -> Search API

Redpanda is provisioned as the event-bus layer for the next streaming stage, while Neo4j and ClickHouse are available for graph and analytics workloads.

## Current capabilities

- Redis-backed URL deduplication, priority scheduling, crawl-job metadata, and page counters.
- Per-host concurrency limits and politeness delays.
- Worker leases with stale-work recovery and lease refresh during long processing.
- Bounded retries with retry-priority decay.
- Async HTML crawler with streaming response-size limits.
- Public-destination checks that reject loopback, private, link-local, multicast, and reserved addresses.
- robots.txt enforcement with cached host policies.
- Domain whitelist and blacklist controls per crawl job.
- Crawl depth and page-count limits.
- HTML parsing and document normalization.
- Sentence-Transformer embeddings.
- Real OpenSearch BM25 retrieval.
- Real Qdrant vector retrieval.
- Reciprocal Rank Fusion for hybrid retrieval.
- FastAPI search and crawl-job APIs.
- Docker-based infrastructure for local deployment.

## Run locally

Copy `.env.example` to `.env`, then start the required infrastructure:

    docker compose -f infrastructure/docker/docker-compose.yml up -d redis opensearch qdrant
    pip install -e .

Start search:

    python apps/search_api/main.py

Search endpoints:

- GET /health
- GET /search/keyword?q=distributed+systems
- GET /search/hybrid?q=distributed+systems

Start a crawler worker:

    python apps/crawler/engine.py

Start the crawl API:

    uvicorn apps.crawler.api:app --host 127.0.0.1 --port 8001

Create a crawl:

    curl -X POST http://127.0.0.1:8001/crawl ^
      -H "Content-Type: application/json" ^
      -d "{\"seeds\":[\"https://example.com\"],\"max_depth\":2,\"max_pages\":25}"

Inspect a crawl:

    curl http://127.0.0.1:8001/crawl/<job_id>
    curl http://127.0.0.1:8001/crawl/<job_id>/stats

## Operational boundaries

ATLAS is not yet an internet-scale search platform. The remaining major stages are event-driven indexing with Redpanda, production observability, graph extraction/indexing, stronger crawl scheduling, and deployment hardening.

The crawler intentionally refuses non-public network destinations. This is a security boundary, not a guarantee against every possible DNS or redirect abuse case.

Do not deploy the default Docker credentials or unrestricted infrastructure configuration to production.

## Stack

Python 3.11+, FastAPI, Typer, aiohttp, Redis, OpenSearch, Qdrant, Sentence Transformers, Redpanda/Kafka, Neo4j, ClickHouse.
