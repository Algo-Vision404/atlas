# ATLAS

ATLAS is a distributed web crawl, indexing, and hybrid-search engine. The 0.2 release focuses on making the core retrieval pipeline real rather than simulating infrastructure.

## Architecture

Seeds -> Redis URL Frontier -> Crawler Workers -> Content Parser -> Embedding Engine -> OpenSearch + Qdrant -> Search API -> RRF Hybrid Search

## Current capabilities

- Redis-backed URL deduplication and priority queue.
- Per-host politeness delay and bounded retries.
- Async crawler with response-size and content-type limits.
- HTML parsing and document normalization.
- Sentence-Transformer embeddings.
- Real OpenSearch BM25 retrieval.
- Real Qdrant vector retrieval.
- Reciprocal Rank Fusion for hybrid retrieval.
- FastAPI search and health endpoints.
- Docker-based infrastructure for local deployment.

## Run locally

Copy .env.example to .env, start infrastructure, then install ATLAS.

docker compose -f infrastructure/docker/docker-compose.yml up -d redis opensearch qdrant
pip install -e .

Start the API with: python apps/search_api/main.py

Endpoints:
- GET /health
- GET /search/keyword?q=distributed+systems
- GET /search/hybrid?q=distributed+systems

Start a crawler worker with: python apps/crawler/engine.py

## Operational boundaries

ATLAS is not yet a complete internet-scale crawler. Kafka/Redpanda event streaming, robots.txt policy enforcement, crawl-job persistence, worker leases, observability, and graph indexing remain explicit next-stage work.

Do not deploy the default Docker credentials or unrestricted crawler configuration to production.

## Stack

Python 3.11+, FastAPI, Typer, aiohttp, Redis, OpenSearch, Qdrant, Sentence Transformers, Redpanda/Kafka, Neo4j.