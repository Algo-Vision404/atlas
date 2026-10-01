import asyncio
import time
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

from fastapi import FastAPI, Query
from pydantic import BaseModel

from apps.indexer.engine import OpenSearchIndex, QdrantIndex
from libs.core.config import settings
from services.embedding_engine.engine import EmbeddingEngine

keyword_store = OpenSearchIndex()
vector_store = QdrantIndex()
embedding_engine: Optional[EmbeddingEngine] = None

class SearchResult(BaseModel):
    title: str
    url: str
    snippet: Optional[str] = None
    score: float
    rank: int
    source: str

class SearchResponse(BaseModel):
    query: str
    total_results: int
    took_ms: float
    results: List[SearchResult]

def reciprocal_rank_fusion(keyword_results: List[Dict], vector_results: List[Dict], k: int = 60) -> List[Dict]:
    merged: Dict[str, Dict] = {}
    for source, results in (("keyword", keyword_results), ("vector", vector_results)):
        for rank, result in enumerate(results):
            url = result["url"]
            entry = merged.setdefault(url, {**result, "score": 0.0, "sources": set()})
            entry["score"] += 1.0 / (k + rank + 1)
            entry["sources"].add(source)
            if not entry.get("title") and result.get("title"):
                entry["title"] = result["title"]

    ranked = sorted(merged.values(), key=lambda item: item["score"], reverse=True)
    output = []
    for rank, result in enumerate(ranked, 1):
        result["rank"] = rank
        result["source"] = "hybrid" if len(result["sources"]) > 1 else next(iter(result["sources"]))
        result.pop("sources", None)
        output.append(result)
    return output

@asynccontextmanager
async def lifespan(app: FastAPI):
    global embedding_engine
    await asyncio.gather(keyword_store.initialize(), vector_store.initialize())
    embedding_engine = EmbeddingEngine()
    yield
    await asyncio.gather(keyword_store.close(), vector_store.close())

app = FastAPI(title="ATLAS Search API", version=settings.VERSION, lifespan=lifespan)

@app.get("/search/hybrid", response_model=SearchResponse)
async def search_hybrid(q: str = Query(min_length=2), limit: int = Query(default=10, ge=1, le=100)):
    start = time.perf_counter()
    query_vector = await asyncio.to_thread(embedding_engine.encode, q)
    keyword_results, vector_results = await asyncio.gather(
        keyword_store.search(q, limit=min(100, limit * 3)),
        vector_store.search(query_vector, limit=min(100, limit * 3)),
    )
    fused = reciprocal_rank_fusion(keyword_results, vector_results)
    return SearchResponse(
        query=q,
        total_results=len(fused),
        took_ms=round((time.perf_counter() - start) * 1000, 2),
        results=[SearchResult(**item) for item in fused[:limit]],
    )

@app.get("/search/keyword", response_model=SearchResponse)
async def search_keyword(q: str = Query(min_length=2), limit: int = Query(default=10, ge=1, le=100)):
    start = time.perf_counter()
    results = await keyword_store.search(q, limit)
    return SearchResponse(
        query=q, total_results=len(results),
        took_ms=round((time.perf_counter() - start) * 1000, 2),
        results=[SearchResult(**{**r, "rank": i + 1, "source": "keyword"}) for i, r in enumerate(results)],
    )

@app.get("/health")
async def health():
    return {"status": "ok", "version": settings.VERSION, "mock_mode": settings.MOCK_MODE}
