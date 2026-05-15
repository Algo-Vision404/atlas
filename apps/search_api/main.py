from fastapi import FastAPI, Query, HTTPException
from typing import List, Optional, Dict
from pydantic import BaseModel
import uvicorn
import time
import asyncio
from libs.core.config import settings
from apps.indexer.engine import OpenSearchIndex, QdrantIndex
from services.embedding_engine.engine import EmbeddingEngine

app = FastAPI(title="ATLAS Search API", version="0.1.0")

# Initialize backend connections
# In a real app, these would be managed via lifespan events
keyword_store = OpenSearchIndex()
vector_store = QdrantIndex()
embedding_engine = EmbeddingEngine()

class SearchResult(BaseModel):
    title: str
    url: str
    snippet: Optional[str] = None
    score: float
    rank: int
    source: str # 'keyword', 'vector', or 'hybrid'

class SearchResponse(BaseModel):
    query: str
    total_results: int
    took_ms: float
    results: List[SearchResult]

def reciprocal_rank_fusion(
    keyword_results: List[Dict], 
    vector_results: List[Dict], 
    k: int = 60
) -> List[Dict]:
    """Combine results from two different search methods using RRF"""
    scores = {} # url -> score
    
    # Keyword results
    for rank, res in enumerate(keyword_results):
        url = res['url']
        scores[url] = scores.get(url, 0) + 1 / (k + rank + 1)
        
    # Vector results
    for rank, res in enumerate(vector_results):
        url = res['url']
        scores[url] = scores.get(url, 0) + 1 / (k + rank + 1)
        
    # Sort and re-format
    combined = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    
    # Merge original metadata (simplified)
    # In a real system, we'd pull the full document metadata for the top N
    final_results = []
    for i, (url, score) in enumerate(combined):
        final_results.append({
            "url": url,
            "score": score,
            "rank": i + 1,
            "title": "Merged Result", # Simplified
            "source": "hybrid"
        })
    return final_results

@app.get("/search/hybrid")
async def search_hybrid(q: str, limit: int = 10) -> SearchResponse:
    """Combined keyword and semantic search with RRF ranking"""
    start_time = time.time()
    
    # 1. Get embedding for query
    query_vector = embedding_engine.encode(q)
    
    # 2. Parallel Search
    # Note: OpenSearch and Qdrant search methods need to be implemented in apps.indexer.engine
    # For now, we'll assume they return lists of dicts with 'url' and metadata
    
    # Mocking the call to unimplemented search methods for logic flow
    # keyword_task = keyword_store.search(q, limit=limit*2)
    # vector_task = vector_store.search(query_vector, limit=limit*2)
    # k_results, v_results = await asyncio.gather(keyword_task, vector_task)
    
    k_results = [] # Placeholder
    v_results = [] # Placeholder
    
    # 3. Perform Fusion
    fused_results = reciprocal_rank_fusion(k_results, v_results)
    
    took_ms = (time.time() - start_time) * 1000
    
    return SearchResponse(
        query=q,
        total_results=len(fused_results),
        took_ms=round(took_ms, 2),
        results=[SearchResult(**res) for res in fused_results[:limit]]
    )

@app.get("/health")
async def health():
    return {"status": "online", "version": "0.1.0"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
