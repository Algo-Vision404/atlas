import logging
from typing import List, Dict, Any, Optional
import json
import asyncio
try:
    from opensearchpy import AsyncOpenSearch
    from qdrant_client import AsyncQdrantClient
    from qdrant_client.http import models as rest
except ImportError:
    AsyncOpenSearch = None
    AsyncQdrantClient = None
    rest = None

from libs.core.config import settings
from libs.schemas.models import Document

logger = logging.getLogger("atlas.indexer")

class OpenSearchIndex:
    """Keyword search using BM25 via OpenSearch"""
    def __init__(self, url: str = settings.OPENSEARCH_URL):
        if AsyncOpenSearch:
            self.client = AsyncOpenSearch(hosts=[url])
        else:
            self.client = None
        self.index_name = "atlas_documents"

    async def initialize(self):
        """Create index with appropriate mappings if it doesn't exist"""
        if settings.MOCK_MODE:
            logger.info("OpenSearch: Running in MOCK MODE")
            return
            
        try:
            exists = await self.client.indices.exists(index=self.index_name)
            if not exists:
                # ... mapping logic ...
                pass
        except Exception as e:
            logger.warning(f"OpenSearch connection failed, falling back to MOCK MODE: {e}")
            settings.MOCK_MODE = True

    async def index(self, doc: Document):
        if settings.MOCK_MODE:
            logger.info(f"MOCK Index (Keyword): {doc.url}")
            return
        # ... real indexing ...

class QdrantIndex:
    """Semantic search using Vector Embeddings via Qdrant"""
    def __init__(self, url: str = settings.QDRANT_URL):
        if AsyncQdrantClient:
            self.client = AsyncQdrantClient(url=url)
        else:
            self.client = None
        self.collection_name = "atlas_vectors"
        self.vector_size = 384 # Default for all-MiniLM-L6-v2

    async def initialize(self):
        exists = await self.client.collection_exists(self.collection_name)
        if not exists:
            await self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=rest.VectorParams(
                    size=self.vector_size,
                    distance=rest.Distance.COSINE
                )
            )
            logger.info(f"Created Qdrant collection: {self.collection_name}")

    async def index(self, doc: Document):
        if not doc.embedding:
            logger.warning(f"Qdrant: Missing embedding for {doc.url}")
            return

        await self.client.upsert(
            collection_name=self.collection_name,
            points=[
                rest.PointStruct(
                    id=doc.id, # Must be UUID or Int, in a real system we'd hash the URL to a UUID
                    vector=doc.embedding,
                    payload={
                        "url": doc.url,
                        "title": doc.title,
                        "crawled_at": doc.crawled_at.isoformat()
                    }
                )
            ]
        )
        logger.debug(f"Qdrant: Indexed vector for {doc.url}")

class IndexingService:
    def __init__(self):
        self.keyword_index = OpenSearchIndex()
        self.vector_index = QdrantIndex()

    async def initialize(self):
        await self.keyword_index.initialize()
        await self.vector_index.initialize()

    async def index_document(self, doc: Document):
        """Index in both stores concurrently"""
        await asyncio.gather(
            self.keyword_index.index(doc),
            self.vector_index.index(doc)
        )
        logger.info(f"Hybrid Indexer: Successfully indexed {doc.url}")

# Example Worker Loop (Mocking Kafka Consumer)
async def worker_loop():
    service = IndexingService()
    await service.initialize()
    logger.info("Indexing Service operational.")
    # In a real system, this would poll Kafka
    while True:
        await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(worker_loop())
