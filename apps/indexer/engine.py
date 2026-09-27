import asyncio
import hashlib
import logging
import uuid
from typing import List, Dict, Any

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
    def __init__(self, url: str = settings.OPENSEARCH_URL):
        self.client = AsyncOpenSearch(hosts=[url]) if AsyncOpenSearch else None
        self.index_name = "atlas_documents"

    async def initialize(self):
        if settings.MOCK_MODE:
            return
        if not self.client:
            raise RuntimeError("opensearch-py is required")
        if not await self.client.indices.exists(index=self.index_name):
            await self.client.indices.create(
                index=self.index_name,
                body={
                    "settings": {"index": {"number_of_shards": 1, "number_of_replicas": 0}},
                    "mappings": {"properties": {
                        "url": {"type": "keyword"},
                        "title": {"type": "text"},
                        "content": {"type": "text"},
                        "text_clean": {"type": "text"},
                        "metadata": {"type": "object", "enabled": True},
                        "crawled_at": {"type": "date"},
                        "checksum": {"type": "keyword"},
                    }},
                },
            )

    async def index(self, doc: Document):
        if settings.MOCK_MODE:
            return
        await self.client.index(index=self.index_name, id=doc.id, body=doc.model_dump(exclude={"embedding"}), refresh=False)

    async def search(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        if settings.MOCK_MODE:
            return []
        response = await self.client.search(
            index=self.index_name,
            body={"size": limit, "query": {"multi_match": {"query": query, "fields": ["title^3", "content", "text_clean"]}}},
        )
        return [
            {"url": hit["_source"]["url"], "title": hit["_source"].get("title") or hit["_source"]["url"],
             "snippet": " ".join(hit["_source"].get("text_clean") or hit["_source"].get("content", "")).strip()[:300],
             "score": float(hit.get("_score") or 0)}
            for hit in response["hits"]["hits"]
        ]

    async def close(self):
        if self.client:
            await self.client.close()

class QdrantIndex:
    def __init__(self, url: str = settings.QDRANT_URL):
        self.client = AsyncQdrantClient(url=url) if AsyncQdrantClient else None
        self.collection_name = "atlas_vectors"
        self.vector_size = 384

    async def initialize(self):
        if settings.MOCK_MODE:
            return
        if not self.client:
            raise RuntimeError("qdrant-client is required")
        if not await self.client.collection_exists(self.collection_name):
            await self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=rest.VectorParams(size=self.vector_size, distance=rest.Distance.COSINE),
            )

    async def index(self, doc: Document):
        if settings.MOCK_MODE or not doc.embedding:
            return
        point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, doc.url))
        await self.client.upsert(
            collection_name=self.collection_name,
            points=[rest.PointStruct(
                id=point_id,
                vector=doc.embedding,
                payload={"url": doc.url, "title": doc.title, "content": doc.text_clean or doc.content, "crawled_at": doc.crawled_at.isoformat()},
            )],
        )

    async def search(self, vector: List[float], limit: int = 20) -> List[Dict[str, Any]]:
        if settings.MOCK_MODE:
            return []
        hits = await self.client.query_points(collection_name=self.collection_name, query=vector, limit=limit, with_payload=True).points
        return [
            {"url": hit.payload.get("url"), "title": hit.payload.get("title") or hit.payload.get("url"),
             "snippet": (hit.payload.get("content") or "")[:300], "score": float(hit.score)}
            for hit in hits
        ]

    async def close(self):
        if self.client:
            await self.client.close()

class IndexingService:
    def __init__(self):
        self.keyword_index = OpenSearchIndex()
        self.vector_index = QdrantIndex()

    async def initialize(self):
        await asyncio.gather(self.keyword_index.initialize(), self.vector_index.initialize())

    async def index_document(self, doc: Document):
        await asyncio.gather(self.keyword_index.index(doc), self.vector_index.index(doc))
