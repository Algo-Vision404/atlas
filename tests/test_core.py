import time
from typing import Any, Dict, List, Optional, Set, Tuple

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from apps.crawler.api import app as crawler_app, domain_allowed, normalize_domains
from apps.crawler.engine import CrawlerEngine
from apps.indexer.engine import IndexingService
from apps.indexer.worker import process_message
from apps.search_api.main import app as search_app, reciprocal_rank_fusion
from libs.core.config import settings
from libs.schemas.models import (
    AtlasEvent,
    CrawlJob,
    CrawlJobStatus,
    CrawlStatus,
    Document,
    EventType,
    URLMetadata,
)
from services.content_parser.parser import ContentParser
from services.crawl_jobs.store import CrawlJobStore
from services.event_bus.bus import EventBus
from services.event_bus.topics import EventTopic
from services.url_frontier.manager import URLFrontier


class FakeRedis:
    """In-memory async Redis replacement for deterministic unit testing."""

    def __init__(self):
        self.kv: Dict[str, str] = {}
        self.sets: Dict[str, Set[str]] = {}
        self.zsets: Dict[str, Dict[str, float]] = {}

    async def get(self, key: str) -> Optional[str]:
        return self.kv.get(key)

    async def set(self, key: str, value: Any, ex: Optional[int] = None) -> bool:
        self.kv[key] = str(value)
        return True

    async def delete(self, key: str) -> int:
        return 1 if self.kv.pop(key, None) is not None else 0

    async def exists(self, key: str) -> int:
        return 1 if key in self.kv else 0

    async def expire(self, key: str, seconds: int) -> bool:
        return key in self.kv

    async def incr(self, key: str) -> int:
        val = int(self.kv.get(key, "0")) + 1
        self.kv[key] = str(val)
        return val

    async def sadd(self, key: str, member: str) -> int:
        bucket = self.sets.setdefault(key, set())
        if member in bucket:
            return 0
        bucket.add(member)
        return 1

    async def srem(self, key: str, member: str) -> int:
        bucket = self.sets.setdefault(key, set())
        if member in bucket:
            bucket.remove(member)
            return 1
        return 0

    async def smembers(self, key: str) -> Set[str]:
        return set(self.sets.get(key, set()))

    async def scard(self, key: str) -> int:
        return len(self.sets.get(key, set()))

    async def zadd(self, key: str, mapping: Dict[str, float]) -> int:
        zset = self.zsets.setdefault(key, {})
        added = 0
        for member, score in mapping.items():
            if member not in zset:
                added += 1
            zset[member] = float(score)
        return added

    async def zpopmax(self, key: str, count: int = 1) -> List[Tuple[str, float]]:
        zset = self.zsets.get(key, {})
        if not zset:
            return []
        ordered = sorted(zset.items(), key=lambda item: item[1], reverse=True)
        picked = ordered[:count]
        for member, _ in picked:
            zset.pop(member, None)
        return picked

    async def zcard(self, key: str) -> int:
        return len(self.zsets.get(key, {}))

    async def aclose(self) -> None:
        return None


def test_rrf_merges_results_from_both_retrievers():
    results = reciprocal_rank_fusion(
        [{"url": "https://a.test", "title": "A"}, {"url": "https://b.test", "title": "B"}],
        [{"url": "https://b.test", "title": "B"}, {"url": "https://c.test", "title": "C"}],
    )
    assert [item["url"] for item in results] == ["https://b.test", "https://a.test", "https://c.test"]
    assert results[0]["source"] == "hybrid"


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("https://example.com/a#section", "https://example.com/a"),
        ("https://example.com/", "https://example.com"),
        ("ftp://example.com/a", None),
    ],
)
def test_url_normalization(raw, expected):
    assert URLFrontier.normalize_url(raw) == expected


def test_url_metadata_supports_job_id():
    metadata = URLMetadata(url="https://example.com", job_id="job-123")
    assert metadata.job_id == "job-123"


def test_crawl_job_defaults_to_queued():
    job = CrawlJob(job_id="job-1", seeds=["https://example.com"])
    assert job.status == CrawlJobStatus.QUEUED


@pytest.mark.asyncio
async def test_private_destinations_are_rejected():
    assert await CrawlerEngine.is_safe_url("http://127.0.0.1:8000") is False
    assert await CrawlerEngine.is_safe_url("http://localhost:8000") is False
    assert await CrawlerEngine.is_safe_url("http://169.254.169.254/latest/meta-data/") is False


def test_extraction_event_round_trips_document():
    document = Document(
        id="doc-1",
        url="https://example.com",
        title="Example",
        content="<html></html>",
        text_clean="Example",
        checksum="abc",
    )
    event = AtlasEvent(
        event_id="evt-1",
        type=EventType.EXTRACTION_COMPLETED,
        payload={"document": document.model_dump(mode="json")},
        source="crawler",
    )

    restored = Document.model_validate(event.payload["document"])
    assert restored.id == document.id
    assert restored.url == document.url


def test_content_parser_preserves_nav_links_and_extracts_clean_text():
    parser = ContentParser()
    html = """
    <html>
      <head>
        <title><span>Atlas</span> Documentation</title>
        <meta property="og:title" content="Atlas OG" />
        <meta name="description" content="Distributed search" />
      </head>
      <body>
        <header><nav><a href="/docs/getting-started#top">Getting Started</a></nav></header>
        <main>
          <h1>Welcome</h1>
          <p>Core retrieval pipeline.</p>
          <a href="https://example.com/blog">Blog</a>
          <img src="/assets/arch.png" />
        </main>
        <footer><a href="mailto:test@example.com">Contact</a></footer>
      </body>
    </html>
    """
    extracted = parser.parse(html, "https://example.com/index.html")
    assert extracted.title == "Atlas Documentation"
    assert "Core retrieval pipeline." in extracted.text
    assert "Getting Started" not in extracted.text
    assert "https://example.com/docs/getting-started" in extracted.links
    assert "https://example.com/blog" in extracted.links
    assert all(not link.startswith("mailto:") for link in extracted.links)
    assert extracted.images == ["https://example.com/assets/arch.png"]
    assert extracted.metadata["og:title"] == "Atlas OG"
    assert extracted.metadata["description"] == "Distributed search"


@pytest.mark.asyncio
async def test_frontier_skips_rate_limited_host_without_head_of_line_blocking(monkeypatch):
    monkeypatch.setattr(settings, "MOCK_MODE", False)
    frontier = URLFrontier()
    frontier.redis = FakeRedis()

    await frontier.add_urls(["https://slow.example.com/1"], depth=0, priority=1.0, job_id="job-1")
    await frontier.add_urls(["https://ready.example.org/1"], depth=0, priority=0.8, job_id="job-1")

    # Put slow.example.com in politeness cooldown
    await frontier.redis.set(f"{frontier.politeness_prefix}slow.example.com", str(time.time() + 60))

    next_item = await frontier.get_next_url()
    assert next_item == ("https://ready.example.org/1", 0)
    # The rate-limited URL remains queued for later
    assert "https://slow.example.com/1" in frontier.redis.zsets[frontier.queue_key]


@pytest.mark.asyncio
async def test_frontier_retries_and_active_job_tracking(monkeypatch):
    monkeypatch.setattr(settings, "MOCK_MODE", False)
    monkeypatch.setattr(settings, "MAX_RETRIES", 2)
    frontier = URLFrontier()
    frontier.redis = FakeRedis()

    await frontier.add_urls(["https://example.com/page"], depth=0, priority=1.0, job_id="job-42")
    assert await frontier.active_for_job("job-42") == 1

    popped = await frontier.get_next_url()
    assert popped == ("https://example.com/page", 0)

    # First failure requeues with decayed priority
    await frontier.mark_failed("https://example.com/page", "timeout")
    meta = await frontier.get_metadata("https://example.com/page")
    assert meta.status == CrawlStatus.QUEUED
    assert meta.retry_count == 1
    assert await frontier.active_for_job("job-42") == 1

    # Second failure exhausts retry budget and clears active job set
    await frontier.mark_failed("https://example.com/page", "timeout again")
    meta = await frontier.get_metadata("https://example.com/page")
    assert meta.status == CrawlStatus.FAILED
    assert meta.retry_count == 2
    assert await frontier.active_for_job("job-42") == 0


@pytest.mark.asyncio
async def test_crawler_rejects_ssrf_redirect_before_following(monkeypatch):
    monkeypatch.setattr(settings, "MOCK_MODE", False)
    frontier = URLFrontier()
    frontier.redis = FakeRedis()
    await frontier.add_urls(["https://public.example.com/start"], depth=0)

    crawler = CrawlerEngine(frontier)
    requested_urls: List[str] = []

    class FakeResponse:
        def __init__(self, status: int, headers: Dict[str, str]):
            self.status = status
            self.headers = headers
            self.charset = "utf-8"

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FakeSession:
        def get(self, url: str, allow_redirects: bool = False):
            assert allow_redirects is False
            requested_urls.append(url)
            return FakeResponse(302, {"Location": "http://127.0.0.1:8000/private-admin"})

    crawler.session = FakeSession()

    async def fake_is_safe(url: str) -> bool:
        return not url.startswith("http://127.0.0.1")

    monkeypatch.setattr(crawler, "is_safe_url", fake_is_safe)

    body = await crawler.fetch("https://public.example.com/start")
    assert body is None
    # Verify the private redirect target was NEVER requested
    assert requested_urls == ["https://public.example.com/start"]
    meta = await frontier.get_metadata("https://public.example.com/start")
    assert meta.status == CrawlStatus.SKIPPED
    assert "redirected to a non-public destination" in (meta.error or "")


@pytest.mark.asyncio
async def test_crawler_process_next_honors_job_max_depth_and_completes_job(monkeypatch):
    monkeypatch.setattr(settings, "MOCK_MODE", False)
    monkeypatch.setattr(settings, "MAX_CRAWL_DEPTH", 2)
    monkeypatch.setattr(settings, "RESPECT_ROBOTS_TXT", False)

    fake_redis = FakeRedis()
    frontier = URLFrontier()
    frontier.redis = fake_redis
    store = CrawlJobStore()
    store.redis = fake_redis

    job = CrawlJob(
        job_id="job-deep",
        seeds=["https://allowed.example.com/d3"],
        max_depth=4,
        max_pages=None,
        domain_whitelist=["allowed.example.com"],
    )
    await store.create(job)
    await frontier.add_urls(["https://allowed.example.com/d3"], depth=3, job_id=job.job_id)

    crawler = CrawlerEngine(frontier)
    crawler.jobs = store
    crawler.event_bus = EventBus()
    monkeypatch.setattr(
        crawler.embedding_engine,
        "encode",
        lambda text: [0.1] * settings.EMBEDDING_DIMENSION,
    )

    async def fake_fetch(url: str, job=None) -> str:
        return """
        <html><body><main>
          <h1>Depth 3 Page</h1>
          <a href="https://allowed.example.com/d4">Allowed Child</a>
          <a href="https://blocked.other.org/d4">Out of Whitelist</a>
        </main></body></html>
        """

    monkeypatch.setattr(crawler, "fetch", fake_fetch)
    monkeypatch.setattr(crawler.event_bus, "publish", lambda topic, event: _async_noop())

    processed = await crawler.process_next()
    assert processed is True
    # Because job.max_depth=4 (> settings.MAX_CRAWL_DEPTH=2), depth 4 allowed link was enqueued
    queued_urls = set(fake_redis.zsets.get(frontier.queue_key, {}).keys())
    assert "https://allowed.example.com/d4" in queued_urls
    assert "https://blocked.other.org/d4" not in queued_urls

    # Clear politeness cooldown and process the final depth-4 URL; job should auto-complete
    fake_redis.kv.pop(f"{frontier.politeness_prefix}allowed.example.com", None)
    processed_child = await crawler.process_next()
    assert processed_child is True

    updated_job = await store.get("job-deep")
    assert updated_job.status == CrawlJobStatus.COMPLETED
    assert await store.pages("job-deep") == 2


async def _async_noop(*args, **kwargs):
    return None


@pytest.mark.asyncio
async def test_indexer_worker_routes_poison_pill_to_dead_letter(monkeypatch):
    monkeypatch.setattr(settings, "MOCK_MODE", True)
    index = IndexingService()
    events = EventBus()
    await index.initialize()

    ok = await process_message({"invalid": "event_payload"}, index, events)
    assert ok is False
    assert len(events.published_events) == 1
    topic, payload = events.published_events[0]
    assert topic == EventTopic.DEAD_LETTER.value
    assert payload["type"] == EventType.CRAWL_FAILED.value


def test_crawler_and_search_api_endpoints(monkeypatch):
    monkeypatch.setattr(settings, "MOCK_MODE", True)

    with TestClient(crawler_app) as client:
        # Reject private/loopback seeds
        bad = client.post("/crawl", json={"seeds": ["http://127.0.0.1:8000/admin"]})
        assert bad.status_code == 400

        res = client.post(
            "/crawl",
            json={
                "seeds": ["https://example.com/docs", "https://blocked.com/a"],
                "max_depth": 4,
                "domain_whitelist": ["*.example.com"],
            },
        )
        assert res.status_code == 201
        job_data = res.json()
        assert job_data["seeds"] == ["https://example.com/docs"]

        stats_res = client.get(f"/crawl/{job_data['job_id']}/stats")
        assert stats_res.status_code == 200
        assert stats_res.json()["job_id"] == job_data["job_id"]

    with TestClient(search_app) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["status"] == "ok"

        for endpoint in ("/search/hybrid", "/search/keyword", "/search/semantic"):
            resp = client.get(endpoint, params={"q": "distributed systems", "limit": 5})
            assert resp.status_code == 200
            assert resp.json()["query"] == "distributed systems"


def test_cli_commands_in_mock_mode(monkeypatch):
    monkeypatch.setattr(settings, "MOCK_MODE", True)
    from apps.cli.main import app as cli_app

    runner = CliRunner()
    assert runner.invoke(cli_app, ["status"]).exit_code == 0
    assert runner.invoke(cli_app, ["infra", "config"]).exit_code == 0
    assert runner.invoke(cli_app, ["crawl", "start", "https://example.com"]).exit_code == 0
    assert runner.invoke(cli_app, ["search", "query", "distributed systems"]).exit_code == 0


def test_domain_policy_helpers():
    assert normalize_domains(["*.Example.com.", " sub.example.com "]) == [
        "example.com",
        "sub.example.com",
    ]
    assert domain_allowed("api.example.com", ["example.com"], ["bad.example.com"]) is True
    assert domain_allowed("bad.example.com", ["example.com"], ["bad.example.com"]) is False
