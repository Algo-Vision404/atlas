import pytest

from apps.search_api.main import reciprocal_rank_fusion
from services.url_frontier.manager import URLFrontier
from libs.schemas.models import CrawlJob, CrawlJobStatus

def test_rrf_merges_results_from_both_retrievers():
    results = reciprocal_rank_fusion(
        [{"url": "https://a.test", "title": "A"}, {"url": "https://b.test", "title": "B"}],
        [{"url": "https://b.test", "title": "B"}, {"url": "https://c.test", "title": "C"}],
    )
    assert [item["url"] for item in results] == ["https://b.test", "https://a.test", "https://c.test"]
    assert results[0]["source"] == "hybrid"

@pytest.mark.parametrize(
    "raw,expected",
    [("https://example.com/a#section", "https://example.com/a"),
     ("https://example.com/", "https://example.com"),
     ("ftp://example.com/a", None)],
)
def test_url_normalization(raw, expected):
    assert URLFrontier.normalize_url(raw) == expected

def test_url_metadata_supports_job_id():
    from libs.schemas.models import URLMetadata

    metadata = URLMetadata(url="https://example.com", job_id="job-123")
    assert metadata.job_id == "job-123"

def test_crawl_job_defaults_to_queued():
    job = CrawlJob(job_id="job-1", seeds=["https://example.com"])
    assert job.status == CrawlJobStatus.QUEUED


@pytest.mark.asyncio
async def test_private_destinations_are_rejected():
    from apps.crawler.engine import CrawlerEngine

    assert await CrawlerEngine.is_safe_url("http://127.0.0.1:8000") is False
    assert await CrawlerEngine.is_safe_url("http://localhost:8000") is False

def test_extraction_event_round_trips_document():
    from libs.schemas.models import AtlasEvent, Document, EventType

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
