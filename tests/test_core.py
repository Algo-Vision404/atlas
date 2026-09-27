import pytest

from apps.search_api.main import reciprocal_rank_fusion
from services.url_frontier.manager import URLFrontier

def test_rrf_merges_results_from_both_retrievers():
    results = reciprocal_rank_fusion(
        [{"url": "https://a.test", "title": "A"}, {"url": "https://b.test", "title": "B"}],
        [{"url": "https://b.test", "title": "B"}, {"url": "https://c.test", "title": "C"}],
    )
    assert [item["url"] for item in results] == ["https://b.test", "https://a.test", "https://c.test"]
    assert results[0]["source"] == "hybrid"

@pytest.mark.parametrize((
    "raw,expected",
    [("https://example.com/a#section", "https://example.com/a"),
     ("https://example.com/", "https://example.com"),
     ("ftp://example.com/a", None)],
)
def test_url_normalization(raw, expected):
    assert URLFrontier.normalize_url(raw) == expected