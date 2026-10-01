from enum import Enum


class EventTopic(str, Enum):
    URL_DISCOVERED = "atlas.url-discovered"
    PAGE_CRAWLED = "atlas.page-crawled"
    EXTRACTION_COMPLETED = "atlas.extraction-completed"
    INDEXING_COMPLETED = "atlas.indexing-completed"
    CRAWL_FAILED = "atlas.crawl-failed"
    DEAD_LETTER = "atlas.dead-letter"
