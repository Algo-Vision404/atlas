import asyncio
import hashlib
import logging
from collections import defaultdict
from typing import List, Optional
from urllib.parse import urljoin, urlparse, urldefrag
from urllib import robotparser

import aiohttp
from bs4 import BeautifulSoup

from apps.indexer.engine import IndexingService
from libs.core.config import settings
from libs.schemas.models import Document
from services.content_parser.parser import ContentParser
from services.embedding_engine.engine import EmbeddingEngine
from services.url_frontier.manager import URLFrontier

logger = logging.getLogger("atlas.crawler")

class CrawlerEngine:
    def __init__(self, frontier: URLFrontier, indexing: Optional[IndexingService] = None,
                 concurrency: int = settings.CONCURRENT_REQUESTS_PER_DOMAIN,
                 user_agent: str = settings.DEFAULT_USER_AGENT):
        self.frontier = frontier
        self.indexing = indexing or IndexingService()
        self.concurrency = concurrency
        self.user_agent = user_agent
        self.session: Optional[aiohttp.ClientSession] = None
        self._host_semaphores = defaultdict(lambda: asyncio.Semaphore(concurrency))
        self.parser = ContentParser()
        self.embedding_engine = EmbeddingEngine()
        self._robots_cache = {}

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            headers={"User-Agent": self.user_agent, "Accept": "text/html,application/xhtml+xml"},
            timeout=aiohttp.ClientTimeout(total=settings.REQUEST_TIMEOUT),
        )
        await self.indexing.initialize()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()
        await self.indexing.keyword_index.close()
        await self.indexing.vector_index.close()
        await self.frontier.close()

    async def allowed_by_robots(self, url: str) -> bool:
        """Check robots.txt and cache policies per origin."""
        if not settings.RESPECT_ROBOTS_TXT:
            return True
        parsed = urlparse(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        cached = self._robots_cache.get(origin)
        now = asyncio.get_running_loop().time()
        if cached and cached[0] > now:
            return cached[1].can_fetch(self.user_agent, url)

        robots_url = f"{origin}/robots.txt"
        parser = robotparser.RobotFileParser()
        parser.set_url(robots_url)
        try:
            async with self.session.get(robots_url, allow_redirects=True) as response:
                if response.status == 404:
                    parser.parse([])
                elif response.status != 200:
                    logger.warning("robots.txt returned HTTP %s for %s", response.status, origin)
                    return False
                else:
                    body = await response.read()
                    if len(body) > 1_000_000:
                        logger.warning("robots.txt too large for %s", origin)
                        return False
                    parser.parse(body.decode(response.charset or "utf-8", errors="replace").splitlines())
        except (aiohttp.ClientError, asyncio.TimeoutError, UnicodeError) as exc:
            logger.warning("robots.txt fetch failed for %s: %s", origin, exc)
            return False
        self._robots_cache[origin] = (now + settings.ROBOTS_CACHE_TTL, parser)
        return parser.can_fetch(self.user_agent, url)

    async def fetch(self, url: str) -> Optional[str]:
        host = urlparse(url).netloc.lower()
        async with self._host_semaphores[host]:
            try:
                async with self.session.get(url, allow_redirects=True) as response:
                    content_type = response.headers.get("Content-Type", "").lower()
                    if response.status != 200 or "text/html" not in content_type:
                        await self.frontier.mark_failed(url, f"HTTP {response.status}; content-type={content_type}")
                        return None

                    body = await response.read()
                    if len(body) > settings.MAX_RESPONSE_BYTES:
                        await self.frontier.mark_failed(url, "response exceeds configured size limit")
                        return None
                    return body.decode(response.charset or "utf-8", errors="replace")
            except (aiohttp.ClientError, asyncio.TimeoutError, UnicodeError) as exc:
                logger.warning("Fetch failed for %s: %s", url, exc)
                await self.frontier.mark_failed(url, str(exc))
                return None

    @staticmethod
    def extract_links(html: str, base_url: str) -> List[str]:
        soup = BeautifulSoup(html, "html.parser")
        links = set()
        for anchor in soup.find_all("a", href=True):
            absolute = urljoin(base_url, anchor["href"])
            absolute, _ = urldefrag(absolute)
            parsed = urlparse(absolute)
            if parsed.scheme in {"http", "https"} and parsed.netloc:
                links.add(absolute)
        return sorted(links)

    async def process_next(self) -> bool:
        result = await self.frontier.get_next_url()
        if not result:
            return False

        url, depth = result
        if not await self.allowed_by_robots(url):
            await self.frontier.mark_skipped(url, "blocked by robots.txt or robots policy unavailable")
            return False
        html = await self.fetch(url)
        if not html:
            return False

        try:
            extracted = self.parser.parse(html, url)
            document = Document(
                id=hashlib.sha256(url.encode("utf-8")).hexdigest(),
                url=url,
                title=extracted.title,
                content=html,
                text_clean=extracted.text,
                metadata=extracted.metadata,
                links=extracted.links,
                images=extracted.images,
                checksum=extracted.checksum,
            )
            document.embedding = self.embedding_engine.encode(extracted.text[:12000])
            await self.indexing.index_document(document)

            links = self.extract_links(html, url)
            if depth < settings.MAX_CRAWL_DEPTH:
                await self.frontier.add_urls(links, depth=depth + 1)
            await self.frontier.mark_completed(url)
            return True
        except Exception as exc:
            logger.exception("Processing failed for %s", url)
            await self.frontier.mark_failed(url, str(exc))
            return False

    async def run_forever(self):
        while True:
            processed = await self.process_next()
            if not processed:
                await asyncio.sleep(1)

async def main():
    frontier = URLFrontier()
    async with CrawlerEngine(frontier) as crawler:
        await crawler.run_forever()

if __name__ == "__main__":
    asyncio.run(main())
