import asyncio
import hashlib
import logging
import ipaddress
import socket
from collections import defaultdict
from datetime import datetime, timezone
from typing import List, Optional
from urllib.parse import urljoin, urlparse, urldefrag
from urllib import robotparser

import aiohttp
from bs4 import BeautifulSoup

from libs.core.config import settings
from libs.schemas.models import AtlasEvent, CrawlJobStatus, Document, EventType
from services.content_parser.parser import ContentParser
from services.crawl_jobs.store import CrawlJobStore
from services.event_bus.bus import EventBus
from services.event_bus.topics import EventTopic
from services.embedding_engine.engine import EmbeddingEngine
from services.url_frontier.manager import URLFrontier

logger = logging.getLogger("atlas.crawler")

class CrawlerEngine:
    def __init__(self, frontier: URLFrontier,
                 concurrency: int = settings.CONCURRENT_REQUESTS_PER_DOMAIN,
                 user_agent: str = settings.DEFAULT_USER_AGENT):
        self.frontier = frontier
        self.concurrency = concurrency
        self.user_agent = user_agent
        self.session: Optional[aiohttp.ClientSession] = None
        self._host_semaphores = defaultdict(lambda: asyncio.Semaphore(concurrency))
        self.parser = ContentParser()
        self.embedding_engine = EmbeddingEngine()
        self._robots_cache = {}
        self.jobs = CrawlJobStore()
        self.event_bus = EventBus()

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            headers={"User-Agent": self.user_agent, "Accept": "text/html,application/xhtml+xml"},
            timeout=aiohttp.ClientTimeout(total=settings.REQUEST_TIMEOUT),
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()
        await self.frontier.close()
        await self.jobs.close()
        await self.event_bus.close()

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

    @staticmethod
    async def is_safe_url(url: str) -> bool:
        """Reject loopback, private, link-local, multicast, and reserved destinations."""
        parsed = urlparse(url)
        hostname = parsed.hostname
        if not hostname:
            return False
        try:
            ip = ipaddress.ip_address(hostname)
            return not (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved)
        except ValueError:
            pass

        if hostname.lower() in {"localhost", "localhost.localdomain"} or hostname.lower().endswith(".localhost"):
            return False

        try:
            infos = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: socket.getaddrinfo(
                    hostname,
                    parsed.port or (443 if parsed.scheme == "https" else 80),
                    type=socket.SOCK_STREAM,
                ),
            )
        except (OSError, ValueError):
            return False

        for info in infos:
            try:
                ip = ipaddress.ip_address(info[4][0])
            except ValueError:
                return False
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved:
                return False
        return True

    async def fetch(self, url: str) -> Optional[str]:
        parsed = urlparse(url)
        host = parsed.hostname or parsed.netloc.lower()
        if not await self.is_safe_url(url):
            await self.frontier.mark_skipped(url, "destination is not a public internet address")
            return None

        async with self._host_semaphores[host]:
            try:
                async with self.session.get(url, allow_redirects=True) as response:
                    final_url = str(response.url)
                    if not await self.is_safe_url(final_url):
                        await self.frontier.mark_skipped(url, "redirected to a non-public destination")
                        return None

                    content_type = response.headers.get("Content-Type", "").lower()
                    if response.status != 200 or "text/html" not in content_type:
                        await self.frontier.mark_failed(url, f"HTTP {response.status}; content-type={content_type}")
                        return None

                    chunks = []
                    total = 0
                    async for chunk in response.content.iter_chunked(64 * 1024):
                        total += len(chunk)
                        if total > settings.MAX_RESPONSE_BYTES:
                            await self.frontier.mark_failed(url, "response exceeds configured size limit")
                            return None
                        chunks.append(chunk)

                    body = b"".join(chunks)
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
        metadata = await self.frontier.get_metadata(url)
        job = await self.jobs.get(metadata.job_id) if metadata and metadata.job_id else None
        if job and job.status == CrawlJobStatus.QUEUED:
            job.status = CrawlJobStatus.RUNNING
            job.updated_at = datetime.now(timezone.utc)
            await self.jobs.update(job)
        if job and (depth > job.max_depth or (job.max_pages is not None and await self.jobs.pages(job.job_id) >= job.max_pages)):
            await self.frontier.mark_skipped(url, "crawl job limit reached")
            return False
        hostname = (urlparse(url).hostname or "").lower().rstrip(".")
        if job and job.domain_whitelist:
            allowed = {d.lower().strip().rstrip(".").lstrip(".") for d in job.domain_whitelist}
            if not any(hostname == d or hostname.endswith("." + d) for d in allowed):
                await self.frontier.mark_skipped(url, "domain outside crawl job whitelist")
                return False
        if job and any(hostname == d.lower().strip().rstrip(".").lstrip(".") or hostname.endswith("." + d.lower().strip().rstrip(".").lstrip(".")) for d in job.domain_blacklist):
            await self.frontier.mark_skipped(url, "domain blocked by crawl job blacklist")
            return False
        if not await self.allowed_by_robots(url):
            await self.frontier.mark_skipped(url, "blocked by robots.txt or robots policy unavailable")
            return False
        html = await self.fetch(url)
        if not html:
            return False

        try:
            await self.frontier.refresh_lease(url)
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
            await self.frontier.refresh_lease(url)

            links = self.extract_links(html, url)
            if depth < settings.MAX_CRAWL_DEPTH:
                await self.frontier.add_urls(links, depth=depth + 1, job_id=metadata.job_id if metadata else None)
            await self.event_bus.publish(EventTopic.EXTRACTION_COMPLETED.value, AtlasEvent(
                event_id=hashlib.sha256(f"extraction:{url}:{extracted.checksum}".encode()).hexdigest(),
                type=EventType.EXTRACTION_COMPLETED,
                payload={
                    "document": document.model_dump(mode="json"),
                    "depth": depth,
                    "job_id": metadata.job_id if metadata else None,
                },
                source="crawler",
            ))
            await self.frontier.mark_completed(url)
            if metadata and metadata.job_id:
                count = await self.jobs.increment_pages(metadata.job_id)
                job = await self.jobs.get(metadata.job_id)
                if job and job.max_pages is not None and count >= job.max_pages:
                    job.status = CrawlJobStatus.COMPLETED
                    job.updated_at = datetime.now(timezone.utc)
                    await self.jobs.update(job)
            return True
        except Exception as exc:
            logger.exception("Processing failed for %s", url)
            await self.frontier.mark_failed(url, str(exc))
            return False

    async def run_forever(self):
        last_recovery = 0.0
        recovery_interval = max(5.0, settings.WORKER_LEASE_TTL / 2)
        while True:
            now = asyncio.get_running_loop().time()
            if now - last_recovery >= recovery_interval:
                recovered = await self.frontier.recover_stale()
                if recovered:
                    logger.warning("Recovered %d stale crawler leases", recovered)
                last_recovery = now

            processed = await self.process_next()
            if not processed:
                await asyncio.sleep(1)

async def main():
    frontier = URLFrontier()
    async with CrawlerEngine(frontier) as crawler:
        await crawler.run_forever()

if __name__ == "__main__":
    asyncio.run(main())
