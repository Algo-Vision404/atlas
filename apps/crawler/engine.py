import asyncio
import hashlib
import ipaddress
import logging
import socket
from collections import defaultdict
from datetime import datetime, timezone
from typing import List, Optional
from urllib import robotparser
from urllib.parse import urldefrag, urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup

from apps.crawler.api import domain_allowed
from libs.core.config import settings
from libs.schemas.models import AtlasEvent, CrawlJob, CrawlJobStatus, Document, EventType
from services.content_parser.parser import ContentParser
from services.crawl_jobs.store import CrawlJobStore
from services.embedding_engine.engine import EmbeddingEngine
from services.event_bus.bus import EventBus
from services.event_bus.topics import EventTopic
from services.url_frontier.manager import URLFrontier

logger = logging.getLogger("atlas.crawler")

REDIRECT_STATUSES = {301, 302, 303, 307, 308}


class CrawlerEngine:
    def __init__(
        self,
        frontier: URLFrontier,
        concurrency: int = settings.CONCURRENT_REQUESTS_PER_DOMAIN,
        user_agent: str = settings.DEFAULT_USER_AGENT,
    ):
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
        """Check robots.txt and cache policies per origin with safe redirect validation."""
        if not settings.RESPECT_ROBOTS_TXT or settings.MOCK_MODE:
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

        current_url = robots_url
        try:
            for _ in range(settings.MAX_REDIRECTS + 1):
                if not await self.is_safe_url(current_url):
                    logger.warning("Unsafe robots.txt destination rejected for %s (%s)", origin, current_url)
                    return False
                async with self.session.get(current_url, allow_redirects=False) as response:
                    if response.status in REDIRECT_STATUSES:
                        location = response.headers.get("Location")
                        if not location:
                            return False
                        next_url, _ = urldefrag(urljoin(current_url, location))
                        next_parsed = urlparse(next_url)
                        if next_parsed.scheme not in {"http", "https"} or not next_parsed.netloc:
                            return False
                        current_url = next_url
                        continue

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
                    break
            else:
                logger.warning("Too many redirects fetching robots.txt for %s", origin)
                return False
        except (aiohttp.ClientError, asyncio.TimeoutError, UnicodeError) as exc:
            logger.warning("robots.txt fetch failed for %s: %s", origin, exc)
            return False

        self._robots_cache[origin] = (now + settings.ROBOTS_CACHE_TTL, parser)
        return parser.can_fetch(self.user_agent, url)

    @staticmethod
    def _is_public_ip(ip_str: str) -> bool:
        try:
            ip = ipaddress.ip_address(ip_str)
        except ValueError:
            return False
        return not (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_multicast
            or ip.is_reserved
            or ip.is_unspecified
        )

    @classmethod
    async def is_safe_url(cls, url: str) -> bool:
        """Reject loopback, private, link-local, multicast, and reserved destinations."""
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False
        hostname = parsed.hostname
        if not hostname:
            return False
        try:
            ipaddress.ip_address(hostname)
            return cls._is_public_ip(hostname)
        except ValueError:
            pass

        lower_host = hostname.lower().rstrip(".")
        if lower_host in {"localhost", "localhost.localdomain"} or lower_host.endswith(".localhost"):
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

        if not infos:
            return False

        for info in infos:
            if not cls._is_public_ip(info[4][0]):
                return False
        return True

    async def fetch(self, url: str, job: Optional[CrawlJob] = None) -> Optional[str]:
        if settings.MOCK_MODE:
            return f"<html><head><title>Simulated Page</title></head><body><main><h1>{url}</h1><p>Simulated content for {url}.</p></main></body></html>"

        current_url = url
        for hop in range(settings.MAX_REDIRECTS + 1):
            parsed = urlparse(current_url)
            host = (parsed.hostname or parsed.netloc).lower().rstrip(".")

            if not await self.is_safe_url(current_url):
                reason = (
                    "destination is not a public internet address"
                    if hop == 0
                    else "redirected to a non-public destination"
                )
                await self.frontier.mark_skipped(url, reason)
                return None

            if hop > 0:
                if job and not domain_allowed(host, job.domain_whitelist, job.domain_blacklist):
                    await self.frontier.mark_skipped(url, "redirected outside crawl job domain policy")
                    return None
                if not await self.allowed_by_robots(current_url):
                    await self.frontier.mark_skipped(url, "redirect target blocked by robots.txt")
                    return None

            async with self._host_semaphores[host]:
                try:
                    async with self.session.get(current_url, allow_redirects=False) as response:
                        if response.status in REDIRECT_STATUSES:
                            location = response.headers.get("Location")
                            if not location:
                                await self.frontier.mark_failed(url, f"HTTP {response.status} without Location header")
                                return None
                            next_url, _ = urldefrag(urljoin(current_url, location))
                            next_parsed = urlparse(next_url)
                            if next_parsed.scheme not in {"http", "https"} or not next_parsed.netloc:
                                await self.frontier.mark_skipped(url, "redirected to unsupported URL scheme")
                                return None
                            current_url = next_url
                            continue

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
                    logger.warning("Fetch failed for %s: %s", current_url, exc)
                    await self.frontier.mark_failed(url, str(exc))
                    return None

        await self.frontier.mark_failed(url, "exceeded maximum redirect hops")
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

    async def _maybe_complete_job(self, job_id: Optional[str], pages_crawled: Optional[int] = None) -> None:
        if not job_id:
            return
        job = await self.jobs.get(job_id)
        if not job or job.status in {CrawlJobStatus.COMPLETED, CrawlJobStatus.FAILED, CrawlJobStatus.CANCELLED}:
            return
        count = pages_crawled if pages_crawled is not None else await self.jobs.pages(job_id)
        reached_max_pages = job.max_pages is not None and count >= job.max_pages
        no_active_urls = await self.frontier.active_for_job(job_id) == 0
        if reached_max_pages or no_active_urls:
            job.status = CrawlJobStatus.COMPLETED
            job.updated_at = datetime.now(timezone.utc)
            await self.jobs.update(job)

    async def process_next(self) -> bool:
        result = await self.frontier.get_next_url()
        if not result:
            return False

        url, depth = result
        metadata = await self.frontier.get_metadata(url)
        job_id = metadata.job_id if metadata else None
        job = await self.jobs.get(job_id) if job_id else None

        if job and job.status == CrawlJobStatus.QUEUED:
            job.status = CrawlJobStatus.RUNNING
            job.updated_at = datetime.now(timezone.utc)
            await self.jobs.update(job)

        max_depth = job.max_depth if job is not None else settings.MAX_CRAWL_DEPTH
        if depth > max_depth or (job and job.max_pages is not None and await self.jobs.pages(job.job_id) >= job.max_pages):
            await self.frontier.mark_skipped(url, "crawl job limit reached")
            await self._maybe_complete_job(job_id)
            return False

        hostname = (urlparse(url).hostname or "").lower().rstrip(".")
        if job and not domain_allowed(hostname, job.domain_whitelist, job.domain_blacklist):
            reason = (
                "domain outside crawl job whitelist"
                if job.domain_whitelist
                else "domain blocked by crawl job blacklist"
            )
            await self.frontier.mark_skipped(url, reason)
            await self._maybe_complete_job(job_id)
            return False

        if not await self.allowed_by_robots(url):
            await self.frontier.mark_skipped(url, "blocked by robots.txt or robots policy unavailable")
            await self._maybe_complete_job(job_id)
            return False

        html = await self.fetch(url, job=job)
        if not html:
            await self._maybe_complete_job(job_id)
            return False

        try:
            await self.frontier.refresh_lease(url)
            extracted = await asyncio.to_thread(self.parser.parse, html, url)
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
            document.embedding = await asyncio.to_thread(
                self.embedding_engine.encode,
                extracted.text[:12000],
            )
            await self.frontier.refresh_lease(url)

            if depth < max_depth:
                candidate_links = extracted.links
                if job:
                    candidate_links = [
                        link
                        for link in candidate_links
                        if domain_allowed(
                            (urlparse(link).hostname or "").lower().rstrip("."),
                            job.domain_whitelist,
                            job.domain_blacklist,
                        )
                    ]
                if candidate_links:
                    await self.frontier.add_urls(candidate_links, depth=depth + 1, job_id=job_id)

            await self.event_bus.publish(
                EventTopic.EXTRACTION_COMPLETED.value,
                AtlasEvent(
                    event_id=hashlib.sha256(f"extraction:{url}:{extracted.checksum}".encode()).hexdigest(),
                    type=EventType.EXTRACTION_COMPLETED,
                    payload={
                        "document": document.model_dump(mode="json"),
                        "depth": depth,
                        "job_id": job_id,
                    },
                    source="crawler",
                ),
            )
            await self.frontier.mark_completed(url)
            if job_id:
                count = await self.jobs.increment_pages(job_id)
                await self._maybe_complete_job(job_id, pages_crawled=count)
            return True
        except Exception as exc:
            logger.exception("Processing failed for %s", url)
            await self.frontier.mark_failed(url, str(exc))
            await self._maybe_complete_job(job_id)
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
            if not processed or settings.MOCK_MODE:
                await asyncio.sleep(1)


async def main():
    frontier = URLFrontier()
    async with CrawlerEngine(frontier) as crawler:
        await crawler.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
