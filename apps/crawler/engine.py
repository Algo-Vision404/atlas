import asyncio
try:
    import aiohttp
    from bs4 import BeautifulSoup
except ImportError:
    aiohttp = None
    BeautifulSoup = None
import time
from typing import List, Optional, Callable
from urllib.parse import urljoin, urlparse
import logging
from datetime import datetime
import uuid
import json

from libs.schemas.models import Document, URLMetadata, EventType, AtlasEvent
from libs.core.config import settings
from services.url_frontier.manager import URLFrontier
# from kafka import KafkaProducer # In a real system, we'd use this

logger = logging.getLogger("atlas.crawler")

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
        self.semaphore = asyncio.Semaphore(concurrency)
        # self.producer = KafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            headers={"User-Agent": self.user_agent},
            timeout=aiohttp.ClientTimeout(total=settings.REQUEST_TIMEOUT)
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()

    async def fetch(self, url: str) -> Optional[str]:
        async with self.semaphore:
            try:
                logger.info(f"Fetching: {url}")
                async with self.session.get(url) as response:
                    if response.status == 200:
                        return await response.text()
                    else:
                        logger.warning(f"Failed to fetch {url}: {response.status}")
                        await self.frontier.mark_failed(url, f"Status {response.status}")
                        return None
            except Exception as e:
                logger.error(f"Error fetching {url}: {str(e)}")
                await self.frontier.mark_failed(url, str(e))
                return None

    def extract_links(self, html: str, base_url: str) -> List[str]:
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        for a in soup.find_all('a', href=True):
            link = urljoin(base_url, a['href'])
            parsed = urlparse(link)
            if parsed.scheme in ('http', 'https'):
                links.append(link.split('#')[0])
        return list(set(links))

    async def process_next(self):
        """Pull next URL from frontier and process it"""
        result = await self.frontier.get_next_url()
        if not result:
            return False
            
        url, depth = result
        html = await self.fetch(url)
        
        if html:
            links = self.extract_links(html, url)
            logger.info(f"Successfully crawled {url}, found {len(links)} links")
            
            # 1. Add new links back to frontier
            if depth < 3: # Max depth 3
                await self.frontier.add_urls(links, depth=depth + 1)
            
            # 2. Mark current URL as completed
            await self.frontier.mark_completed(url)
            
            # 3. Publish Event (Mock for now)
            event = AtlasEvent(
                event_id=str(uuid.uuid4()),
                type=EventType.PAGE_CRAWLED,
                source="crawler-node-01",
                payload={
                    "url": url,
                    "html_size": len(html),
                    "links_count": len(links)
                }
            )
            logger.debug(f"Published event: {event.event_id}")
            # self.producer.send("atlas.events.crawled", event.json().encode('utf-8'))
            
            return True
        return False

    async def run_forever(self):
        """Main worker loop"""
        logger.info("Crawler Engine started. Polling Frontier...")
        while True:
            processed = await self.process_next()
            if not processed:
                await asyncio.sleep(1) # Backoff if no URLs

# Worker Entrypoint
async def main():
    frontier = URLFrontier()
    async with CrawlerEngine(frontier=frontier) as engine:
        await engine.run_forever()

if __name__ == "__main__":
    asyncio.run(main())
