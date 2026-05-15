import re
from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup
from dataclasses import dataclass
import hashlib

@dataclass
class ExtractedContent:
    title: str
    text: str
    summary: str
    metadata: Dict[str, Any]
    links: List[str]
    images: List[str]
    checksum: str

class ContentParser:
    def __init__(self):
        # Tags to remove entirely
        self.blacklist_tags = [
            'script', 'style', 'nav', 'footer', 'header', 
            'aside', 'iframe', 'noscript', 'svg'
        ]

    def parse(self, html: str, url: str) -> ExtractedContent:
        soup = BeautifulSoup(html, 'html.parser')
        
        # 1. Basic Metadata Extraction
        title = soup.title.string if soup.title else ""
        if not title:
            h1 = soup.find('h1')
            title = h1.get_text() if h1 else ""
            
        # 2. Schema.org / OpenGraph / Meta
        metadata = self._extract_metadata(soup)
        
        # 3. Clean Boilerplate
        for tag in soup.find_all(self.blacklist_tags):
            tag.decompose()
            
        # 4. Content Extraction (Heuristic)
        # We look for the main content container
        main_content = self._find_main_content(soup)
        text = main_content.get_text(separator='\n', strip=True)
        
        # 5. Link & Image Discovery
        links = [a.get('href') for a in soup.find_all('a', href=True)]
        images = [img.get('src') for img in soup.find_all('img', src=True)]
        
        # 6. Checksum for deduplication
        checksum = hashlib.md5(text.encode('utf-8')).hexdigest()
        
        return ExtractedContent(
            title=title.strip(),
            text=text,
            summary=text[:200] + "...", # Simple summary
            metadata=metadata,
            links=links,
            images=images,
            checksum=checksum
        )

    def _extract_metadata(self, soup: BeautifulSoup) -> Dict[str, Any]:
        meta = {}
        # OpenGraph
        for tag in soup.find_all('meta', property=re.compile(r'^og:')):
            meta[tag['property']] = tag.get('content', '')
        # Standard Meta
        for tag in soup.find_all('meta', attrs={'name': True}):
            meta[tag['name']] = tag.get('content', '')
        return meta

    def _find_main_content(self, soup: BeautifulSoup) -> BeautifulSoup:
        # Try common main content IDs/classes
        for selector in ['main', 'article', '#content', '.content', '#main-content', '.post-content']:
            found = soup.select_one(selector)
            if found:
                return found
        return soup.body if soup.body else soup

# Example integration
if __name__ == "__main__":
    parser = ContentParser()
    sample_html = "<html><body><header>Nav</header><main><h1>Hello World</h1><p>This is the main content.</p></main><footer>Footer</footer></body></html>"
    result = parser.parse(sample_html, "https://example.com")
    print(f"Title: {result.title}")
    print(f"Checksum: {result.checksum}")
    print(f"Clean Text: {result.text[:100]}")
