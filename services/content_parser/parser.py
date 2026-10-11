import hashlib
import re
from dataclasses import dataclass
from typing import Any, Dict, List
from urllib.parse import urldefrag, urljoin, urlparse

from bs4 import BeautifulSoup


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
        self.non_content_tags = ["script", "style", "iframe", "noscript", "svg"]
        self.boilerplate_tags = ["nav", "footer", "header", "aside"]

    def parse(self, html: str, url: str) -> ExtractedContent:
        soup = BeautifulSoup(html, "html.parser")

        # 1. Basic Metadata Extraction
        title = " ".join(soup.title.get_text().split()) if soup.title else ""
        if not title:
            h1 = soup.find("h1")
            title = " ".join(h1.get_text().split()) if h1 else ""

        # 2. Schema.org / OpenGraph / Meta
        metadata = self._extract_metadata(soup)

        # 3. Strip non-visible / code tags before link discovery
        for tag in soup.find_all(self.non_content_tags):
            tag.decompose()

        # 4. Link & Image Discovery (preserve navigation links before decomposing nav/header/footer)
        seen_links = set()
        links: List[str] = []
        for anchor in soup.find_all("a", href=True):
            href = (anchor.get("href") or "").strip()
            if not href:
                continue
            absolute, _ = urldefrag(urljoin(url, href))
            parsed = urlparse(absolute)
            if parsed.scheme in {"http", "https"} and parsed.netloc and absolute not in seen_links:
                seen_links.add(absolute)
                links.append(absolute)

        images = []
        seen_images = set()
        for img in soup.find_all("img", src=True):
            src = (img.get("src") or "").strip()
            if not src:
                continue
            resolved = urljoin(url, src)
            if resolved not in seen_images:
                seen_images.add(resolved)
                images.append(resolved)

        # 5. Clean layout boilerplate before main text extraction
        for tag in soup.find_all(self.boilerplate_tags):
            tag.decompose()

        # 6. Content Extraction (Heuristic)
        main_content = self._find_main_content(soup)
        text = main_content.get_text(separator="\n", strip=True)

        # 7. SHA-256 checksum for deterministic content deduplication
        checksum = hashlib.sha256(text.encode("utf-8")).hexdigest()
        summary = (text[:200] + "...") if len(text) > 200 else text

        return ExtractedContent(
            title=title.strip(),
            text=text,
            summary=summary,
            metadata=metadata,
            links=links,
            images=images,
            checksum=checksum,
        )

    def _extract_metadata(self, soup: BeautifulSoup) -> Dict[str, Any]:
        meta = {}
        for tag in soup.find_all("meta", property=re.compile(r"^og:")):
            meta[tag["property"]] = tag.get("content", "")
        for tag in soup.find_all("meta", attrs={"name": True}):
            meta[tag["name"]] = tag.get("content", "")
        return meta

    # Ordered main-content heuristics, kept equivalent to the previous CSS
    # selectors ("main", "article", "#content", ".content", "#main-content",
    # ".post-content") but expressed with BeautifulSoup's native finders:
    # soupsieve (used by ``select_one``) ships regexes with possessive
    # quantifiers that silently misbehave on CPython 3.11.0-3.11.4 (gh-106052),
    # which made every parse raise SelectorSyntaxError.
    MAIN_CONTENT_FINDERS = [
        {"name": "main"},
        {"name": "article"},
        {"attrs": {"id": "content"}},
        {"attrs": {"class": "content"}},
        {"attrs": {"id": "main-content"}},
        {"attrs": {"class": "post-content"}},
    ]

    def _find_main_content(self, soup: BeautifulSoup) -> BeautifulSoup:
        for finder in self.MAIN_CONTENT_FINDERS:
            found = soup.find(**finder)
            if found:
                return found
        return soup.body if soup.body else soup


if __name__ == "__main__":
    parser = ContentParser()
    sample_html = "<html><body><header><a href='/about'>About</a></header><main><h1>Hello World</h1><p>This is the main content.</p></main><footer>Footer</footer></body></html>"
    result = parser.parse(sample_html, "https://example.com")
    print(f"Title: {result.title}")
    print(f"Checksum: {result.checksum}")
    print(f"Links: {result.links}")
    print(f"Clean Text: {result.text[:100]}")
