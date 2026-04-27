#!/usr/bin/env python3
"""
SC/FSA-Guidance -- Seychelles FSA Guidance Documents

Fetches guidance documents (guidelines, circulars, codes, VASP guidance,
white papers, consultation papers) from the Seychelles Financial Services
Authority. All documents are PDFs managed by the Joomla edocman component.

Strategy:
  - Scrape listing pages for each section (no pagination needed)
  - Extract PDF download links from edocman component
  - Download PDFs and extract text via common.pdf_extract

Data Coverage:
  - ~186 guidance documents across 6 sections
  - Guidelines (43), Circulars (107), Codes (14), VASP (15), White Papers (7)

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records
  python bootstrap.py update              # Incremental update
  python bootstrap.py test-api            # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SC.FSA-Guidance")

BASE_URL = "https://fsaseychelles.sc"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Sections to crawl with their URL paths and category labels
SECTIONS = [
    {
        "path": "/legal-framework/guidelines",
        "category": "guideline",
        "name": "Guidelines",
    },
    {
        "path": "/legal-framework/circulars",
        "category": "circular",
        "name": "Circulars",
    },
    {
        "path": "/legal-framework/codes",
        "category": "code",
        "name": "Codes",
    },
    {
        "path": "/vasp/legal-framework",
        "category": "vasp_guidance",
        "name": "VASP Legal Framework",
    },
    {
        "path": "/media-corner/white-papers-and-industry-comments/white-papers",
        "category": "white_paper",
        "name": "White Papers",
    },
    {
        "path": "/media-corner/white-papers-and-industry-comments/industry-consultation",
        "category": "consultation",
        "name": "Industry Consultation",
    },
]

# Match edocman download links
EDOCMAN_LINK_RE = re.compile(
    r'<a\s+[^>]*href="([^"]*(?:edocman|download)[^"]*)"[^>]*>(.*?)</a>',
    re.DOTALL | re.IGNORECASE,
)

# Match generic PDF links
PDF_LINK_RE = re.compile(
    r'<a\s+[^>]*href="([^"]+\.pdf)"[^>]*>(.*?)</a>',
    re.DOTALL | re.IGNORECASE,
)

# Extract year from circular/document titles
YEAR_RE = re.compile(r'\b(20\d{2})\b')


def _clean_html(html_text: str) -> str:
    """Strip HTML tags and clean up text."""
    text = re.sub(r'<[^>]+>', '', html_text)
    text = html_module.unescape(text)
    return text.strip()


def _extract_date_from_title(title: str) -> str:
    """Try to extract a year-based date from a document title."""
    m = YEAR_RE.search(title)
    if m:
        return f"{m.group(1)}-01-01"
    return ""


class FSAGuidanceScraper(BaseScraper):
    """
    Scraper for SC/FSA-Guidance -- Seychelles FSA guidance documents.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.7",
            "Accept-Language": "en,fr;q=0.9",
        })

    def _fetch_page(self, url: str, timeout: int = 30) -> str:
        """Fetch an HTML page with rate limiting."""
        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return ""

    def _download_pdf(self, url: str, doc_id: str) -> str:
        """Download a PDF and extract text."""
        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=60, headers={
                "Accept": "application/pdf,*/*",
            })
            if resp.status_code != 200:
                logger.warning(f"PDF download failed {url}: HTTP {resp.status_code}")
                return ""
            content = resp.content
            if not content or (len(content) > 4 and content[:4] != b"%PDF"):
                # Some edocman links redirect to HTML - check content type
                ct = resp.headers.get("Content-Type", "")
                if "pdf" not in ct.lower() and content[:4] != b"%PDF":
                    logger.warning(f"Not a PDF from {url} (Content-Type: {ct})")
                    return ""
        except Exception as e:
            logger.warning(f"PDF download error {url}: {e}")
            return ""

        text = extract_pdf_markdown(
            source="SC/FSA-Guidance",
            source_id=doc_id,
            pdf_bytes=content,
            table="doctrine",
        )
        return text or ""

    def _extract_links(self, html_content: str, section_path: str) -> List[Dict[str, str]]:
        """Extract document links from a section page."""
        links = []
        seen_urls = set()

        # Try edocman links first
        for match in EDOCMAN_LINK_RE.finditer(html_content):
            href = match.group(1)
            title = _clean_html(match.group(2))
            if not title or len(title) < 3:
                continue
            # Skip non-document links
            if any(skip in title.lower() for skip in ["read more", "click here", "download"]):
                if len(title) < 20:
                    continue

            full_url = urljoin(BASE_URL, href)
            if full_url not in seen_urls:
                seen_urls.add(full_url)
                links.append({"url": full_url, "title": title})

        # Also try direct PDF links
        for match in PDF_LINK_RE.finditer(html_content):
            href = match.group(1)
            title = _clean_html(match.group(2))
            if not title or len(title) < 3:
                continue

            full_url = urljoin(BASE_URL, href)
            if full_url not in seen_urls:
                seen_urls.add(full_url)
                links.append({"url": full_url, "title": title})

        return links

    def _crawl_section(self, section: Dict[str, str]) -> Generator[Dict[str, Any], None, None]:
        """Crawl a section and yield document metadata + text."""
        path = section["path"]
        category = section["category"]
        name = section["name"]
        url = urljoin(BASE_URL, path)

        logger.info(f"Crawling section: {name} ({path})")
        html = self._fetch_page(url)
        if not html:
            logger.error(f"Could not access section: {name}")
            return

        links = self._extract_links(html, path)
        logger.info(f"{name}: found {len(links)} document links")

        for link in links:
            doc_url = link["url"]
            title = link["title"]

            # Generate doc ID from title slug
            slug = re.sub(r'[^a-zA-Z0-9]+', '-', title.lower()).strip('-')[:80]
            doc_id = f"{category}-{slug}"

            # Download and extract PDF text
            text = self._download_pdf(doc_url, doc_id)
            if not text or len(text) < 20:
                logger.warning(f"Insufficient text for {doc_id}: {len(text) if text else 0} chars")
                continue

            date = _extract_date_from_title(title)

            yield {
                "doc_id": doc_id,
                "title": title,
                "text": text,
                "date": date,
                "url": doc_url,
                "category": category,
                "section": name,
            }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all guidance documents from all sections."""
        logger.info("Starting FSA Guidance crawl...")
        for section in SECTIONS:
            yield from self._crawl_section(section)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield all documents (no date filtering possible on listing pages)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw data into standard schema."""
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "SC/FSA-Guidance",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
            "category": raw.get("category", ""),
            "section": raw.get("section", ""),
            "language": "en",
        }

    def test_api(self):
        """Quick connectivity test for each section."""
        print("Testing FSA Guidance sections...")

        total_links = 0
        for section in SECTIONS:
            url = urljoin(BASE_URL, section["path"])
            name = section["name"]
            print(f"\n--- {name} ---")

            html = self._fetch_page(url)
            if not html:
                print("  ERROR: Could not fetch page")
                continue

            links = self._extract_links(html, section["path"])
            print(f"  Document links: {len(links)}")
            total_links += len(links)

            if links:
                link = links[0]
                print(f"  First: {link['title'][:70]}")
                print(f"  URL: {link['url'][:100]}")

                # Test PDF download for first document
                slug = re.sub(r'[^a-zA-Z0-9]+', '-', link['title'].lower()).strip('-')[:80]
                doc_id = f"test-{slug}"
                text = self._download_pdf(link["url"], doc_id)
                if text:
                    print(f"  Text: {len(text)} chars")
                    print(f"  Preview: {text[:150]}...")
                else:
                    print("  WARNING: No text extracted from PDF")

        print(f"\nTotal document links: {total_links}")
        print("Test complete!")


def main():
    scraper = FSAGuidanceScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
