#!/usr/bin/env python3
"""
AW/DIMP-TaxGuidance -- Aruba Departamento di Impuesto Tax Guidance

Fetches tax guidance documents from impuesto.aw via sitemap + HTML scraping,
plus PDF brochures/toelichtingen from the CDN.

Content: Profit tax (winstbelasting), BBO turnover tax, income tax,
dividend tax, fiscal unity, international tax, free zone incentives.
Languages: Dutch and Papiamento.

Strategy:
  1. Parse sitemap.xml to get all page URLs
  2. Scrape each page's HTML content (skip navigation-only pages)
  3. Collect PDF links from pages and download + extract text
  WP REST API is blocked (403), hence HTML scraping approach.

Usage:
  python bootstrap.py bootstrap          # Full pull (~400 pages + PDFs)
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py update             # Fetch recent news items
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
import html
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, List, Set
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from xml.etree import ElementTree as ET

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AW.DIMP-TaxGuidance")

USER_AGENT = "LegalDataHunter/1.0 (open-data research; https://github.com/worldwidelaw/legal-sources)"
BASE_URL = "https://www.impuesto.aw"
CDN_BASE = "https://cuatro.sim-cdn.nl/impuesto/uploads/"
REQUEST_DELAY = 1.5

TAG_RE = re.compile(r"<[^>]+>")
MULTI_SPACE_RE = re.compile(r"[ \t]+")
MULTI_NL_RE = re.compile(r"\n{3,}")

# Pages to skip (navigation hubs, forms-only, portal links)
SKIP_PATTERNS = [
    r"/tag/",
    r"/categorie/",
    r"^/$",
    r"/home$",
    r"/contact$",
    r"/openingstijden",
    r"/deadlines$",
    r"/wisselkoersen$",
    r"/bo-impuesto",
    r"/cookie",
    r"/privacy",
    r"/disclaimer",
    r"/sitemap$",
]
SKIP_RE = [re.compile(p) for p in SKIP_PATTERNS]

# Content pages with substantive guidance (prioritize these for sampling)
PRIORITY_PAGES = [
    "/fiscale-eenheid",
    "/winstbelasting-home",
    "/bbo",
    "/inkomstenbelasting",
    "/dividendbelasting-home",
    "/toeristenheffing",
    "/successiebelasting",
    "/grondbelasting",
    "/loonbelasting",
    "/bezwaar-en-beroep",
    "/woning",
    "/betalen-en-teruggaaf",
]


def _fetch_url(url: str, timeout: int = 30) -> Optional[str]:
    """Fetch URL content as text."""
    req = Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "nl,en;q=0.5",
    })
    try:
        resp = urlopen(req, timeout=timeout)
        return resp.read().decode("utf-8", errors="replace")
    except (HTTPError, URLError, TimeoutError) as e:
        logger.warning(f"Fetch error for {url}: {e}")
        return None


def _fetch_bytes(url: str, timeout: int = 60) -> Optional[bytes]:
    """Fetch URL content as bytes."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        resp = urlopen(req, timeout=timeout)
        return resp.read()
    except (HTTPError, URLError, TimeoutError) as e:
        logger.debug(f"Download failed for {url}: {e}")
        return None


def _clean_html(text: str) -> str:
    """Strip HTML tags, decode entities, normalize whitespace."""
    text = TAG_RE.sub("\n", text)
    text = html.unescape(text)
    text = MULTI_SPACE_RE.sub(" ", text)
    lines = [line.strip() for line in text.split("\n")]
    text = "\n".join(line for line in lines if line)
    text = MULTI_NL_RE.sub("\n\n", text)
    return text.strip()


def _extract_main_content(page_html: str) -> str:
    """Extract main content area from page HTML, skipping nav/footer."""
    # Try to isolate <main> or content div
    main_match = re.search(
        r'<main[^>]*>(.*?)</main>',
        page_html, re.DOTALL | re.IGNORECASE
    )
    if main_match:
        return _clean_html(main_match.group(1))

    # Try article or content div
    content_match = re.search(
        r'<(?:article|div)[^>]*class="[^"]*(?:content|entry|post-body|page-content)[^"]*"[^>]*>(.*?)</(?:article|div)>',
        page_html, re.DOTALL | re.IGNORECASE
    )
    if content_match:
        return _clean_html(content_match.group(1))

    # Fallback: strip everything between <body> and </body>
    body_match = re.search(r'<body[^>]*>(.*?)</body>', page_html, re.DOTALL | re.IGNORECASE)
    if body_match:
        body = body_match.group(1)
        # Remove nav, header, footer, script, style
        body = re.sub(r'<(nav|header|footer|script|style|aside)[^>]*>.*?</\1>', '', body, flags=re.DOTALL | re.IGNORECASE)
        return _clean_html(body)

    return _clean_html(page_html)


def _extract_title(page_html: str) -> str:
    """Extract page title."""
    # Try <h1>
    h1_match = re.search(r'<h1[^>]*>(.*?)</h1>', page_html, re.DOTALL | re.IGNORECASE)
    if h1_match:
        return _clean_html(h1_match.group(1)).strip()
    # Fallback to <title>
    title_match = re.search(r'<title>(.*?)</title>', page_html, re.DOTALL | re.IGNORECASE)
    if title_match:
        title = _clean_html(title_match.group(1)).strip()
        # Remove site suffix
        title = re.sub(r'\s*[-|–]\s*Departamento di Impuesto.*$', '', title)
        return title
    return ""


def _extract_pdf_links(page_html: str) -> List[str]:
    """Find all PDF download links in page HTML."""
    urls = set()
    # Links to PDFs
    for match in re.finditer(r'href="([^"]*\.pdf[^"]*)"', page_html, re.IGNORECASE):
        url = match.group(1)
        if url.startswith("//"):
            url = "https:" + url
        elif url.startswith("/"):
            url = BASE_URL + url
        urls.add(url)
    # Also match CDN URLs in src or data attributes
    for match in re.finditer(r'(https?://cuatro\.sim-cdn\.nl/impuesto/uploads/[^\s"\'<>]+\.pdf)', page_html, re.IGNORECASE):
        urls.add(match.group(1))
    return list(urls)


def _parse_sitemap(xml_text: str) -> List[str]:
    """Parse sitemap.xml and return all URLs."""
    urls = []
    try:
        root = ET.fromstring(xml_text)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        for url_elem in root.findall(".//sm:url/sm:loc", ns):
            if url_elem.text:
                urls.append(url_elem.text.strip())
    except ET.ParseError as e:
        logger.error(f"Sitemap parse error: {e}")
    return urls


def _should_skip(url: str) -> bool:
    """Check if URL should be skipped."""
    path = url.replace(BASE_URL, "")
    for pattern in SKIP_RE:
        if pattern.search(path):
            return True
    return False


class ArubaDIMPScraper(BaseScraper):
    """
    Scraper for AW/DIMP-TaxGuidance.
    Country: AW
    URL: https://www.impuesto.aw

    Data types: doctrine
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _get_sitemap_urls(self) -> List[str]:
        """Fetch and parse sitemap.xml."""
        logger.info("Fetching sitemap.xml...")
        xml_text = _fetch_url(f"{BASE_URL}/sitemap.xml")
        if not xml_text:
            logger.error("Failed to fetch sitemap.xml")
            return []
        urls = _parse_sitemap(xml_text)
        logger.info(f"Sitemap contains {len(urls)} URLs")
        return urls

    def _scrape_page(self, url: str) -> Optional[dict]:
        """Scrape a single page and return raw record."""
        page_html = _fetch_url(url)
        if not page_html:
            return None

        title = _extract_title(page_html)
        text = _extract_main_content(page_html)

        # Skip pages with insufficient content (nav hubs)
        if not text or len(text) < 100:
            return None

        # Extract PDF links for later processing
        pdf_links = _extract_pdf_links(page_html)

        return {
            "page_url": url,
            "content_type": "page",
            "title": title,
            "text": text,
            "pdf_links": pdf_links,
            "date": None,
        }

    def _fetch_pdf(self, pdf_url: str, parent_title: str = "") -> Optional[dict]:
        """Download and extract text from a PDF."""
        pdf_bytes = _fetch_bytes(pdf_url)
        if not pdf_bytes or b"%PDF" not in pdf_bytes[:20]:
            return None

        # Use filename as ID
        filename = pdf_url.split("/")[-1].split("?")[0]
        source_id = f"pdf-{filename}"

        text = extract_pdf_markdown(
            source="AW/DIMP-TaxGuidance",
            source_id=source_id,
            pdf_bytes=pdf_bytes,
            table="doctrine",
        ) or ""

        if not text or len(text) < 50:
            return None

        # Clean title from filename
        title = filename.replace(".pdf", "").replace("_", " ").replace("-", " ")
        title = re.sub(r'\s*\d+$', '', title)  # Remove trailing numbers
        title = title.strip().title()

        return {
            "page_url": pdf_url,
            "content_type": "pdf",
            "title": title or parent_title,
            "text": text,
            "pdf_links": [],
            "date": None,
        }

    def _fetch_pages(self, urls: List[str], max_records: int = 999999) -> Generator[dict, None, None]:
        """Scrape pages from URL list."""
        count = 0
        seen_pdfs: Set[str] = set()

        for url in urls:
            if count >= max_records:
                return

            if _should_skip(url):
                continue

            time.sleep(REQUEST_DELAY)
            record = self._scrape_page(url)
            if not record:
                continue

            yield record
            count += 1
            logger.info(f"  [{count}] Page: {record['title'][:60]} ({len(record['text'])} chars)")

            # Also fetch PDFs found on this page
            for pdf_url in record.get("pdf_links", []):
                if count >= max_records:
                    return
                if pdf_url in seen_pdfs:
                    continue
                seen_pdfs.add(pdf_url)

                time.sleep(REQUEST_DELAY)
                pdf_record = self._fetch_pdf(pdf_url, parent_title=record["title"])
                if pdf_record:
                    yield pdf_record
                    count += 1
                    logger.info(f"  [{count}] PDF: {pdf_record['title'][:60]} ({len(pdf_record['text'])} chars)")

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents: pages + PDFs from sitemap."""
        urls = self._get_sitemap_urls()
        if not urls:
            return
        yield from self._fetch_pages(urls)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch recent news items."""
        urls = self._get_sitemap_urls()
        # Filter to news-like URLs
        news_urls = [u for u in urls if "/nieuws" in u or "2026" in u or "2025" in u]
        yield from self._fetch_pages(news_urls, max_records=50)

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        url = raw.get("page_url", "")
        # Create stable ID from URL
        path = url.replace(BASE_URL, "").strip("/")
        if not path:
            path = "home"
        doc_id = re.sub(r'[^a-zA-Z0-9_-]', '_', path)[:120]

        content_type = raw.get("content_type", "page")

        return {
            "_id": f"{content_type}-{doc_id}",
            "_source": "AW/DIMP-TaxGuidance",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw["text"],
            "date": raw.get("date"),
            "url": url,
            "content_type": content_type,
        }


# === CLI entry point ===
if __name__ == "__main__":
    scraper = ArubaDIMPScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        html_content = _fetch_url(f"{BASE_URL}/sitemap.xml")
        if html_content and "<urlset" in html_content:
            urls = _parse_sitemap(html_content)
            print(f"OK: Sitemap returned {len(urls)} URLs")
        else:
            print("FAIL: Could not fetch sitemap.xml")
            sys.exit(1)

    elif command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        max_records = 15 if sample else 999999

        if sample:
            logger.info("=== SAMPLE MODE: fetching ~15 records ===")
            # Prioritize substantive content pages
            urls = [BASE_URL + p for p in PRIORITY_PAGES]
            # Also add a few from sitemap
            sitemap_urls = scraper._get_sitemap_urls()
            # Add URLs not already in priority list
            extra = [u for u in sitemap_urls if not _should_skip(u) and u not in urls][:20]
            urls.extend(extra)
        elif command == "update":
            logger.info("=== UPDATE MODE ===")
            for raw in scraper.fetch_updates(""):
                if count >= max_records:
                    break
                record = scraper.normalize(raw)
                out_file = sample_dir / f"{record['_id']}.json"
                out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                count += 1
                logger.info(f"Saved [{count}]: {record['title'][:70]}")
            logger.info(f"Done. Total records: {count}")
            sys.exit(0 if count > 0 else 1)
        else:
            urls = scraper._get_sitemap_urls()

        for raw in scraper._fetch_pages(urls, max_records=max_records):
            record = scraper.normalize(raw)
            out_file = sample_dir / f"{record['_id']}.json"
            out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
            count += 1
            logger.info(f"Saved [{count}]: {record['title'][:70]}")

        logger.info(f"Done. Total records: {count}")
        if count == 0:
            logger.error("No records fetched — check connectivity")
            sys.exit(1)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
