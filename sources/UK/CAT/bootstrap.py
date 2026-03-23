#!/usr/bin/env python3
"""
World Wide Law - UK Competition Appeal Tribunal (CAT) Scraper

Fetches judgments from the CAT using:
  - GET /sitemap.xml (discover all judgment URLs)
  - GET /judgments/{slug} (judgment page with metadata + PDF link)
  - GET /sites/cat/files/{path} (PDF full text)

Coverage: ~1,041 judgments from 2001 to present.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
"""

import io
import re
import sys
import json
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape

from bs4 import BeautifulSoup

# PDF text extraction
try:
    from pdfminer.high_level import extract_text as pdfminer_extract
    HAS_PDFMINER = True
except ImportError:
    HAS_PDFMINER = False

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("UK/CAT")


class UKCATScraper(BaseScraper):
    """
    Scraper for: UK Competition Appeal Tribunal (CAT)
    Country: UK
    URL: https://www.catribunal.org.uk

    Data types: case_law
    Auth: none

    Strategy:
    - Parse sitemap.xml to discover all judgment URLs
    - Fetch each judgment page for metadata and PDF download link
    - Download PDF and extract full text with pdfminer/pdfplumber
    """

    BASE_URL = "https://www.catribunal.org.uk"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=self.BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
            },
            timeout=60,
        )

    def _extract_pdf_text(self, pdf_content: bytes) -> str:
        """Extract text from PDF bytes using available library."""
        if HAS_PDFPLUMBER:
            try:
                with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                    pages = []
                    for page in pdf.pages:
                        text = page.extract_text()
                        if text:
                            pages.append(text)
                    return "\n\n".join(pages)
            except Exception as e:
                logger.debug(f"pdfplumber extraction failed: {e}")

        if HAS_PDFMINER:
            try:
                return pdfminer_extract(io.BytesIO(pdf_content))
            except Exception as e:
                logger.debug(f"pdfminer extraction failed: {e}")

        logger.error("No PDF extraction library available. Install pdfplumber or pdfminer.six")
        return ""

    def _get_judgment_urls_from_sitemap(self) -> list:
        """Parse sitemap.xml to get all judgment URLs with lastmod dates."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get("/sitemap.xml")
            if resp.status_code != 200:
                logger.error(f"Sitemap returned {resp.status_code}")
                return []
        except Exception as e:
            logger.error(f"Failed to fetch sitemap: {e}")
            return []

        entries = []
        try:
            root = ET.fromstring(resp.content)
            # Handle XML namespace
            ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            for url_elem in root.findall(".//sm:url", ns):
                loc = url_elem.find("sm:loc", ns)
                lastmod = url_elem.find("sm:lastmod", ns)
                if loc is not None and "/judgments/" in loc.text:
                    entry = {"url": loc.text}
                    if lastmod is not None:
                        entry["lastmod"] = lastmod.text
                    entries.append(entry)
        except ET.ParseError as e:
            logger.error(f"Failed to parse sitemap XML: {e}")
            return []

        logger.info(f"Found {len(entries)} judgment URLs in sitemap")
        return entries

    def _fetch_judgment(self, judgment_url: str) -> Optional[dict]:
        """Fetch a judgment page, extract metadata and PDF text."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(judgment_url)
            if resp.status_code != 200:
                logger.warning(f"Judgment page {judgment_url} returned {resp.status_code}")
                return None
        except Exception as e:
            logger.warning(f"Failed to fetch judgment {judgment_url}: {e}")
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract title
        title = ""
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(strip=True)

        # Extract slug from URL
        slug = judgment_url.rstrip("/").split("/")[-1]

        # Extract neutral citation components
        neutral_citation = ""
        citation_year = ""
        citation_number = ""

        # Look for neutral citation in page content
        # Typical format: [2024] CAT 76
        citation_match = re.search(r"\[(\d{4})\]\s*CAT\s*(\d+)", resp.text)
        if citation_match:
            citation_year = citation_match.group(1)
            citation_number = citation_match.group(2)
            neutral_citation = f"[{citation_year}] CAT {citation_number}"

        # Extract publication date
        pub_date = ""
        time_elem = soup.find("time", datetime=True)
        if time_elem:
            pub_date = time_elem.get("datetime", "")

        # Also check for date in span/div elements
        if not pub_date:
            for elem in soup.find_all(["span", "div"]):
                text = elem.get_text(strip=True)
                date_match = re.match(r"(\d{1,2}\s+\w+\s+\d{4})", text)
                if date_match:
                    pub_date = date_match.group(1)
                    break

        # Extract case number from related case links
        case_number = ""
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "/cases/" in href:
                case_number = link.get_text(strip=True)
                break

        # Extract summary text
        summary = ""
        # Look for summary section - often in a field or div
        summary_section = soup.find("div", class_=re.compile(r"summary|field--name-body|field--name-field-summary", re.I))
        if summary_section:
            summary = summary_section.get_text(strip=True)
        else:
            # Try finding paragraphs in the main content area
            article = soup.find("article") or soup.find("main")
            if article:
                paragraphs = []
                for p in article.find_all("p"):
                    text = p.get_text(strip=True)
                    if len(text) > 30 and "cookie" not in text.lower() and "download" not in text.lower():
                        paragraphs.append(text)
                if paragraphs:
                    summary = " ".join(paragraphs[:5])

        # Find PDF download link
        pdf_url = None
        for link in soup.find_all("a", href=True):
            href = link["href"]
            link_text = link.get_text(strip=True).lower()
            # Look for PDF links: /sites/cat/files/ or link text containing "download"
            if ("/sites/cat/files/" in href and href.lower().endswith(".pdf")) or \
               (href.lower().endswith(".pdf") and "download" in link_text):
                if href.startswith("/"):
                    pdf_url = f"{self.BASE_URL}{href}"
                elif href.startswith("http"):
                    pdf_url = href
                else:
                    pdf_url = f"{self.BASE_URL}/{href}"
                break

        # Also check for PDF links without .pdf extension but in files directory
        if not pdf_url:
            for link in soup.find_all("a", href=True):
                href = link["href"]
                link_text = link.get_text(strip=True).lower()
                if "download" in link_text and ("judgment" in link_text or "decision" in link_text):
                    if href.startswith("/"):
                        pdf_url = f"{self.BASE_URL}{href}"
                    elif href.startswith("http"):
                        pdf_url = href
                    else:
                        pdf_url = f"{self.BASE_URL}/{href}"
                    break

        # Download PDF and extract text
        text = ""
        if pdf_url:
            self.rate_limiter.wait()
            try:
                resp_pdf = self.client.get(pdf_url)
                if resp_pdf.status_code == 200:
                    content_type = resp_pdf.headers.get("Content-Type", "")
                    if "pdf" in content_type.lower() or pdf_url.lower().endswith(".pdf"):
                        text = self._extract_pdf_text(resp_pdf.content)
                    else:
                        logger.debug(f"Non-PDF content at {pdf_url}: {content_type}")
                else:
                    logger.warning(f"PDF download returned {resp_pdf.status_code}: {pdf_url}")
            except Exception as e:
                logger.warning(f"Failed to download PDF {pdf_url}: {e}")

        # Parse date to ISO format
        date_iso = self._parse_date(pub_date)

        return {
            "slug": slug,
            "title": title,
            "text": text,
            "summary": summary,
            "neutral_citation": neutral_citation,
            "citation_year": citation_year,
            "citation_number": citation_number,
            "case_number": case_number,
            "date_raw": pub_date,
            "date_iso": date_iso,
            "url": judgment_url,
            "pdf_url": pdf_url or "",
        }

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse various date formats to ISO 8601."""
        if not date_str:
            return None
        # Try ISO format first (from datetime attribute)
        for fmt in [
            "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S",
            "%d %B %Y", "%d %b %Y", "%d/%m/%Y", "%B %d, %Y",
        ]:
            try:
                return datetime.strptime(date_str.strip()[:10], fmt[:min(len(fmt), 10)]).strftime("%Y-%m-%d")
            except ValueError:
                continue
        # Try extracting date
        match = re.search(r"(\d{4}-\d{2}-\d{2})", date_str)
        if match:
            return match.group(1)
        match = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})", date_str)
        if match:
            try:
                return datetime.strptime(match.group(0), "%d %B %Y").strftime("%Y-%m-%d")
            except ValueError:
                pass
        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all judgments discovered via sitemap."""
        entries = self._get_judgment_urls_from_sitemap()

        for i, entry in enumerate(entries):
            doc = self._fetch_judgment(entry["url"])
            if doc and doc.get("text"):
                yield doc
                if (i + 1) % 10 == 0:
                    logger.info(f"Progress: {i+1}/{len(entries)} judgments fetched")
            elif doc:
                logger.debug(f"Skipping judgment with no text: {entry['url']}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield judgments updated since the given date."""
        entries = self._get_judgment_urls_from_sitemap()
        since_str = since.strftime("%Y-%m-%d")

        for entry in entries:
            lastmod = entry.get("lastmod", "")
            if lastmod and lastmod[:10] >= since_str:
                doc = self._fetch_judgment(entry["url"])
                if doc and doc.get("text"):
                    yield doc

    def normalize(self, raw: dict) -> dict:
        """Transform raw judgment data into standard schema."""
        text = raw.get("text", "").strip()
        if not text:
            return None

        # Use neutral citation as ID if available, else slug
        doc_id = raw.get("neutral_citation") or raw.get("slug", "")
        doc_id = doc_id.replace(" ", "_").replace("[", "").replace("]", "")

        return {
            "_id": f"UK/CAT/{doc_id}",
            "_source": "UK/CAT",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "summary": raw.get("summary", ""),
            "neutral_citation": raw.get("neutral_citation", ""),
            "case_number": raw.get("case_number", ""),
            "date": raw.get("date_iso"),
            "url": raw.get("url", ""),
            "pdf_url": raw.get("pdf_url", ""),
        }


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKCATScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py bootstrap [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "bootstrap":
        sample = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample, sample_size=12)
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
