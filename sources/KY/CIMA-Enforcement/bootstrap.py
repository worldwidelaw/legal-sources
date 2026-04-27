#!/usr/bin/env python3
"""
KY/CIMA-Enforcement -- Cayman Islands Monetary Authority Enforcement Actions

Fetches enforcement notices from cima.ky:
  - 4 category pages: warning notices, decision notices, admin fines, general notices
  - HTML detail pages for general notices & administrative fines
  - PDF documents for warning notices & decision notices
  - ~150 enforcement notices (2017-present)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Connectivity test
"""

import sys
import re
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KY.CIMA-Enforcement")

BASE_URL = "https://www.cima.ky"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html, */*",
}

CATEGORIES = [
    {"slug": "general-notices", "label": "general", "url": "/general-notices"},
    {"slug": "warning-notices", "label": "warning", "url": "/warning-notices"},
    {"slug": "decision-notices", "label": "decision", "url": "/decision-notices"},
    {"slug": "administrative-fines", "label": "administrative_fine", "url": "/administrative-fines"},
]

MONTH_MAP = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}


def clean_html(html: str) -> str:
    """Strip HTML tags and decode entities, preserving paragraph breaks."""
    text = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.DOTALL)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</(p|div|li|h[1-6]|tr)>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def parse_date(date_str: str) -> Optional[str]:
    """Parse dates like '12 November 2025' or 'Mon, 29 September 2025' to ISO."""
    if not date_str:
        return None
    # Strip leading day-of-week
    date_str = re.sub(r"^[A-Za-z]+,\s*", "", date_str.strip())
    m = re.match(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", date_str)
    if not m:
        return None
    day, month_name, year = m.groups()
    month = MONTH_MAP.get(month_name.lower())
    if not month:
        return None
    return f"{year}-{month}-{int(day):02d}"


class CIMAScraper(BaseScraper):
    """Scraper for CIMA enforcement notices."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _parse_listing_page(self, category: dict) -> list[dict]:
        """Parse a category listing page and return all notice entries."""
        url = BASE_URL + category["url"]
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {category['label']} listing: {e}")
            return []

        html = resp.text
        items = []

        # Find all news-item blocks
        for match in re.finditer(
            r'<div\s+class="news-item[^"]*">(.*?)</div>\s*(?=<div\s+class="(?:news-item|load-more)|</div>\s*</div>)',
            html, re.DOTALL
        ):
            block = match.group(1)

            # Extract date
            date_match = re.search(r'<div\s+class="date">\s*(.*?)\s*</div>', block, re.DOTALL)
            date_str = clean_html(date_match.group(1)) if date_match else ""
            date_iso = parse_date(date_str)

            # Extract title
            title_match = re.search(r'<h2[^>]*>(.*?)</h2>', block, re.DOTALL)
            title = clean_html(title_match.group(1)) if title_match else ""

            # Extract summary/excerpt
            summary_match = re.search(r'<p[^>]*>(.*?)</p>', block, re.DOTALL)
            summary = clean_html(summary_match.group(1)) if summary_match else ""

            # Extract detail page link (Read More) — href appears before title
            detail_match = re.search(r'<a[^>]*href="([^"]+)"[^>]*title="Read More"', block)
            detail_url = detail_match.group(1) if detail_match else None
            if detail_url and not detail_url.startswith("http"):
                detail_url = BASE_URL + detail_url

            # Extract PDF link (View PDF)
            pdf_match = re.search(r'<a[^>]*(?:title="View PDF"|href="[^"]*\.pdf")[^>]*href="([^"]+\.pdf)"', block)
            if not pdf_match:
                pdf_match = re.search(r'href="([^"]*\.pdf)"', block)
            pdf_url = pdf_match.group(1) if pdf_match else None
            if pdf_url and not pdf_url.startswith("http"):
                pdf_url = BASE_URL + pdf_url

            # Build notice ID from category + date + title slug (date ensures uniqueness)
            slug = re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')[:60] if title else "unknown"
            date_part = date_iso or "undated"
            notice_id = f"{category['label']}-{date_part}-{slug}"

            if not title:
                continue

            items.append({
                "notice_id": notice_id,
                "title": title,
                "date": date_iso,
                "summary": summary,
                "category": category["label"],
                "detail_url": detail_url,
                "pdf_url": pdf_url,
                "url": detail_url or pdf_url or url,
            })

        logger.info(f"{category['label']}: found {len(items)} notices")
        return items

    def _fetch_html_detail(self, item: dict) -> Optional[str]:
        """Fetch full text from an HTML detail page."""
        url = item.get("detail_url")
        if not url:
            return None
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch detail for {item['notice_id']}: {e}")
            return None

        html = resp.text

        # Find the news-detail cms block and extract everything after </div> (meta)
        # Structure: <div class="news-detail cms"> <h1>...</h1> <div class="meta">...</div> <p>content</p>... </div>
        idx = html.find('class="news-detail cms"')
        if idx < 0:
            return None

        # Get the chunk from that point to end of section
        chunk = html[idx:]
        section_end = chunk.find("</section>")
        if section_end > 0:
            chunk = chunk[:section_end]

        # Remove the meta div (title + date + PDF link area)
        chunk = re.sub(r'<div\s+class="meta">.*?</div>\s*</div>', '', chunk, count=1, flags=re.DOTALL)
        # Remove h1 title
        chunk = re.sub(r'<h1>.*?</h1>', '', chunk, count=1, flags=re.DOTALL)

        text = clean_html(chunk)
        if text and len(text) > 30:
            return text
        return None

    def _fetch_pdf_text(self, item: dict) -> Optional[str]:
        """Extract text from a PDF notice using pdfplumber/pypdf directly."""
        pdf_url = item.get("pdf_url")
        if not pdf_url:
            return None
        try:
            resp = self.session.get(pdf_url, timeout=60)
            if resp.status_code != 200:
                return None
        except requests.RequestException as e:
            logger.warning(f"PDF download failed for {item['notice_id']}: {e}")
            return None

        import io
        pdf_bytes = io.BytesIO(resp.content)

        # Try pdfplumber first
        try:
            import pdfplumber
            with pdfplumber.open(pdf_bytes) as pdf:
                pages_text = []
                for page in pdf.pages:
                    t = page.extract_text()
                    if t:
                        pages_text.append(t)
                if pages_text:
                    text = "\n\n".join(pages_text)
                    if len(text) > 30:
                        return text
        except Exception:
            pass

        # Fallback to pypdf
        pdf_bytes.seek(0)
        try:
            from pypdf import PdfReader
            reader = PdfReader(pdf_bytes)
            pages_text = []
            for page in reader.pages:
                t = page.extract_text()
                if t:
                    pages_text.append(t)
            if pages_text:
                text = "\n\n".join(pages_text)
                if len(text) > 30:
                    return text
        except Exception:
            pass

        logger.debug(f"No text extracted from PDF for {item['notice_id']}")
        return None

    def _fetch_full_text(self, item: dict) -> Optional[str]:
        """Get full text from HTML detail page or PDF, whichever is available."""
        # Try HTML detail page first
        if item.get("detail_url"):
            text = self._fetch_html_detail(item)
            if text:
                return text

        # Fall back to PDF
        if item.get("pdf_url"):
            text = self._fetch_pdf_text(item)
            if text:
                return text

        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all CIMA enforcement notices with full text."""
        all_items = []
        for cat in CATEGORIES:
            items = self._parse_listing_page(cat)
            all_items.extend(items)
            time.sleep(1)

        logger.info(f"Total notices found: {len(all_items)}")

        for i, item in enumerate(all_items):
            time.sleep(0.5)
            text = self._fetch_full_text(item)
            if not text:
                logger.debug(f"No text for {item['notice_id']}, using summary")
                # Some items only have a listing excerpt - skip if too short
                if item.get("summary") and len(item["summary"]) > 50:
                    text = item["summary"]
                else:
                    continue

            item["text"] = text
            yield item

            if (i + 1) % 25 == 0:
                logger.info(f"Processed {i + 1}/{len(all_items)} notices")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent notices only."""
        since_str = since.strftime("%Y-%m-%d")
        for cat in CATEGORIES:
            items = self._parse_listing_page(cat)
            recent = [it for it in items if it.get("date") and it["date"] >= since_str]
            logger.info(f"{cat['label']}: {len(recent)} notices since {since_str}")

            for item in recent:
                time.sleep(0.5)
                text = self._fetch_full_text(item)
                if text:
                    item["text"] = text
                    yield item
            time.sleep(1)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 30:
            return None

        return {
            "_id": raw["notice_id"],
            "_source": "KY/CIMA-Enforcement",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "notice_id": raw["notice_id"],
            "title": raw.get("title", ""),
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "category": raw.get("category"),
            "summary": raw.get("summary"),
            "pdf_url": raw.get("pdf_url"),
            "language": "eng",
        }

    def test_connection(self) -> bool:
        """Test connectivity to cima.ky."""
        try:
            resp = self.session.get(f"{BASE_URL}/enforcement-notices", timeout=30)
            resp.raise_for_status()
            if "enforcement" in resp.text.lower():
                logger.info("Connection test passed")
                return True
            logger.error("Connection test: unexpected content")
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


if __name__ == "__main__":
    scraper = CIMAScraper()

    if len(sys.argv) < 2:
        print("Usage: bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        success = scraper.test_connection()
        sys.exit(0 if success else 1)
    elif command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample_mode)
        print(f"Bootstrap complete: {result}")
    elif command == "update":
        result = scraper.update()
        print(f"Update complete: {result}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
