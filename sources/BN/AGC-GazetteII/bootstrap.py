#!/usr/bin/env python3
"""
BN/AGC-GazetteII -- Brunei Government Gazette Part II (Subsidiary Legislation)

Fetches subsidiary legislation, orders, and notifications from the Attorney
General's Chambers Gazette Part II. Published on a SharePoint 2013 site
with year-by-year listings linking to English/Malay PDFs.

Endpoint:
  - Year listing: https://www.agc.gov.bn/_layouts/15/listform.aspx?PageType=4&ListId=...&ID={id}
  - PDFs: https://www.agc.gov.bn/AGC%20Images/LAWS/Gazette_PDF/{year}/EN/...

Data:
  - ~500+ gazette items (1998-2026)
  - Full text extracted from PDFs (English versions)
  - Covers: subsidiary legislation, orders, notifications, regulations

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import html as html_mod
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Tuple
from urllib.parse import unquote, quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BN.AGC-GazetteII")

BASE_URL = "https://www.agc.gov.bn"
LIST_ID = "%7BEB94CB12%2D8EA9%2D4B06%2D92BA%2DD7A1A046FE80%7D"
CONTENT_TYPE = "0x01000065382E4B39024896109E93FEBACB15"

# Year IDs: 97=2026, 96=2025, ... mapping discovered from the site
# Years 1998-2026 = 29 years. IDs go from 69 to 97.
YEAR_IDS = {year: 97 - (2026 - year) for year in range(1998, 2027)}

# Regex to extract PDF links from year pages
PDF_LINK_RE = re.compile(
    r'href=["\'](/AGC%20Images/LAWS/Gazette_PDF/[^"\']+\.pdf)["\']',
    re.IGNORECASE,
)

# Extract notification number from filename
NOTIF_RE = re.compile(r'S\s*0*(\d+)', re.IGNORECASE)

# Clean HTML tags
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def strip_html(s: str) -> str:
    text = TAG_RE.sub(" ", s)
    text = html_mod.unescape(text)
    return WS_RE.sub(" ", text).strip()


def extract_notif_no(pdf_path: str) -> str:
    """Extract notification number (e.g., 'S 1') from PDF filename."""
    decoded = unquote(pdf_path)
    fname = decoded.rsplit("/", 1)[-1]
    m = NOTIF_RE.search(fname)
    if m:
        return f"S {m.group(1)}"
    return ""


class BNGazetteIIScraper(BaseScraper):
    """Scraper for BN/AGC-GazetteII -- Government Gazette Part II."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        import requests
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
        })

    def _get(self, url: str, **kwargs) -> "requests.Response":
        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=120, **kwargs)
        resp.raise_for_status()
        return resp

    def _fetch_year_page(self, year: int) -> str:
        """Fetch the HTML for a specific year's gazette listing."""
        item_id = YEAR_IDS[year]
        url = (
            f"{BASE_URL}/_layouts/15/listform.aspx?"
            f"PageType=4&ListId={LIST_ID}&ID={item_id}"
            f"&ContentTypeID={CONTENT_TYPE}"
        )
        logger.info(f"Fetching year {year} (ID={item_id})")
        resp = self._get(url)
        return resp.text

    def _parse_year_items(self, html: str, year: int) -> List[Dict[str, Any]]:
        """Parse gazette items from a year page HTML.

        Extracts rows from the table containing notification numbers,
        titles, and PDF links (English versions).
        """
        items = []
        seen_pdfs = set()

        # Strategy: find all English PDF links and extract surrounding context
        # The page has a table with columns: Notification No. | Malay Text | English Text | Gazette No.
        # We look for English PDF links (not BM ones)
        for match in PDF_LINK_RE.finditer(html):
            pdf_path = html_mod.unescape(match.group(1))
            decoded_path = unquote(pdf_path)

            # Skip Malay versions
            if "/BM/" in decoded_path or "[M]" in decoded_path or "(M)" in decoded_path:
                continue

            if pdf_path in seen_pdfs:
                continue
            seen_pdfs.add(pdf_path)

            notif_no = extract_notif_no(pdf_path)
            pdf_url = f"{BASE_URL}{pdf_path}"

            # Try to extract title from the link text or surrounding context
            # Look for text in the anchor tag containing this PDF
            anchor_re = re.compile(
                r'<a[^>]*href=["\']' + re.escape(pdf_path) + r'["\'][^>]*>(.*?)</a>',
                re.DOTALL | re.IGNORECASE,
            )
            title = ""
            anchor_match = anchor_re.search(html)
            if anchor_match:
                title = strip_html(anchor_match.group(1))

            if not title:
                # Derive title from filename
                fname = unquote(pdf_path.rsplit("/", 1)[-1])
                title = fname.replace(".pdf", "").replace(".PDF", "")
                title = title.replace("_", " ").replace("[E]", "").replace("[e]", "").strip()

            # Extract gazette number if visible nearby
            gazette_no = ""
            # Look for gazette number in a nearby table cell
            pos = match.start()
            context = html[max(0, pos - 200):min(len(html), pos + 500)]
            gaz_match = re.search(r'(?:Gazette\s*No\.?\s*|No\.\s*)(\d+)', context, re.IGNORECASE)
            if gaz_match:
                gazette_no = gaz_match.group(1)

            doc_id = f"S{notif_no.replace('S ', '')}_{year}" if notif_no else f"{year}_{len(items)}"

            items.append({
                "doc_id": doc_id,
                "title": title,
                "notification_no": notif_no,
                "gazette_no": gazette_no,
                "year": year,
                "pdf_url": pdf_url,
                "pdf_path": pdf_path,
            })

        logger.info(f"  Year {year}: found {len(items)} English gazette items")
        return items

    def _extract_text(self, pdf_url: str, doc_id: str) -> str:
        """Download PDF and extract text."""
        try:
            resp = self._get(pdf_url)
            pdf_bytes = resp.content
            if len(pdf_bytes) < 100:
                logger.warning(f"PDF too small ({len(pdf_bytes)} bytes): {pdf_url}")
                return ""
            text = extract_pdf_markdown(
                "BN/AGC-GazetteII", doc_id, pdf_bytes=pdf_bytes
            )
            return text
        except Exception as e:
            logger.warning(f"Failed to extract text from {pdf_url}: {e}")
            return ""

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all gazette items with full text."""
        for year in range(2026, 1997, -1):
            try:
                html = self._fetch_year_page(year)
                items = self._parse_year_items(html, year)
                for item in items:
                    text = self._extract_text(item["pdf_url"], item["doc_id"])
                    if text:
                        item["text"] = text
                        yield self.normalize(item)
                    else:
                        logger.warning(f"No text extracted for {item['doc_id']}")
            except Exception as e:
                logger.error(f"Error processing year {year}: {e}")
                continue

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch gazette items from current year only."""
        current_year = datetime.now().year
        try:
            html = self._fetch_year_page(current_year)
            items = self._parse_year_items(html, current_year)
            for item in items:
                text = self._extract_text(item["pdf_url"], item["doc_id"])
                if text:
                    item["text"] = text
                    yield self.normalize(item)
        except Exception as e:
            logger.error(f"Error fetching updates for {current_year}: {e}")

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        doc_id = raw.get("doc_id", "")
        notif_no = raw.get("notification_no", "")
        year = raw.get("year", "")
        title = raw.get("title", "")

        # Build a better title if we have notification number
        if notif_no and notif_no not in title:
            display_title = f"{notif_no}/{year} — {title}" if title else f"{notif_no}/{year}"
        else:
            display_title = title or f"Gazette II {doc_id}"

        return {
            "_id": f"BN/AGC-GazetteII/{doc_id}",
            "_source": "BN/AGC-GazetteII",
            "_type": "legislation",
            "_fetched_at": now,
            "title": display_title,
            "text": raw.get("text", ""),
            "date": f"{year}-01-01",
            "url": raw.get("pdf_url", ""),
            "doc_id": doc_id,
            "notification_no": notif_no,
            "gazette_no": raw.get("gazette_no", ""),
            "year": year,
            "pdf_url": raw.get("pdf_url", ""),
            "language": "en",
            "jurisdiction": "BN",
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = BNGazetteIIScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        import requests
        try:
            url = (
                f"{BASE_URL}/_layouts/15/listform.aspx?"
                f"PageType=4&ListId={LIST_ID}&ID=97"
                f"&ContentTypeID={CONTENT_TYPE}"
            )
            resp = requests.get(
                url,
                headers={"User-Agent": "LegalDataHunter/1.0"},
                timeout=30,
            )
            print(f"Connection OK: {resp.status_code}")
        except Exception as e:
            print(f"Connection failed: {e}")
            sys.exit(1)
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
