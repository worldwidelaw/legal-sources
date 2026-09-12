#!/usr/bin/env python3
"""
LC/GovtPortal -- Saint Lucia Government Legislation Portal

Fetches legislation from the Government of Saint Lucia web portal.
Full text extracted from PDFs via pdfplumber.

Strategy:
  1. Call /api/services.asmx/GetResourceSummaries to get all ~103 items
  2. Download each PDF from media.govt.lc
  3. Extract full text via pdfplumber
  4. Skip non-PDF files (DOCX/DOC)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import hashlib
import logging
import time
from io import BytesIO
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
import pdfplumber
from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.LC.GovtPortal")

SOURCE_ID = "LC/GovtPortal"
BASE_URL = "https://www.govt.lc"
API_URL = f"{BASE_URL}/api/services.asmx/GetResourceSummaries"
MEDIA_BASE = f"{BASE_URL}/media.govt.lc/www/resources"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

API_QUERY = {
    "ResourceTypeNames": "legislation",
    "FilterType": 0,
    "FilterValue": "",
    "StartDate": "0001-01-01T05:00:00.0000000Z",
    "EndDate": "0001-01-01T05:00:00.0000000Z",
    "StatusFilter": 2,
    "ParentResourceId": None,
    "ParentResourceMatchType": 0,
    "FriendlyDescription": None,
    "SortType": 3,
    "IncrementalSearch": None,
    "Route": {"Subject": "legislation", "Preposition": "", "Object": ""},
}


def _make_id(guid: str) -> str:
    """Use the GUID from the API as document ID."""
    return guid


def _extract_year(title: str) -> Optional[str]:
    """Extract a year from the title for date estimation."""
    m = re.search(r"\b(19\d{2}|20\d{2})\b", title)
    return f"{m.group(1)}-01-01" if m else None


def _title_from_filename(filename: str) -> str:
    """Derive a readable title from the PDF filename."""
    name = unquote(filename)
    name = re.sub(r"\.\w+$", "", name)  # strip extension
    name = name.replace("-", " ").replace("_", " ")
    name = re.sub(r"\s+", " ", name).strip()
    return name


def _is_pdf(filename: str) -> bool:
    return filename.lower().endswith(".pdf")


class LCGovtPortalScraper(BaseScraper):
    """Scraper for LC/GovtPortal -- Saint Lucia Govt Legislation Portal."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = None

    def _get_session(self) -> requests.Session:
        if self.session is None:
            self.session = requests.Session()
            self.session.headers.update({
                "User-Agent": USER_AGENT,
                "Accept": "application/json, text/html, */*",
            })
            # Initialize session cookies
            self.session.get(f"{BASE_URL}/legislations", timeout=30)
        return self.session

    def _fetch_listing(self) -> list:
        """Fetch all legislation items from the JSON API."""
        sess = self._get_session()
        payload = {
            "query": API_QUERY,
            "cursor": {"StartRowIndex": 0, "PageSize": 500},
        }
        resp = sess.post(
            API_URL,
            json=payload,
            headers={"Content-Type": "application/json; charset=utf-8"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        result = data.get("d", {}).get("Result", {})
        items = result.get("Items", [])
        logger.info("API returned %d legislation items", len(items))
        return items

    def _download_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download a PDF and extract text via pdfplumber."""
        sess = self._get_session()
        try:
            self.rate_limiter.wait()
            resp = sess.get(pdf_url, timeout=60)
            resp.raise_for_status()
        except Exception as e:
            logger.warning("Failed to download %s: %s", pdf_url, e)
            return None

        if resp.content[:4] != b"%PDF":
            logger.warning("Not a PDF: %s (content-type: %s)",
                           pdf_url, resp.headers.get("content-type", ""))
            return None

        try:
            pages_text = []
            with pdfplumber.open(BytesIO(resp.content)) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
            full_text = "\n\n".join(pages_text)
            if len(full_text.strip()) < 50:
                logger.warning("Insufficient text from %s: %d chars",
                               pdf_url, len(full_text))
                return None
            return full_text
        except Exception as e:
            logger.warning("PDF extraction failed for %s: %s", pdf_url, e)
            return None

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all legislation records with full text."""
        items = self._fetch_listing()
        if not items:
            logger.error("No items returned from API")
            return

        pdf_items = [i for i in items if _is_pdf(i.get("FileName", ""))]
        logger.info("%d PDF items out of %d total", len(pdf_items), len(items))

        count = 0
        for item in pdf_items:
            title = item.get("Title", "").strip()
            filename = item.get("FileName", "")
            folder = item.get("UrlFolder", "legislation")
            guid = item.get("Id", "")
            date_str = item.get("Date", "")

            if not title:
                title = _title_from_filename(filename)

            pdf_url = f"{MEDIA_BASE}/{folder}/{filename}"
            page_url = f"{BASE_URL}{item.get('Url', '')}"

            # Parse date
            doc_date = None
            if date_str:
                try:
                    dt = datetime.fromisoformat(date_str.replace("Z", "+00:00").rstrip("0").rstrip("."))
                    doc_date = dt.strftime("%Y-%m-%d")
                except (ValueError, AttributeError):
                    pass
            if not doc_date:
                doc_date = _extract_year(title)

            logger.info("Downloading [%d/%d]: %s", count + 1, len(pdf_items), title[:80])
            text = self._download_pdf_text(pdf_url)
            if text is None:
                logger.warning("Skipping (no text): %s", title[:80])
                continue

            yield {
                "id": guid,
                "title": title,
                "text": text,
                "date": doc_date,
                "url": page_url,
                "pdf_url": pdf_url,
                "filename": filename,
            }
            count += 1

        logger.info("Completed: %d records with full text", count)

    def test_connection(self) -> bool:
        """Quick connectivity test."""
        try:
            items = self._fetch_listing()
            has_items = len(items) > 0
            logger.info("API test: %d items — %s", len(items), "OK" if has_items else "FAIL")

            # Test PDF download
            pdf_items = [i for i in items if _is_pdf(i.get("FileName", ""))]
            if pdf_items:
                item = pdf_items[0]
                folder = item.get("UrlFolder", "legislation")
                pdf_url = f"{MEDIA_BASE}/{folder}/{item['FileName']}"
                sess = self._get_session()
                self.rate_limiter.wait()
                resp = sess.get(pdf_url, timeout=30)
                pdf_ok = resp.content[:4] == b"%PDF"
                logger.info("PDF test: %s (%d bytes) — %s",
                            item["FileName"], len(resp.content),
                            "OK" if pdf_ok else "FAIL")
                return has_items and pdf_ok

            return has_items
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Yield documents updated since a given date."""
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        try:
            since_dt = datetime.fromisoformat(since)
        except ValueError:
            logger.error("Invalid since date: %s", since)
            return

        for record in self.fetch_all():
            if record.get("date"):
                try:
                    rec_dt = datetime.fromisoformat(record["date"])
                    if rec_dt >= since_dt:
                        yield record
                except ValueError:
                    yield record
            else:
                yield record

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw data into standard schema."""
        return {
            "_id": raw["id"],
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "pdf_url": raw.get("pdf_url"),
            "filename": raw.get("filename"),
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = LCGovtPortalScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
