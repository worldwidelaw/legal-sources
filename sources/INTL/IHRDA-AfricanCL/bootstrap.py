#!/usr/bin/env python3
"""
INTL/IHRDA-AfricanCL -- African Human Rights Case Law Analyser (IHRDA)

Fetches case law decisions from the IHRDA Case Law Analyser at caselaw.ihrda.org,
which runs on the Uwazi platform with a public JSON API.

Strategy:
  - Query /api/search with decision template IDs (ACHPR, AfCHPR, ECOWAS, etc.)
  - Paginate through all results (30 per page)
  - For each decision, fetch full text via /api/documents/page (page-by-page)
  - No authentication required

Covers: ACHPR, AfCHPR, ECOWAS Court, EACJ, ACERWC, SADC Tribunal

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import html as html_mod
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.IHRDA-AfricanCL")

BASE_URL = "https://caselaw.ihrda.org"
SEARCH_URL = f"{BASE_URL}/api/search"
TEXT_URL = f"{BASE_URL}/api/documents/page"

# Decision template IDs on the Uwazi platform
DECISION_TEMPLATES = {
    "59f8a857c01ffe324453ba2f": "ACHPR",
    "59fb1f73c01ffe324453baed": "AfCHPR",
    "5a0424a43fd6452f890c3f69": "ECOWAS",
    "5a09c5e03fd6452f890c4f28": "EACJ",
    "5a003120c01ffe324453c3b9": "ACERWC",
    "5a02de238d198d0468c16fec": "SADC_Matter",
    "5a02f1f78d198d0468c17041": "SADC",
    "5a02d63d8d198d0468c16f95": "AfCHPR_Matter",
}

PAGE_SIZE = 30


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    if not text:
        return ""
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</(?:p|div|h[1-6]|li|tr|blockquote)>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = html_mod.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def timestamp_to_iso(ts) -> Optional[str]:
    """Convert Unix timestamp (seconds) to ISO 8601 date string."""
    if not ts:
        return None
    try:
        if isinstance(ts, (list,)):
            ts = ts[0].get("value") if ts else None
            if not ts:
                return None
        if isinstance(ts, dict):
            ts = ts.get("value")
            if not ts:
                return None
        ts = int(ts)
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError, OSError):
        return None


def get_metadata_value(meta: dict, key: str) -> str:
    """Extract a single value from Uwazi metadata field."""
    val = meta.get(key, [])
    if isinstance(val, list) and val:
        first = val[0]
        if isinstance(first, dict):
            return first.get("label", first.get("value", ""))
        return str(first)
    return ""


def get_metadata_values(meta: dict, key: str) -> list:
    """Extract multiple values from Uwazi metadata field."""
    val = meta.get(key, [])
    if isinstance(val, list):
        result = []
        for item in val:
            if isinstance(item, dict):
                label = item.get("label", item.get("value", ""))
                if label:
                    result.append(label)
            elif item:
                result.append(str(item))
        return result
    return []


class IHRDAScraper(BaseScraper):
    """
    Scraper for INTL/IHRDA-AfricanCL -- African Human Rights Case Law.
    Country: INTL
    URL: https://caselaw.ihrda.org/

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, */*",
        })

    def _search_entities(self, template_id: str, offset: int = 0) -> dict:
        """Search for entities of a given template type."""
        params = {
            "limit": PAGE_SIZE,
            "from": offset,
            "types": json.dumps([template_id]),
        }
        self.rate_limiter.wait()
        resp = self.session.get(SEARCH_URL, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _fetch_document_text(self, doc_id: str, total_pages: int) -> str:
        """Fetch full text from a document by iterating through all pages."""
        all_text = []
        for page in range(1, total_pages + 1):
            self.rate_limiter.wait()
            try:
                resp = self.session.get(
                    TEXT_URL,
                    params={"_id": doc_id, "page": page},
                    timeout=20,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    text = data.get("data", "")
                    if text:
                        all_text.append(text.strip())
            except Exception as e:
                logger.warning("Failed to fetch page %d of doc %s: %s", page, doc_id, e)
        return "\n\n".join(all_text)

    def _entity_to_raw(self, entity: dict, tribunal: str) -> dict:
        """Convert an Uwazi entity to a raw document dict."""
        meta = entity.get("metadata", {})
        docs = entity.get("documents", [])

        # Get full text from the first English document (or any available)
        text = ""
        for doc in docs:
            doc_id = doc.get("_id", "")
            total_pages = doc.get("totalPages", 0)
            lang = doc.get("language", "")
            if total_pages > 0 and lang in ("eng", "en", ""):
                text = self._fetch_document_text(doc_id, total_pages)
                if text:
                    break

        # If no English doc found, try any document
        if not text:
            for doc in docs:
                doc_id = doc.get("_id", "")
                total_pages = doc.get("totalPages", 0)
                if total_pages > 0:
                    text = self._fetch_document_text(doc_id, total_pages)
                    if text:
                        break

        # Extract date
        year_decided = meta.get("year_decided", [])
        date = None
        if year_decided and isinstance(year_decided, list) and year_decided:
            date = timestamp_to_iso(year_decided)

        return {
            "shared_id": entity.get("sharedId", ""),
            "title": entity.get("title", ""),
            "text": text,
            "date": date,
            "tribunal": tribunal,
            "country": get_metadata_values(meta, "country"),
            "outcome": get_metadata_value(meta, "outcome"),
            "doc_type": get_metadata_value(meta, "type_of_document"),
            "keywords": get_metadata_values(meta, "keywords"),
            "headnotes": get_metadata_value(meta, "case_head_notes") or get_metadata_value(meta, "case_headnotes"),
            "language": entity.get("language", "en"),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions from all African HR courts."""
        for template_id, tribunal in DECISION_TEMPLATES.items():
            logger.info("Fetching %s decisions (template %s)", tribunal, template_id)
            offset = 0

            while True:
                data = self._search_entities(template_id, offset)
                rows = data.get("rows", [])
                total = data.get("totalRows", 0)

                if not rows:
                    break

                for entity in rows:
                    raw = self._entity_to_raw(entity, tribunal)
                    yield raw

                offset += PAGE_SIZE
                logger.info("  Fetched %d/%d %s decisions", min(offset, total), total, tribunal)

                if offset >= total:
                    break

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch decisions updated since a given date."""
        since_ts = int(since.timestamp())
        for template_id, tribunal in DECISION_TEMPLATES.items():
            offset = 0
            while True:
                data = self._search_entities(template_id, offset)
                rows = data.get("rows", [])
                total = data.get("totalRows", 0)

                if not rows:
                    break

                for entity in rows:
                    edit_date = entity.get("editDate", 0)
                    if edit_date and edit_date / 1000 >= since_ts:
                        raw = self._entity_to_raw(entity, tribunal)
                        yield raw

                offset += PAGE_SIZE
                if offset >= total:
                    break

    def normalize(self, raw: dict) -> dict:
        """Transform raw decision data into standard schema."""
        shared_id = raw["shared_id"]
        tribunal = raw["tribunal"]

        # Build URL
        url = f"{BASE_URL}/entity/{shared_id}"

        countries = raw.get("country", [])
        country_str = ", ".join(countries) if countries else ""

        return {
            "_id": f"IHRDA-{tribunal}-{shared_id}",
            "_source": "INTL/IHRDA-AfricanCL",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": url,
            "tribunal": tribunal,
            "country": country_str,
            "outcome": raw.get("outcome", ""),
            "doc_type": raw.get("doc_type", ""),
            "keywords": raw.get("keywords", []),
            "headnotes": raw.get("headnotes", ""),
        }


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = IHRDAScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        print("Testing IHRDA connectivity...")
        # Test search
        resp = requests.get(SEARCH_URL, params={
            "limit": 1,
            "types": json.dumps(["59f8a857c01ffe324453ba2f"]),
        }, timeout=30)
        data = resp.json()
        total = data.get("totalRows", 0)
        print(f"  ACHPR decisions: {total}")

        # Test text extraction
        rows = data.get("rows", [])
        if rows:
            docs = rows[0].get("documents", [])
            if docs:
                doc_id = docs[0].get("_id")
                pages = docs[0].get("totalPages", 0)
                print(f"  First doc: {doc_id}, {pages} pages")
                if pages > 0:
                    text_resp = requests.get(TEXT_URL, params={"_id": doc_id, "page": 1}, timeout=20)
                    text_data = text_resp.json()
                    text = text_data.get("data", "")
                    print(f"  Page 1 text: {len(text)} chars")
                    print(f"  Preview: {text[:200]}...")
        print("OK")

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(json.dumps(result, indent=2, default=str))

    elif command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=90)
        result = scraper.update(since=since)
        print(json.dumps(result, indent=2, default=str))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
