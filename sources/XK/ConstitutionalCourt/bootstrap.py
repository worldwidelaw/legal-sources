#!/usr/bin/env python3
"""
XK/ConstitutionalCourt -- Kosovo Constitutional Court (Gjykata Kushtetuese) Fetcher

Fetches Constitutional Court decisions with full text via the official API.

Data access method:
  - REST API at https://api.webgjk-ks.org
  - POST /publish/CdmsCase/getAllFilteredDecisions for paginated list
  - Full text embedded in description/descriptionSR fields (HTML)
  - PDF links available in documents array

Coverage:
  - Kosovo Constitutional Court decisions since 2009
  - Judgments, Resolutions, Decisions
  - Available in Albanian (sq), Serbian (sr), English (titles only)
  - ~3000 total decisions

Usage:
  python bootstrap.py bootstrap           # Full historical pull
  python bootstrap.py bootstrap --sample  # Fetch 10+ sample records
  python bootstrap.py update              # Incremental update
  python bootstrap.py test-api            # Quick connectivity test
"""

import html
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Configuration
SOURCE_ID = "XK/ConstitutionalCourt"
API_BASE = "https://api.webgjk-ks.org"
DECISIONS_ENDPOINT = f"{API_BASE}/publish/CdmsCase/getAllFilteredDecisions"
WEB_URL = "https://gjk-ks.org"

PAGE_SIZE = 100
REQUEST_DELAY = 0.5  # seconds between requests
REQUEST_TIMEOUT = 60

# HTTP headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; WorldWideLaw/1.0; +https://github.com/worldwidelaw/legal-sources)",
    "Accept": "application/json",
    "Content-Type": "application/json",
}


def clean_html(html_text: str) -> str:
    """Strip HTML tags and clean text."""
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text)
    # Decode HTML entities
    text = html.unescape(text)
    return text.strip()


def normalize(raw: Dict) -> Dict:
    """Transform raw API data into normalized schema."""
    case_number = raw.get("caseNumber", "")
    record_id = raw.get("id", "")

    # Get title (prefer Albanian, fallback to Serbian)
    title = raw.get("title", "") or raw.get("titleSR", "")
    if not title:
        title = f"Decision {case_number}"

    # Get full text - combine Albanian and Serbian descriptions
    text_sq = clean_html(raw.get("description", ""))
    text_sr = clean_html(raw.get("descriptionSR", ""))

    # Use Albanian as primary, but include Serbian if different
    if text_sq and text_sr and text_sq != text_sr:
        text = f"{text_sq}\n\n--- Serbian / Srpski ---\n\n{text_sr}"
    else:
        text = text_sq or text_sr

    # Parse date
    entry_date = raw.get("entryDate", "")
    if entry_date:
        try:
            dt = datetime.fromisoformat(entry_date.replace("Z", "+00:00"))
            date_str = dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            date_str = entry_date[:10] if len(entry_date) >= 10 else entry_date
    else:
        date_str = None

    # Build URL
    url = f"{WEB_URL}/en/decision/{case_number.replace('/', '-').lower()}" if case_number else ""

    # Extract PDF links
    documents = raw.get("documents", [])
    pdf_urls = [doc.get("documentUrl") for doc in documents if doc.get("documentUrl")]

    # Get decision type
    decision_type = raw.get("titleENFilter", "") or raw.get("titleFilter", "")

    # Get complainant
    complainant = raw.get("complainant", "") or raw.get("complainantSR", "")

    return {
        "_id": case_number or str(record_id),
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "title_sr": raw.get("titleSR", ""),
        "text": text,
        "date": date_str,
        "url": url,
        "case_number": case_number,
        "decision_type": decision_type,
        "complainant": complainant,
        "pdf_urls": pdf_urls,
        "languages": ["sq", "sr"],  # Albanian and Serbian
    }


class KosovoConstitutionalCourtAPI:
    """API client for Kosovo Constitutional Court."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _post(self, url: str, data: Dict) -> Optional[Dict]:
        """Make a POST request with rate limiting."""
        try:
            time.sleep(REQUEST_DELAY)
            resp = self.session.post(url, json=data, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.warning(f"Request failed for {url}: {e}")
            return None

    def test_connection(self) -> bool:
        """Test API connectivity."""
        result = self._post(DECISIONS_ENDPOINT, {"PageNumber": 1, "PageSize": 1})
        if result and result.get("status") == 0:
            total = result.get("data", [{}])[0].get("totalCount", 0) if result.get("data") else 0
            logger.info(f"API connected. Total decisions: {total}")
            return True
        logger.error(f"API test failed: {result}")
        return False

    def get_total_count(self) -> int:
        """Get total number of decisions."""
        result = self._post(DECISIONS_ENDPOINT, {"PageNumber": 1, "PageSize": 1})
        if result and result.get("status") == 0 and result.get("data"):
            return result["data"][0].get("totalCount", 0)
        return 0

    def fetch_page(self, page: int, page_size: int = PAGE_SIZE) -> List[Dict]:
        """Fetch a page of decisions."""
        result = self._post(DECISIONS_ENDPOINT, {
            "PageNumber": page,
            "PageSize": page_size
        })
        if result and result.get("status") == 0:
            return result.get("data", [])
        return []

    def fetch_all(self, sample_mode: bool = False) -> Iterator[Dict]:
        """Fetch all decisions, yielding normalized records."""
        total = self.get_total_count()
        logger.info(f"Total decisions to fetch: {total}")

        if sample_mode:
            # Fetch just first 2 pages (up to 15 records) for sample
            max_pages = 2
            page_size = 10
        else:
            max_pages = (total // PAGE_SIZE) + 2
            page_size = PAGE_SIZE

        page = 1
        fetched = 0

        while page <= max_pages:
            logger.info(f"Fetching page {page}...")
            records = self.fetch_page(page, page_size)

            if not records:
                logger.info(f"No more records at page {page}")
                break

            for raw in records:
                normalized = normalize(raw)
                if normalized.get("text"):
                    fetched += 1
                    yield normalized
                else:
                    logger.warning(f"No text for {raw.get('caseNumber')}")

            page += 1

            if sample_mode and fetched >= 15:
                break

        logger.info(f"Fetched {fetched} decisions with full text")

    def fetch_updates(self, since: datetime) -> Iterator[Dict]:
        """Fetch decisions published since a given date."""
        # API returns most recent first, so we can stop when we hit old records
        page = 1
        done = False

        while not done:
            records = self.fetch_page(page)
            if not records:
                break

            for raw in records:
                entry_date = raw.get("entryDate", "")
                if entry_date:
                    try:
                        dt = datetime.fromisoformat(entry_date.replace("Z", "+00:00"))
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        if dt < since:
                            done = True
                            break
                    except (ValueError, TypeError):
                        pass

                normalized = normalize(raw)
                if normalized.get("text"):
                    yield normalized

            page += 1


def save_sample(records: List[Dict], sample_dir: Path):
    """Save sample records to JSONL file."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    # Save individual samples for inspection
    for i, record in enumerate(records[:10]):
        filename = f"sample_{i+1}_{record['_id'].replace('/', '_')}.json"
        with open(sample_dir / filename, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    # Save all samples to JSONL
    jsonl_path = sample_dir / "samples.jsonl"
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    logger.info(f"Saved {len(records)} samples to {sample_dir}")


def validate_samples(records: List[Dict]) -> bool:
    """Validate that samples meet quality requirements."""
    if len(records) < 10:
        logger.error(f"Only {len(records)} records - need at least 10")
        return False

    # Check text field
    empty_text = sum(1 for r in records if not r.get("text"))
    if empty_text > 0:
        logger.error(f"{empty_text} records have empty text field")
        return False

    # Check text is not just metadata
    short_text = sum(1 for r in records if len(r.get("text", "")) < 500)
    if short_text > len(records) * 0.5:
        logger.warning(f"{short_text} records have very short text (<500 chars)")

    # Check required fields
    required = ["_id", "_source", "_type", "title", "text", "date"]
    for field in required:
        missing = sum(1 for r in records if not r.get(field))
        if missing > len(records) * 0.1:
            logger.warning(f"{missing} records missing {field}")

    logger.info("Sample validation passed")
    return True


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    api = KosovoConstitutionalCourtAPI()

    if command == "test-api":
        if api.test_connection():
            print("API connection successful")
            sys.exit(0)
        else:
            print("API connection failed")
            sys.exit(1)

    elif command == "bootstrap":
        records = list(api.fetch_all(sample_mode=sample_mode))

        if not records:
            logger.error("No records fetched")
            sys.exit(1)

        save_sample(records, sample_dir)

        if validate_samples(records):
            print(f"Bootstrap complete: {len(records)} records with full text")
            sys.exit(0)
        else:
            print("Validation failed")
            sys.exit(1)

    elif command == "update":
        # For incremental updates, fetch last 30 days
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=30)
        records = list(api.fetch_updates(since))

        if records:
            save_sample(records, sample_dir)
            print(f"Update complete: {len(records)} new records")
        else:
            print("No new records")
        sys.exit(0)

    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
