#!/usr/bin/env python3
"""
UN/UPRDocuments - OHCHR Universal Periodic Review Documents

Fetches UPR documents (national reports, UN compilations, stakeholder summaries)
for each country review across all UPR cycles.

Data flow:
  1. UPR Info Uwazi API -> review metadata (country, session, cycle)
  2. UN ODS -> PDF download via document symbol
  3. common/pdf_extract -> full text extraction

~676 reviews x 3 document types = ~2000 PDF documents.

License: UN public domain
Auth: None

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UN.UPRDocuments")

SOURCE_ID = "UN/UPRDocuments"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LegalDataHunter/1.0",
    "Accept": "application/json",
}

UWAZI_BASE = "https://upr-info-database.uwazi.io/api"
UWAZI_DOC_TEMPLATE = "5e57cdd6f54e0a1304c0d50d"
UWAZI_STATE_TEMPLATE = "5d8cdec361cde0408222d3ec"
ODS_SYMBOL_URL = "https://documents.un.org/api/symbol/access"

# Document types and their symbol suffix
DOC_TYPES = {
    "national_report": {"suffix": "1", "label": "National Report"},
    "un_compilation": {"suffix": "2", "label": "UN Compilation"},
    "stakeholder_summary": {"suffix": "3", "label": "Stakeholder Summary"},
}

RATE_LIMIT_DELAY = 2.0


def fetch_url(url: str, timeout: int = 60) -> Optional[requests.Response]:
    """Fetch a URL with error handling."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout)
        resp.raise_for_status()
        return resp
    except requests.RequestException as e:
        logger.warning(f"Error fetching {url[:120]}: {e}")
        return None


def fetch_state_map() -> dict:
    """Fetch State entities from Uwazi to build sharedId -> (name, ISO-3) map."""
    url = (
        f"{UWAZI_BASE}/search"
        f"?limit=200&types=%5B%22{UWAZI_STATE_TEMPLATE}%22%5D"
    )
    resp = fetch_url(url, timeout=30)
    if not resp:
        return {}
    data = resp.json()
    mapping = {}
    for r in data.get("rows", []):
        sid = r.get("sharedId", "")
        name = r.get("title", "")
        icon = r.get("icon", {}) or {}
        code = icon.get("_id", "")
        if sid and code:
            mapping[sid] = (name, code)
    logger.info(f"State map: {len(mapping)} countries")
    return mapping


def fetch_reviews(limit: int = 0) -> list[dict]:
    """Fetch all UPR review metadata from Uwazi API."""
    logger.info("Loading state -> ISO-3 mapping...")
    state_map = fetch_state_map()

    reviews = []
    page_size = 100
    offset = 0

    while True:
        url = (
            f"{UWAZI_BASE}/search"
            f"?limit={page_size}&from={offset}"
            f"&types=%5B%22{UWAZI_DOC_TEMPLATE}%22%5D"
            f"&order=asc&sort=creationDate"
        )
        resp = fetch_url(url, timeout=30)
        if not resp:
            break

        data = resp.json()
        rows = data.get("rows", [])
        total = data.get("totalRows", 0)

        for row in rows:
            md = row.get("metadata", {})

            # Resolve country via state_map
            state = md.get("state_reviewed", [{}])
            if isinstance(state, list):
                state = state[0] if state else {}
            state_id = state.get("value", "")
            country_name, country_code = state_map.get(state_id, (state.get("label", "Unknown"), ""))

            # Fallback: get ISO-3 from inline icon if state_map lookup failed
            if not country_code:
                icon = state.get("icon", {}) or {}
                country_code = icon.get("_id", "")

            # Extract session number
            session_data = md.get("session_of_the_document", [{}])
            if isinstance(session_data, list):
                session_data = session_data[0] if session_data else {}
            session_label = session_data.get("label", "")
            session_match = re.match(r"(\d+)", session_label)
            session_num = int(session_match.group(1)) if session_match else None

            # Extract cycle
            cycle_data = md.get("cycle_of_the_document", [{}])
            if isinstance(cycle_data, list):
                cycle_data = cycle_data[0] if cycle_data else {}
            cycle_label = cycle_data.get("label", "")
            cycle_match = re.search(r"Cycle\s+(\d+)", cycle_label)
            cycle_num = int(cycle_match.group(1)) if cycle_match else None

            # Extract date
            date_data = md.get("document_date", [{}])
            if isinstance(date_data, list):
                date_data = date_data[0] if date_data else {}
            date_ts = date_data.get("value")
            date_str = None
            if date_ts and isinstance(date_ts, (int, float)):
                date_str = datetime.fromtimestamp(date_ts, tz=timezone.utc).strftime("%Y-%m-%d")

            if country_code and session_num:
                reviews.append({
                    "country_name": country_name,
                    "country_code": country_code,
                    "session": session_num,
                    "cycle": cycle_num,
                    "date": date_str,
                    "title": row.get("title", ""),
                })

        logger.info(f"Fetched {len(reviews)}/{total} reviews...")
        offset += page_size

        if offset >= total or len(rows) == 0:
            break
        if limit > 0 and len(reviews) >= limit:
            reviews = reviews[:limit]
            break

        time.sleep(1.0)

    logger.info(f"Total: {len(reviews)} reviews with valid metadata")
    return reviews


def fetch_upr_pdf(symbol: str) -> Optional[bytes]:
    """Fetch a UPR document PDF from UN ODS by document symbol."""
    url = f"{ODS_SYMBOL_URL}?s={requests.utils.quote(symbol)}&l=en&t=pdf"
    try:
        resp = requests.get(
            url, headers=HEADERS, timeout=120, allow_redirects=True
        )
        if resp.status_code == 200 and resp.content[:5] == b"%PDF-":
            return resp.content
        logger.debug(f"Not a PDF or error ({resp.status_code}) for {symbol}")
        return None
    except requests.RequestException as e:
        logger.warning(f"Error downloading {symbol}: {e}")
        return None


def make_symbol(session: int, country_code: str, suffix: str) -> str:
    """Construct a UN document symbol for a UPR document."""
    return f"A/HRC/WG.6/{session}/{country_code}/{suffix}"


class UPRDocumentsScraper(BaseScraper):
    """Scraper for UN/UPRDocuments - OHCHR Universal Periodic Review Documents."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize a UPR document into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 50:
            return None

        review = raw["review"]
        doc_type_key = raw["doc_type_key"]
        symbol = raw["symbol"]
        doc_info = DOC_TYPES[doc_type_key]
        cc = review["country_code"]
        sess = review["session"]

        title = (
            f"{review['country_name']} - {doc_info['label']} "
            f"(Cycle {review.get('cycle', '?')}, Session {sess})"
        )

        return {
            "_id": f"UPR-{cc}-C{review.get('cycle', 0)}-S{sess}-{doc_info['suffix']}",
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": review.get("date"),
            "url": f"https://documents.un.org/api/symbol/access?s={requests.utils.quote(symbol)}&l=en&t=pdf",
            "country_reviewed": review["country_name"],
            "country_code": cc,
            "session": sess,
            "cycle": review.get("cycle"),
            "doc_type": doc_type_key,
            "symbol": symbol,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all UPR documents across all reviews."""
        logger.info("Fetching review metadata from Uwazi API...")
        reviews = fetch_reviews()

        total_docs = 0
        total_text = 0

        for i, review in enumerate(reviews):
            cc = review["country_code"]
            sess = review["session"]
            logger.info(
                f"[{i+1}/{len(reviews)}] {review['country_name']} - "
                f"Session {sess}, Cycle {review.get('cycle')}"
            )

            for doc_key, doc_info in DOC_TYPES.items():
                symbol = make_symbol(sess, cc, doc_info["suffix"])
                logger.info(f"  {doc_info['label']}: {symbol}")

                pdf_bytes = fetch_upr_pdf(symbol)
                if not pdf_bytes:
                    time.sleep(RATE_LIMIT_DELAY)
                    continue

                logger.info(f"    PDF: {len(pdf_bytes)} bytes")

                text = extract_pdf_markdown(
                    source=SOURCE_ID,
                    source_id=f"{cc}-S{sess}-{doc_info['suffix']}",
                    pdf_bytes=pdf_bytes,
                    table="doctrine",
                ) or ""

                logger.info(f"    Text: {len(text)} chars")

                total_docs += 1
                if text:
                    total_text += 1

                yield {
                    "review": review,
                    "doc_type_key": doc_key,
                    "symbol": symbol,
                    "text": text,
                }

                time.sleep(RATE_LIMIT_DELAY)

        logger.info(f"Total: {total_docs} documents, {total_text} with text")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """UPR documents are static, so updates re-fetch everything."""
        yield from self.fetch_all()


def test_connectivity():
    """Test connectivity to required APIs."""
    print("Testing UPR Documents connectivity...\n")

    # Test Uwazi API
    url = f"{UWAZI_BASE}/search?limit=1&types=%5B%22{UWAZI_DOC_TEMPLATE}%22%5D"
    resp = fetch_url(url, timeout=30)
    if resp:
        data = resp.json()
        total = data.get("totalRows", 0)
        print(f"  Uwazi API: OK ({total} review documents)")
    else:
        print("  Uwazi API: FAILED")
        return False

    # Test UN ODS symbol access
    symbol = "A/HRC/WG.6/43/FRA/1"
    pdf_bytes = fetch_upr_pdf(symbol)
    if pdf_bytes:
        print(f"  UN ODS PDF: OK ({len(pdf_bytes)} bytes for {symbol})")
    else:
        print("  UN ODS PDF: FAILED")
        return False

    # Test PDF extraction
    text = extract_pdf_markdown(
        source=SOURCE_ID,
        source_id="test-FRA-S44-1",
        pdf_bytes=pdf_bytes,
        table="doctrine",
    ) or ""
    if len(text) > 100:
        print(f"  PDF extraction: OK ({len(text)} chars)")
    else:
        print(f"  PDF extraction: WEAK ({len(text)} chars)")

    print("\nConnectivity test complete.")
    return True


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        success = test_connectivity()
        sys.exit(0 if success else 1)
    elif command in ("bootstrap", "validate", "fetch"):
        scraper = UPRDocumentsScraper()
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
