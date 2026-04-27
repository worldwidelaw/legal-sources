#!/usr/bin/env python3
"""
SI/AdminCourt -- Slovenian Administrative Court (Upravno sodišče) Fetcher

Fetches UPRS decisions from sodnapraksa.si (Slovenia's case law database).

Strategy:
  - Search API at sodnapraksa.si with database[UPRS]=UPRS filter
  - Paginate through results (50 per page)
  - For each result, fetch document page and extract full text
  - Text sections: jedro (summary), izrek (ruling), obrazložitev (reasoning)

Endpoints:
  - Search: https://www.sodnapraksa.si/search.php?q=*&database[UPRS]=UPRS&rowsPerPage=50&page=N
  - Document: https://www.sodnapraksa.si/search.php?...&id=NNNNN

Data:
  - ~35,650 UPRS (Administrative Court) decisions
  - Language: Slovenian (SL)
  - ECLI identifiers: ECLI:SI:UPRS:YYYY:*
  - Rate limit: 2 seconds between requests

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urlencode

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SI.admincourt")

BASE_URL = "https://www.sodnapraksa.si"
SEARCH_PATH = "/search.php"
DATABASE = "UPRS"
ROWS_PER_PAGE = 50
RATE_LIMIT_SECONDS = 2


class SlovenianAdminCourtScraper(BaseScraper):
    """
    Scraper for SI/AdminCourt -- Slovenian Administrative Court.
    Country: SI
    URL: https://www.sodnapraksa.si

    Data types: case_law
    Auth: none (Open public access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "sl,en",
            },
            timeout=60,
        )

    def _search_page(self, page: int = 0) -> tuple:
        """
        Search UPRS database, return (list of doc IDs/titles, total count).
        """
        params = {
            "q": "*",
            f"database[{DATABASE}]": DATABASE,
            "_submit": "išči",
            "rowsPerPage": ROWS_PER_PAGE,
            "page": page,
            "order": "date",
            "direction": "desc",
        }

        url = f"{BASE_URL}{SEARCH_PATH}?{urlencode(params)}"
        logger.info(f"Searching page {page}: {url}")

        time.sleep(RATE_LIMIT_SECONDS)
        resp = self.client.session.get(url, timeout=60, headers={
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
        })
        resp.raise_for_status()
        html_content = resp.text

        # Extract total count from <span id="num-hits">
        total = 0
        total_match = re.search(r'id="num-hits"[^>]*>([^<]+)', html_content)
        if total_match:
            num_str = total_match.group(1).replace(".", "").replace(",", "").strip()
            digits = re.search(r"(\d+)", num_str)
            if digits:
                total = int(digits.group(1))

        # Extract document IDs from results table
        results = []
        # Links in results table contain id=NNNNN
        for match in re.finditer(r'<a[^>]*href="[^"]*id=(\d+)[^"]*"[^>]*>([^<]+)</a>', html_content):
            doc_id = match.group(1)
            title = html_module.unescape(match.group(2)).strip()
            if doc_id and title:
                results.append({"id": doc_id, "title": title})

        return results, total

    def _fetch_document(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a single document by ID and extract all content.
        """
        params = {
            "q": "*",
            f"database[{DATABASE}]": DATABASE,
            "_submit": "išči",
            "rowsPerPage": "10",
            "page": "0",
            "id": doc_id,
        }

        url = f"{BASE_URL}{SEARCH_PATH}?{urlencode(params)}"

        time.sleep(RATE_LIMIT_SECONDS)
        try:
            resp = self.client.session.get(url, timeout=60, headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            })
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch document {doc_id}: {e}")
            return None

        html_content = resp.text

        # Check for doc-content div
        doc_content_match = re.search(
            r'<div\s+id="doc-content">(.*?)(?=<div\s+id="doc-footer"|<footer|</body)',
            html_content,
            re.DOTALL,
        )
        if not doc_content_match:
            logger.warning(f"No doc-content found for {doc_id}")
            return None

        doc_html = doc_content_match.group(1)

        # Extract decision number from doc-head-right
        decision_number = ""
        head_match = re.search(r'id="doc-head-right"[^>]*>(.*?)</p>', doc_html, re.DOTALL)
        if head_match:
            decision_number = re.sub(r'<[^>]+>', '', head_match.group(1)).strip()

        # Extract metadata table
        metadata = {}
        meta_match = re.search(r'<table\s+id="doc-meta">(.*?)</table>', doc_html, re.DOTALL)
        if meta_match:
            for row_match in re.finditer(r'<th[^>]*>(.*?)</th>\s*<td[^>]*>(.*?)</td>', meta_match.group(1), re.DOTALL):
                key = re.sub(r'<[^>]+>', '', row_match.group(1)).strip().rstrip(":")
                value = re.sub(r'<[^>]+>', '', row_match.group(2)).strip()
                metadata[key] = value

        # Extract content sections by h2 headers
        def get_section(header_text):
            pattern = rf'<h2[^>]*>\s*{re.escape(header_text)}\s*</h2>(.*?)(?=<h2|<div\s+id="doc-|$)'
            match = re.search(pattern, doc_html, re.DOTALL)
            if not match:
                return ""
            section_html = match.group(1)
            # Strip HTML tags
            text = re.sub(r'<[^>]+>', ' ', section_html)
            text = html_module.unescape(text)
            text = re.sub(r'\s+', ' ', text).strip()
            return text

        jedro = get_section("Jedro")
        izrek = get_section("Izrek")
        obrazlozitev = get_section("Obrazložitev")

        # Extract legal references
        references = ""
        ref_match = re.search(r'id="doc-connection"[^>]*>(.*?)</div>', doc_html, re.DOTALL)
        if ref_match:
            references = re.sub(r'<[^>]+>', ' ', ref_match.group(1)).strip()

        # Parse date
        date = metadata.get("Datum odločbe", "")
        if date:
            try:
                parts = date.split(".")
                if len(parts) == 3:
                    date = f"{parts[2].strip()}-{parts[1].strip().zfill(2)}-{parts[0].strip().zfill(2)}"
            except Exception:
                pass

        return {
            "doc_id": doc_id,
            "decision_number": decision_number,
            "ecli": metadata.get("ECLI", ""),
            "evidence_number": metadata.get("Evidenčna številka", ""),
            "court": metadata.get("Sodišče", ""),
            "department": metadata.get("Oddelek", ""),
            "date": date,
            "legal_area": metadata.get("Področje", ""),
            "keywords": metadata.get("Institut", ""),
            "jedro": jedro,
            "izrek": izrek,
            "obrazlozitev": obrazlozitev,
            "references": references,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all UPRS documents."""
        # Get first page to discover total
        results, total = self._search_page(page=0)
        logger.info(f"Total UPRS documents: {total:,}")

        total_pages = (total // ROWS_PER_PAGE) + 1
        fetched = 0

        for page_num in range(total_pages):
            if page_num > 0:
                results, _ = self._search_page(page=page_num)

            if not results:
                logger.info(f"No results on page {page_num}, stopping")
                break

            for result in results:
                doc = self._fetch_document(result["id"])
                if doc:
                    yield doc
                    fetched += 1
                    if fetched % 50 == 0:
                        logger.info(f"Fetched {fetched} documents so far")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents modified since the given date."""
        # Fetch recent pages and check dates
        results, total = self._search_page(page=0)
        since_str = since.strftime("%Y-%m-%d")

        for page_num in range(min(10, (total // ROWS_PER_PAGE) + 1)):
            if page_num > 0:
                results, _ = self._search_page(page=page_num)

            if not results:
                break

            for result in results:
                doc = self._fetch_document(result["id"])
                if doc and doc.get("date", "") >= since_str:
                    yield doc
                elif doc and doc.get("date", "") < since_str:
                    return  # Results are sorted by date desc, so we can stop

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        # Combine text sections
        text_parts = []
        if raw.get("jedro"):
            text_parts.append(f"JEDRO (Summary):\n{raw['jedro']}")
        if raw.get("izrek"):
            text_parts.append(f"IZREK (Ruling):\n{raw['izrek']}")
        if raw.get("obrazlozitev"):
            text_parts.append(f"OBRAZLOŽITEV (Reasoning):\n{raw['obrazlozitev']}")

        full_text = "\n\n".join(text_parts)

        if not full_text or len(full_text) < 50:
            return None

        doc_id = raw.get("ecli") or raw.get("doc_id", "")
        return {
            "_id": f"SI_AdminCourt_{doc_id}",
            "_source": "SI/AdminCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("decision_number", ""),
            "text": full_text,
            "date": raw.get("date", ""),
            "url": f"{BASE_URL}{SEARCH_PATH}?id={raw['doc_id']}",
            "ecli": raw.get("ecli", ""),
            "decision_number": raw.get("decision_number", ""),
            "evidence_number": raw.get("evidence_number", ""),
            "court": raw.get("court", ""),
            "department": raw.get("department", ""),
            "legal_area": raw.get("legal_area", ""),
            "keywords": raw.get("keywords", ""),
            "references": raw.get("references", ""),
            "language": "sl",
        }

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            results, total = self._search_page(page=0)
            logger.info(f"Connectivity OK: {total:,} UPRS documents, {len(results)} on first page")

            if results:
                doc = self._fetch_document(results[0]["id"])
                if doc:
                    record = self.normalize(doc)
                    if record:
                        text_len = len(record.get("text", ""))
                        logger.info(f"Document extraction OK: {text_len} chars from {doc.get('ecli', doc['doc_id'])}")
                        return True
            return True
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            return False


# ── CLI entry point ──────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="SI/AdminCourt data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (10 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SlovenianAdminCourtScraper()

    if args.command == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample)
        print(json.dumps(stats, indent=2))
    elif args.command == "update":
        stats = scraper.update()
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
