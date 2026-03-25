#!/usr/bin/env python3
"""
ME/Courts -- Montenegro Court Decisions (sudovi.me)

Fetches court decisions from all Montenegrin courts via REST API.
~594K decisions with full HTML text. No authentication required.

Strategy:
  1. GET /api/decisions/courts to list all courts and codes
  2. For each court, POST /api/search/decisions with pagination to get dbids
  3. GET /api/decision/{dbid} to fetch full text for each decision
  4. Strip HTML tags from tekst field for clean text

API endpoints:
  - POST /api/search/decisions  (Solr search, returns dbid list)
  - GET  /api/decision/{dbid}   (full text as HTML in 'tekst' field)
  - GET  /api/decisions/courts   (list of all courts)

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py test-api             # Quick connectivity test
"""

import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Generator

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ME.Courts")

BASE_URL = "https://sudovi.me"
API_URL = f"{BASE_URL}/api"


class TextExtractor(HTMLParser):
    """Extract clean text from HTML, stripping tags."""

    def __init__(self):
        super().__init__()
        self.text_parts = []
        self._skip = False
        self._block_tags = {"script", "style", "nav", "header", "footer", "noscript"}
        self._newline_tags = {
            "p", "div", "br", "h1", "h2", "h3", "h4", "h5", "h6",
            "li", "tr", "section", "article", "blockquote", "table",
        }

    def handle_starttag(self, tag, attrs):
        if tag in self._block_tags:
            self._skip = True
        if tag in self._newline_tags:
            self.text_parts.append("\n")

    def handle_endtag(self, tag):
        if tag in self._block_tags:
            self._skip = False
        if tag in self._newline_tags:
            self.text_parts.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self.text_parts.append(data)

    def get_text(self) -> str:
        text = "".join(self.text_parts)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)
        return text.strip()


def strip_html(html: str) -> str:
    """Convert HTML to clean text."""
    if not html:
        return ""
    extractor = TextExtractor()
    try:
        extractor.feed(html)
        return extractor.get_text()
    except Exception:
        # Fallback: crude tag stripping
        text = re.sub(r'<[^>]+>', ' ', html)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()


class MontenegroCourtscraper(BaseScraper):
    """Scraper for Montenegro Court Decisions API."""

    def __init__(self, source_dir: str):
        super().__init__(source_dir)
        self.client = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def _get_courts(self) -> list[dict]:
        """Fetch list of all courts."""
        resp = self.client.get(f"{API_URL}/decisions/courts", timeout=30)
        resp.raise_for_status()
        courts = resp.json()
        # Filter to courts that likely have decisions
        return [c for c in courts if c.get("code")]

    def _search_decisions(self, court_code: str, start: int = 0,
                          rows: int = 1000) -> dict:
        """Search decisions for a given court."""
        payload = {
            "courtCode": court_code,
            "start": start,
            "rows": rows,
        }
        self.rate_limiter.wait()
        resp = self.client.post(
            f"{API_URL}/search/decisions",
            json_data=payload,
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()

    def _get_decision(self, dbid: int) -> dict:
        """Fetch a single decision with full text."""
        self.rate_limiter.wait()
        resp = self.client.get(f"{API_URL}/decision/{dbid}", timeout=30)
        resp.raise_for_status()
        return resp.json()

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all court decisions with full text."""
        courts = self._get_courts()
        logger.info(f"Found {len(courts)} courts")

        total_yielded = 0
        for court in courts:
            code = court.get("code", "")
            name = court.get("name", "")
            if not code:
                continue

            # Get total count for this court
            try:
                result = self._search_decisions(code, start=0, rows=1)
            except Exception as e:
                logger.warning(f"Failed to query court {code}: {e}")
                continue

            num_found = result.get("numFound", 0)
            if num_found == 0:
                logger.info(f"  {code} ({name}): 0 decisions, skipping")
                continue

            logger.info(f"  {code} ({name}): {num_found} decisions")

            # Paginate through search results to collect dbids
            start = 0
            page_size = 1000
            while start < num_found:
                try:
                    result = self._search_decisions(code, start=start, rows=page_size)
                except Exception as e:
                    logger.warning(f"Search page failed for {code} at {start}: {e}")
                    break

                docs = result.get("docs", [])
                if not docs:
                    break

                for doc in docs:
                    dbid = doc.get("dbid")
                    if not dbid:
                        continue

                    # Fetch full decision
                    try:
                        decision = self._get_decision(dbid)
                    except Exception as e:
                        logger.warning(f"Failed to fetch decision {dbid}: {e}")
                        continue

                    # Extract text from HTML
                    html_text = decision.get("tekst", "")
                    clean_text = strip_html(html_text)

                    if not clean_text or len(clean_text) < 50:
                        continue

                    # Merge search metadata with full decision
                    decision["_search_meta"] = doc
                    decision["_court_code"] = code
                    decision["_court_name"] = name
                    decision["_clean_text"] = clean_text
                    yield decision
                    total_yielded += 1

                start += len(docs)

        logger.info(f"Finished: yielded {total_yielded} decisions")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch decisions since a given date."""
        since_str = since.strftime("%Y-%m-%d")
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        courts = self._get_courts()
        for court in courts:
            code = court.get("code", "")
            name = court.get("name", "")
            if not code:
                continue

            start = 0
            page_size = 1000
            while True:
                try:
                    payload = {
                        "courtCode": code,
                        "dateFrom": since_str,
                        "dateTo": today_str,
                        "start": start,
                        "rows": page_size,
                    }
                    self.rate_limiter.wait()
                    resp = self.client.post(
                        f"{API_URL}/search/decisions",
                        json_data=payload,
                        timeout=60,
                    )
                    resp.raise_for_status()
                    result = resp.json()
                except Exception:
                    break

                docs = result.get("docs", [])
                if not docs:
                    break

                for doc in docs:
                    dbid = doc.get("dbid")
                    if not dbid:
                        continue
                    try:
                        decision = self._get_decision(dbid)
                    except Exception:
                        continue
                    html_text = decision.get("tekst", "")
                    clean_text = strip_html(html_text)
                    if clean_text and len(clean_text) >= 50:
                        decision["_search_meta"] = doc
                        decision["_court_code"] = code
                        decision["_court_name"] = name
                        decision["_clean_text"] = clean_text
                        yield decision

                start += len(docs)
                if start >= result.get("numFound", 0):
                    break

    def normalize(self, raw: dict) -> dict:
        """Transform raw API response into standard schema."""
        meta = raw.get("_search_meta", {})
        court_code = raw.get("_court_code", "")
        court_name = raw.get("_court_name", "")

        dbid = meta.get("dbid") or raw.get("dbid", "")
        case_code = meta.get("sifraPredmeta", "") or raw.get("upisnik_sifra", "")
        case_num = meta.get("upisnikBroj", "") or raw.get("upisnik_broj", "")
        case_year = meta.get("upisnikGodina", "") or raw.get("upisnik_godina", "")
        case_number = f"{case_code} {case_num}/{case_year}".strip()

        # Decision date
        date_raw = meta.get("datumVijecanja", "") or raw.get("datum_vijecanja", "")
        date = None
        if date_raw:
            try:
                dt = datetime.fromisoformat(date_raw.replace("Z", "+00:00"))
                date = dt.strftime("%Y-%m-%d")
            except (ValueError, AttributeError):
                pass

        # Court name from detail or search
        court = raw.get("name", "") or meta.get("courtName", "") or court_name

        # Decision type
        decision_type = meta.get("vrstaOdluke", "") or raw.get("vrsta_odluke", "")

        # Department
        department = meta.get("odjeljenjeName", "") or raw.get("odjeljenje", "")

        # Case subject type
        case_type = meta.get("vrstaPredmeta", "")

        # Build title
        title = f"{court}: {case_number}"
        if decision_type:
            title += f" ({decision_type})"

        return {
            "_id": str(dbid),
            "_source": "ME/Courts",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("_clean_text", ""),
            "date": date,
            "url": f"{BASE_URL}/{court_code}/odluka/{dbid}",
            "language": "cnr",
            "court": court,
            "case_number": case_number,
            "decision_type": decision_type,
            "department": department,
            "case_type": case_type,
        }


def main():
    source_dir = Path(__file__).parent
    scraper = MontenegroCourtscraper(str(source_dir))

    if len(sys.argv) < 2:
        print("Usage:")
        print("  bootstrap.py bootstrap --sample   Fetch sample records")
        print("  bootstrap.py bootstrap             Full bootstrap")
        print("  bootstrap.py test-api              Test API connectivity")
        return

    cmd = sys.argv[1]

    if cmd == "test-api":
        logger.info("Testing API connectivity...")
        courts = scraper._get_courts()
        logger.info(f"Found {len(courts)} courts")
        for c in courts[:5]:
            logger.info(f"  {c.get('code')}: {c.get('name')}")

        # Test search
        result = scraper._search_decisions("vrhs", start=0, rows=2)
        logger.info(f"Supreme Court search: {result.get('numFound')} total")

        # Test full text fetch
        docs = result.get("docs", [])
        if docs:
            dbid = docs[0].get("dbid")
            decision = scraper._get_decision(dbid)
            html = decision.get("tekst", "")
            text = strip_html(html)
            logger.info(f"Decision {dbid}: {len(text)} chars of text")
            logger.info(f"Preview: {text[:200]}...")
        print("API test passed!")

    elif cmd == "bootstrap":
        is_sample = "--sample" in sys.argv
        if is_sample:
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        else:
            stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Bootstrap stats: {stats}")

    elif cmd == "update":
        since_str = sys.argv[2] if len(sys.argv) > 2 else "2024-01-01"
        since = datetime.strptime(since_str, "%Y-%m-%d")
        stats = scraper.update()
        logger.info(f"Update stats: {stats}")


if __name__ == "__main__":
    main()
