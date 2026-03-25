#!/usr/bin/env python3
"""
INTL/InforMEA -- InforMEA Environmental Treaties Portal

Fetches COP/MOP decisions from Multilateral Environmental Agreements via
the OData API at odata.informea.org.

Strategy:
  - OData API at odata.informea.org/informea.svc/Decisions
  - $expand=content,title,summary,keywords for full data
  - Paginate with $top/$skip (500 per page)
  - ~12,400 decisions across 68 treaties

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import html
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.InforMEA")

API_BASE = "https://odata.informea.org/informea.svc"
DECISIONS_URL = f"{API_BASE}/Decisions"
PAGE_SIZE = 500


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    if not text:
        return ""
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</(?:p|div|h[1-6]|li|tr|blockquote)>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def parse_odata_date(date_str: str) -> str:
    """Parse OData date format /Date(epoch)/ to ISO 8601."""
    if not date_str:
        return None
    m = re.search(r'/Date\((-?\d+)\)/', str(date_str))
    if m:
        epoch_ms = int(m.group(1))
        dt = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=epoch_ms)
        return dt.strftime("%Y-%m-%d")
    # Try ISO format
    try:
        return datetime.fromisoformat(str(date_str).replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return str(date_str)


class InforMEAScraper(BaseScraper):
    """
    Scraper for INTL/InforMEA -- InforMEA Environmental Treaties Portal.
    Country: INTL
    URL: https://www.informea.org

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
            "Accept": "application/json",
        })

    def _fetch_page(self, skip: int = 0, top: int = PAGE_SIZE,
                    extra_filter: str = None) -> list:
        """Fetch a page of decisions from the OData API."""
        params = {
            "$top": top,
            "$skip": skip,
            "$expand": "content,title,summary,keywords",
            "$format": "json",
            "$orderby": "updated desc",
        }
        if extra_filter:
            params["$filter"] = extra_filter

        self.rate_limiter.wait()
        resp = self.session.get(DECISIONS_URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("d", {}).get("results", [])
        return results

    def _get_text_value(self, nav_prop: dict, language: str = "en") -> str:
        """Extract text value from a navigation property (content/title/summary)."""
        if not nav_prop:
            return ""
        results = nav_prop.get("results", [])
        if not results:
            return ""
        # Prefer English
        for item in results:
            if item.get("language", "").lower().startswith(language):
                return item.get("value", "")
        # Fall back to first available
        if results:
            return results[0].get("value", "")
        return ""

    def _get_keywords(self, keywords_prop: dict) -> list:
        """Extract keywords from navigation property."""
        if not keywords_prop:
            return []
        results = keywords_prop.get("results", [])
        return [k.get("term", "") for k in results if k.get("term")]

    def _decision_to_raw(self, decision: dict) -> dict:
        """Convert an OData decision to a raw document dict."""
        content_html = self._get_text_value(decision.get("content"), "en")
        title = self._get_text_value(decision.get("title"), "en")
        summary_html = self._get_text_value(decision.get("summary"), "en")

        # Use content as primary text, fall back to summary
        text = strip_html(content_html) if content_html else strip_html(summary_html)

        return {
            "id": decision.get("id", ""),
            "number": decision.get("number", ""),
            "treaty": decision.get("treaty", ""),
            "type": decision.get("type", ""),
            "status": decision.get("status", ""),
            "link": decision.get("link", ""),
            "published": decision.get("published", ""),
            "updated": decision.get("updated", ""),
            "title": title,
            "text": text,
            "summary": strip_html(summary_html) if summary_html else "",
            "keywords": self._get_keywords(decision.get("keywords")),
            "meeting_title": decision.get("meetingTitle", ""),
            "meeting_url": decision.get("meetingUrl", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions."""
        skip = 0
        total_fetched = 0
        while True:
            results = self._fetch_page(skip=skip)
            if not results:
                break
            for decision in results:
                raw = self._decision_to_raw(decision)
                # Only yield if we have text content
                if raw["text"]:
                    yield raw
                    total_fetched += 1
            logger.info("Fetched %d decisions so far (skip=%d, page=%d)",
                       total_fetched, skip, len(results))
            if len(results) < PAGE_SIZE:
                break
            skip += PAGE_SIZE

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch decisions updated since a given date."""
        since_str = since.strftime("%Y-%m-%dT%H:%M:%S")
        odata_filter = f"updated gt datetime'{since_str}'"
        skip = 0
        while True:
            results = self._fetch_page(skip=skip, extra_filter=odata_filter)
            if not results:
                break
            for decision in results:
                raw = self._decision_to_raw(decision)
                if raw["text"]:
                    yield raw
            if len(results) < PAGE_SIZE:
                break
            skip += PAGE_SIZE

    def normalize(self, raw: dict) -> dict:
        """Transform raw decision into standard schema."""
        date = parse_odata_date(raw.get("published"))

        return {
            "_id": f"InforMEA-{raw['id']}",
            "_source": "INTL/InforMEA",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", raw.get("number", "")),
            "text": raw.get("text", ""),
            "date": date,
            "url": raw.get("link", f"https://www.informea.org/en/decision/{raw['id']}"),
            "decision_number": raw.get("number", ""),
            "treaty": raw.get("treaty", ""),
            "decision_type": raw.get("type", ""),
            "status": raw.get("status", ""),
            "summary": raw.get("summary", ""),
            "keywords": raw.get("keywords", []),
            "meeting_title": raw.get("meeting_title", ""),
        }


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = InforMEAScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        print("Testing InforMEA OData API connectivity...")
        resp = requests.get(f"{DECISIONS_URL}/$count", timeout=30)
        print(f"  Total decisions: {resp.text.strip()}")
        results = scraper._fetch_page(skip=0, top=3)
        print(f"  Sample page: {len(results)} results")
        if results:
            raw = scraper._decision_to_raw(results[0])
            print(f"  First: treaty={raw['treaty']}, number={raw['number']}")
            print(f"  Text length: {len(raw['text'])} chars")
            if raw['text']:
                print(f"  Preview: {raw['text'][:200]}...")
        print("OK")

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(json.dumps(result, indent=2, default=str))

    elif command == "update":
        since = datetime.now(timezone.utc) - timedelta(days=90)
        result = scraper.update(since=since)
        print(json.dumps(result, indent=2, default=str))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
