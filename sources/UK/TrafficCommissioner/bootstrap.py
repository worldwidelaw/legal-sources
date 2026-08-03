#!/usr/bin/env python3
"""
Legal Data Hunter - UK Traffic Commissioners Regulatory Decisions Scraper

Fetches regulatory decisions of the Traffic Commissioners for Great Britain
from GOV.UK using:
  - GET /api/search.json?filter_format=traffic_commissioner_regulatory_decision (discovery)
  - GET /api/content/{path} (full text via Content Store API)

The Traffic Commissioners are the independent regulators of the heavy goods
vehicle (HGV) and public service vehicle (PSV/bus & coach) industries and of
local bus registration in Great Britain. Their regulatory decisions (operator
licence revocations, curtailments, suspensions, formal warnings, transport
manager repute findings, disqualifications) are quasi-judicial and binding.

Coverage: ~185 decisions. Full text is served in the Content Store `body`
field (born-digital HTML → stripped to plain text). Unlike the tribunal
finders, hidden_indexable_content is empty for this format.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
  python bootstrap.py bootstrap-fast     # Alias for full pull (fleet runner)
"""

import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("UK/TrafficCommissioner")


class UKTrafficCommissionerScraper(BaseScraper):
    """
    Scraper for the Traffic Commissioners for Great Britain regulatory
    decisions on GOV.UK.
    """

    BASE_URL = "https://www.gov.uk"
    SEARCH_URL = "/api/search.json"
    CONTENT_URL = "/api/content"
    FILTER_FORMAT = "traffic_commissioner_regulatory_decision"
    PAGE_SIZE = 500

    SEARCH_FIELDS = [
        "title", "link", "public_timestamp",
    ]

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=self.BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (legal research project)",
                "Accept": "application/json",
            },
            timeout=30,
        )

    def _search(self, start: int = 0) -> dict:
        params = {
            "filter_format": self.FILTER_FORMAT,
            "count": self.PAGE_SIZE,
            "start": start,
            "order": "-public_timestamp",
            "fields": ",".join(self.SEARCH_FIELDS),
        }
        self.rate_limiter.wait()
        try:
            resp = self.client.get(self.SEARCH_URL, params=params)
            if resp.status_code == 200:
                return resp.json()
            logger.warning(f"Search API returned {resp.status_code} for start={start}")
            return {}
        except Exception as e:
            logger.error(f"Search API failed: {e}")
            return {}

    def _fetch_content(self, path: str) -> Optional[dict]:
        if not path.startswith("/"):
            path = "/" + path
        url = f"{self.CONTENT_URL}{path}"
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 404:
                logger.debug(f"Content not found: {path}")
            else:
                logger.warning(f"Content API returned {resp.status_code} for {path}")
            return None
        except Exception as e:
            logger.warning(f"Content API failed for {path}: {e}")
            return None

    def _extract_text(self, content: dict) -> str:
        """Full text is in details.body (HTML). Strip to clean plain text."""
        details = content.get("details", {})
        body = details.get("body", "")
        if body:
            soup = BeautifulSoup(body, "html.parser")
            for tag in soup(["script", "style"]):
                tag.decompose()
            text = soup.get_text(separator="\n")
            # collapse excess blank lines
            lines = [ln.strip() for ln in text.splitlines()]
            text = "\n".join(ln for ln in lines if ln)
            return text.strip()

        # Fallback: hidden_indexable_content (empty for this format, kept for safety)
        meta = details.get("metadata", {})
        return (meta.get("hidden_indexable_content", "") or "").strip()

    def _build_raw(self, item: dict, content: dict, text: str) -> dict:
        meta = content.get("details", {}).get("metadata", {})
        return {
            "content_id": content.get("content_id", ""),
            "title": content.get("title", ""),
            "text": text,
            "description": content.get("description", ""),
            "case_type": meta.get("case_type", []),
            "decision_subject": meta.get("decision_subject", []),
            "outcome_type": meta.get("outcome_type", []),
            "regions": meta.get("regions", []),
            "meta_date": meta.get("first_published_at", ""),
            "first_published_at": content.get("first_published_at", ""),
            "public_updated_at": content.get("public_updated_at", ""),
            "link": item.get("link", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        start = 0
        first = self._search(start)
        total = first.get("total", 0)
        logger.info(f"Total Traffic Commissioner decisions: {total}")

        result = first
        count = 0
        skipped = 0
        while True:
            results = result.get("results", [])
            if not results:
                break
            for item in results:
                link = item.get("link", "")
                if not link:
                    continue
                content = self._fetch_content(link)
                if not content:
                    skipped += 1
                    continue
                text = self._extract_text(content)
                if not text or len(text) < 50:
                    skipped += 1
                    continue
                count += 1
                yield self._build_raw(item, content, text)
                if count % 50 == 0:
                    logger.info(f"  {count} decisions fetched ({skipped} skipped)")
            start += len(results)
            if start >= total:
                break
            result = self._search(start)
        logger.info(f"Total: {count} decisions with text ({skipped} skipped)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        since_str = since.strftime("%Y-%m-%d")
        params = {
            "filter_format": self.FILTER_FORMAT,
            "filter_public_timestamp": f"from:{since_str}",
            "count": self.PAGE_SIZE,
            "start": 0,
            "order": "-public_timestamp",
            "fields": ",".join(self.SEARCH_FIELDS),
        }
        self.rate_limiter.wait()
        try:
            resp = self.client.get(self.SEARCH_URL, params=params)
            if resp.status_code != 200:
                logger.warning(f"Updates search returned {resp.status_code}")
                return
            result = resp.json()
        except Exception as e:
            logger.error(f"Updates search failed: {e}")
            return

        total = result.get("total", 0)
        logger.info(f"Updates since {since_str}: {total} decisions")
        start = 0
        while True:
            results = result.get("results", [])
            if not results:
                break
            for item in results:
                link = item.get("link", "")
                if not link:
                    continue
                content = self._fetch_content(link)
                if not content:
                    continue
                text = self._extract_text(content)
                if not text or len(text) < 50:
                    continue
                yield self._build_raw(item, content, text)
            start += len(results)
            if start >= total:
                break
            params["start"] = start
            self.rate_limiter.wait()
            try:
                resp = self.client.get(self.SEARCH_URL, params=params)
                if resp.status_code == 200:
                    result = resp.json()
                else:
                    break
            except Exception:
                break

    def normalize(self, raw: dict) -> dict:
        text = raw.get("text", "").strip()
        if not text:
            return None

        meta_date = raw.get("meta_date", "")
        if meta_date:
            date_iso = meta_date[:10]
        else:
            fp = raw.get("first_published_at", "") or raw.get("public_updated_at", "")
            date_iso = fp[:10] if fp else None

        def _titlecase(lst):
            return [s.replace("-", " ").strip().title() for s in (lst or []) if s]

        return {
            "_id": f"UK/TrafficCommissioner/{raw.get('content_id', '')}",
            "_source": "UK/TrafficCommissioner",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "content_id": raw.get("content_id", ""),
            "title": raw.get("title", ""),
            "text": text,
            "description": raw.get("description", ""),
            "date": date_iso,
            "case_type": _titlecase(raw.get("case_type")),
            "decision_subject": _titlecase(raw.get("decision_subject")),
            "outcome_type": _titlecase(raw.get("outcome_type")),
            "regions": _titlecase(raw.get("regions")),
            "url": f"{self.BASE_URL}{raw.get('link', '')}",
            "updated_at": raw.get("public_updated_at", ""),
        }


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKTrafficCommissionerScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py bootstrap [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd in ("bootstrap", "bootstrap-fast"):
        sample = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample, sample_size=12)
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
