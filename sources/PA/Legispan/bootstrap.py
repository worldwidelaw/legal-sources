#!/usr/bin/env python3
"""
PA/Legispan -- Panama National Assembly Legislation

Fetches Panama legislation from the Legispan public REST API.

Strategy:
  - Use /api/search/norm endpoint (public, no auth)
  - Paginate with limit=100, sorted by publishedAt ascending
  - Full text available directly in JSON response (original.content)
  - 57,000+ norms covering laws, decrees, resolutions from 1903+

Source: https://legispan.asamblea.gob.pa (Panama National Assembly)
Rate limit: 1 req/2 sec (responses are slow, 2-5s each)

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urlencode

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PA.Legispan")

API_BASE = "https://legispan.asamblea.gob.pa/api"


class LegispanScraper(BaseScraper):
    """
    Scraper for PA/Legispan -- Panama National Assembly Legislation.
    Country: PA
    URL: https://legispan.asamblea.gob.pa

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json",
            },
            timeout=60,
        )

    def _clean_html(self, text: str) -> str:
        """Strip HTML tags and clean whitespace."""
        if not text:
            return ""
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&lt;', '<', text)
        text = re.sub(r'&gt;', '>', text)
        text = re.sub(r'&#\d+;', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def _search_norms(self, page: int = 1, limit: int = 100, order: str = "asc") -> Optional[dict]:
        """Query the /api/search/norm endpoint."""
        url = f"{API_BASE}/search/norm?page={page}&limit={limit}&sortKey=publishedAt&sortOrder={order}"
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url, timeout=60)
            if resp is None or resp.status_code != 200:
                logger.error(f"API error page {page}: {resp.status_code if resp else 'None'}")
                return None
            return resp.json()
        except Exception as e:
            logger.error(f"Request failed page {page}: {e}")
            return None

    def _extract_norm(self, hit: dict) -> Optional[dict]:
        """Extract norm data from a search result hit."""
        orig = hit.get("original", {})
        if not orig:
            return None

        content = orig.get("content", "")
        text = self._clean_html(content) if content else ""

        if not text or len(text) < 20:
            return None

        title = orig.get("title", "")
        norm_type_obj = orig.get("type", {})
        norm_type = norm_type_obj.get("type", "") if isinstance(norm_type_obj, dict) else str(norm_type_obj)

        authority_obj = orig.get("authority", {})
        authority = authority_obj.get("authority", "") if isinstance(authority_obj, dict) else str(authority_obj)

        norm_id = orig.get("id", "")
        number = orig.get("number", "")
        year = orig.get("year", "")

        published_at = orig.get("publishedAt", "")
        date_iso = None
        if published_at:
            try:
                dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                date_iso = dt.strftime("%Y-%m-%d")
            except Exception:
                date_iso = published_at[:10] if len(published_at) >= 10 else None

        norm_url = orig.get("normUrl", "")
        law_url = orig.get("lawUrl", "")

        keywords = []
        for kw in orig.get("keywords", []) or []:
            if isinstance(kw, dict):
                keywords.append(kw.get("keyword", ""))
            elif isinstance(kw, str):
                keywords.append(kw)

        return {
            "id": norm_id,
            "title": title,
            "text": text,
            "date": date_iso,
            "norm_type": norm_type,
            "norm_number": number,
            "year": year,
            "authority": authority,
            "keywords": [k for k in keywords if k],
            "norm_url": norm_url,
            "law_url": law_url,
        }

    # -- Core scraper methods ------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all norms from the Legispan API."""
        page = 1
        total_yielded = 0

        while True:
            data = self._search_norms(page=page, limit=100)
            if not data:
                break

            hits = data.get("data", [])
            if not hits:
                break

            total = data.get("count", 0)
            for hit in hits:
                norm = self._extract_norm(hit)
                if norm:
                    total_yielded += 1
                    yield norm

            logger.info(f"Page {page}: {len(hits)} hits, {total_yielded} yielded so far (total: {total})")

            if len(hits) < 100:
                break
            page += 1

        logger.info(f"Fetch complete: {total_yielded} norms")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch norms published since a given date."""
        since_str = since.strftime("%Y-%m-%d")
        page = 1

        while True:
            url = (f"{API_BASE}/search/norm?page={page}&limit=100"
                   f"&sortKey=publishedAt&sortOrder=desc"
                   f"&publishedAt_start={since_str}")
            self.rate_limiter.wait()
            try:
                resp = self.client.get(url, timeout=60)
                if not resp or resp.status_code != 200:
                    break
                data = resp.json()
            except Exception:
                break

            hits = data.get("data", [])
            if not hits:
                break

            for hit in hits:
                norm = self._extract_norm(hit)
                if norm:
                    yield norm

            if len(hits) < 100:
                break
            page += 1

    def fetch_sample(self, count: int = 15) -> Generator[dict, None, None]:
        """Fetch a sample of norms, spread across different types."""
        # Get recent norms (desc order) to ensure good content
        found = 0
        page = 1

        while found < count:
            data = self._search_norms(page=page, limit=100, order="desc")
            if not data:
                break

            hits = data.get("data", [])
            if not hits:
                break

            for hit in hits:
                if found >= count:
                    break
                norm = self._extract_norm(hit)
                if norm and len(norm.get("text", "")) > 100:
                    found += 1
                    title = norm.get("title", "N/A")[:60]
                    text_len = len(norm.get("text", ""))
                    logger.info(f"Sample {found}/{count}: [{norm.get('norm_type', '?')}] {title} ({text_len} chars)")
                    yield norm

            if found >= count or len(hits) < 100:
                break
            page += 1

        logger.info(f"Sample complete: {found} records")

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw norm record to standard schema."""
        norm_id = raw.get("id", "unknown")
        safe_id = str(norm_id)[:36]

        web_url = f"https://legispan.asamblea.gob.pa/tabloids"

        return {
            "_id": f"PA-Legispan-{safe_id}",
            "_source": "PA/Legispan",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": web_url,
            "norm_type": raw.get("norm_type", ""),
            "norm_number": raw.get("norm_number", ""),
            "year": raw.get("year", ""),
            "authority": raw.get("authority", ""),
            "keywords": raw.get("keywords", []),
            "pdf_url": raw.get("norm_url", ""),
            "language": "es",
        }

    def test_api(self) -> bool:
        """Test connectivity to the Legispan API."""
        logger.info("Testing Legispan API access...")

        data = self._search_norms(page=1, limit=5, order="desc")
        if not data:
            logger.error("API returned no data")
            return False

        total = data.get("count", 0)
        hits = data.get("data", [])
        logger.info(f"API total norms: {total}, returned {len(hits)} hits")

        if not hits:
            logger.error("No hits returned")
            return False

        norm = self._extract_norm(hits[0])
        if not norm:
            logger.error("Could not extract norm from first hit")
            return False

        text_len = len(norm.get("text", ""))
        logger.info(f"First norm: {norm.get('title', 'N/A')[:60]}")
        logger.info(f"Text length: {text_len} chars")

        if text_len < 50:
            logger.error("Text too short — may not contain full text")
            return False

        logger.info("All tests passed!")
        return True


# ── CLI ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    scraper = LegispanScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample] [--count N]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        count = 15
        for i, arg in enumerate(sys.argv):
            if arg == "--count" and i + 1 < len(sys.argv):
                count = int(sys.argv[i + 1])

        if sample_mode:
            gen = scraper.fetch_sample(count=count)
        else:
            gen = scraper.fetch_all()

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in gen:
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1
            logger.info(f"Saved: {out_path.name}")

        logger.info(f"Bootstrap complete: {saved} records saved to {sample_dir}")

    elif command == "update":
        logger.info("Running full fetch")
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in scraper.fetch_all():
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1

        logger.info(f"Update complete: {saved} records saved")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
