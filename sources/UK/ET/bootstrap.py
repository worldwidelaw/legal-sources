#!/usr/bin/env python3
"""
Legal Data Hunter - UK Employment Tribunals Scraper

Fetches employment tribunal decisions from GOV.UK using:
  - GET /api/search.json?filter_format=employment_tribunal_decision (discovery)
  - GET /api/content/{path} (full text via Content Store API)

Coverage: ~130,000 decisions. Full text from hidden_indexable_content field.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
"""

import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("UK/ET")


class UKETScraper(BaseScraper):
    """
    Scraper for UK Employment Tribunal decisions on GOV.UK.

    Strategy:
    - Search API to discover all ET decisions (paginated, 500 per page)
    - Content Store API for each decision to get full text from
      details.metadata.hidden_indexable_content
    """

    BASE_URL = "https://www.gov.uk"
    SEARCH_URL = "/api/search.json"
    CONTENT_URL = "/api/content"
    PAGE_SIZE = 500

    SEARCH_FIELDS = [
        "title", "link", "public_timestamp",
        "tribunal_decision_decision_date",
        "tribunal_decision_categories",
        "tribunal_decision_country",
    ]

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        # Checkpoint/resume: the full crawl is ~132K decisions × 1 content
        # fetch each ≈ 18h at 2 rps. A fleet slot that times out (exit-124)
        # relaunches fetch_all from the top; without a checkpoint it re-crawls
        # start=0 every restart and re-appends the same first N records to the
        # JSONL, so the pipeline dedups a 132K JSONL down to a few thousand
        # unique _ids (issue #1089). We persist the last completed search
        # offset NEXT TO THE MODULE (survives the fleet's temp CWD) and resume
        # from it. Combined with ascending public_timestamp order (new
        # decisions append at the tail, past the checkpoint) the resume offset
        # stays valid across corpus drift.
        self._checkpoint_path = source_dir / "data" / "et_checkpoint.json"

        self.client = HttpClient(
            base_url=self.BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (legal research project)",
                "Accept": "application/json",
            },
            timeout=30,
        )

    def _load_checkpoint(self) -> int:
        """Return the saved start offset to resume from (0 if none)."""
        try:
            with open(self._checkpoint_path) as f:
                start = int(json.load(f).get("start", 0))
            if start > 0:
                logger.info(f"Resuming from checkpoint: start={start}")
            return start
        except Exception:
            return 0

    def _save_checkpoint(self, start: int) -> None:
        """Atomically persist the last completed start offset."""
        try:
            self._checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._checkpoint_path.with_suffix(".json.tmp")
            with open(tmp, "w") as f:
                json.dump({"start": start}, f)
            tmp.replace(self._checkpoint_path)
        except Exception as e:
            logger.warning(f"Could not write checkpoint: {e}")

    def _search_decisions(self, start: int = 0,
                          order: str = "public_timestamp") -> Optional[dict]:
        """Search for ET decisions with pagination.

        Returns the parsed response on success (possibly with an empty
        ``results`` list = genuine end of listing), or None after exhausting
        retries on a transient failure — so callers can distinguish a real
        end-of-data from a transient outage and avoid stopping early
        (under-write class of issue #1089 / cf. IE/WRC #1196).
        """
        params = {
            "filter_format": "employment_tribunal_decision",
            "count": self.PAGE_SIZE,
            "start": start,
            "order": order,
            "fields": ",".join(self.SEARCH_FIELDS),
        }
        for attempt in range(4):
            self.rate_limiter.wait()
            try:
                resp = self.client.get(self.SEARCH_URL, params=params)
                if resp.status_code == 200:
                    return resp.json()
                logger.warning(
                    f"Search API returned {resp.status_code} for start={start} "
                    f"(attempt {attempt + 1}/4)"
                )
            except Exception as e:
                logger.warning(f"Search API failed for start={start}: {e} "
                               f"(attempt {attempt + 1}/4)")
            time.sleep(2 * (attempt + 1))
        logger.error(f"Search API gave up for start={start} after 4 attempts")
        return None

    def _fetch_content(self, path: str) -> Optional[dict]:
        """Fetch full decision content from Content Store API."""
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
        """Extract full text from Content Store response.

        Primary source: details.metadata.hidden_indexable_content
        Fallback: details.body (HTML stripped)
        """
        details = content.get("details", {})
        metadata = details.get("metadata", {})

        # Primary: hidden_indexable_content has the full plain text
        text = metadata.get("hidden_indexable_content", "")
        if text and len(text.strip()) > 100:
            return text.strip()

        # Fallback: body field (usually just a short HTML snippet with PDF links)
        body = details.get("body", "")
        if body:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(body, "html.parser")
            for tag in soup(["script", "style"]):
                tag.decompose()
            return soup.get_text(separator="\n").strip()

        return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all ET decisions with full text.

        Paginates in ascending public_timestamp order (oldest first) so newly
        published decisions append at the tail and the resume offset stays
        valid across restarts. Resumes from the persisted checkpoint offset.
        """
        start = self._load_checkpoint()

        result = self._search_decisions(start)
        if result is None:
            logger.error("Initial search failed after retries; aborting (checkpoint preserved)")
            return
        total = result.get("total", 0)
        logger.info(f"Total ET decisions: {total} (starting at offset {start})")

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
                yield {
                    "content_id": content.get("content_id", ""),
                    "title": content.get("title", ""),
                    "text": text,
                    "description": content.get("description", ""),
                    "decision_date": item.get("tribunal_decision_decision_date", ""),
                    "categories": item.get("tribunal_decision_categories", []),
                    "country": item.get("tribunal_decision_country", ""),
                    "first_published_at": content.get("first_published_at", ""),
                    "public_updated_at": content.get("public_updated_at", ""),
                    "link": link,
                }

                if count % 100 == 0:
                    logger.info(f"  {count} decisions fetched ({skipped} skipped)")

            start += len(results)
            # Persist progress AFTER the page is fully yielded so a restart
            # resumes at the next unprocessed page instead of re-crawling.
            self._save_checkpoint(start)
            if start >= total:
                break
            result = self._search_decisions(start)
            if result is None:
                # Transient outage after retries — stop cleanly; the checkpoint
                # lets the next run resume from here rather than from the top.
                logger.warning(f"Stopping at offset {start} after search failure; "
                               f"checkpoint saved for resume")
                break

        logger.info(f"Total: {count} decisions with text ({skipped} skipped)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions published since a given date."""
        since_str = since.strftime("%Y-%m-%d")

        params = {
            "filter_format": "employment_tribunal_decision",
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
                yield {
                    "content_id": content.get("content_id", ""),
                    "title": content.get("title", ""),
                    "text": text,
                    "description": content.get("description", ""),
                    "decision_date": item.get("tribunal_decision_decision_date", ""),
                    "categories": item.get("tribunal_decision_categories", []),
                    "country": item.get("tribunal_decision_country", ""),
                    "first_published_at": content.get("first_published_at", ""),
                    "public_updated_at": content.get("public_updated_at", ""),
                    "link": link,
                }

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
        """Transform raw decision data into standard schema."""
        text = raw.get("text", "").strip()
        if not text:
            return None

        decision_date = raw.get("decision_date", "")
        if decision_date:
            date_iso = decision_date[:10]
        else:
            date_str = raw.get("first_published_at", "") or raw.get("public_updated_at", "")
            date_iso = date_str[:10] if date_str else None

        categories = raw.get("categories", [])
        if isinstance(categories, list):
            categories = [c.replace("-", " ").title() for c in categories]

        return {
            "_id": f"UK/ET/{raw.get('content_id', '')}",
            "_source": "UK/ET",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "content_id": raw.get("content_id", ""),
            "title": raw.get("title", ""),
            "text": text,
            "description": raw.get("description", ""),
            "date": date_iso,
            "decision_date": raw.get("decision_date", ""),
            "categories": categories,
            "country": raw.get("country", ""),
            "url": f"{self.BASE_URL}{raw.get('link', '')}",
            "updated_at": raw.get("public_updated_at", ""),
        }


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = UKETScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py bootstrap [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "bootstrap":
        sample = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample, sample_size=12)
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
