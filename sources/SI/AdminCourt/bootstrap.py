#!/usr/bin/env python3
"""
SI/AdminCourt -- Slovenian Administrative Court (Upravno sodišče) Fetcher

Fetches UPRS decisions from sodnapraksa.si (Slovenia's case law database).

Strategy:
  sodnapraksa.si was rebuilt as a Vue SPA (the legacy /search.php now 302s to
  /iskanje/{base64-search-state}).  The SPA is backed by an unauthenticated
  JSON search API which returns the FULL TEXT of every hit inline, so a single
  paged request yields 100 complete decisions -- no per-document fetch needed.

  POST https://sodnapraksa.si/backend/api/search/documents
    {"simpleSearch": false,
     "query": {"q": "*", "f": [{"n": "docType", "v": ["uprs"], "and": false}]},
     "page": N, "pageSize": 100,
     "sortField": "date", "sortDirection": "ASC"}

  Response docs carry: ecli, ordinalNumber, sessionDate, court, department,
  courtPanel, areas, keywords, legislation, plus the three text sections
  coreText (jedro), ruling (izrek) and motivation (obrazložitev).

  Ordering is by decision date ASCENDING so newly published decisions always
  land at the tail -- earlier pages never shift, which makes the page-number
  checkpoint in data/uprs_checkpoint.json safe to resume from.

Data:
  - ~36,050 UPRS (Administrative Court) decisions, 2000-present
  - Language: Slovenian (SL)
  - ECLI identifiers: ECLI:SI:UPRS:YYYY:*

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py bootstrap-fast     # Alias for the full pull (fleet runner)
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

BASE_URL = "https://sodnapraksa.si"
SEARCH_API = f"{BASE_URL}/backend/api/search/documents"
DOC_TYPE = "uprs"
PAGE_SIZE = 100          # 200 is rejected by the backend
RATE_LIMIT_SECONDS = 1.5
MAX_ATTEMPTS = 6


def _strip_html(fragment: str) -> str:
    """Turn an HTML fragment from the API into clean plain text."""
    if not fragment:
        return ""
    # Preserve paragraph/line breaks before dropping tags
    text = re.sub(r"(?i)</p\s*>|<br\s*/?>", "\n", fragment)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_module.unescape(text)
    text = re.sub(r"[ \t ]+", " ", text)
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
    return text.strip()


class SlovenianAdminCourtScraper(BaseScraper):
    """
    Scraper for SI/AdminCourt -- Slovenian Administrative Court.
    Country: SI
    URL: https://sodnapraksa.si

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
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Accept-Language": "sl,en",
            },
            timeout=90,
        )
        self.checkpoint_path = source_dir / "data" / "uprs_checkpoint.json"

    # ── checkpoint ────────────────────────────────────────────────────

    def _load_checkpoint(self) -> int:
        """Return the first page that still needs fetching."""
        try:
            with open(self.checkpoint_path, encoding="utf-8") as f:
                return int(json.load(f).get("next_page", 0))
        except Exception:
            return 0

    def _save_checkpoint(self, next_page: int) -> None:
        try:
            self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.checkpoint_path, "w", encoding="utf-8") as f:
                json.dump({"next_page": next_page}, f)
        except Exception as e:
            logger.warning(f"Could not write checkpoint: {e}")

    # ── API ───────────────────────────────────────────────────────────

    def _search(self, page: int, page_size: int = PAGE_SIZE,
                direction: str = "ASC") -> Dict[str, Any]:
        """POST one page of the UPRS result set. Raises on repeated failure."""
        payload = {
            "simpleSearch": False,
            "query": {
                "q": "*",
                "f": [{"n": "docType", "v": [DOC_TYPE], "and": False}],
            },
            "page": page,
            "pageSize": page_size,
            "sortField": "date",
            "sortDirection": direction,
        }

        last_error = None
        for attempt in range(MAX_ATTEMPTS):
            time.sleep(RATE_LIMIT_SECONDS)
            try:
                resp = self.client.session.post(SEARCH_API, json=payload, timeout=90)
                if resp.status_code in (429, 500, 502, 503, 504):
                    retry_after = resp.headers.get("Retry-After")
                    delay = int(retry_after) if (retry_after or "").isdigit() else min(120, 5 * 2 ** attempt)
                    logger.warning(f"HTTP {resp.status_code} on page {page}, retrying in {delay}s")
                    time.sleep(delay)
                    last_error = f"HTTP {resp.status_code}"
                    continue
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                last_error = e
                delay = min(120, 5 * 2 ** attempt)
                logger.warning(f"Search page {page} failed ({e}); retrying in {delay}s")
                time.sleep(delay)

        raise RuntimeError(
            f"sodnapraksa.si search API unreachable for page {page} after "
            f"{MAX_ATTEMPTS} attempts (last error: {last_error})"
        )

    # ── scraper interface ─────────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all UPRS documents (raw API dicts) oldest-first, resumable."""
        first = self._search(page=0)
        total = int(first.get("hits") or 0)
        if total == 0:
            raise RuntimeError(
                "sodnapraksa.si search API returned 0 UPRS hits — refusing to "
                "report success on an empty corpus"
            )
        total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
        logger.info(f"Total UPRS documents: {total:,} across {total_pages:,} pages")

        start_page = self._load_checkpoint()
        if start_page:
            logger.info(f"Resuming from checkpoint at page {start_page}")

        fetched = 0
        for page in range(start_page, total_pages):
            data = first if page == 0 else self._search(page=page)
            docs = data.get("docs") or []
            if not docs:
                logger.info(f"No results on page {page}, stopping")
                break

            for doc in docs:
                yield doc
                fetched += 1

            self._save_checkpoint(page + 1)
            if page % 20 == 0:
                logger.info(f"Page {page}/{total_pages} — {fetched:,} documents fetched")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents decided since the given date (newest-first walk)."""
        since_str = since.strftime("%Y-%m-%d")
        page = 0
        while True:
            data = self._search(page=page, direction="DESC")
            docs = data.get("docs") or []
            if not docs:
                return
            for doc in docs:
                if (doc.get("sessionDate") or "") < since_str:
                    return  # sorted newest-first, everything after is older
                yield doc
            page += 1

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a raw API document into the standard schema."""
        jedro = _strip_html(raw.get("coreText") or "")
        izrek = _strip_html(raw.get("ruling") or "")
        obrazlozitev = _strip_html(raw.get("motivation") or "")

        text_parts = []
        if jedro:
            text_parts.append(f"JEDRO (Summary):\n{jedro}")
        if izrek:
            text_parts.append(f"IZREK (Ruling):\n{izrek}")
        if obrazlozitev:
            text_parts.append(f"OBRAZLOŽITEV (Reasoning):\n{obrazlozitev}")

        full_text = "\n\n".join(text_parts)
        if len(full_text) < 50:
            return None

        ecli = raw.get("ecli") or ""
        doc_id = raw.get("id")
        key = ecli or f"id{doc_id}"

        date = raw.get("sessionDate") or ""
        if date and not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
            date = date[:10] if re.match(r"^\d{4}-\d{2}-\d{2}", date) else ""

        legislation = raw.get("legislation") or []
        references = "; ".join(
            f"{item.get('krap') or item.get('name', '')} {item.get('value', '')}".strip()
            for item in legislation
            if isinstance(item, dict)
        )

        return {
            "_id": f"SI_AdminCourt_{key}",
            "_source": "SI/AdminCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("ordinalNumber") or ecli,
            "text": full_text,
            "date": date or None,
            "url": f"{BASE_URL}/dokument/{raw.get('documentType') or DOC_TYPE}/{doc_id}",
            "ecli": ecli,
            "decision_number": raw.get("ordinalNumber") or "",
            "evidence_number": raw.get("registryNumber") or "",
            "court": raw.get("court") or "",
            "department": raw.get("department") or "",
            "court_panel": raw.get("courtPanel") or "",
            "legal_area": "; ".join(raw.get("areas") or []),
            "keywords": "; ".join(raw.get("keywords") or []),
            "references": references,
            "published_at": raw.get("publishedAt") or "",
            "language": "sl",
        }

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            data = self._search(page=0, page_size=5)
            total = int(data.get("hits") or 0)
            docs = data.get("docs") or []
            logger.info(f"Connectivity OK: {total:,} UPRS documents, {len(docs)} on first page")
            if total == 0 or not docs:
                logger.error("Search API returned no UPRS hits")
                return False
            record = self.normalize(docs[0])
            if not record:
                logger.error("First document produced no text")
                return False
            logger.info(
                f"Document extraction OK: {len(record['text']):,} chars from "
                f"{record['ecli'] or record['_id']}"
            )
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
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (10 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SlovenianAdminCourtScraper()

    if args.command == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)
    elif args.command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(sample_mode=args.sample)
        print(json.dumps(stats, indent=2))
    elif args.command == "update":
        stats = scraper.update()
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
