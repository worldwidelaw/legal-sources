#!/usr/bin/env python3
"""
SI/SupremeCourt -- Slovenian Supreme Court case law (sodnapraksa.si)

Fetches the full-text decisions of the Vrhovno sodišče Republike Slovenije
(Supreme Court of Slovenia) from the official public case-law database
sodnapraksa.si.

Strategy (2026-07 rewrite — the site migrated from a server-rendered ASP
pager to a Vue SPA backed by a JSON search API):

  - The old ``?q=*&database[SOVS]=SOVS`` HTML pager now 302-redirects to a Vue
    SPA (``/iskanje/{base64-json}``) whose shell carries no data, so the legacy
    ``span#num-hits`` / ``table#results-table`` scraper found 0 documents
    (issue #1212). The new SPA talks to:

        POST https://sodnapraksa.si/backend/api/search/documents
        body: {"simpleSearch": true, "similarityCutoff": 0.75,
               "similarityTopK": 100, "topN": 10,
               "query": {"q": "*"},
               "filterQueries": ["docType:vsrs"],
               "page": N, "pageSize": 50,
               "sortField": "date", "sortDirection": "ASC"|"DESC"}

  - ``docType:vsrs`` restricts to the Supreme Court (66,710 decisions as of
    2026-07). The search response embeds the FULL text of every hit inline
    under ``coreText`` (Jedro / core summary), ``ruling`` (Izrek / disposition)
    and ``motivation`` (Obrazložitev / reasoning) — no separate detail fetch is
    needed (the ``search/documents/{type}/{id}`` detail endpoint is Keycloak
    auth-gated and returns 401/400). Text is HTML fragments; tags are stripped.

  - Full crawl paginates date-ASC (oldest first) so newly published decisions
    append at the tail — a page checkpoint makes fleet reruns resume-safe.
    Incremental update paginates date-DESC and stops once past the cutoff.

Usage:
  python bootstrap.py bootstrap            # Full initial pull (streams to data/records.jsonl)
  python bootstrap.py bootstrap-fast       # Alias for full bootstrap (fleet runner)
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Incremental update (recent decisions)
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from bs4 import BeautifulSoup
from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SI.SupremeCourt")

API_URL = "https://sodnapraksa.si/backend/api/search/documents"
DOC_URL = "https://sodnapraksa.si/dokument/{type}/{id}"
DOC_TYPE = "vsrs"  # Vrhovno sodišče RS (Supreme Court)
PAGE_SIZE = 50
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


class SISupremeCourtScraper(BaseScraper):
    """
    Scraper for SI/SupremeCourt -- Slovenian Supreme Court case law.
    Country: SI
    URL: https://sodnapraksa.si/
    Data types: case_law
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.checkpoint_file = source_dir / "checkpoint.json"

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Accept-Language": "sl,en;q=0.9",
        })

    # ── search API ───────────────────────────────────────────────────
    def _search(self, page: int, direction: str = "ASC", timeout: int = 60) -> Dict[str, Any]:
        """One page of the Supreme Court search API. Returns the parsed JSON."""
        body = {
            "simpleSearch": True,
            "similarityCutoff": 0.75,
            "similarityTopK": 100,
            "topN": 10,
            "query": {"q": "*"},
            "filterQueries": [f"docType:{DOC_TYPE}"],
            "page": page,
            "pageSize": PAGE_SIZE,
            "sortField": "date",
            "sortDirection": direction,
        }
        self.rate_limiter.wait()
        resp = self.session.post(API_URL, json=body, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    # ── checkpoint (page-level, resume-safe with date-ASC crawl) ──────
    def _load_checkpoint(self) -> int:
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file) as f:
                    cp = json.load(f)
                page = int(cp.get("next_page", 0))
                if page > 0:
                    logger.info(f"Resuming from page {page}")
                return page
            except Exception:
                pass
        return 0

    def _save_checkpoint(self, next_page: int, fetched: int):
        try:
            with open(self.checkpoint_file, "w") as f:
                json.dump({
                    "next_page": next_page,
                    "fetched": fetched,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }, f)
        except Exception as e:
            logger.warning(f"Could not write checkpoint: {e}")

    def _clear_checkpoint(self):
        if self.checkpoint_file.exists():
            try:
                self.checkpoint_file.unlink()
            except Exception:
                pass

    # ── framework hooks ──────────────────────────────────────────────
    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield every Supreme Court decision (date-ASC, oldest first).

        Page-checkpointed: on restart we skip already-completed pages with no
        network calls; the loader dedups on _id, so re-fetching an in-progress
        page is harmless. New decisions append at the tail so the checkpoint
        stays valid across fleet reruns.
        """
        start_page = self._load_checkpoint()

        first = self._search(start_page)
        total = first.get("hits", 0)
        total_pages = first.get("totalPages", 0)
        logger.info(f"Total Supreme Court decisions: {total:,} across {total_pages} pages")

        fetched = 0
        page = start_page
        data = first
        while True:
            docs = data.get("docs", []) or []
            if not docs:
                break
            for doc in docs:
                fetched += 1
                yield doc
            # Page fully yielded → advance checkpoint.
            page += 1
            self._save_checkpoint(page, fetched)
            if page % 50 == 0:
                logger.info(f"Progress: page {page}/{total_pages}, {fetched:,} decisions")
            if total_pages and page >= total_pages:
                break
            data = self._search(page)

        self._clear_checkpoint()
        logger.info(f"Fetched {fetched:,} Supreme Court decisions")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions with a session date newer than `since` (date-DESC)."""
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        page = 0
        while True:
            data = self._search(page, direction="DESC")
            docs = data.get("docs", []) or []
            if not docs:
                break
            crossed = False
            for doc in docs:
                iso = self._iso_date(doc.get("sessionDate"))
                if iso:
                    try:
                        d = datetime.strptime(iso, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                        if d < since:
                            crossed = True
                            break
                    except ValueError:
                        pass
                yield doc
            if crossed:
                break
            page += 1
            total_pages = data.get("totalPages", 0)
            if total_pages and page >= total_pages:
                break

    # ── helpers ──────────────────────────────────────────────────────
    @staticmethod
    def _iso_date(raw: Optional[str]) -> str:
        """sessionDate arrives as 'YYYY-MM-DD' (occasionally with a T-suffix)."""
        if not raw or not isinstance(raw, str):
            return ""
        raw = raw.strip()
        if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
            return raw[:10]
        return ""

    @staticmethod
    def _strip_html(fragment: Optional[str]) -> str:
        if not fragment:
            return ""
        text = BeautifulSoup(fragment, "html.parser").get_text("\n")
        # Collapse whitespace produced by the stripped markup.
        lines = [ln.strip() for ln in text.splitlines()]
        text = "\n".join(ln for ln in lines if ln)
        return text.strip()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a raw search hit into the standard schema (with full text)."""
        doc_id = raw.get("id")
        if doc_id is None:
            return None

        core = self._strip_html(raw.get("coreText"))
        ruling = self._strip_html(raw.get("ruling"))
        motivation = self._strip_html(raw.get("motivation"))

        parts = []
        if core:
            parts.append(f"JEDRO (Summary):\n{core}")
        if ruling:
            parts.append(f"IZREK (Disposition):\n{ruling}")
        if motivation:
            parts.append(f"OBRAZLOŽITEV (Reasoning):\n{motivation}")
        full_text = "\n\n".join(parts).strip()

        if not full_text:
            return None  # skip metadata-only stubs

        ecli = (raw.get("ecli") or "").strip()
        ordinal = (raw.get("ordinalNumber") or "").strip()
        reg = (raw.get("registryNumber") or "").strip()
        title = ordinal or ecli or reg or f"VSRS {doc_id}"

        areas = raw.get("areas") or []
        keywords = raw.get("keywords") or []
        legislation = raw.get("legislation") or []
        refs = "; ".join(
            f"{l.get('name', '')} {l.get('value', '')}".strip()
            for l in legislation if isinstance(l, dict)
        ).strip()

        doc_type = raw.get("documentType") or DOC_TYPE

        return {
            # Required base fields
            "_id": f"SI/SupremeCourt/{ecli or doc_id}",
            "_source": "SI/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,
            "date": self._iso_date(raw.get("sessionDate")) or None,
            "url": DOC_URL.format(type=doc_type, id=doc_id),
            # Source-specific fields
            "ecli": ecli,
            "decision_number": ordinal,
            "registry_number": reg,
            "court": raw.get("court", ""),
            "department": raw.get("department", ""),
            "legal_area": ", ".join(a for a in areas if isinstance(a, str)),
            "keywords": ", ".join(k for k in keywords if isinstance(k, str)),
            "references": refs,
            "summary": core,
            "disposition": ruling,
            "reasoning": motivation,
            "last_modified": self._iso_date(raw.get("updatedAt")),
            "language": "sl",
        }

    # ── connectivity test ────────────────────────────────────────────
    def test_api(self):
        print("Testing SI/SupremeCourt (sodnapraksa.si backend API)...")
        data = self._search(0, direction="DESC")
        print(f"\n1. Search API total hits: {data.get('hits'):,} "
              f"({data.get('totalPages')} pages)")
        docs = data.get("docs", [])
        print(f"   Docs on page 0: {len(docs)}")
        if docs:
            rec = self.normalize(docs[0])
            if rec:
                print("\n2. Newest decision (normalized):")
                print(f"   Title:   {rec['title']}")
                print(f"   ECLI:    {rec['ecli']}")
                print(f"   Date:    {rec['date']}")
                print(f"   URL:     {rec['url']}")
                print(f"   Text:    {len(rec['text']):,} chars")
                print(f"   Preview: {rec['text'][:200]}...")
            else:
                print("   ERROR: newest doc normalized to None")
        print("\nAPI test complete!")


def main():
    scraper = SISupremeCourtScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test-api] "
              "[--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command in ("bootstrap", "bootstrap-fast"):
        # bootstrap-fast is an alias for the full path (fleet runner invokes it).
        if sample_mode:
            stats = scraper.bootstrap(sample_mode=True, sample_size=sample_size)
            print(f"\nSample complete: "
                  f"{stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, "
                  f"{stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, "
              f"{stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
