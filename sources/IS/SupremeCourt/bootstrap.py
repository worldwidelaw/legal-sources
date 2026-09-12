#!/usr/bin/env python3
"""
IS/SupremeCourt -- Icelandic Supreme Court (Hæstiréttur) case law

Fetches the full-text judgments of the Supreme Court of Iceland (Hæstiréttur
Íslands).

Strategy (2026-07 rewrite — the court site migrated to island.is):

  - www.haestirettur.is now 302-redirects to the national portal
    https://island.is/domar (a Next.js app), so the legacy .aspx pager 404s
    past offset 100 (issue #1213). The portal is backed by a public GraphQL
    API at https://island.is/api/graphql:

        webVerdicts(input: {court:["Hæstiréttur"], page:N,
                            dateFrom, dateTo})   -> list (id, caseNumber,
                                                    verdictDate, keywords,
                                                    presentings), 10 per page
        webVerdictById(input: {id})             -> item.richText (Contentful
                                                    JSON AST = FULL judgment)

  - ~12,220 Hæstiréttur judgments, 1999-present. The full judgment text lives
    in ``richText`` (a rich-text document AST) which is flattened to plain text.
    ``presentings`` is the case summary/headnote.

  - Crawled year-by-year (dateFrom/dateTo) newest-first, with a completed-year
    checkpoint so fleet reruns resume safely and newly published judgments only
    ever affect the current year's partition.

Usage:
  python bootstrap.py bootstrap            # Full initial pull (streams to data/records.jsonl)
  python bootstrap.py bootstrap-fast       # Alias for full bootstrap (fleet runner)
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Incremental update (recent judgments)
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IS.SupremeCourt")

GQL_URL = "https://island.is/api/graphql"
COURT = "Hæstiréttur"
DOC_URL = "https://island.is/domar/{id}"
FIRST_YEAR = 1999
PAGE_SIZE = 10  # island.is fixes the verdict page size at 10
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

_LIST_QUERY = (
    "query V($input: WebVerdictsInput!){ webVerdicts(input: $input){ total "
    "items{ id title court caseNumber verdictDate keywords presentings } } }"
)
_DETAIL_QUERY = (
    "query D($input: WebVerdictByIdInput!){ webVerdictById(input: $input){ "
    "item{ title court caseNumber verdictDate keywords presentings richText } } }"
)


class ISSupremeCourtScraper(BaseScraper):
    """
    Scraper for IS/SupremeCourt -- Icelandic Supreme Court judgments.
    Country: IS
    URL: https://island.is/domar
    Data types: case_law
    Auth: none (public court judgments)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.checkpoint_file = source_dir / "checkpoint.json"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    # ── GraphQL ──────────────────────────────────────────────────────
    def _gql(self, query: str, variables: dict, timeout: int = 60) -> dict:
        self.rate_limiter.wait()
        resp = self.session.post(
            GQL_URL, json={"query": query, "variables": variables}, timeout=timeout
        )
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("errors"):
            raise RuntimeError(f"GraphQL error: {payload['errors'][:1]}")
        return payload["data"]

    def _list_page(self, page: int, year: Optional[int] = None) -> Dict[str, Any]:
        inp: Dict[str, Any] = {"court": [COURT], "page": page}
        if year is not None:
            inp["dateFrom"] = f"{year}-01-01"
            inp["dateTo"] = f"{year}-12-31"
        return self._gql(_LIST_QUERY, {"input": inp})["webVerdicts"]

    def _detail(self, verdict_id: str) -> Optional[Dict[str, Any]]:
        data = self._gql(_DETAIL_QUERY, {"input": {"id": verdict_id}})
        return (data.get("webVerdictById") or {}).get("item")

    # ── rich-text flattening ─────────────────────────────────────────
    @staticmethod
    def _flatten_richtext(rich: Any) -> str:
        """Flatten a Contentful rich-text document AST into plain text."""
        if not rich:
            return ""
        doc = rich.get("document") if isinstance(rich, dict) else None
        if doc is None:
            doc = rich
        out: List[str] = []

        def walk(node):
            if isinstance(node, dict):
                nt = node.get("nodeType", "")
                if nt == "text":
                    out.append(node.get("value", ""))
                for child in node.get("content", []) or []:
                    walk(child)
                if nt.startswith(("heading", "paragraph", "list-item", "blockquote", "hr")):
                    out.append("\n")
            elif isinstance(node, list):
                for child in node:
                    walk(child)

        walk(doc)
        text = "".join(out)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n[ \t]+", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    # ── checkpoint (completed years) ─────────────────────────────────
    def _load_done_years(self) -> set:
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file) as f:
                    return set(json.load(f).get("done_years", []))
            except Exception:
                pass
        return set()

    def _save_done_years(self, done: set):
        try:
            with open(self.checkpoint_file, "w") as f:
                json.dump({"done_years": sorted(done),
                           "timestamp": datetime.now(timezone.utc).isoformat()}, f)
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
        current_year = datetime.now(timezone.utc).year
        done_years = self._load_done_years()
        total_all = self._list_page(1).get("total", 0)
        logger.info(f"Total Hæstiréttur judgments: {total_all:,}")

        for year in range(current_year, FIRST_YEAR - 1, -1):
            if year in done_years:
                continue
            page = 1
            year_count = 0
            while True:
                data = self._list_page(page, year=year)
                items = data.get("items", []) or []
                if not items:
                    break
                total = data.get("total", 0)
                for item in items:
                    raw = self._enrich(item)
                    if raw:
                        year_count += 1
                        yield raw
                if page * PAGE_SIZE >= total:
                    break
                page += 1
            logger.info(f"Year {year}: {year_count} judgments")
            done_years.add(year)
            self._save_done_years(done_years)

        self._clear_checkpoint()

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        page = 1
        while True:
            data = self._list_page(page)  # newest-first across all years
            items = data.get("items", []) or []
            if not items:
                break
            total = data.get("total", 0)
            crossed = False
            for item in items:
                iso = self._iso_date(item.get("verdictDate"))
                if iso:
                    try:
                        d = datetime.strptime(iso, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                        if d < since:
                            crossed = True
                            break
                    except ValueError:
                        pass
                raw = self._enrich(item)
                if raw:
                    yield raw
            if crossed or page * PAGE_SIZE >= total:
                break
            page += 1

    def _enrich(self, list_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch the detail record and merge its full-text richText."""
        vid = list_item.get("id")
        if not vid:
            return None
        try:
            item = self._detail(vid)
        except Exception as e:
            logger.warning(f"Detail fetch failed for {vid}: {e}")
            return None
        if not item:
            return None
        item["id"] = vid
        return item

    # ── helpers ──────────────────────────────────────────────────────
    @staticmethod
    def _iso_date(raw: Optional[str]) -> str:
        if not raw or not isinstance(raw, str):
            return ""
        return raw[:10] if len(raw) >= 10 and raw[4] == "-" else ""

    def normalize(self, raw: dict) -> Optional[dict]:
        vid = raw.get("id")
        if not vid:
            return None

        text = self._flatten_richtext(raw.get("richText"))
        presentings = (raw.get("presentings") or "").strip()
        if not text and presentings:
            text = presentings
        if not text or len(text) < 40:
            return None  # PDF-only / empty stub

        case_number = (raw.get("caseNumber") or "").strip()
        title = (raw.get("title") or "").strip() or (
            f"Hæstiréttur {case_number}" if case_number else f"Hæstiréttur {vid}")
        keywords = raw.get("keywords") or []

        return {
            "_id": f"IS/SupremeCourt/{vid}",
            "_source": "IS/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": self._iso_date(raw.get("verdictDate")) or None,
            "url": DOC_URL.format(id=vid),
            "case_number": case_number,
            "court": raw.get("court", COURT),
            "keywords": ", ".join(k for k in keywords if isinstance(k, str)),
            "summary": presentings,
            "language": "is",
        }

    # ── connectivity test ────────────────────────────────────────────
    def test_api(self):
        print("Testing IS/SupremeCourt (island.is GraphQL)...")
        data = self._list_page(1)
        print(f"\n1. webVerdicts total: {data.get('total'):,}")
        items = data.get("items", [])
        print(f"   Items on page 1: {len(items)}")
        if items:
            raw = self._enrich(items[0])
            rec = self.normalize(raw) if raw else None
            if rec:
                print("\n2. Newest judgment (normalized):")
                print(f"   Title:   {rec['title'][:70]}")
                print(f"   Case:    {rec['case_number']}")
                print(f"   Date:    {rec['date']}")
                print(f"   URL:     {rec['url']}")
                print(f"   Text:    {len(rec['text']):,} chars")
                print(f"   Preview: {rec['text'][:180]}...")
            else:
                print("   ERROR: newest judgment normalized to None")
        print("\nAPI test complete!")


def main():
    scraper = ISSupremeCourtScraper()

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
