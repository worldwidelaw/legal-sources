#!/usr/bin/env python3
"""
AU/OpenAusLegalCorpus -- State-level Australian Legal Corpus Fetcher

Fetches state-level legislation and case law from the Open Australian
Legal Corpus on HuggingFace (isaacus/open-australian-legal-corpus).

Excludes sources already covered by AU/FedCourt and AU/FederalRegister:
  - federal_court_of_australia -> AU/FedCourt
  - federal_register_of_legislation -> AU/FederalRegister

Includes:
  - high_court_of_australia (~8K decisions)
  - nsw_caselaw (~117K decisions)
  - nsw_legislation
  - qld_legislation
  - wa_legislation
  - sa_legislation
  - tas_legislation

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap-fast     # Alias for the full pull (fleet wrapper)
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Incremental refresh (see fetch_updates)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AU.OpenAusLegalCorpus")

DATASET_ID = "isaacus/open-australian-legal-corpus"
CONFIG = "corpus"
SPLIT = "corpus"

HF_DATASET_API = f"https://huggingface.co/api/datasets/{DATASET_ID}"
HF_ROWS_API = "https://datasets-server.huggingface.co/rows"
BATCH_SIZE = 100  # rows API hard cap

# Transient server-side failures. A single 502 from the HF gateway must not end
# a refresh — that is the #1453 class.
RETRY_STATUSES = {429, 500, 502, 503, 504}

# Sources already covered by other AU scrapers
EXCLUDED_SOURCES = {
    "federal_court_of_australia",       # AU/FedCourt
    "federal_register_of_legislation",  # AU/FederalRegister
}

# Map HuggingFace source names to human-readable court/source names
SOURCE_MAP = {
    "high_court_of_australia": ("High Court of Australia", "case_law"),
    "nsw_caselaw": ("NSW Courts", "case_law"),
    "nsw_legislation": ("NSW Legislation", "legislation"),
    "queensland_legislation": ("QLD Legislation", "legislation"),
    "western_australian_legislation": ("WA Legislation", "legislation"),
    "south_australian_legislation": ("SA Legislation", "legislation"),
    "tasmanian_legislation": ("TAS Legislation", "legislation"),
}

# Map HuggingFace type field to our _type
TYPE_MAP = {
    "decision": "case_law",
    "primary_legislation": "legislation",
    "secondary_legislation": "legislation",
    "bill": "legislation",
}

# Map jurisdiction field to short form
JURISDICTION_MAP = {
    "commonwealth": "AU-CTH",
    "new_south_wales": "AU-NSW",
    "queensland": "AU-QLD",
    "western_australia": "AU-WA",
    "south_australia": "AU-SA",
    "tasmania": "AU-TAS",
    "norfolk_island": "AU-NFK",
}


class OpenAusLegalCorpusScraper(BaseScraper):
    """
    Scraper for AU/OpenAusLegalCorpus -- State + High Court documents.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _parse_date(self, date_str: str) -> Optional[str]:
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str.strip()[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        version_id = raw.get("version_id", "")
        text = raw.get("text", "")
        citation = raw.get("citation", "") or version_id
        date_str = raw.get("date", "")
        url = raw.get("url", "")
        source = raw.get("source", "")
        jurisdiction = raw.get("jurisdiction", "")
        doc_type = raw.get("type", "")

        source_info = SOURCE_MAP.get(source, (source, "legislation"))
        mapped_type = TYPE_MAP.get(doc_type, "legislation")

        return {
            "_id": version_id,
            "_source": "AU/OpenAusLegalCorpus",
            "_type": mapped_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": citation,
            "text": text,
            "date": self._parse_date(date_str),
            "url": url,
            "jurisdiction": JURISDICTION_MAP.get(jurisdiction, jurisdiction),
            "source_name": source_info[0],
            "doc_type": doc_type,
            "version_id": version_id,
            "when_scraped": raw.get("when_scraped") or None,
        }

    # ── HuggingFace access ────────────────────────────────────────────

    def _api_get(self, url: str, params: Dict[str, Any] = None, attempts: int = 6) -> Dict[str, Any]:
        """GET a HuggingFace JSON endpoint, retrying the transient statuses.

        Raises on the last attempt rather than returning an empty payload: a
        silent `{}` here reads downstream as "the dataset is empty", which is
        exactly the false-completion this issue is about.
        """
        delay = 2.0
        for attempt in range(1, attempts + 1):
            try:
                resp = requests.get(url, params=params, timeout=120)
                if resp.status_code in RETRY_STATUSES and attempt < attempts:
                    wait = float(resp.headers.get("Retry-After") or delay)
                    logger.warning(
                        f"HTTP {resp.status_code} from {url} "
                        f"(attempt {attempt}/{attempts}) — retrying in {wait:.0f}s"
                    )
                    time.sleep(min(wait, 120))
                    delay = min(delay * 2, 120)
                    continue
                resp.raise_for_status()
                return resp.json()
            except (requests.ConnectionError, requests.Timeout, ValueError) as e:
                if attempt >= attempts:
                    raise
                logger.warning(f"{type(e).__name__} from {url} "
                               f"(attempt {attempt}/{attempts}) — retrying in {delay:.0f}s")
                time.sleep(delay)
                delay = min(delay * 2, 120)
        raise RuntimeError(f"exhausted {attempts} attempts against {url}")

    def _dataset_revision(self) -> Tuple[Optional[str], Optional[str]]:
        """(commit sha, lastModified) for the dataset, or (None, None)."""
        try:
            info = self._api_get(HF_DATASET_API)
            return info.get("sha"), info.get("lastModified")
        except Exception as e:
            logger.warning(f"Could not read dataset revision: {e}")
            return None, None

    def _rows(self, offset: int, length: int = BATCH_SIZE) -> Tuple[List[Dict[str, Any]], int]:
        """A page of raw rows plus the dataset's total row count."""
        payload = self._api_get(
            HF_ROWS_API,
            {"dataset": DATASET_ID, "config": CONFIG, "split": SPLIT,
             "offset": offset, "length": length},
        )
        rows = [item.get("row", {}) for item in payload.get("rows", [])]
        return rows, int(payload.get("num_rows_total") or 0)

    @staticmethod
    def _scraped_day(row: Dict[str, Any]) -> str:
        """The YYYY-MM-DD the upstream corpus scraped this row, or ''.

        This is the availability stamp — when the document entered the corpus —
        not `date`, which is when the Act commenced or the judgment issued. Only
        the former answers "is this new to us"; `date` is unsorted here and a
        1998 Act re-added tomorrow would be filtered straight back out.
        """
        return str(row.get("when_scraped") or "")[:10]

    def _first_offset_on_or_after(self, cutoff: str, total: int) -> int:
        """Lowest offset whose `when_scraped` day is >= `cutoff`, else `total`.

        `when_scraped` rises monotonically with offset (the corpus is stored in
        scrape order — verified live across offsets 0/30K/60K/90K/118K), so the
        boundary is findable in ~17 requests instead of walking 118K rows.
        """
        lo, hi = 0, total
        while lo < hi:
            mid = (lo + hi) // 2
            rows, _ = self._rows(mid, 1)
            if not rows:
                hi = mid
                continue
            if self._scraped_day(rows[0]) >= cutoff:
                hi = mid
            else:
                lo = mid + 1
        return lo

    def _walk_rows(self, start: int, total: int, cutoff: str = "") -> Generator[Dict[str, Any], None, None]:
        """Yield normalized records from `start` to the end of the dataset."""
        offset = start
        emitted = 0
        while offset < total:
            rows, total = self._rows(offset, BATCH_SIZE)
            if not rows:
                break
            for row in rows:
                offset += 1
                if cutoff and self._scraped_day(row) < cutoff:
                    continue
                if row.get("source", "") in EXCLUDED_SOURCES:
                    continue
                text = row.get("text", "")
                if not text or len(text.strip()) < 50:
                    continue
                emitted += 1
                yield self.normalize(row)
            if emitted and emitted % 1000 == 0:
                logger.info(f"  Emitted {emitted} records (offset {offset}/{total})")
        logger.info(f"Walk complete: {emitted} record(s) from offset {start} to {total}")

    # ── Checkpoint ────────────────────────────────────────────────────

    def _checkpoint_path(self) -> Path:
        return self.source_dir / "data" / "hf_checkpoint.json"

    def _load_checkpoint(self) -> Dict[str, Any]:
        try:
            with open(self._checkpoint_path(), encoding="utf-8") as f:
                return json.load(f)
        except (OSError, ValueError):
            return {}

    def _save_checkpoint(self, sha: str, rows_total: int, max_scraped: str) -> None:
        path = self._checkpoint_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "sha": sha,
                    "rows_total": rows_total,
                    "max_when_scraped": max_scraped,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
                f,
                indent=2,
            )
        tmp.replace(path)  # atomic: a half-written checkpoint would skip real rows

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        # Read the revision before the walk, so a corpus released mid-crawl is
        # not recorded as consumed — the next refresh should still pick it up.
        current_sha, _ = self._dataset_revision()

        try:
            from datasets import load_dataset
        except ImportError:
            logger.warning(
                "HuggingFace datasets library unavailable — falling back to the "
                "rows API. Install `datasets` for a faster full pull."
            )
            _, total = self._rows(0, 1)
            last_rows, _ = self._rows(max(total - 1, 0), 1)
            yield from self._walk_rows(0, total)
            self._save_checkpoint(
                current_sha or "", total,
                self._scraped_day(last_rows[0]) if last_rows else "",
            )
            return

        logger.info(f"Streaming dataset {DATASET_ID} (excluding {EXCLUDED_SOURCES})")

        ds = load_dataset(DATASET_ID, split="corpus", streaming=True)
        count = 0
        skipped = 0
        total_scanned = 0
        max_scraped = ""

        for row in ds:
            total_scanned += 1
            max_scraped = max(max_scraped, self._scraped_day(row))
            if total_scanned % 25000 == 0:
                logger.info(
                    f"Scanned {total_scanned} records, yielded {count}, "
                    f"skipped {skipped} (excluded sources)"
                )

            source = row.get("source", "")
            if source in EXCLUDED_SOURCES:
                skipped += 1
                continue

            text = row.get("text", "")
            if not text or len(text.strip()) < 50:
                continue

            normalized = self.normalize(row)
            count += 1
            yield normalized

        # Baseline for the incremental refresh, written only on a clean finish.
        self._save_checkpoint(current_sha or "", total_scanned, max_scraped)
        logger.info(
            f"Completed: {count} documents from {total_scanned} total "
            f"({skipped} excluded)"
        )

    def fetch_updates(self, since=None) -> Generator[Dict[str, Any], None, None]:
        """Yield only documents that entered the corpus since the last refresh.

        The old body was the `log; return; yield` stub, so `BaseScraper.update()`
        saw zero records and the fleet wrapper fell back to a full bootstrap —
        re-crawling 118K rows on every update-stale slot to find the handful that
        were new (#1502).

        This is a static, versioned HuggingFace corpus, so "what changed" is a
        release question, not a date-range query. Three filters, cheapest first:

        * the commit `sha` identifies content exactly. Unchanged sha means no
          release happened, answered in one request. Between releases — which the
          CHANGELOG shows arrive a few times a year — that is the whole refresh.
        * `when_scraped` is the availability stamp and rises monotonically with
          offset, so the first genuinely new row is found by binary search and
          only the tail is read. `date` cannot drive this: it is the commencement
          or judgment date, is unsorted, and is null for High Court rows.
        * if the sha moved but no row is newer than the last one we saw, the
          release rewrote existing rows in place rather than appending (the
          7.1.0 Unicode-fix release did exactly this). Every row's text may have
          changed, so that case re-emits the corpus — loudly, not silently.
        """
        checkpoint = self._load_checkpoint()
        prev_sha = checkpoint.get("sha")
        prev_max_scraped = checkpoint.get("max_when_scraped") or ""
        current_sha, last_modified = self._dataset_revision()

        if current_sha and prev_sha and current_sha == prev_sha:
            logger.info(
                f"Dataset revision unchanged ({current_sha[:12]}, last modified "
                f"{last_modified}) — no new documents. Skipped walking the corpus."
            )
            return

        _, total = self._rows(0, 1)
        if not total:
            raise RuntimeError(
                f"{DATASET_ID} reports 0 rows — refusing to treat that as "
                f"'nothing new' (it would look like a clean refresh)"
            )

        last_rows, _ = self._rows(max(total - 1, 0), 1)
        max_scraped = self._scraped_day(last_rows[0]) if last_rows else ""

        cutoff = max(as_date_str(since), prev_max_scraped) or ""

        if cutoff and max_scraped and max_scraped < cutoff:
            # Nothing in the corpus was scraped after our cutoff.
            if prev_sha:
                logger.warning(
                    f"Revision moved {prev_sha[:12]} -> {str(current_sha)[:12]} but the "
                    f"newest row is still from {max_scraped}, before the {cutoff} cutoff: "
                    f"the release rewrote existing rows rather than adding any. "
                    f"Re-emitting all {total} rows so the corrections land."
                )
                yield from self._walk_rows(0, total)
            else:
                logger.info(
                    f"Newest row scraped {max_scraped}, before the {cutoff} cutoff "
                    f"— no new documents."
                )
            self._save_checkpoint(current_sha or "", total, max_scraped)
            return

        start = self._first_offset_on_or_after(cutoff, total) if cutoff else 0
        logger.info(
            f"Revision {str(prev_sha)[:12]} -> {str(current_sha)[:12]}: rows scraped on or "
            f"after {cutoff or '(no cutoff)'} start at offset {start} of {total}"
        )
        yield from self._walk_rows(start, total, cutoff)
        self._save_checkpoint(current_sha or prev_sha or "", total, max_scraped)

    def test(self) -> bool:
        try:
            from datasets import load_dataset
            ds = load_dataset(DATASET_ID, split="corpus", streaming=True)
            count = 0
            scanned = 0
            for row in ds:
                scanned += 1
                source = row.get("source", "")
                if source not in EXCLUDED_SOURCES:
                    text = row.get("text", "")
                    if text and len(text.strip()) >= 50:
                        count += 1
                        logger.info(
                            f"  [{count}] {row.get('citation', '?')[:80]} "
                            f"(source={source}, {len(text):,} chars)"
                        )
                        if count >= 3:
                            logger.info(f"Test passed: found {count} records in {scanned} scanned")
                            return True
                if scanned > 5000:
                    break
            return count > 0
        except Exception as e:
            logger.error(f"Test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="AU/OpenAusLegalCorpus data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run ('bootstrap-fast' is the fleet wrapper's name for "
             "the full pull; without it argparse exits 2 and the wrapper falls "
             "back to re-ingesting sample/)",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = OpenAusLegalCorpusScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
if __name__ == "__main__":
    main()
