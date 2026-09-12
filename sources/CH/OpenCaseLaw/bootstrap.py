#!/usr/bin/env python3
"""
CH/OpenCaseLaw -- Swiss Court Decisions (OpenCaseLaw / Entscheidsuche)

Fetches Swiss court decisions from the voilaj/swiss-caselaw dataset on HuggingFace.
963K+ decisions from all cantons and federal courts, with full text.

Strategy:
  - Uses HuggingFace datasets-server rows API (no auth needed)
  - For sample: fetches small batches via rows API
  - For full: iterates through all rows in batches of 100

Data fields:
  - decision_id: unique decision identifier
  - court: court name
  - canton: Swiss canton code
  - docket_number: case reference number
  - decision_date: decision date
  - language: de/fr/it/rm
  - full_text: complete decision text
  - regeste: summary/headnote
  - legal_area: area of law
  - cited_decisions: JSON list of cited references

License: Open Data

Usage:
  python bootstrap.py bootstrap          # Full initial pull (warning: 963K rows)
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import hashlib
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CH.OpenCaseLaw")

HF_ROWS_API = "https://datasets-server.huggingface.co/rows"
HF_PARQUET_API = "https://datasets-server.huggingface.co/parquet"
HF_DATASET_API = "https://huggingface.co/api/datasets"
DATASET = "voilaj/swiss-caselaw"
CONFIG = "default"
SPLIT = "train"
BATCH_SIZE = 100

# Rows decoded per pyarrow batch. The parquet row groups are 5,000 rows /
# ~138MB uncompressed, and pyarrow materialises a whole row group regardless of
# this value, so it bounds only the Python-side dict churn — the real memory
# ceiling is one row group, which is comfortable on the fleet's 4GB boxes.
PARQUET_BATCH_ROWS = 500

# Transient server-side failures. A single 502 from the HF gateway must not kill
# a 1M-row crawl, so these are retried alongside 429 rather than raised.
RETRY_STATUS = {429, 500, 502, 503, 504}

# Persist the full-crawl resume pointer every N batches (= 5,000 rows). Cheap
# enough to be invisible next to the 1.5s/batch sleep, and bounds the rows a
# killed run has to re-walk.
CHECKPOINT_EVERY = 50


class OpenCaseLawScraper(BaseScraper):
    """Scraper for CH/OpenCaseLaw -- Swiss Court Decisions."""

    def __init__(self, restart: bool = False):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        # BaseScraper.bootstrap() calls fetch_all() with no arguments, so the
        # CLI flag has to ride on the instance.
        self.restart = restart

    def _fetch_batch(self, offset: int, length: int) -> dict:
        """Fetch a batch of rows from HuggingFace datasets API with retry."""
        import requests

        params = {
            "dataset": DATASET,
            "config": CONFIG,
            "split": SPLIT,
            "offset": offset,
            "length": length,
        }
        max_retries = 4
        for attempt in range(max_retries):
            try:
                resp = requests.get(HF_ROWS_API, params=params, timeout=120)
                resp.raise_for_status()
                return resp.json()
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                wait = 2 ** attempt * 5  # 5, 10, 20, 40 seconds
                logger.warning(f"Timeout/connection error at offset {offset}, retry {attempt+1}/{max_retries} in {wait}s: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(wait)
            except requests.exceptions.HTTPError:
                if resp.status_code in RETRY_STATUS:
                    wait = 2 ** attempt * 10
                    logger.warning(
                        f"HTTP {resp.status_code} at offset {offset}, "
                        f"retry {attempt+1}/{max_retries} in {wait}s"
                    )
                    if attempt == max_retries - 1:
                        raise
                    time.sleep(wait)
                    continue
                raise

    def _parquet_files(self) -> list:
        """List the split's parquet shards, in dataset row order.

        HuggingFace auto-converts every dataset to parquet under the
        `refs/convert/parquet` revision, and the shards are numbered in the same
        order the rows API pages them, so a global row count indexes both.
        """
        import requests

        resp = requests.get(HF_PARQUET_API, params={"dataset": DATASET}, timeout=60)
        resp.raise_for_status()
        files = [
            f for f in resp.json().get("parquet_files", [])
            if f.get("config") == CONFIG and f.get("split") == SPLIT
        ]
        return sorted(files, key=lambda f: f["filename"])

    def _download_parquet(self, url: str, dest: Path) -> None:
        """Stream one shard to disk, retrying the same transient classes as the
        rows API. Shards reach ~1.3GB, so this streams rather than buffering."""
        import requests

        max_retries = 4
        for attempt in range(max_retries):
            try:
                with requests.get(url, stream=True, timeout=(30, 300)) as resp:
                    resp.raise_for_status()
                    with open(dest, "wb") as fh:
                        for chunk in resp.iter_content(1 << 20):
                            fh.write(chunk)
                return
            except (requests.exceptions.Timeout,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.ChunkedEncodingError) as e:
                if attempt == max_retries - 1:
                    raise
                wait = 2 ** attempt * 5
                logger.warning(f"Download error for {url} ({e}); retry {attempt+1} in {wait}s")
                time.sleep(wait)
            except requests.exceptions.HTTPError:
                if resp.status_code not in RETRY_STATUS or attempt == max_retries - 1:
                    raise
                wait = 2 ** attempt * 10
                logger.warning(
                    f"HTTP {resp.status_code} for {url}; retry {attempt+1} in {wait}s"
                )
                time.sleep(wait)

    def _shard_rows(self, url: str) -> int:
        """Row count of a shard, read from its parquet footer over HTTP range
        requests — a few KB, versus downloading the whole shard to skip it."""
        import fsspec
        import pyarrow.parquet as pq

        with fsspec.open(url).open() as fh:
            return pq.ParquetFile(fh).metadata.num_rows

    def _fetch_all_parquet(self, resume_from: int, current_sha: Optional[str],
                           checkpoint: dict) -> Generator[dict, None, None]:
        """Walk the corpus via the parquet export, resuming at a global row offset.

        Yields dicts shaped exactly like the rows API's `row` objects: every
        column normalize() reads is a parquet string column, so `to_pylist()`
        produces the same `str | None` values the JSON path does.
        """
        import pyarrow.parquet as pq

        shards = self._parquet_files()
        total_bytes = sum(f.get("size", 0) for f in shards)
        logger.info(
            f"Parquet export: {len(shards)} shards, {total_bytes / 1e9:.2f}GB "
            f"(starting at row {resume_from:,})"
        )

        tmp = self._checkpoint_path().parent / "_shard.parquet"
        emitted = 0

        for shard in shards:
            # Skip shards entirely consumed by a previous run. The footer read
            # costs a couple of seconds; downloading the shard to discard it
            # would cost minutes.
            if emitted < resume_from:
                rows = self._shard_rows(shard["url"])
                if emitted + rows <= resume_from:
                    emitted += rows
                    continue

            self._download_parquet(shard["url"], tmp)
            try:
                pf = pq.ParquetFile(tmp)
                for batch in pf.iter_batches(batch_size=PARQUET_BATCH_ROWS):
                    rows = batch.to_pylist()
                    if emitted + len(rows) <= resume_from:
                        emitted += len(rows)
                        continue
                    for row in rows:
                        # Partially-consumed shard: drop the prefix already emitted.
                        if emitted < resume_from:
                            emitted += 1
                            continue
                        emitted += 1
                        yield row
                del pf
            finally:
                tmp.unlink(missing_ok=True)

            self._save_checkpoint(
                checkpoint.get("sha"), checkpoint.get("rows_seen") or 0,
                full_sha=current_sha, full_offset=emitted,
            )
            logger.info(f"Shard {shard['filename']} done — {emitted:,} rows")

        self._finish_full_walk(current_sha, emitted)

    def _fetch_all_rows_api(self, resume_from: int, current_sha: Optional[str],
                            checkpoint: dict) -> Generator[dict, None, None]:
        """Paginated rows-API walk. Correct but slow: it delivered only 53% of
        the corpus inside the fleet's 100h cap (#1505), so it is the fallback
        for when the parquet export is unavailable."""
        offset = resume_from
        total = None
        batches = 0

        while True:
            data = self._fetch_batch(offset, BATCH_SIZE)

            if total is None:
                total = data.get("num_rows_total", 0)
                logger.info(f"Total rows in dataset: {total:,} (starting at {offset:,})")

            rows = data.get("rows", [])
            if not rows:
                break

            for item in rows:
                yield item.get("row", {})

            offset += len(rows)
            batches += 1

            if batches % CHECKPOINT_EVERY == 0:
                self._save_checkpoint(
                    checkpoint.get("sha"), checkpoint.get("rows_seen") or 0,
                    full_sha=current_sha, full_offset=offset,
                )
                logger.info(f"Fetched {offset:,}/{total:,} rows")

            if offset >= total:
                break

            time.sleep(1.5)

        self._finish_full_walk(current_sha, offset)

    def _finish_full_walk(self, current_sha: Optional[str], emitted: int) -> None:
        """A completed walk seeds the incremental checkpoint. Without this the
        first fetch_updates after a bootstrap sees rows_seen=0 and degrades into
        a full 1.06M-row re-walk (#1502)."""
        self._save_checkpoint(current_sha, emitted, full_sha=None, full_offset=None)
        logger.info(f"Fetched {emitted:,} rows total; checkpoint seeded at {emitted:,}")

    def fetch_all(self, restart: Optional[bool] = None) -> Generator[dict, None, None]:
        """Yield every row in the dataset, resuming a truncated previous run.

        Reads the parquet export by default and falls back to the rows API. The
        rows API is uncapped but far too slow for this corpus: it delivered
        559,850 of 1,060,259 rows before the fleet's 100h cap cut the run, which
        is the coverage gap in #1505. The parquet export carries the same rows
        in the same order as ~8GB across 115 shards, which transfers in minutes.

        Both paths checkpoint the same global row offset, so a run killed on
        either can resume on either.

        Resume is only valid within one dataset revision. Rows are append-only
        under a given sha, but a new sha may rewrite existing rows, so a revision
        change restarts from zero rather than skipping rewritten rows.

        Args:
            restart: Ignore the resume pointer and re-walk from offset 0.
                Defaults to the value passed to the constructor.
        """
        if restart is None:
            restart = self.restart

        current_sha = self._dataset_revision()
        checkpoint = self._load_checkpoint()

        resume_from = checkpoint.get("full_offset") or 0
        if restart:
            if resume_from:
                logger.info(f"--restart given, discarding resume pointer at {resume_from:,}")
            resume_from = 0
        elif resume_from:
            if current_sha and checkpoint.get("full_sha") == current_sha:
                logger.info(
                    f"Resuming full crawl at offset {resume_from:,} "
                    f"(revision {current_sha[:12]} unchanged). Rows before this "
                    f"offset were already emitted; pass --restart to re-walk them."
                )
            else:
                logger.info(
                    f"Discarding resume pointer at {resume_from:,} — dataset revision "
                    f"changed ({str(checkpoint.get('full_sha'))[:12]} -> "
                    f"{str(current_sha)[:12]}), earlier rows may have been rewritten"
                )
                resume_from = 0

        try:
            walk = self._fetch_all_parquet(resume_from, current_sha, checkpoint)
            first = next(walk, None)
        except Exception as e:
            # Only a failure *before* the first row is safe to fall back on. Once
            # rows are flowing, a mid-walk error must surface so the run exits
            # non-zero and resumes from its checkpoint rather than silently
            # re-emitting the prefix down the slow path.
            logger.warning(f"Parquet path unavailable ({e}); falling back to the rows API")
            yield from self._fetch_all_rows_api(resume_from, current_sha, checkpoint)
            return

        if first is None:
            # Already-complete resumed walk; the generator ran to completion and
            # seeded the incremental checkpoint on its way out.
            return

        yield first
        yield from walk

    # ── Incremental refresh (#1502) ───────────────────────────────────

    def _checkpoint_path(self) -> Path:
        return self.source_dir / "data" / "hf_checkpoint.json"

    def _load_checkpoint(self) -> dict:
        try:
            with open(self._checkpoint_path(), encoding="utf-8") as f:
                return json.load(f)
        except (OSError, ValueError):
            return {}

    def _save_checkpoint(self, sha: Optional[str], rows_seen: int,
                         full_sha: Optional[str] = None,
                         full_offset: Optional[int] = None) -> None:
        """Persist both checkpoints.

        `rows_seen` is the incremental pointer and means "rows a COMPLETED walk
        emitted". `full_offset` is the resume pointer for a walk still in flight.
        They are kept apart on purpose: if a truncated full crawl advanced
        `rows_seen`, the next fetch_updates would start past rows that may never
        have reached the database. Leaving `rows_seen` behind instead degrades
        that run to a slow full walk, which backfills rather than skips.
        """
        path = self._checkpoint_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"sha": sha, "rows_seen": rows_seen,
                       "full_sha": full_sha, "full_offset": full_offset,
                       "updated_at": datetime.now(timezone.utc).isoformat()}, f, indent=2)
        tmp.replace(path)  # atomic: a half-written checkpoint would skip real rows

    def _dataset_revision(self) -> Optional[str]:
        """Current commit sha of the HF dataset, or None if unreachable."""
        import requests

        try:
            resp = requests.get(f"{HF_DATASET_API}/{DATASET}", timeout=60)
            resp.raise_for_status()
            return resp.json().get("sha")
        except Exception as e:
            logger.warning(f"Could not read dataset revision: {e}")
            return None

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield only rows added since the last run.

        This is a static HuggingFace corpus, so "what changed" is a dataset
        revision question, not a date-range query. Three facts drive the design,
        all verified against the live API:

        * the commit `sha` identifies content exactly — an unchanged sha means
          nothing to do, which costs one request instead of re-walking 1.06M rows
        * rows are appended in crawl order (`scraped_at` rises monotonically with
          offset), so a row-count checkpoint can resume at the new tail
        * `decision_date` is NOT sorted and `publication_date` is almost always
          null, so neither can drive a cutoff — `scraped_at` is the only usable
          date, and it is the fallback filter when the offset checkpoint is void
        """
        checkpoint = self._load_checkpoint()
        prev_sha = checkpoint.get("sha")
        prev_rows = checkpoint.get("rows_seen") or 0

        current_sha = self._dataset_revision()
        if current_sha and prev_sha and current_sha == prev_sha:
            logger.info(
                f"Dataset revision unchanged ({current_sha[:12]}) since last run — "
                f"no new decisions. Skipped re-walking {prev_rows:,} rows."
            )
            return

        head = self._fetch_batch(0, 1)
        total = head.get("num_rows_total", 0)
        logger.info(
            f"Dataset revision {str(prev_sha)[:12]} -> {str(current_sha)[:12]}, "
            f"{prev_rows:,} rows seen, {total:,} rows upstream"
        )

        # A shrunk or equal-sized corpus under a new sha means rows were rewritten,
        # not appended, so the offset checkpoint is meaningless. Fall back to the
        # scraped_at cutoff rather than silently skipping re-issued rows.
        appended = prev_rows and total > prev_rows
        cutoff = as_date_str(since)

        offset = prev_rows if appended else 0
        if appended:
            logger.info(f"Resuming at offset {offset:,} for {total - prev_rows:,} new rows")
        else:
            logger.info(f"Checkpoint unusable — full walk filtered to scraped_at >= {cutoff}")

        emitted = 0
        while offset < total:
            data = self._fetch_batch(offset, BATCH_SIZE)
            rows = data.get("rows", [])
            if not rows:
                break

            for item in rows:
                row = item.get("row", {})
                if not appended:
                    scraped = (row.get("scraped_at") or "")[:10]
                    if scraped and scraped < cutoff:
                        continue
                yield row
                emitted += 1

            offset += len(rows)
            time.sleep(1.5)

        logger.info(f"Update yielded {emitted:,} new rows (corpus now {total:,})")
        if current_sha:
            self._save_checkpoint(current_sha, total)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a raw OpenCaseLaw row into standardized schema."""
        full_text = (raw.get("full_text") or "").strip()
        regeste = (raw.get("regeste") or "").strip()

        # Build text from full_text + regeste
        text_parts = []
        if regeste:
            text_parts.append(regeste)
        if full_text:
            text_parts.append(full_text)
        text = "\n\n".join(text_parts)

        if not text:
            return None

        decision_id = (raw.get("decision_id") or "").strip()
        if decision_id:
            doc_id = f"CH-OCL-{decision_id}"
        else:
            text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
            doc_id = f"CH-OCL-{text_hash}"

        # Parse date
        date = raw.get("decision_date") or raw.get("publication_date")

        # Title
        title = (raw.get("title") or "").strip()
        if not title:
            docket = (raw.get("docket_number") or "").strip()
            court = (raw.get("court") or "").strip()
            title = f"{court} — {docket}" if docket else court or "Swiss Court Decision"

        source_url = (raw.get("source_url") or "").strip()

        return {
            "_id": doc_id,
            "_source": "CH/OpenCaseLaw",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": source_url or f"https://opencaselaw.ch/",
            "decision_id": decision_id,
            "court": (raw.get("court") or "").strip(),
            "canton": (raw.get("canton") or "").strip(),
            "docket_number": (raw.get("docket_number") or "").strip(),
            "language": (raw.get("language") or "").strip(),
            "legal_area": (raw.get("legal_area") or "").strip(),
            "decision_type": (raw.get("decision_type") or "").strip(),
            "judges": (raw.get("judges") or "").strip(),
            "outcome": (raw.get("outcome") or "").strip(),
            "cited_decisions": (raw.get("cited_decisions") or "").strip(),
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CH/OpenCaseLaw data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run ('bootstrap-fast' is the fleet wrapper's name for "
             "the full pull; without it argparse exits 2 and the wrapper falls "
             "back to re-ingesting sample/)",
    )
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch a small sample (for validation)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--restart", action="store_true",
                        help="Ignore the resume pointer and re-walk from offset 0")
    args = parser.parse_args()

    scraper = OpenCaseLawScraper(restart=args.restart)
    cmd = args.command

    if cmd == "test":
        print("Testing HuggingFace datasets API connectivity...")
        try:
            data = scraper._fetch_batch(0, 1)
            total = data.get("num_rows_total", 0)
            rows = data.get("rows", [])
            if rows:
                row = rows[0].get("row", {})
                has_text = bool(row.get("full_text", "").strip())
                print(f"OK: Dataset has {total} rows, full_text present: {has_text}")
            else:
                print("FAIL: No rows returned")
                sys.exit(1)
        except Exception as e:
            print(f"FAIL: {e}")
            sys.exit(1)

    elif cmd in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif cmd == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
