#!/usr/bin/env python3
"""
Latvian Courts (elieta.lv) Data Fetcher

Official anonymized court decisions from the Latvian Courts Portal.
https://www.elieta.lv/web/

Uses the gateway.elieta.lv REST API to search decisions and download
full-text PDFs. Text extracted with PyMuPDF (fitz).

420,000+ decisions covering all Latvian courts since 2007.
No authentication required.
"""

import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, List, Optional, Tuple

import fitz  # PyMuPDF

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown, preload_existing_ids


# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
API_BASE = "https://gateway.elieta.lv/api/v1"
SEARCH_URL = f"{API_BASE}/PublicMaterial"
DOWNLOAD_URL = f"{API_BASE}/PublicMaterialDownload"


class _RateLimiter:
    """Spaces request starts by at least ``min_interval`` across threads.

    Replaces the old per-document ``sleep`` so politeness is a global
    requests-per-second ceiling rather than a serial cost paid by the one
    thread doing the work.
    """

    def __init__(self, min_interval: float):
        self._min_interval = min_interval
        self._lock = threading.Lock()
        self._next_start = 0.0

    def wait(self) -> None:
        with self._lock:
            start = max(time.monotonic(), self._next_start)
            self._next_start = start + self._min_interval
        delay = start - time.monotonic()
        if delay > 0:
            time.sleep(delay)


class LatvianCourtsFetcher:
    """Fetcher for Latvian court decisions from elieta.lv"""

    # Issue #1483: the serial path did ~1 doc per 3s (1.5s curl + 1.5s
    # sleep), so a ~390K-decision corpus needed ~325h — more than three
    # 100h fleet slots.  PDF download runs in a `curl` subprocess and
    # PyMuPDF releases the GIL while parsing, so a thread pool actually
    # overlaps both halves.  Politeness is preserved by _RateLimiter: at
    # most 1/DOC_MIN_INTERVAL requests start per second no matter how many
    # workers are running.
    DEFAULT_WORKERS = 8
    SLOW_WORKERS = 3
    DOC_MIN_INTERVAL = 0.25  # ≈4 req/s → full corpus in ~27h
    SLOW_DOC_MIN_INTERVAL = 1.0

    def __init__(self, slow_mode: bool = False, workers: Optional[int] = None,
                 force: bool = False):
        self.slow_mode = slow_mode
        self.force = force
        self._skipped = 0
        self._skip_lock = threading.Lock()
        self.page_delay = 5.0 if slow_mode else 2.0
        self.workers = max(1, workers or (self.SLOW_WORKERS if slow_mode else self.DEFAULT_WORKERS))
        self.doc_delay = self.SLOW_DOC_MIN_INTERVAL if slow_mode else self.DOC_MIN_INTERVAL
        self._limiter = _RateLimiter(self.doc_delay)

        if slow_mode:
            logger.info("Running in SLOW MODE")
        logger.info(
            f"Fetch concurrency: {self.workers} workers, "
            f"max {1 / self.doc_delay:.1f} PDF req/s"
        )

    def _curl_post(self, url: str, body: dict, max_attempts: int = 3) -> Optional[dict]:
        """POST JSON via curl"""
        body_json = json.dumps(body)
        for attempt in range(max_attempts):
            try:
                result = subprocess.run(
                    ['curl', '-s', '--max-time', '60',
                     '-X', 'POST', url,
                     '-H', 'Content-Type: application/json',
                     '-H', 'Accept: application/json',
                     '-d', body_json],
                    capture_output=True, text=True, timeout=70
                )
                if result.returncode == 0 and result.stdout:
                    return json.loads(result.stdout)
                delay = min(5 * (2 ** attempt), 60)
                logger.warning(f"POST failed attempt {attempt+1}, waiting {delay}s...")
                time.sleep(delay)
            except (subprocess.TimeoutExpired, json.JSONDecodeError) as e:
                delay = min(5 * (2 ** attempt), 60)
                logger.warning(f"POST error attempt {attempt+1}: {e}, waiting {delay}s...")
                time.sleep(delay)
            except Exception as e:
                logger.warning(f"POST unexpected error: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(5)
                else:
                    return None
        return None

    def _download_pdf(self, file_id: str) -> Optional[bytes]:
        """Download a PDF file by materialFileId"""
        url = f"{DOWNLOAD_URL}/{file_id}"
        for attempt in range(3):
            try:
                self._limiter.wait()
                result = subprocess.run(
                    ['curl', '-s', '--max-time', '60', '-o', '-', url],
                    capture_output=True, timeout=70
                )
                if result.returncode == 0 and result.stdout and len(result.stdout) > 100:
                    return result.stdout
                delay = min(5 * (2 ** attempt), 30)
                logger.warning(f"PDF download failed attempt {attempt+1}, waiting {delay}s...")
                time.sleep(delay)
            except subprocess.TimeoutExpired:
                delay = min(5 * (2 ** attempt), 30)
                logger.warning(f"PDF timeout attempt {attempt+1}, waiting {delay}s...")
                time.sleep(delay)
            except Exception as e:
                logger.warning(f"PDF download error: {e}")
                if attempt < 2:
                    time.sleep(5)
                else:
                    return None
        return None

    @staticmethod
    def _doc_id(item: Dict[str, Any]) -> str:
        """The document's stable id — must match what normalize() emits as _id."""
        return str(item.get('ecliCode') or item.get('id') or '')

    def _extract_text_from_pdf(self, pdf_bytes: bytes, source_id: str = "") -> str:
        """Extract text from PDF using centralized extractor.

        ``source_id`` must be the same string ``normalize()`` emits as
        ``_id``: the extractor uses it to skip documents already in Neon
        (issue #1480).  Passing "" disabled that guard, so every refresh
        re-extracted all ~390K PDFs — the other half of the #1483 runtime
        problem.
        """
        return extract_pdf_markdown(
            source="LV/AllCourts",
            source_id=source_id,
            pdf_bytes=pdf_bytes,
            table="case_law",
            force=self.force,
        ) or ""

    def _search_decisions(self, page: int = 1, limit: int = 50,
                          year_month: str = None) -> Optional[dict]:
        """Search for court decisions"""
        body = {
            "page": page,
            "limit": limit,
            "orderBy": "registrationDate",
            "sortOrderDescending": True,
            "institutionSourceRegistryCode": "TIS_COURTS"
        }
        if year_month:
            body["registrationDateYearMonth"] = year_month
        return self._curl_post(SEARCH_URL, body)

    def _parse_timestamp(self, ts) -> Optional[str]:
        """Parse Unix timestamp (ms) to ISO date"""
        if not ts:
            return None
        try:
            if isinstance(ts, str):
                ts = int(ts)
            # Could be ms or seconds
            if ts > 1e12:
                ts = ts / 1000
            return datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
        except (ValueError, OSError):
            return None

    # ------------------------------------------------------------------
    # Checkpoint / resume (Issue #1217)
    #
    # The corpus is ~390K decisions and each PDF download+extract takes
    # ~1.5s, so a single full run cannot finish inside one 100h fleet
    # slot.  Without a checkpoint every relaunch re-walked the newest
    # registrationDate-desc pages from the top and re-appended the same
    # first records (112,518 "fetched" / 24 unique written).  The fix
    # partitions the corpus by registration year-month (the API's
    # registrationDateYearMonth filter) and records which months are
    # fully done plus the in-progress page, so a relaunch skips completed
    # months (no network calls) and resumes the current month at the
    # right page — successive slots advance monotonically to completion.
    # Kept next to the module so it survives the fleet's temp CWD.
    # ------------------------------------------------------------------
    CHECKPOINT_PATH = Path(__file__).parent / "lv_allcourts_checkpoint.json"

    FIRST_YEAR = 2007  # elieta.lv publishes anonymized decisions since 2007

    def _load_checkpoint(self) -> dict:
        try:
            with open(self.CHECKPOINT_PATH, encoding="utf-8") as f:
                data = json.load(f)
            return {
                "completed_months": set(data.get("completed_months", [])),
                "current_month": data.get("current_month"),
                "current_page": data.get("current_page", 1),
            }
        except (FileNotFoundError, json.JSONDecodeError, ValueError):
            return {"completed_months": set(), "current_month": None, "current_page": 1}

    def _save_checkpoint(self, completed: set, current_month, current_page: int) -> None:
        tmp = self.CHECKPOINT_PATH.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "completed_months": sorted(completed),
                    "current_month": current_month,
                    "current_page": current_page,
                },
                f,
            )
        tmp.replace(self.CHECKPOINT_PATH)

    def _iter_months(self):
        """Yield 'MM.YYYY' partitions newest-first back to FIRST_YEAR."""
        now = datetime.now()
        y, m = now.year, now.month
        while (y, m) >= (self.FIRST_YEAR, 1):
            yield f"{m:02d}.{y}"
            m -= 1
            if m == 0:
                m = 12
                y -= 1

    def _already_stored(self, doc_id: str) -> bool:
        """True if Neon already holds this decision's text.

        extract_pdf_markdown runs the same check, but only *after* the PDF
        has been downloaded — and on this source the download is the
        expensive half.  Checking first means a refresh costs one search
        request per page instead of one PDF fetch per already-known
        decision.  preload_existing_ids caches in-process and returns an
        empty set when Neon is unreachable, so this degrades to "extract
        everything", never to "skip everything".
        """
        if not doc_id or self.force:
            return False
        return doc_id in preload_existing_ids("LV/AllCourts", "case_law")

    def _download_and_extract(self, item: Dict[str, Any]) -> Optional[str]:
        doc_id = self._doc_id(item)
        if self._already_stored(doc_id):
            with self._skip_lock:
                self._skipped += 1
            return None
        files = item.get('materialFiles', [])
        if not files:
            logger.warning(f"No files for {item.get('caseNumber', '?')}")
            return None
        file_id = files[0].get('id')
        if not file_id:
            return None
        pdf_bytes = self._download_pdf(file_id)
        if not pdf_bytes:
            logger.warning(f"Failed to download PDF for {item.get('caseNumber', '?')}")
            return None
        text = self._extract_text_from_pdf(pdf_bytes, source_id=self._doc_id(item))
        if not text or len(text) <= 100:
            logger.warning(f"Text too short for {item.get('caseNumber', '?')}")
            return None
        return text

    def _download_page(self, items: List[Dict[str, Any]],
                       executor: ThreadPoolExecutor) -> Iterator[Dict[str, Any]]:
        """Download+extract a page's documents concurrently.

        Yields the items that produced usable text, in the page's original
        order, so the caller's ``limit`` and checkpoint semantics are the
        same as they were on the serial path.
        """
        if not items:
            return
        for item, text in zip(items, executor.map(self._download_and_extract_safe, items)):
            if text:
                item['_full_text'] = text
                yield item

    def _download_and_extract_safe(self, item: Dict[str, Any]) -> Optional[str]:
        """Worker wrapper: one bad document must not kill the whole page."""
        try:
            return self._download_and_extract(item)
        except Exception as e:  # noqa: BLE001 - per-document isolation
            logger.warning(f"Extract failed for {item.get('caseNumber', '?')}: {e}")
            return None

    def fetch_all(self, limit: int = None, use_checkpoint: bool = True) -> Iterator[Dict[str, Any]]:
        """Fetch all Latvian court decisions with full text.

        Resume-safe: partitions by registration year-month and persists a
        checkpoint (completed months + in-progress page) so fleet
        relaunches skip finished months instead of re-walking from the top.
        Sample runs pass ``use_checkpoint=False`` so they don't move the
        fleet's resume pointer.
        """
        def _save(cm, cp):
            if use_checkpoint:
                self._save_checkpoint(completed, cm, cp)

        ckpt = self._load_checkpoint() if use_checkpoint else {
            "completed_months": set(), "current_month": None, "current_page": 1}
        completed = ckpt["completed_months"]
        if completed:
            logger.info(
                f"Resuming: {len(completed)} month(s) already complete, "
                f"in-progress month={ckpt['current_month']} page={ckpt['current_page']}"
            )

        count = 0
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            for month in self._iter_months():
                if month in completed:
                    continue

                start_page = ckpt["current_page"] if month == ckpt["current_month"] else 1
                page = start_page
                consecutive_failures = 0
                total_pages = None

                logger.info(f"Fetching month {month} (from page {page})...")
                while True:
                    data = self._search_decisions(page=page, year_month=month)

                    if not data:
                        consecutive_failures += 1
                        if consecutive_failures >= 5:
                            logger.error(
                                f"Too many consecutive search failures for {month} "
                                f"page {page}; leaving month in-progress and stopping"
                            )
                            _save(month, page)
                            return
                        time.sleep(10)
                        continue
                    consecutive_failures = 0

                    if total_pages is None:
                        total_pages = data.get('totalPages', 0)

                    items = data.get('items', [])
                    if not items:
                        break

                    for item in self._download_page(items, executor):
                        yield item
                        count += 1
                        if limit and count >= limit:
                            _save(month, page)
                            return

                    # Page fully processed — persist resume point.
                    page += 1
                    _save(month, page)

                    if total_pages and page > total_pages:
                        break
                    time.sleep(self.page_delay)

                # Month finished — mark complete and clear the in-progress pointer.
                completed.add(month)
                _save(None, 1)

        logger.info(
            f"Fetched {count} decisions total "
            f"({self._skipped} skipped — already in Neon, no PDF fetched)"
        )

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch decisions registered since a given date"""
        # Generate year-month filters from since to now
        now = datetime.now()
        months = []
        current = since.replace(day=1)
        while current <= now:
            months.append(current.strftime('%m.%Y'))
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)

        year_month_filter = ','.join(months)
        page = 1
        count = 0

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            while True:
                data = self._search_decisions(page=page, year_month=year_month_filter)
                if not data:
                    break

                items = data.get('items', [])
                if not items:
                    break

                for item in self._download_page(items, executor):
                    yield item
                    count += 1

                total_pages = data.get('totalPages', 0)
                if page >= total_pages:
                    break
                page += 1
                time.sleep(self.page_delay)

        logger.info(f"Fetched {count} updated decisions")

    def normalize(self, raw_item: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a decision to the standard schema"""
        ecli = raw_item.get('ecliCode', '')
        case_number = raw_item.get('caseNumber', '')
        doc_id = self._doc_id(raw_item)

        # Institution
        institution = raw_item.get('institution', {})
        court_name = institution.get('name', '')
        department = institution.get('departmentName', '')

        # Process type
        process_type = raw_item.get('processType', {})
        process_name = process_type.get('name', '')

        # Material type (judgment, ruling, etc.)
        material_type = raw_item.get('materialType', {})
        material_name = material_type.get('name', '')

        # Status
        material_status = raw_item.get('materialStatus', {})
        status_name = material_status.get('name', '')

        # Date
        reg_date = self._parse_timestamp(raw_item.get('registrationDate'))

        # Title: construct from material type + case number
        title = f"{material_name} - {case_number}" if material_name else case_number
        if court_name:
            title = f"{court_name}: {title}"

        # URL
        url = f"https://www.elieta.lv/web/"

        return {
            '_id': doc_id,
            '_source': 'LV/AllCourts',
            '_type': 'case_law',
            '_fetched_at': datetime.now().isoformat(),
            'title': title,
            'text': raw_item.get('_full_text', ''),
            'date': reg_date,
            'url': url,
            'language': 'lv',
            'ecli': ecli,
            'case_number': case_number,
            'court': court_name,
            'department': department,
            'process_type': process_name,
            'material_type': material_name,
            'status': status_name,
        }


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else ''
    # The fleet invokes `bootstrap-fast`; treat it as a full streaming run.
    if cmd in ('bootstrap', 'bootstrap-fast'):
        is_sample = '--sample' in sys.argv
        is_fast = '--fast' in sys.argv or cmd == 'bootstrap-fast'
        slow_mode = not is_fast and ('--slow' in sys.argv or os.environ.get('VPS_MODE') == '1')
        workers = None
        if '--workers' in sys.argv:
            idx = sys.argv.index('--workers')
            if idx + 1 < len(sys.argv):
                workers = int(sys.argv[idx + 1])
        # Sample runs must always produce fresh files, so they bypass the
        # skip-if-already-in-Neon guard (which would otherwise yield nothing
        # on a machine that has Neon credentials and a populated corpus).
        force = is_sample or '--force' in sys.argv
        fetcher = LatvianCourtsFetcher(slow_mode=slow_mode, workers=workers, force=force)

        if is_sample:
            # Write a small validation sample set to sample/.
            sample_dir = Path(__file__).parent / 'sample'
            sample_dir.mkdir(exist_ok=True)
            logger.info("Starting sample bootstrap...")

            sample_count = 0
            target_count = 15

            for raw_item in fetcher.fetch_all(limit=target_count + 10, use_checkpoint=False):
                if sample_count >= target_count:
                    break
                normalized = fetcher.normalize(raw_item)
                text_len = len(normalized.get('text', ''))
                if text_len < 100:
                    continue
                doc_id = str(normalized['_id']).replace('/', '_').replace(':', '-')
                filepath = sample_dir / f"{doc_id}.json"
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(normalized, f, indent=2, ensure_ascii=False)
                logger.info(f"Saved [{sample_count+1}/{target_count}]: {normalized.get('ecli', '')} ({text_len} chars)")
                sample_count += 1

            files = list(sample_dir.glob('*.json'))
            total_chars = sum(len(json.load(open(f, encoding='utf-8')).get('text', '')) for f in files)
            print("\n=== SUMMARY ===")
            print(f"Sample files: {len(files)}")
            print(f"Total text chars: {total_chars:,}")
            print(f"Average chars/doc: {total_chars // max(len(files), 1):,}")
        else:
            # Full corpus: stream normalized JSON lines to stdout so the fleet
            # captures the whole corpus to data/records.jsonl.  fetch_all is
            # checkpointed, so a relaunch resumes instead of re-walking.
            logger.info("Starting full bootstrap (streaming to stdout)...")
            count = 0
            for raw_item in fetcher.fetch_all():
                normalized = fetcher.normalize(raw_item)
                if len(normalized.get('text', '')) < 100:
                    continue
                print(json.dumps(normalized, ensure_ascii=False), flush=True)
                count += 1
                if count % 100 == 0:
                    logger.info(f"Streamed {count} records")
            logger.info(f"Bootstrap complete: {count} records streamed")

    elif len(sys.argv) > 1 and sys.argv[1] == 'updates':
        since_str = None
        for i, arg in enumerate(sys.argv):
            if arg == '--since' and i + 1 < len(sys.argv):
                since_str = sys.argv[i + 1]
        if not since_str:
            print("Usage: bootstrap.py updates --since YYYY-MM-DD")
            sys.exit(1)

        since = datetime.strptime(since_str, '%Y-%m-%d')
        fetcher = LatvianCourtsFetcher()
        for raw_item in fetcher.fetch_updates(since):
            normalized = fetcher.normalize(raw_item)
            print(f"{normalized['ecli']}: {normalized['title'][:60]} ({len(normalized.get('text', ''))} chars)")

    elif len(sys.argv) > 1 and sys.argv[1] == 'validate':
        sample_dir = Path(__file__).parent / 'sample'
        files = list(sample_dir.glob('*.json'))
        if not files:
            print("No sample files found. Run bootstrap --sample first.")
            sys.exit(1)

        print(f"Validating {len(files)} sample files...")
        issues = 0
        for f in files:
            with open(f, 'r', encoding='utf-8') as fp:
                data = json.load(fp)
            text = data.get('text', '')
            title = data.get('title', '')
            if not text or len(text) < 100:
                print(f"  FAIL: {f.name} — text too short ({len(text)} chars)")
                issues += 1
            if not title:
                print(f"  WARN: {f.name} — no title")
            if '<' in text and '>' in text and re.search(r'<[a-z]+[^>]*>', text):
                print(f"  WARN: {f.name} — possible HTML in text")
                issues += 1

        print(f"\nValidation: {len(files)} files, {issues} issues")
        sys.exit(1 if issues > 0 else 0)

    else:
        print("Usage:")
        print("  bootstrap.py bootstrap --sample   Fetch 15 sample decisions")
        print("  bootstrap.py bootstrap             Fetch 100 decisions")
        print("  bootstrap.py updates --since DATE  Fetch updates since DATE")
        print("  bootstrap.py validate              Validate sample data")


if __name__ == '__main__':
    main()
