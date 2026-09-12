#!/usr/bin/env python3
"""
ES/Andalusia -- Andalusian Regional Legislation Data Fetcher

Fetches regional legislation from Andalusia via the BOJA content search API.

Strategy:
  - Query the official BOJA dataset search API (Elasticsearch-backed) one
    gazette issue at a time: q=data.t_year:{YYYY} AND data.t_number:{NNN}.
  - Each hit carries the full text in 'data.t_bodyNoHtml'.
  - Issue-level partitioning keeps every query well under the API's 10,000-hit
    window cap, so no year is ever silently truncated.

Why not the bulk per-year dump?
  https://datos.juntadeandalucia.es/api/v0/boja/all?year={YYYY}&format=json
  redirects to .../festa/download-pro/dataset-boja_{YYYY}.json, which only
  exists for 2018+ (2017 and earlier return 404) and regularly times out or
  drops the connection on the 20-30 MB payloads. See issue #1482.

Endpoints:
  - Search API: https://www.juntadeandalucia.es/ssdigitales/datasets/contentapi/search/boja.json
      ?q=data.t_year:{YYYY} AND data.t_number:{NNN}&_source=data.*&size=50&from={N}
    (size is capped at 50 by the server; larger values return zero hits.)

Data:
  - Legislation types: Ordenes, Decretos, Resoluciones, Leyes, etc.
  - Coverage 1980-present, ~2,000-8,000 dispositions per year
  - License: CC BY 4.0
  - Language: Spanish (es)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional, Tuple

try:
    import requests
except ImportError:
    print("ERROR: requests is required. Install with: pip install requests")
    sys.exit(1)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ES.andalusia")

# API URLs
API_BASE = (
    "https://www.juntadeandalucia.es/ssdigitales/datasets/contentapi/search/boja.json"
)

# Year range - the BOJA dataset index starts in 1980
START_YEAR = 1980

# Server-side hard cap: size=51 or higher returns zero hits.
PAGE_SIZE = 50

# Gazette issue numbers are contiguous within a year, but a year may start at
# number > 1 (1980 starts at 5) and the odd number is unused. Stop scanning a
# year after this many consecutive empty issue numbers.
MISS_TOLERANCE = 12
MAX_ISSUE_NUMBER = 400

# Issues are probed in parallel chunks; results are re-ordered before yielding.
PROBE_CHUNK = 12
PROBE_WORKERS = 6

# HTTP retry policy for the search API.
MAX_ATTEMPTS = 4
BACKOFF_SECONDS = 3

# Maps the search API's prefixed field names onto the field names the rest of
# this scraper (and the previously ingested corpus) expects.
FIELD_MAP = {
    "x_id": "id",
    "t_bodyNoHtml": "bodyNoHtml",
    "t_body": "body",
    "t_asumarioNoHtml": "summaryNoHtml",
    "t_asumario": "summary",
    "d_dateUTC": "dateUTC",
    "d_dateDispositionUTC": "dateDispositionUTC",
    "t_year": "year",
    "t_number": "number",
    "t_dispositionNumber": "dispositionNumber",
    "t_typeDisposition": "type",
    "t_organisation": "organisation",
    "t_titleSec": "titleSec",
    "t_subtitle": "sectionN2",
    "b_hasPdf": "hasPdf",
    "t_version": "version",
    "t_lawDisposition": "lawDisposition",
}


class AndalusiaScraper(BaseScraper):
    """
    Scraper for ES/Andalusia -- Andalusian Regional Legislation.
    Country: ES
    URL: https://datos.juntadeandalucia.es

    Data types: legislation
    Auth: none (CC BY 4.0)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self._local = threading.local()
        self._rate_lock = threading.Lock()

        # Partition -> error message for every issue we could not read after
        # exhausting retries. Surfaced loudly at the end of a run so a
        # coverage hole can never pass as a clean exit (issue #1482).
        self._fetch_failures: Dict[str, str] = {}
        self._failures_lock = threading.Lock()

    @property
    def session(self) -> requests.Session:
        """Per-thread session (issue probing runs on a small thread pool)."""
        sess = getattr(self._local, "session", None)
        if sess is None:
            sess = requests.Session()
            sess.headers.update({
                "User-Agent": "Legal-Data-Hunter/1.0 (Open Data Research)",
                "Accept": "application/json",
            })
            self._local.session = sess
        return sess

    def _api_get(self, params: dict) -> dict:
        """
        GET the search API with retries.

        Raises the last exception if every attempt fails -- callers record the
        failure rather than swallowing it.
        """
        last_exc = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                with self._rate_lock:
                    self.rate_limiter.wait()
                resp = self.session.get(API_BASE, params=params, timeout=90)
                resp.raise_for_status()
                # The endpoint serves JSON with leading whitespace/tabs and
                # occasionally text/html content-type, so parse the body text.
                return json.loads(resp.text.strip())
            except Exception as e:
                last_exc = e
                if attempt < MAX_ATTEMPTS:
                    delay = BACKOFF_SECONDS * attempt
                    logger.warning(
                        f"Search API attempt {attempt}/{MAX_ATTEMPTS} failed "
                        f"({e}); retrying in {delay}s"
                    )
                    time.sleep(delay)
        raise last_exc

    @staticmethod
    def _map_fields(data: dict) -> dict:
        """Rename the search API's prefixed keys to the canonical names."""
        record = {FIELD_MAP.get(k, k): v for k, v in data.items()}
        # sectionN1 is what normalize() looks at first.
        if "titleSec" in record:
            record.setdefault("sectionN1", record["titleSec"])
        return record

    def _record_failure(self, key: str, error: Exception) -> None:
        with self._failures_lock:
            self._fetch_failures[key] = f"{type(error).__name__}: {error}"
        logger.error(f"Fetch FAILED for {key}: {error}")

    def _fetch_issue(self, year: int, number: int) -> Tuple[List[dict], bool]:
        """
        Fetch every disposition in one BOJA issue.

        Returns (records, ok). ok is False when the issue could not be read,
        which is deliberately distinct from an issue that genuinely has no
        dispositions -- only the former is a coverage hole.
        """
        params_base = {
            "q": f"data.t_year:{year} AND data.t_number:{number}",
            "_source": "data.*",
            "size": PAGE_SIZE,
        }
        records: List[dict] = []
        offset = 0
        total = None

        while True:
            params = dict(params_base, **{"from": offset})
            try:
                payload = self._api_get(params)
            except Exception as e:
                self._record_failure(f"{year}/issue {number}@{offset}", e)
                return records, False

            if total is None:
                total = int(payload.get("numResultados") or 0)
                if total == 0:
                    return [], True

            hits = payload.get("resultado") or []
            if not hits:
                break

            for hit in hits:
                data = (hit.get("_source") or {}).get("data")
                if not data:
                    continue
                record = self._map_fields(data)
                record.setdefault(
                    "id", str(hit.get("_id", "")).replace("disposiciones_boja-", "")
                )
                records.append(record)

            offset += PAGE_SIZE
            if offset >= total or offset >= 10000:
                break

        if total and len(records) < total:
            logger.warning(
                f"{year} issue {number}: read {len(records)} of {total} hits"
            )
        return records, True

    def _clean_html(self, text: str) -> str:
        """Strip HTML tags and clean whitespace."""
        if not text:
            return ""
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)
        # Decode HTML entities
        text = html.unescape(text)
        # Clean whitespace
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _fetch_year(self, year: int) -> Generator[dict, None, None]:
        """
        Yield every record for a given year by walking its gazette issues.

        Issue numbers are contiguous, so the scan advances in parallel chunks
        and stops after MISS_TOLERANCE consecutive empty issues. Streaming
        (rather than returning a list) keeps sample runs short and keeps a
        full bootstrap from holding a whole year in memory.
        """
        logger.info(f"Fetching BOJA records for year {year}...")
        found = 0
        consecutive_misses = 0
        number = 1

        with ThreadPoolExecutor(max_workers=PROBE_WORKERS) as pool:
            while number <= MAX_ISSUE_NUMBER and consecutive_misses < MISS_TOLERANCE:
                chunk = range(number, min(number + PROBE_CHUNK, MAX_ISSUE_NUMBER + 1))
                results = list(pool.map(lambda n: self._fetch_issue(year, n), chunk))

                for issue_records, ok in results:
                    if issue_records:
                        consecutive_misses = 0
                        found += len(issue_records)
                        yield from issue_records
                    elif ok:
                        # Genuinely empty issue number -- counts toward the gap.
                        consecutive_misses += 1
                    else:
                        # Unreadable issue: already recorded as a failure. Do not
                        # let it look like the end of the year.
                        consecutive_misses = 0

                number += PROBE_CHUNK

        logger.info(f"Found {found} records for {year}")

    def _prepare(self, record: dict) -> Optional[dict]:
        """Attach cleaned full text, or return None if the record is unusable."""
        if not record.get("id"):
            return None

        body = record.get("bodyNoHtml", "") or record.get("body", "")
        if not body:
            logger.debug(f"No body for {record.get('id')}")
            return None

        if "<" in body and ">" in body:
            body = self._clean_html(body)

        if len(body) < 100:
            logger.debug(f"Text too short for {record.get('id')} ({len(body)} chars)")
            return None

        record["full_text"] = body
        return record

    def _report_fetch_failures(self, years_with_no_records: List[int]) -> None:
        """
        Fail loudly. A swallowed per-year error previously let a five-year hole
        exit 0 with "0 errors" (issue #1482).
        """
        if not self._fetch_failures and not years_with_no_records:
            return

        if self._fetch_failures:
            logger.error(
                f"FETCH FAILURES: {len(self._fetch_failures)} BOJA issue(s) could "
                f"not be read after {MAX_ATTEMPTS} attempts each"
            )
            for key, err in sorted(self._fetch_failures.items())[:50]:
                logger.error(f"  {key}: {err}")

            failures_path = self.source_dir / "data" / "fetch_failures.json"
            try:
                failures_path.parent.mkdir(parents=True, exist_ok=True)
                failures_path.write_text(
                    json.dumps(self._fetch_failures, indent=2, ensure_ascii=False),
                    encoding="utf-8",
                )
            except OSError as e:
                logger.warning(f"Could not write {failures_path}: {e}")

        if years_with_no_records:
            years = ", ".join(str(y) for y in sorted(years_with_no_records))
            raise RuntimeError(
                f"BOJA coverage hole: year(s) {years} returned 0 records while "
                f"{len(self._fetch_failures)} issue fetch(es) failed. "
                "Corpus is incomplete -- re-run these years."
            )

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all regional legislation from Andalusia (1980-present).
        """
        current_year = datetime.now().year
        empty_years: List[int] = []

        for year in range(current_year, START_YEAR - 1, -1):
            failures_before = len(self._fetch_failures)
            yielded = 0

            for record in self._fetch_year(year):
                prepared = self._prepare(record)
                if prepared is not None:
                    yielded += 1
                    yield prepared

            if yielded == 0 and len(self._fetch_failures) > failures_before:
                empty_years.append(year)

        self._report_fetch_failures(empty_years)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents updated since the given date.

        Since the API only supports year-based queries, we fetch recent years
        and filter by date.
        """
        current_year = datetime.now().year
        since_year = since.year

        for year in range(current_year, since_year - 1, -1):
            for record in self._fetch_year(year):
                # Parse date from record
                date_str = record.get("dateUTC", "") or record.get("dateDispositionUTC", "")
                if date_str:
                    try:
                        doc_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                        if doc_date < since.replace(tzinfo=timezone.utc):
                            continue
                    except (ValueError, TypeError):
                        pass

                prepared = self._prepare(record)
                if prepared is not None:
                    yield prepared

        if self._fetch_failures:
            self._report_fetch_failures([])

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        doc_id = str(raw.get("id", ""))
        summary = raw.get("summaryNoHtml", "") or raw.get("summary", "")
        if summary and "<" in summary:
            summary = self._clean_html(summary)

        full_text = raw.get("full_text", "")

        # Parse dates
        date_disp = raw.get("dateDispositionUTC", "")
        date_pub = raw.get("dateUTC", "")
        if date_disp:
            date_disp = date_disp[:10]
        if date_pub:
            date_pub = date_pub[:10]

        # Get gazette info
        year = raw.get("year", "")
        number = raw.get("number", "")
        disp_number = raw.get("dispositionNumber", "")

        # Document type
        doc_type = raw.get("type", "")  # e.g., "Órdenes", "Decretos"

        # Organization
        org = raw.get("organisation", "") or ""
        if org and "<" in org:
            org = self._clean_html(org)

        # Section info
        section = raw.get("sectionN1", "") or raw.get("titleSec", "")

        # PDF URL
        pdf_url = ""
        if raw.get("hasPdf") and str(number).isdigit():
            # Construct PDF URL from the BOJA pattern
            pdf_url = (
                f"https://www.juntadeandalucia.es/boja/{year}/"
                f"{int(number):03d}/d{disp_number}.pdf"
            )

        # Internal version
        version = raw.get("version", "")

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "ES/Andalusia",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": summary,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_disp or date_pub,
            "url": f"https://www.juntadeandalucia.es/eboja/buscador/disposition/{doc_id}" if doc_id else "",
            # Additional metadata
            "document_type": doc_type,
            "year": year,
            "gazette_number": number,
            "disposition_number": disp_number,
            "publication_date": date_pub,
            "organization": org,
            "section": section,
            "has_pdf": raw.get("hasPdf", False),
            "pdf_url": pdf_url,
            "version": version,
            "language": "es",
            "region": "Andalucía",
            "country": "ES",
        }

    def test_connection(self):
        """Quick connectivity test across the full year range."""
        print("Testing Andalusia BOJA search API...")

        current_year = datetime.now().year

        print(f"\n1. Fetching one issue from {current_year}...")
        records, ok = self._fetch_issue(current_year, 10)
        print(f"   ok={ok}, records={len(records)}")
        if records:
            r = records[0]
            print(f"   ID: {r.get('id', 'N/A')}")
            print(f"   Summary: {(r.get('summaryNoHtml') or '')[:70]}...")
            print(f"   Type: {r.get('type', 'N/A')}")
            print(f"   Date: {r.get('dateUTC', 'N/A')}")
            body = r.get("bodyNoHtml", "") or ""
            print(f"   Body length: {len(body)} characters")
            print(f"   Body sample: {self._clean_html(body)[:150]}...")

        # The years the old bulk-download path could not reach (issue #1482).
        print("\n2. Probing years the bulk dataset could not serve...")
        for year in (1980, 2000, 2015, 2016, 2017, 2018, 2019):
            recs, ok = self._fetch_issue(year, 10)
            with_text = sum(1 for r in recs if r.get("bodyNoHtml"))
            print(
                f"   {year} issue 10: ok={ok}, {len(recs)} records, "
                f"{with_text} with full text"
            )

        print(f"\n3. Year range: {START_YEAR}-{current_year}")
        if self._fetch_failures:
            print(f"   FAILURES: {self._fetch_failures}")
        print("\nTest complete!")


def main():
    scraper = AndalusiaScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command in ("bootstrap-fast", "bootstrap_fast"):
        stats = scraper.bootstrap_fast()
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
