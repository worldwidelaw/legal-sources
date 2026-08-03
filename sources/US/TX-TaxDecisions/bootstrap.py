#!/usr/bin/env python3
"""
US/TX-TaxDecisions -- Texas Comptroller of Public Accounts, STAR system
(State Tax Automated Research)

Fetches the FULL TEXT of the Texas Comptroller's published tax-research
documents from the STAR database (https://star.comptroller.texas.gov/):

  * Hearings (doc_type_code "H")     -> case_law  (redacted administrative
        adjudications of taxpayer protests; SOAH/Comptroller decisions)
  * Court Cases (doc_type_code "C")  -> case_law  (summaries/opinions of
        tax litigation the Comptroller was party to)
  * Rules (doc_type_code "R")        -> legislation (adopted Comptroller
        administrative rules with preamble, 34 TAC)
  * Letters "L", Memos "M",
    Publications "P", Web Content "W" -> doctrine (letter rulings, policy
        memos, tax publications, guidance webpages)

Access (no JavaScript needed, no CAPTCHA, no auth):
  The public STAR SPA is backed by a JSON API at
  https://api.comptroller.texas.gov/star/v1
    - GET /search    server-side search; params q, date_range, doc_type_code,
                     limit. The `start` offset param is NOT honored and large
                     `limit` values (>=10000) time out, so the corpus is
                     partitioned by YEAR via date_range=YYYY-01-01,YYYY-12-31
                     (each year returns only a few hundred rows, well within
                     limits). Response: data.found + data.documents[] with
                     acc_no, title, doc_type, doc_type_code, doc_date,
                     tax_type_short, subjects.
    - GET /view/{acc_no}  full document, data.contents holds the body as HTML.

Strategy:
  1. For each year from START_YEAR to the current year, GET /search with
     date_range for that year (limit LIMIT) and collect every row's acc_no
     + metadata (dedup on acc_no across years).
  2. For each acc_no, GET /view/{acc_no}; strip HTML from data.contents to
     plain text. A <MIN_TEXT_CHARS guard skips rows with no real body.
  3. Classify _type from doc_type_code.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import html
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.TX-TaxDecisions")

API = "https://api.comptroller.texas.gov/star/v1"
SEARCH_URL = f"{API}/search"
VIEW_URL = f"{API}/view/{{acc_no}}"

# The document corpus starts in the early 1950s for a handful of court cases,
# but the vast bulk of hearings/rulings begin in the mid-1980s. Scan from a
# safe lower bound so nothing is missed; empty years cost one cheap call each.
START_YEAR = 1950
LIMIT = 5000
MIN_TEXT_CHARS = 200

# doc_type_code -> normalized _type
CASE_LAW_CODES = {"H", "C"}          # Hearing, Court Case
LEGISLATION_CODES = {"R"}            # Rule
# everything else (L, M, P, W, N, ...) -> doctrine


def clean_text(raw: str) -> str:
    """Strip HTML tags/entities from a STAR contents blob to plain text."""
    if not raw:
        return ""
    text = raw
    # Preserve block structure: turn common block-closers into newlines.
    text = re.sub(r"(?i)</(p|div|tr|li|h[1-6])>", "\n", text)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</td>", " ", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _iso_date(value: str) -> str | None:
    if not value:
        return None
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", value)
    if not m:
        return None
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if 1900 <= y <= 2100 and 1 <= mo <= 12 and 1 <= d <= 31:
        return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


def _classify(doc_type_code: str) -> str:
    code = (doc_type_code or "").upper()
    if code in CASE_LAW_CODES:
        return "case_law"
    if code in LEGISLATION_CODES:
        return "legislation"
    return "doctrine"


class TXTaxDecisionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Origin": "https://star.comptroller.texas.gov",
                "Referer": "https://star.comptroller.texas.gov/",
            },
            timeout=90,
        )
        self.delay = 1.0
        # When True, fetch_all() draws from a single recent year so the base
        # bootstrap(sample_mode=True) loop reaches its sample_size quickly
        # instead of scanning empty early years first.
        self.sample_mode = False

    # ---- fetch helpers -------------------------------------------------

    def _search_year(self, year: int, retries: int = 4) -> list[dict]:
        params = {
            "q": "",
            "date_range": f"{year}-01-01,{year}-12-31",
            "limit": str(LIMIT),
            "sort": "doc_date asc",
        }
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(SEARCH_URL, params=params)
                if resp.status_code == 200:
                    payload = resp.json()
                    data = payload.get("data") or {}
                    docs = data.get("documents") or []
                    found = int(data.get("found") or 0)
                    if found > len(docs):
                        logger.warning(
                            f"Year {year}: found {found} but only {len(docs)} "
                            f"returned (limit {LIMIT}); some rows may be missed"
                        )
                    return docs
                logger.warning(f"HTTP {resp.status_code} for year {year}")
            except Exception as e:
                logger.warning(
                    f"Error searching year {year} (attempt {attempt + 1}): {e}"
                )
            if attempt < retries:
                time.sleep(2 ** attempt)
        return []

    def _get_view(self, acc_no: str, retries: int = 3) -> dict | None:
        url = VIEW_URL.format(acc_no=acc_no)
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    payload = resp.json()
                    data = payload.get("data")
                    if isinstance(data, dict):
                        return data
                    return None
                logger.warning(f"HTTP {resp.status_code} for view {acc_no}")
            except Exception as e:
                logger.warning(
                    f"Error fetching view {acc_no} (attempt {attempt + 1}): {e}"
                )
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- discovery -----------------------------------------------------

    def discover_documents(self) -> Generator[dict, None, None]:
        current_year = datetime.now(timezone.utc).year
        seen: set[str] = set()
        emitted = 0
        for year in range(START_YEAR, current_year + 1):
            rows = self._search_year(year)
            if not rows:
                continue
            for row in rows:
                acc_no = (row.get("acc_no") or "").strip()
                if not acc_no or acc_no in seen:
                    continue
                seen.add(acc_no)
                subjects = row.get("subjects") or []
                if isinstance(subjects, dict):
                    subjects = list(subjects.values())
                yield {
                    "acc_no": acc_no,
                    "title": (row.get("title") or "").strip(),
                    "doc_type": row.get("doc_type"),
                    "doc_type_code": row.get("doc_type_code"),
                    "tax_type": row.get("tax_type_short") or row.get("tax_type_long"),
                    "date": _iso_date(row.get("doc_date") or ""),
                    "subjects": [str(s) for s in subjects if s],
                    "url": f"https://star.comptroller.texas.gov/view/{acc_no}",
                }
                emitted += 1
        logger.info(f"Discovered {emitted} STAR documents")

    # ---- build ---------------------------------------------------------

    def _build_raw(self, doc: dict) -> dict | None:
        data = self._get_view(doc["acc_no"])
        if not data:
            return None
        text = clean_text(data.get("contents") or "")
        if len(text) < MIN_TEXT_CHARS:
            logger.info(
                f"Insufficient text ({len(text)} chars), skipping "
                f"{doc['acc_no']} [{doc.get('doc_type')}]"
            )
            return None
        doc = dict(doc)
        doc["text"] = text
        doc["_type"] = _classify(doc.get("doc_type_code"))
        # Prefer the authoritative date from the view payload if present.
        doc["date"] = _iso_date(data.get("doc_date") or "") or doc.get("date")
        return doc

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing Texas Comptroller STAR API...")
        try:
            rows = self._search_year(2024)
            if not rows:
                logger.error("  No documents discovered for 2024")
                return False
            logger.info(f"  Search OK — {len(rows)} documents in 2024")
            hearing = next(
                (r for r in rows if (r.get("doc_type_code") or "").upper() == "H"),
                rows[0],
            )
            raw = self._build_raw(
                {
                    "acc_no": hearing["acc_no"],
                    "title": hearing.get("title"),
                    "doc_type": hearing.get("doc_type"),
                    "doc_type_code": hearing.get("doc_type_code"),
                    "tax_type": hearing.get("tax_type_short"),
                    "date": _iso_date(hearing.get("doc_date") or ""),
                    "subjects": [],
                    "url": f"https://star.comptroller.texas.gov/view/{hearing['acc_no']}",
                }
            )
            if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                logger.info(
                    f"  Text extraction OK ({len(raw['text'])} chars) — "
                    f"{raw['acc_no']} [{raw['_type']}]"
                )
            else:
                logger.error("  Text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        acc_no = raw["acc_no"]
        title = (raw.get("title") or "").strip() or acc_no
        # STAR titles look like "202410024H [Tax Type: ...] [Document Type: ...]"
        title = f"Texas Comptroller STAR — {title}"[:400]
        return {
            "_id": f"US/TX-TaxDecisions/{acc_no}",
            "_source": "US/TX-TaxDecisions",
            "_type": raw.get("_type") or "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "acc_no": acc_no,
            "doc_type": raw.get("doc_type"),
            "tax_type": raw.get("tax_type"),
            "subjects": raw.get("subjects") or None,
            "issuer": "Texas Comptroller of Public Accounts",
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-TX",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        examined = 0
        for doc in self.discover_documents():
            examined += 1
            raw = self._build_raw(doc)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return
            if sample and examined >= 60:
                return

    def _iter_sample(self) -> Generator[dict, None, None]:
        """Sample from a single recent year to keep the sample crawl cheap
        while still covering multiple doc types (Hearing/Letters/Rule/etc.)."""
        rows = self._search_year(2024)
        emitted = 0
        examined = 0
        # Order so a couple of case_law hearings lead, then variety.
        rows_sorted = sorted(
            rows,
            key=lambda r: 0 if (r.get("doc_type_code") or "").upper() == "H" else 1,
        )
        for row in rows_sorted:
            acc_no = (row.get("acc_no") or "").strip()
            if not acc_no:
                continue
            subjects = row.get("subjects") or []
            if isinstance(subjects, dict):
                subjects = list(subjects.values())
            doc = {
                "acc_no": acc_no,
                "title": (row.get("title") or "").strip(),
                "doc_type": row.get("doc_type"),
                "doc_type_code": row.get("doc_type_code"),
                "tax_type": row.get("tax_type_short") or row.get("tax_type_long"),
                "date": _iso_date(row.get("doc_date") or ""),
                "subjects": [str(s) for s in subjects if s],
                "url": f"https://star.comptroller.texas.gov/view/{acc_no}",
            }
            examined += 1
            raw = self._build_raw(doc)
            if raw:
                yield raw
                emitted += 1
                if emitted >= 12:
                    return
            if examined >= 60:
                return

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        if self.sample_mode:
            yield from self._iter_sample()
            return
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_sample()

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/TX-TaxDecisions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = TXTaxDecisionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    scraper.sample_mode = bool(args.sample)
    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
