#!/usr/bin/env python3
"""
US/FERC-eLibrary -- FERC eLibrary Commission issuances (orders & decisions)

Fetches the full text of adjudicatory issuances from the Federal Energy
Regulatory Commission's eLibrary — the Commission's docket/issuance
repository. Targets the decisional issuances that resolve specific
proceedings (rate filings, hydropower/gas certificates, complaints,
enforcement, rulemaking dockets): Commission Orders/Opinions, Delegated
(letter) Orders, Procedural Orders, ALJ Initial Decisions, Compliance
Directives, Protective Orders and rehearing orders = case_law. FERC
issuances are official U.S. Government works in the public domain
(17 U.S.C. § 105 / government edicts).

This is DISTINCT from the existing US/FERC source, which only ingests
FERC rulemakings/notices published in the Federal Register. This source
targets the full eLibrary issuance corpus (born-digital, ~post-2000,
text-layer PDF + DOCX).

Access (public JSON API behind the eLibrary Angular SPA, no auth):
  Search:
    POST https://elibrary.ferc.gov/eLibrarywebapi/api/Search/AdvancedSearch
    JSON body with dateSearches / categories / resultsPerPage / curPage.
    Returns searchHits[] each carrying accessionNumber, docketNumbers,
    description, issuedDate, classTypes (documentClass/documentType), and
    transmittals[] (fileId, fileType, fileName, fileSize).
  File download (raw bytes of a transmittal file):
    POST https://elibrary.ferc.gov/eLibrarywebapi/api/File/DownloadP8File
    JSON body: {FileType, accession, fileid:0, FileIDAll:<fileId>,
                fileidLst:[<fileId>], Islegacy:false}

Strategy:
  1. Page the AdvancedSearch endpoint month-by-month (newest first) over
     category "Issuance", from the current month back to FIRST_YEAR.
  2. Keep only decisional issuances (documentType/documentClass matching
     order / opinion / decision / directive / ruling / rehearing).
  3. For each accession, download every text-bearing transmittal
     (PDF via common.pdf_extract, DOCX/TXT via a pure-python extractor)
     and concatenate. A <200-char guard skips scanned/empty issuances.

Usage:
  python bootstrap.py bootstrap            # Full pull (present -> FIRST_YEAR)
  python bootstrap.py bootstrap --sample   # ~12 recent samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
"""

from __future__ import annotations

import sys
import io
import json
import html
import logging
import re
import time
import zipfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common import pdf_extract

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.FERC-eLibrary")

API = "https://elibrary.ferc.gov/eLibrarywebapi/api"
SEARCH_URL = API + "/Search/AdvancedSearch"
DOWNLOAD_URL = API + "/File/DownloadP8File"
DOCINFO_URL = "https://elibrary.ferc.gov/eLibrary/docinfo?accession_Number={acc}"
SPA_URL = "https://elibrary.ferc.gov/eLibrary/search"

FIRST_YEAR = 2000  # born-digital era; older issuances are scanned images
RESULTS_PER_PAGE = 250

# Keep only decisional / adjudicatory issuances. Matched against the
# documentType and documentClass strings in classTypes[].
DECISION_RE = re.compile(
    r"order|opinion|decision|directive|ruling|rehearing", re.I
)

# Transmittal file types we can extract text from.
PDF_TYPES = {"PDF"}
DOCX_TYPES = {"DOCX"}
TXT_TYPES = {"TXT"}

MONTH_DAYS = {1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30,
              7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}

# --- stall bounds (issue #1546) ------------------------------------------
# A single logical request may never burn more than this. Without it the
# 5 attempts x (SPA re-warm + exponential backoff) of a 401 loop cost
# minutes per transmittal, which is what flatlined the fleet worker at
# 22,400 records for 1h42m without ever raising.
POST_DEADLINE = 150.0
# Re-warming clears the cookie jar and reloads the SPA. Doing that on every
# 401 attempt of every document is thousands of pointless SPA loads under a
# sustained block, so it is throttled to once per interval run-wide.
WARM_MIN_INTERVAL = 60.0
WARM_TIMEOUT = 30
# Consecutive DownloadP8File failures, with no success in between, that mean
# the endpoint is down rather than one file being unavailable. At ~1 doc/s
# this trips in well under the 1h42m the stalled worker wasted.
DOWNLOAD_FAIL_STREAK_MAX = 40


class FERCUnreachable(RuntimeError):
    """Raised when the eLibrary API refuses this vantage entirely.

    elibrary.ferc.gov sits behind Cloudflare bot management; a datacenter IP
    can be served a challenge/403 for every request. Failing loud here keeps
    that from looking like an empty corpus (issue #1248).
    """


def _docx_text(data: bytes) -> str | None:
    """Pure-python DOCX text extraction (no python-docx dependency)."""
    try:
        z = zipfile.ZipFile(io.BytesIO(data))
    except Exception:
        return None
    try:
        xml = z.read("word/document.xml").decode("utf-8", "replace")
    except KeyError:
        return None
    xml = re.sub(r"</w:p>", "\n", xml)
    xml = re.sub(r"<w:tab[^>]*/>", "\t", xml)
    xml = re.sub(r"<w:br[^>]*/>", "\n", xml)
    txt = re.sub(r"<[^>]+>", "", xml)
    txt = html.unescape(txt)
    txt = txt.replace("﻿", "")
    txt = re.sub(r"[ \t]+\n", "\n", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)
    return txt.strip() or None


class FERCeLibraryScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self.session = requests.Session()
        self.session.headers.update({
            "content-type": "application/json",
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9",
            "origin": "https://elibrary.ferc.gov",
            "referer": SPA_URL,
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
            ),
        })
        self._warmed = False
        self._last_warm = 0.0
        self._any_search_ok = False
        # Download health, for the circuit breaker and the honest tally of
        # why a discovered document produced no record (#1546).
        self._dl_ok = 0
        self._dl_failed = 0
        self._dl_fail_streak = 0
        self._dropped_download = 0
        self._dropped_no_text = 0
        self.checkpoint_path = Path(self.source_dir) / "data" / "months_done.json"
        self._done_months, self._partial = self._load_checkpoint()
        self._partial_month = None
        self._since_save = 0

    # ---------------------------------------------------------- checkpoint
    def _load_checkpoint(self) -> tuple[set, dict]:
        """Return (completed months, {month: set of accessions already done}).

        The accession half is what lets a re-launch resume *inside* a month.
        A month is only marked done after its last hit, so before #1546 a
        worker that stalled 22,400 records into a month replayed all of it.
        """
        try:
            with open(self.checkpoint_path) as fh:
                d = json.load(fh)
            partial = {k: set(v) for k, v in (d.get("partial") or {}).items()}
            return set(d.get("months_done") or []), partial
        except Exception:
            return set(), {}

    def _save_checkpoint(self):
        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.checkpoint_path.with_suffix(".tmp")
        with open(tmp, "w") as fh:
            json.dump({
                "months_done": sorted(self._done_months),
                # Only the in-flight month is kept, so the file stays small;
                # finished months are covered by months_done.
                "partial": {k: sorted(v) for k, v in self._partial.items()},
            }, fh)
        tmp.replace(self.checkpoint_path)
        self._since_save = 0

    # ---------------------------------------------------------------- http
    def _warm(self, force: bool = False):
        """Load the SPA so Cloudflare/BIG-IP hand us their cookies.

        A bare POST with no __cf_bm/TS cookie is what bot management scores
        hardest, so this runs before the first API call and again after a
        401/403 — but at most once per WARM_MIN_INTERVAL run-wide. Re-warming
        per attempt per document (the pre-#1546 behaviour) meant a sustained
        401 spent nearly all its wall clock reloading the SPA.
        """
        now = time.monotonic()
        if not force and self._warmed and (now - self._last_warm) < WARM_MIN_INTERVAL:
            return
        self.session.cookies.clear()
        try:
            self.session.get(
                SPA_URL,
                headers={"accept": "text/html,application/xhtml+xml"},
                timeout=WARM_TIMEOUT,
            )
        except Exception as e:
            logger.warning(f"  cookie warm-up failed: {e}")
        self._warmed = True
        self._last_warm = time.monotonic()

    def _post(self, url: str, *, json_body=None, data=None):
        """POST with bounded retries AND a bounded wall clock.

        Returns the response, or None once the attempts or POST_DEADLINE are
        spent. The deadline is what keeps a degraded endpoint from turning
        each call into minutes of backoff (#1546).
        """
        if not self._warmed:
            self._warm(force=True)
        deadline = time.monotonic() + POST_DEADLINE
        for attempt in range(5):
            if time.monotonic() >= deadline:
                logger.warning(f"  giving up on {url} after "
                               f"{POST_DEADLINE:.0f}s (attempt {attempt + 1})")
                break
            time.sleep(self.delay)
            try:
                r = self.session.post(
                    url, json=json_body, data=data, timeout=120
                )
                if r.status_code == 200:
                    return r
                if r.status_code in (401, 403):
                    logger.warning(f"  {r.status_code} from {url} "
                                   f"(attempt {attempt + 1}); re-warming cookies")
                    self._warm()
                elif r.status_code in (429, 500, 502, 503, 504):
                    logger.warning(f"  {r.status_code} from {url} "
                                   f"(attempt {attempt + 1}); backing off")
                else:
                    return r
            except Exception as e:
                logger.warning(f"  request error {url} "
                               f"(attempt {attempt + 1}): {e}")
            # Sleep only as far as the deadline allows.
            time.sleep(min(2 ** attempt, max(0.0, deadline - time.monotonic())))
        return None

    # --------------------------------------------------------- search
    def _search(self, start: str, end: str, page: int) -> dict | None:
        body = {
            "searchText": "*",
            "searchFullText": False,
            "searchDescription": True,
            "dateSearches": [
                {"dateType": "issued_date", "startDate": start, "endDate": end}
            ],
            "availability": None,
            "affiliations": [],
            "categories": ["Issuance"],
            "libraries": [],
            "accessionNumber": None,
            "eFiling": False,
            "docketSearches": [],
            "resultsPerPage": RESULTS_PER_PAGE,
            "curPage": page,
            "classTypes": [],
            "sortBy": "",
            "groupBy": "NONE",
            "idolResultID": "",
            "allDates": False,
        }
        r = self._post(SEARCH_URL, json_body=body)
        if r is None:
            raise FERCUnreachable(
                f"eLibrary AdvancedSearch unreachable for {start}..{end} after "
                f"5 attempts — {SEARCH_URL} is refusing this vantage "
                f"(Cloudflare bot management blocks datacenter IPs); "
                f"needs a US residential vantage/proxy"
            )
        if r.status_code != 200:
            raise FERCUnreachable(
                f"eLibrary AdvancedSearch returned HTTP {r.status_code} for "
                f"{start}..{end} — {SEARCH_URL} is refusing this vantage; "
                f"needs a US residential vantage/proxy"
            )
        try:
            return r.json()
        except Exception:
            raise FERCUnreachable(
                f"eLibrary AdvancedSearch returned non-JSON for {start}..{end} "
                f"({len(r.content)} bytes, content-type "
                f"{r.headers.get('content-type')!r}) — most likely a Cloudflare "
                f"challenge page rather than the API; needs a US residential "
                f"vantage/proxy"
            )

    @staticmethod
    def _is_decision(hit: dict) -> bool:
        for ct in hit.get("classTypes") or []:
            if DECISION_RE.search(ct.get("documentType") or ""):
                return True
            if DECISION_RE.search(ct.get("documentClass") or ""):
                return True
        return False

    @staticmethod
    def _doc_type(hit: dict) -> str | None:
        cts = hit.get("classTypes") or []
        if cts:
            return cts[0].get("documentType") or cts[0].get("documentClass")
        return None

    def _month_hits(self, year: int, month: int) -> list[dict]:
        last = MONTH_DAYS[month]
        if month == 2 and (year % 4 != 0 or (year % 100 == 0 and year % 400 != 0)):
            last = 28
        start = f"{year:04d}-{month:02d}-01"
        end = f"{year:04d}-{month:02d}-{last:02d}"
        out: list[dict] = []
        page = 1
        total = None
        while True:
            d = self._search(start, end, page)
            self._any_search_ok = True
            if not d:
                break
            if total is None:
                total = d.get("totalHits", 0)
            hits = d.get("searchHits") or []
            if not hits:
                break
            out.extend(hits)
            if len(out) >= (total or 0) or len(hits) < RESULTS_PER_PAGE:
                break
            page += 1
            if page > 200:  # safety
                break
        decisions = [h for h in out if self._is_decision(h)]
        logger.info(f"  {start}..{end}: {len(out)} issuances, "
                    f"{len(decisions)} decisions")
        return decisions

    # --------------------------------------------------------- discovery
    def _iter_months(self, sample: bool):
        now = datetime.now(timezone.utc)
        year, month = now.year, now.month
        while year >= FIRST_YEAR:
            yield year, month
            month -= 1
            if month == 0:
                month = 12
                year -= 1

    # ------------------------------------------------------- build record
    def _download_file(self, acc: str, tx: dict) -> bytes | None:
        fid = tx.get("fileId")
        if not fid:
            return None
        body = {
            "FileType": tx.get("fileType") or "",
            "accession": acc,
            "fileid": 0,
            "FileIDAll": fid,
            "fileidLst": [fid],
            "Islegacy": False,
        }
        r = self._post(DOWNLOAD_URL, json_body=body)
        if not r or r.status_code != 200:
            self._dl_fail_streak += 1
            self._dl_failed += 1
            if self._dl_fail_streak >= DOWNLOAD_FAIL_STREAK_MAX:
                raise FERCUnreachable(
                    f"{DOWNLOAD_URL} failed {self._dl_fail_streak} times in a "
                    f"row with no success in between (last accession {acc}, "
                    f"status {getattr(r, 'status_code', 'no response')}). The "
                    f"file-download endpoint is refusing this vantage — it "
                    f"oscillates 401 (cookie re-warm) / 500 under sustained "
                    f"load from a datacenter IP. Search still works, so every "
                    f"document would be discovered and then silently dropped "
                    f"for want of text; failing loud instead (#1546). Needs a "
                    f"US residential vantage/proxy, or a re-launch later — the "
                    f"accession checkpoint means it resumes where it stopped."
                )
            return None
        self._dl_fail_streak = 0
        self._dl_ok += 1
        return r.content

    def _extract_transmittal(self, acc: str, tx: dict) -> str | None:
        ftype = (tx.get("fileType") or "").upper()
        if ftype not in PDF_TYPES | DOCX_TYPES | TXT_TYPES:
            return None
        blob = self._download_file(acc, tx)
        if not blob:
            return None
        if ftype in PDF_TYPES:
            if blob[:4] != b"%PDF":
                return None
            return pdf_extract.extract_pdf_markdown(
                "US/FERC-eLibrary", f"{acc}:{tx.get('fileId')}",
                pdf_bytes=blob, table="case_law", force=True,
            )
        if ftype in DOCX_TYPES:
            if blob[:2] != b"PK":
                return None
            return _docx_text(blob)
        if ftype in TXT_TYPES:
            try:
                return blob.decode("utf-8", "replace").strip() or None
            except Exception:
                return None
        return None

    def _build_raw(self, hit: dict) -> dict | None:
        acc = hit.get("acesssionNumber") or hit.get("accessionNumber")
        if not acc:
            return None
        texts: list[str] = []
        download_failed = False
        for tx in hit.get("transmittals") or []:
            before = self._dl_failed
            try:
                t = self._extract_transmittal(acc, tx)
            except FERCUnreachable:
                raise
            except Exception as e:
                logger.warning(f"  extract failed {acc}: {e}")
                t = None
            if self._dl_failed > before:
                download_failed = True
            if t and t.strip():
                texts.append(t.strip())
        text = "\n\n".join(texts).strip()
        if len(text) < 200:
            # A document with no text is either genuinely scanned/empty or a
            # download that failed. Pre-#1546 both returned None identically,
            # so a broken DownloadP8File read as "this corpus has no text".
            if download_failed:
                self._dropped_download += 1
            else:
                self._dropped_no_text += 1
            return None
        return {
            "accession": acc,
            "text": text,
            "docket_numbers": hit.get("docketNumbers") or [],
            "description": (hit.get("description") or "").strip(),
            "document_type": self._doc_type(hit),
            "issued_date": hit.get("issuedDate"),
            "filed_date": hit.get("filedDate"),
        }

    # --------------------------------------------------------- normalize
    @staticmethod
    def _iso_date(mdy: str | None) -> str | None:
        if not mdy:
            return None
        m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", mdy.strip())
        if not m:
            return None
        mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if not (1 <= mo <= 12 and 1 <= d <= 31 and 1900 <= y <= 2035):
            return None
        return f"{y:04d}-{mo:02d}-{d:02d}"

    def normalize(self, raw: dict) -> dict:
        acc = raw["accession"]
        dockets = raw.get("docket_numbers") or []
        dt = raw.get("document_type") or "Issuance"
        desc = raw.get("description") or ""
        docket_str = ", ".join(dockets[:6])
        title = f"FERC {dt}"
        if docket_str:
            title = f"{title} — Docket {docket_str}"
        if desc:
            title = f"{title}: {desc}"
        title = re.sub(r"\s+", " ", title).strip()[:300]
        return {
            "_id": f"US/FERC-eLibrary/{acc}",
            "_source": "US/FERC-eLibrary",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "accession_number": acc,
            "docket_numbers": dockets,
            "document_type": dt,
            "description": desc or None,
            "issuer": "Federal Energy Regulatory Commission",
            "title": title,
            "text": raw["text"],
            "url": DOCINFO_URL.format(acc=acc),
            "date": self._iso_date(raw.get("issued_date")),
            "jurisdiction": "US",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False,
                  checkpoint: bool = True) -> Generator[dict, None, None]:
        emitted = 0
        # Sample runs stop after a handful of records and incremental runs
        # re-walk recent months, so neither reads or writes the checkpoint.
        checkpoint = checkpoint and not sample
        if checkpoint and self._done_months:
            logger.info(f"Resuming: {len(self._done_months)} months already "
                        f"completed, skipping them with no network calls")
        for year, month in self._iter_months(sample):
            key = f"{year:04d}-{month:02d}"
            if checkpoint and key in self._done_months:
                continue
            try:
                hits = self._month_hits(year, month)
            except FERCUnreachable:
                # Nothing has ever come back on this run: the vantage itself is
                # refused, so fail loud instead of reporting an empty corpus.
                if not self._any_search_ok:
                    raise
                # Otherwise treat it as transient — leave the month unmarked so
                # the next run retries it, and keep going.
                logger.error(f"  {key}: search failed, will retry next run")
                continue
            seen = self._partial.get(key, set()) if checkpoint else set()
            if seen:
                logger.info(f"  {key}: resuming, {len(seen)} of {len(hits)} "
                            f"accessions already done")
            for hit in hits:
                acc = hit.get("acesssionNumber") or hit.get("accessionNumber")
                if checkpoint and acc and acc in seen:
                    continue
                try:
                    raw = self._build_raw(hit)
                except FERCUnreachable:
                    # The download endpoint has gone down mid-month. Persist
                    # what this run got through so the re-launch resumes here
                    # instead of replaying the month, then fail loud.
                    if checkpoint:
                        self._save_checkpoint()
                        logger.error(
                            f"  {key}: checkpointed {len(seen)} accessions "
                            f"before failing"
                        )
                    raise
                if raw:
                    yield raw
                    emitted += 1
                    if sample and emitted >= 12:
                        return
                if checkpoint and acc:
                    # Recorded whether or not it produced a record: a hit with
                    # no extractable text is done too, and replaying it costs
                    # the same downloads.
                    seen.add(acc)
                    self._partial[key] = seen
                    self._since_save += 1
                    if self._since_save >= 25:
                        self._save_checkpoint()
            if checkpoint:
                self._done_months.add(key)
                self._partial.pop(key, None)
                self._save_checkpoint()
                logger.info(
                    f"  {key} complete — downloads {self._dl_ok} ok / "
                    f"{self._dl_failed} failed; dropped "
                    f"{self._dropped_no_text} no-text, "
                    f"{self._dropped_download} download-failed"
                )

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self._iter_raw(sample=False, checkpoint=False):
            iso = self._iso_date(raw.get("issued_date"))
            if not since or (iso and iso >= since):
                yield raw

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing FERC eLibrary AdvancedSearch + extraction...")
        try:
            now = datetime.now(timezone.utc)
            # walk back until we find a decision with extractable text
            year, month = now.year, now.month
            for _ in range(6):
                hits = self._month_hits(year, month)
                for hit in hits:
                    raw = self._build_raw(hit)
                    if raw and len(raw["text"]) > 200:
                        logger.info(
                            f"  OK: {raw['accession']} "
                            f"[{raw.get('document_type')}] "
                            f"{len(raw['text'])} chars, "
                            f"dockets={raw.get('docket_numbers')}")
                        logger.info("API test PASSED")
                        return True
                month -= 1
                if month == 0:
                    month, year = 12, year - 1
            logger.error("  No extractable decision found in recent months")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/FERC-eLibrary bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = FERCeLibraryScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
