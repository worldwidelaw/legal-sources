#!/usr/bin/env python3
"""
BE/DBRC -- Dienst van de Bestuursrechtscolleges (Flemish Administrative Courts)

Full text of the arresten (judgments) of the Flemish administrative courts
administered by the DBRC (Dienst van de Bestuursrechtscolleges):

  * Raad voor Vergunningsbetwistingen (RvVb)   — planning-permit disputes
  * Handhavingscollege (HHC)                   — environmental enforcement
  * Raad voor Verkiezingsbetwistingen (R.Verkb.) — local election disputes
  * Raad voor Studievoortgangsbetwistingen (R.Stvb.) — study-progress disputes

⚠️ COMMERCIAL USE RESTRICTED — the DBRC disclaimer requires prior express
written consent for commercial reuse/reproduction/publication of the arresten.

Strategy (server-rendered Drupal facet listing + born-digital PDFs):

  1. fetch_all() walks the judgment listing at /rechtspraak. The listing is
     plain server-rendered HTML (Drupal Views + facets), 21 PDF links per
     page, ~19,410 arresten in total (work-years 2009-10 .. 2025-26).

     The walk is partitioned by the ``document_type`` facet — one unit per
     court/procedure (6 units, 19,402 of the 19,410 records) — followed by
     an unfaceted sweep that picks up the handful of arresten carrying no
     document_type term. Partitioning keeps a restart from re-walking the
     whole listing and gives every record its court and procedure label
     straight from the facet, and completed units plus the in-progress
     page are persisted to ``data/checkpoint.json``.

  2. normalize() downloads the arrest PDF and extracts the full text with
     PyMuPDF (fitz), falling back to pdfplumber. The documents are
     born-digital, so no OCR path is needed. The decision number, roll
     number and pronouncement date are parsed out of the PDF header
     ("ARREST van 27 maart 2026 met nummer RvVb-UDN-2526-0627 in de zaak
     met rolnummer 2526-RvVb-0613-UDN"); the court code, procedure and
     work-year also come from the filename
     ``{COURT}[.{PROC}].{WORKYEAR}.{NNNN}.pdf``.

  NOTE: use www.dbrc.be, NOT the www.rvvb.be mirror — rvvb.be rewrites
  direct PDF requests into a JS-aggregator redirect.

Usage:
  python bootstrap.py bootstrap            # Full pull (all arresten)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BE.DBRC")

SOURCE_ID = "BE/DBRC"
BASE_URL = "https://www.dbrc.be"
LISTING_PATH = "/rechtspraak"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

REQUEST_DELAY = 1.0
MAX_RETRIES = 5
MAX_PAGES = 2000  # hard stop; the largest facet is ~700 pages

PDF_HREF_RE = re.compile(r"/sites/default/files/[^\"']+?\.pdf", re.I)
RESULT_COUNT_RE = re.compile(r"([\d\.]+)\s+resultaten")

# Filename: RVVB.UDN.2526.0627.pdf / RVVB.S.2526.0633.pdf /
#           MHHC.M.1718.0069_0.pdf / RSTVB.2526.0870.pdf / RVERKB.2425.0041.pdf
FILENAME_RE = re.compile(
    r"^(?P<court>[A-Z]+)"
    r"(?:\.(?P<proc>[A-Z]+))?"
    r"\.(?P<workyear>\d{4})"
    r"\.(?P<seq>\d+)"
    r"(?:_\d+)?$"
)

COURT_NAMES = {
    "RVVB": "Raad voor Vergunningsbetwistingen",
    "MHHC": "Handhavingscollege",
    "HHC": "Handhavingscollege",
    "RSTVB": "Raad voor Studievoortgangsbetwistingen",
    "RVERKB": "Raad voor Verkiezingsbetwistingen",
}

PROCEDURE_NAMES = {
    "UDN": "schorsing bij uiterst dringende noodzakelijkheid",
    "S": "schorsingsprocedure",
    "A": "vernietigingsprocedure",
    "M": "handhavingsprocedure",
}

# document_type facet ids on /rechtspraak (verified live 2026-08-02)
DOCUMENT_TYPES = [
    ("49", "RvVb - arresten vernietigingsprocedure"),
    ("53", "RvVb - arresten schorsingsprocedure"),
    ("57", "HHC - arresten"),
    ("107", "R.Stvb. - arresten"),
    ("54", "RvVb - arresten UDN-procedure"),
    ("58", "R.Verkb. - arresten"),
]

DUTCH_MONTHS = {
    "januari": 1, "februari": 2, "maart": 3, "april": 4, "mei": 5, "juni": 6,
    "juli": 7, "augustus": 8, "september": 9, "oktober": 10, "november": 11,
    "december": 12,
}

DATE_RE = re.compile(
    r"\bvan\s+(\d{1,2})\s+(" + "|".join(DUTCH_MONTHS) + r")\s+(\d{4})", re.I
)
NUMMER_RE = re.compile(r"met\s+nummer\s+([A-Za-z0-9\.\-/]+)", re.I)
ROLNUMMER_RE = re.compile(r"rolnummer\s+([A-Za-z0-9\.\-/]+)", re.I)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_with_fitz(pdf_bytes: bytes) -> str:
    try:
        import fitz  # PyMuPDF
    except ImportError:
        return ""
    try:
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            return "\n".join(page.get_text() for page in doc)
    except Exception as exc:
        logger.debug(f"fitz failed: {exc}")
        return ""


def _extract_with_pdfplumber(pdf_bytes: bytes) -> str:
    try:
        import pdfplumber
    except ImportError:
        return ""
    out = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                try:
                    out.append(page.extract_text() or "")
                finally:
                    # release the per-page layout + textmap LRU cache, which
                    # pdfplumber otherwise keeps for the document's lifetime
                    try:
                        page.flush_cache()
                        page.get_textmap.cache_clear()
                    except Exception:
                        pass
    except Exception as exc:
        logger.debug(f"pdfplumber failed: {exc}")
        return ""
    return "\n".join(out)


def extract_text(pdf_bytes: bytes) -> str:
    text = _extract_with_fitz(pdf_bytes)
    if len(text.strip()) < 200:
        alt = _extract_with_pdfplumber(pdf_bytes)
        if len(alt.strip()) > len(text.strip()):
            text = alt
    return clean_text(text)


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def parse_dutch_date(text: str) -> Optional[str]:
    m = DATE_RE.search(text)
    if not m:
        return None
    day, month, year = int(m.group(1)), DUTCH_MONTHS[m.group(2).lower()], int(m.group(3))
    try:
        return datetime(year, month, day).date().isoformat()
    except ValueError:
        return None


def workyear_label(workyear: Optional[str]) -> Optional[str]:
    """'2526' -> '2025-2026'."""
    if not workyear or len(workyear) != 4 or not workyear.isdigit():
        return None
    start, end = int(workyear[:2]), int(workyear[2:])
    century = 2000 if start < 90 else 1900
    return f"{century + start}-{century + end if end >= start else century + 100 + end}"


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class DBRCScraper(BaseScraper):
    """Flemish administrative courts (DBRC) arresten."""

    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "nl-BE,nl;q=0.9,en;q=0.7",
        })
        self._module_dir = Path(__file__).resolve().parent
        self._ckpt_path = self._module_dir / "data" / "checkpoint.json"
        self._ckpt = self._load_checkpoint()
        self._use_checkpoint = True

    # ---- checkpoint -------------------------------------------------------

    def _load_checkpoint(self) -> dict:
        try:
            with open(self._ckpt_path, encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                return {
                    "done_units": list(data.get("done_units", [])),
                    "unit": data.get("unit"),
                    "page": int(data.get("page", 0)),
                    "seen": list(data.get("seen", [])),
                }
        except FileNotFoundError:
            pass
        except Exception as exc:
            logger.warning(f"Ignoring unreadable checkpoint: {exc}")
        return {"done_units": [], "unit": None, "page": 0, "seen": []}

    def _save_checkpoint(self, seen: set) -> None:
        try:
            self._ckpt_path.parent.mkdir(parents=True, exist_ok=True)
            payload = dict(self._ckpt)
            payload["seen"] = sorted(seen)
            tmp = self._ckpt_path.with_suffix(".json.tmp")
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh)
            tmp.replace(self._ckpt_path)
        except Exception as exc:
            logger.warning(f"Could not persist checkpoint: {exc}")

    # ---- HTTP -------------------------------------------------------------

    def _get(self, url: str, params: Optional[dict] = None,
             timeout: int = 60) -> Optional[requests.Response]:
        delay = 2.0
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.get(url, params=params, timeout=timeout)
                if resp.status_code == 200:
                    return resp
                if resp.status_code in (404, 410):
                    return None
                if resp.status_code in (429, 500, 502, 503, 504):
                    retry_after = resp.headers.get("Retry-After")
                    wait = float(retry_after) if (retry_after or "").isdigit() else delay
                    logger.warning(
                        f"HTTP {resp.status_code} for {url} "
                        f"(attempt {attempt}/{MAX_RETRIES}) — sleeping {wait:.0f}s"
                    )
                    time.sleep(min(wait, 120))
                    delay = min(delay * 2, 120)
                    continue
                logger.warning(f"HTTP {resp.status_code} for {url}")
                return None
            except requests.RequestException as exc:
                logger.warning(
                    f"{type(exc).__name__} for {url} "
                    f"(attempt {attempt}/{MAX_RETRIES}): {exc}"
                )
                time.sleep(min(delay, 120))
                delay = min(delay * 2, 120)
        return None

    # ---- listing ----------------------------------------------------------

    def _listing(self, page: int, doctype: Optional[str]) -> Optional[str]:
        params = {"page": page}
        if doctype:
            params["f[0]"] = f"document_type:{doctype}"
        resp = self._get(BASE_URL + LISTING_PATH, params=params)
        return resp.text if resp is not None else None

    @staticmethod
    def _parse_pdf_links(html: str) -> list:
        soup = BeautifulSoup(html, "html.parser")
        links = []
        for a in soup.select("article a[href]"):
            href = a["href"]
            if not href.lower().endswith(".pdf"):
                continue
            if href.startswith("/"):
                href = BASE_URL + href
            links.append(href)
        if not links:  # layout fallback — keep only judgment filenames, so the
            for m in PDF_HREF_RE.finditer(html):  # footer's accessibility PDF
                href = BASE_URL + m.group(0)      # never leaks in
                if FILENAME_RE.match(href.rsplit("/", 1)[-1][:-4]):
                    links.append(href)
        seen, out = set(), []
        for href in links:
            if href not in seen:
                seen.add(href)
                out.append(href)
        return out

    @staticmethod
    def _result_count(html: str) -> Optional[int]:
        m = RESULT_COUNT_RE.search(html)
        if not m:
            return None
        return int(m.group(1).replace(".", ""))

    def _walk_unit(self, unit_key: str, doctype: Optional[str], label: Optional[str],
                   seen: set, start_page: int = 0) -> Generator[dict, None, None]:
        page = start_page
        empty_streak = 0
        while page < MAX_PAGES:
            html = self._listing(page, doctype)
            if html is None:
                logger.warning(f"{unit_key}: page {page} unavailable — stopping unit")
                break
            if page == 0 and start_page == 0:
                total = self._result_count(html)
                logger.info(f"{unit_key}: {total} arresten")
            links = self._parse_pdf_links(html)
            if not links:
                empty_streak += 1
                if empty_streak >= 2:
                    break
            else:
                empty_streak = 0
            fresh = 0
            for href in links:
                if href in seen:
                    continue
                seen.add(href)
                fresh += 1
                yield {"pdf_url": href, "document_type": label}

            if self._use_checkpoint:
                self._ckpt["unit"] = unit_key
                self._ckpt["page"] = page
                if page % 20 == 0:
                    self._save_checkpoint(seen)
                    logger.info(f"  {unit_key} page {page} ({len(seen)} arresten seen)")
            page += 1
            time.sleep(REQUEST_DELAY)

        if self._use_checkpoint:
            if unit_key not in self._ckpt["done_units"]:
                self._ckpt["done_units"].append(unit_key)
            self._ckpt["unit"] = None
            self._ckpt["page"] = 0
            self._save_checkpoint(seen)

    def fetch_all(self) -> Generator[dict, None, None]:
        # Fail loud rather than reporting an empty corpus if the site is
        # unreachable / WAF-blocked from this vantage.
        probe = self._listing(0, None)
        if probe is None or not self._parse_pdf_links(probe):
            raise RuntimeError(
                "dbrc.be /rechtspraak returned no judgment links — the site is "
                "unreachable, blocked from this vantage, or its layout changed."
            )
        logger.info(f"DBRC: {self._result_count(probe)} arresten in total")

        seen = set(self._ckpt["seen"]) if self._use_checkpoint else set()
        done = set(self._ckpt["done_units"]) if self._use_checkpoint else set()

        units = [(f"doctype:{tid}", tid, label) for tid, label in DOCUMENT_TYPES]
        units.append(("all", None, None))

        for unit_key, doctype, label in units:
            if unit_key in done:
                logger.info(f"{unit_key}: already done (checkpoint) — skipping")
                continue
            start = 0
            if self._use_checkpoint and self._ckpt.get("unit") == unit_key:
                start = max(0, int(self._ckpt.get("page", 0)))
                if start:
                    logger.info(f"{unit_key}: resuming at page {start}")
            yield from self._walk_unit(unit_key, doctype, label, seen, start)

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Newest-first sweep of the unfaceted listing; normalize filters by date."""
        if isinstance(since, str):
            since = datetime.fromisoformat(since.replace("Z", "+00:00"))
        seen = set()
        for page in range(0, 40):
            html = self._listing(page, None)
            if html is None:
                break
            links = self._parse_pdf_links(html)
            if not links:
                break
            for href in links:
                if href in seen:
                    continue
                seen.add(href)
                yield {"pdf_url": href, "document_type": None, "_since": since.date().isoformat()}
            time.sleep(REQUEST_DELAY)

    # ---- normalization ----------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        pdf_url = raw.get("pdf_url")
        if not pdf_url:
            return None
        stem = pdf_url.rsplit("/", 1)[-1][:-4]  # strip .pdf

        meta = FILENAME_RE.match(stem)
        court_code = meta.group("court") if meta else None
        procedure = meta.group("proc") if meta else None
        workyear = meta.group("workyear") if meta else None
        sequence = meta.group("seq") if meta else None

        resp = self._get(pdf_url, timeout=120)
        if resp is None:
            logger.debug(f"{stem}: PDF unavailable")
            return None
        text = extract_text(resp.content)
        if len(text) < 300:
            logger.debug(f"{stem}: insufficient text ({len(text)} chars)")
            return None

        head = text[:1500]
        decision_number = None
        m = NUMMER_RE.search(head)
        if m:
            decision_number = m.group(1).rstrip(".,")
        roll_number = None
        m = ROLNUMMER_RE.search(head)
        if m:
            roll_number = m.group(1).rstrip(".,")
        date = parse_dutch_date(head)

        court = COURT_NAMES.get((court_code or "").upper())
        if not court:
            first = next((ln.strip() for ln in text.split("\n") if ln.strip()), "")
            court = first.title() if first.isupper() and len(first) < 90 else None

        proc_label = PROCEDURE_NAMES.get((procedure or "").upper())
        title_bits = ["Arrest"]
        if decision_number:
            title_bits.append(decision_number)
        elif sequence and workyear:
            title_bits.append(f"{court_code}-{workyear}-{sequence}")
        title = " ".join(title_bits)
        if court:
            title = f"{title} — {court}"
        if proc_label:
            title = f"{title} ({proc_label})"

        since = raw.get("_since")
        if since and date and date < since:
            return None

        return {
            "_id": f"dbrc-{stem}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": pdf_url,
            "decision_number": decision_number,
            "roll_number": roll_number,
            "court": court,
            "court_code": court_code,
            "procedure": proc_label or procedure,
            "document_type": raw.get("document_type"),
            "work_year": workyear_label(workyear),
            "language": "nl",
            "country": "BE",
        }

    # ---- diagnostics ------------------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing www.dbrc.be ...")
        try:
            html = self._listing(0, None)
            if html is None:
                logger.error("  listing unreachable")
                return False
            links = self._parse_pdf_links(html)
            logger.info(f"  listing OK — {self._result_count(html)} arresten, "
                        f"{len(links)} links on page 0")
            if not links:
                logger.error("  no PDF links parsed")
                return False
            rec = self.normalize({"pdf_url": links[0], "document_type": None})
            if not rec:
                logger.error("  first arrest yielded no text")
                return False
            logger.info(
                f"  {rec['_id']} OK — {len(rec['text'])} chars, date={rec['date']}, "
                f"nr={rec['decision_number']}, court={rec['court']}"
            )
            logger.info("API test PASSED")
            return True
        except Exception as exc:
            logger.error(f"API test FAILED: {exc}")
            return False


def main():
    parser = argparse.ArgumentParser(description="BE/DBRC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api", "updates"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--count", type=int, default=15, help="Sample size")
    parser.add_argument("--since", type=str, help="ISO date for updates")
    args = parser.parse_args()

    scraper = DBRCScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        return

    if args.command == "updates":
        since = args.since or datetime.now(timezone.utc).date().isoformat()
        for raw in scraper.fetch_updates(since):
            rec = scraper.normalize(raw)
            if rec:
                print(json.dumps(rec, ensure_ascii=False))
        return

    if args.sample:
        scraper._use_checkpoint = False
    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=args.count)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
