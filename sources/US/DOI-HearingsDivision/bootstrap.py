#!/usr/bin/env python3
"""
US/DOI-HearingsDivision -- U.S. DOI, OHA Departmental Cases Hearings Division

Fetches the full text of the decisions issued by the Departmental Cases
Hearings Division (formerly "Hearings Division") of the U.S. Department of the
Interior's Office of Hearings and Appeals -- the trial-level tribunal whose
administrative law judges hold hearings and decide contested cases arising
under Interior programs: Bureau of Land Management grazing decisions and
trespass, Surface Mining (OSM) civil-penalty and permit cases, hunting/fishing
and other public-land enforcement matters, etc. Each decision resolves a
specific contested case, so the corpus is `case_law`. These are
federal-government works in the public domain (17 U.S.C. § 105).

Access (no JavaScript execution needed, no CAPTCHA, no auth):
  Interior's OHA publishes the decisions in a full-text ISYS / Perceptive
  Enterprise Search database reachable over plain HTTP GET at
  https://www.oha.doi.gov:8080/ -- the same portal and identical query flow as
  US/DOI-IBLA, with IW_DATABASE="Hearings Division Decisions 2000-Present":

    1. GET /search/?IW_FIELD_NATURAL_LANGUAGE=<query>&IW_DATABASE=Hearings Division Decisions 2000-Present
       -> results page carrying a server-cached query GUID + document count.
    2. GET /isysquery/<GUID>/<start>-<end>/list/
       -> a page of hits; each hit N links its PDF at
          /isysquery/<GUID>/<N>/doc/<filename>.pdf. Here the filename is a
          descriptive name (with spaces, URL-encoded as %20) that embeds the
          party, case reference and a "dtd MM-DD-YYYY" decision date, e.g.
          "Western Watersheds Project ID-JFO-2020-006 et al dtd 10-20-2021.pdf".
    3. GET /isysquery/<GUID>/<N>/doc/<filename>.pdf -> the born-digital PDF.

  The GUID persists server-side so no cookie/session handling is needed and
  there is no stable direct PDF path.

Enumeration:
  The corpus is enumerated by running broad natural-language seed queries whose
  union matches essentially every decision, paging each query's full hit list,
  and de-duplicating by the (decoded) PDF filename.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.DOI-HearingsDivision")

BASE_URL = "https://www.oha.doi.gov:8080"
DATABASE = "Hearings Division Decisions 2000-Present"

MIN_TEXT_CHARS = 200
PAGE = 50  # hits per /list/ request

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Broad seed queries whose union covers the corpus; deduped by filename.
SEED_QUERIES = [
    "Bureau of Land Management",   # near-universal
    "grazing decision",
    "trespass",
    "surface mining civil penalty",
    "oil and gas",
    "mining claim",
    "permit hearing",
    "wild horse burro",
    "administrative law judge",
    "appeal petition stay",
]

# Filenames here contain spaces/encoded chars, so capture up to ".pdf" but not
# across an href quote or angle bracket. Requires a real filename (not the
# empty "/doc/" link).
HIT_RE = re.compile(
    r'/isysquery/([0-9a-fA-F-]{36})/(\d+)/doc/([^"\'<>]+?\.pdf)', re.I
)
GUID_RE = re.compile(r'/isysquery/([0-9a-fA-F-]{36})/', re.I)
DOCS_RE = re.compile(r'([0-9][0-9,]*)\s*documents', re.I)
HITS_RE = re.compile(r'([0-9][0-9,]*)\s*(?:hits|matches)', re.I)

_MONTHS = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}
# Decision date embedded in the filename, e.g. "... dtd 10-20-2021" (also
# "dated"/"dec"). Accept M-D-YYYY or M/D/YYYY.
FNAME_DATE_RE = re.compile(
    r'(?:dtd|dated|dec(?:ision)?|order)\s*[-_ ]*(\d{1,2})[-/](\d{1,2})[-/](\d{2,4})',
    re.I,
)
DECIDED_RE = re.compile(
    r'Decided\s+(' + "|".join(_MONTHS) + r')\s+(\d{1,2}),?\s+(\d{4})', re.I
)
BARE_DATE_RE = re.compile(
    r'\b(' + "|".join(_MONTHS) + r')\s+(\d{1,2}),\s+(\d{4})\b', re.I
)
# Interior case references, e.g. "ID-JFO-2020-006", "DCHD-2022-0067",
# "UT-060-02-01", "OSM ..." -- best effort.
DOCKET_RE = re.compile(r'\b([A-Z]{2,5}-[A-Z0-9]{1,6}-\d{2,4}-\d{1,5})\b')


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _valid_ymd(y: int, m: int, d: int) -> str | None:
    if y < 100:
        y += 2000 if y < 50 else 1900
    if 1990 <= y <= 2100 and 1 <= m <= 12 and 1 <= d <= 31:
        return f"{y}-{m:02d}-{d:02d}"
    return None


def _date_from_filename(name: str) -> str | None:
    m = FNAME_DATE_RE.search(name)
    if not m:
        return None
    return _valid_ymd(int(m.group(3)), int(m.group(1)), int(m.group(2)))


def _date_from_body(text: str) -> str | None:
    head = text[:1500]
    m = DECIDED_RE.search(head) or BARE_DATE_RE.search(head)
    if not m:
        return None
    mon = int(_MONTHS[m.group(1).lower()])
    try:
        d, y = int(m.group(2)), int(m.group(3))
    except ValueError:
        return None
    return _valid_ymd(y, mon, d)


class DOIHearingsDivisionScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": BROWSER_UA,
                "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=90,
        )
        self.delay = 1.0

    # ---- fetch helpers -------------------------------------------------

    def _get_text(self, url: str, retries: int = 3) -> str | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.text:
                    return resp.text
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _get_bytes(self, url: str, retries: int = 3) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.content:
                    ctype = (resp.headers.get("Content-Type") or "").lower()
                    if "pdf" in ctype or resp.content[:5] == b"%PDF-":
                        return resp.content
                    logger.warning(f"Non-PDF content ({ctype}) for {url}")
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching PDF {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- discovery -----------------------------------------------------

    def _run_query(self, query: str) -> tuple[str | None, int]:
        url = (
            f"{BASE_URL}/search/?"
            + urllib.parse.urlencode({
                "IW_FIELD_NATURAL_LANGUAGE": query,
                "IW_DATABASE": DATABASE,
            })
        )
        html = self._get_text(url)
        if not html:
            return None, 0
        gm = GUID_RE.search(html)
        dm = DOCS_RE.search(html)
        hm = HITS_RE.search(html)
        guid = gm.group(1) if gm else None
        if dm:
            total = int(dm.group(1).replace(",", ""))
        elif hm:
            total = int(hm.group(1).replace(",", ""))
        else:
            total = 0
        return guid, total

    def _iter_hits(self, guid: str, total: int) -> Generator[tuple[int, str], None, None]:
        start = 1
        hard_cap = max(total, 0) + PAGE
        while start <= hard_cap:
            end = start + PAGE - 1
            url = f"{BASE_URL}/isysquery/{guid}/{start}-{end}/list/"
            html = self._get_text(url)
            if not html:
                break
            hits = [(int(n), fn) for (_g, n, fn) in HIT_RE.findall(html)]
            seen_ranks = set()
            page_hits = []
            for n, fn in hits:
                if start <= n <= end and n not in seen_ranks:
                    seen_ranks.add(n)
                    page_hits.append((n, fn))
            if not page_hits:
                break
            for n, fn in page_hits:
                yield n, fn
            start += PAGE

    def discover_documents(self, sample: bool = False) -> Generator[dict, None, None]:
        seen: set[str] = set()
        queries = SEED_QUERIES[:1] if sample else SEED_QUERIES
        total_yielded = 0
        for query in queries:
            guid, total = self._run_query(query)
            if not guid:
                logger.warning(f"No GUID for query {query!r}")
                continue
            logger.info(f"Query {query!r}: {total} documents (guid {guid})")
            for rank, fname_enc in self._iter_hits(guid, total):
                name = urllib.parse.unquote(fname_enc)
                stem = name.rsplit(".", 1)[0].strip()
                key = stem.upper()
                if key in seen:
                    continue
                seen.add(key)
                doc_url = f"{BASE_URL}/isysquery/{guid}/{rank}/doc/{fname_enc}"
                yield {
                    "file_name": name,
                    "file_id": stem,
                    "doc_url": doc_url,
                    "slug": re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-")[:100],
                }
                total_yielded += 1
                if sample and total_yielded >= 30:
                    return
        logger.info(f"Discovered {len(seen)} unique Hearings Division decisions")

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._get_bytes(doc["doc_url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/DOI-HearingsDivision",
            doc["slug"],
            pdf_bytes=pdf_bytes,
            table="case_law",
            force=True,
        )
        text = clean_text(text or "")
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars), likely "
                           f"scanned: {doc['slug']}")
            return None
        doc = dict(doc)
        doc["text"] = text
        dm = DOCKET_RE.search(doc.get("file_name") or "") or DOCKET_RE.search(text[:1500])
        doc["docket"] = dm.group(1) if dm else None
        doc["date"] = _date_from_filename(doc.get("file_name") or "") or _date_from_body(text)
        return doc

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing DOI Departmental Cases Hearings Division (ISYS)...")
        try:
            docs = []
            for d in self.discover_documents(sample=True):
                docs.append(d)
                if len(docs) >= 5:
                    break
            if not docs:
                logger.error("  No decisions discovered")
                return False
            logger.info(f"  Discovered {len(docs)}+ decisions (partial crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('docket') or raw.get('file_id')}")
            else:
                logger.error("  Text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        docket = raw.get("docket")
        # For this DB the PDF filename is the descriptive case name (party +
        # case reference); it makes a far better title than the DCHD
        # letterhead. Strip the trailing "dtd MM-DD-YYYY" date token.
        caption = raw.get("file_id") or ""
        caption = re.sub(
            r'\s*(?:dtd|dated|dec(?:ision)?|order)\s*[-_ ]*\d{1,2}[-/]\d{1,2}[-/]\d{2,4}.*$',
            "", caption, flags=re.I,
        ).strip(" -_")
        # Fall back to the first substantive body line if the filename is thin.
        if len(caption) < 6:
            for line in (raw.get("text") or "").split("\n"):
                s = line.strip()
                if (s and len(s) >= 6
                        and not s.upper().startswith("UNITED STATES DEPARTMENT")
                        and not s.upper().startswith("OFFICE OF HEARINGS")
                        and not s.upper().startswith("DEPARTMENTAL CASES")
                        and "TEMPLE" not in s.upper() and "SUITE" not in s.upper()
                        and "TELEPHONE" not in s.upper()
                        and not BARE_DATE_RE.match(s)):
                    caption = s
                    break
        caption = caption[:180]
        title = caption or "Departmental Cases Hearings Division decision"
        if docket and docket not in title:
            title = f"{title} ({docket})"
        title = title[:300]
        return {
            "_id": f"US/DOI-HearingsDivision/{raw['slug']}",
            "_source": "US/DOI-HearingsDivision",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "docket_number": docket,
            "file_id": raw.get("file_id"),
            "court": "Departmental Cases Hearings Division (U.S. DOI, Office of Hearings and Appeals)",
            "title": title,
            "text": raw["text"],
            "url": raw["doc_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        examined = 0
        for doc in self.discover_documents(sample=sample):
            examined += 1
            raw = self._build_raw(doc)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return
            if sample and examined >= 30:
                return

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/DOI-HearingsDivision bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = DOIHearingsDivisionScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
