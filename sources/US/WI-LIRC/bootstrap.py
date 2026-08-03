#!/usr/bin/env python3
"""
US/WI-LIRC -- Wisconsin Labor & Industry Review Commission (LIRC) Decisions

Fetches the full text of the decisions of the Wisconsin Labor and Industry
Review Commission (LIRC), the independent Wisconsin state agency that
provides appellate review of the decisions of the administrative law
judges of the Department of Workforce Development (DWD) in three
programs:

  - Unemployment Insurance (UI) appeals            ucdecsns/
  - Worker's Compensation (WC) appeals             wcdecsns/
  - Equal Rights / discrimination (ER) appeals     erdecsns/

Each numbered decision resolves a specific contested case = case_law, and
they are official Wisconsin state-government works in the public domain
(government edicts). This source is DISTINCT from US/WI-WERC (the
Employment Relations Commission, which handles public-sector collective
bargaining) -- LIRC reviews UI/WC/ER appeals.

BUILD RECIPE (no auth, no CAPTCHA, builds locally): individual decisions
render as server-side HTML (a minority as born-digital PDF) at
deterministic per-program paths:

  https://lirc.wisconsin.gov/ucdecsns/{N}.htm   (UI)
  https://lirc.wisconsin.gov/wcdecsns/{N}.htm   (WC)
  https://lirc.wisconsin.gov/erdecsns/{N}.htm   (ER)

where {N} is a numeric decision id (plus a minority of surname / other
slugs, e.g. ucdecsns/bondar.pdf, erdecsns/black&decker-v-dilhr.htm). The
site directory listing is 403 and robots.txt blocks specific named
crawlers, so enumeration is HYBRID:

  (1) numeric-id range probing per program (skip 404 gaps), unioned with
  (2) a broad-term sweep of the site's Mindbreeze search JSON API,
        POST https://mbsearchlirc.wisconsin.gov/api/v2/search
             {"query":{"unparsed":"<term>"},"count":100}
      whose result ids embed the decision URL as "Web:lirc:{url}:" --
      this captures the non-numeric slugs the numeric probe misses.

The UI pages are MS-Word HTML exports whose <head> holds huge Word CSS/XML
and only the <body> WordSection carries the decision, so the extractor
takes body-only text (BeautifulSoup) and drops the head junk. PDF
decisions go through the shared common.pdf_extract extractor. Title,
program, claimant/party and decision date come from the page <title> and
body text.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import os
import sys
import json
import logging
import re
import time
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import requests

# Make locally-installed OCR tools (tesseract/poppler) discoverable so that
# common.pdf_extract's OCR fallback works for any scanned PDF decisions.
os.environ["PATH"] = "/opt/homebrew/bin:" + os.environ.get("PATH", "")

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

try:
    from bs4 import BeautifulSoup
    _HAVE_BS = True
except Exception:  # pragma: no cover
    _HAVE_BS = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.WI-LIRC")

HOST = "https://lirc.wisconsin.gov"
SEARCH_API = "https://mbsearchlirc.wisconsin.gov/api/v2/search"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")

# Program directory -> (short code, human label). Numeric-probe upper bound
# is set generously above the observed max id per program (uc~4550, wc~2300,
# er~1770 as of 2026-07); gaps are skipped as 404s.
PROGRAMS = {
    "ucdecsns": ("uc", "Unemployment Insurance", 5200),
    "wcdecsns": ("wc", "Worker's Compensation", 2600),
    "erdecsns": ("er", "Equal Rights"),
}
ER_MAX = 2000

# Broad query terms for the Mindbreeze sweep -- captures non-numeric slugs
# (surnames, versioned/pdf files) the numeric probe cannot construct.
SWEEP_TERMS = [
    "the", "commission", "decision", "affirmed", "reversed", "modified",
    "employer", "claimant", "employee", "discrimination", "wages", "injury",
    "benefits", "order", "appeal", "hearing", "award", "findings",
    "misconduct", "quit", "discharge", "disability", "compensation",
    "complainant", "respondent", "retaliation", "harassment", "termination",
    "and", "of", "a", "is", "was", "not", "with", "for",
]
for _y in range(1990, 2027):
    SWEEP_TERMS.append(str(_y))

DECISION_URL_RE = re.compile(
    r"(https?://lirc\.wisconsin\.gov/(?:ucdecsns|wcdecsns|erdecsns)/[^/\s:]+\.(?:htm|pdf))",
    re.IGNORECASE,
)
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")
MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"], start=1)}
LONG_DATE_RE = re.compile(
    r"\b(january|february|march|april|may|june|july|august|september|"
    r"october|november|december)\s+(\d{1,2}),?\s+(\d{4})\b", re.IGNORECASE)
NUM_DATE_RE = re.compile(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\b")


class LIRCScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.5
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": UA})
        self._existing: set[str] = set()

    # ---------------------------------------------------------------- http
    def _get(self, url: str, tries: int = 4) -> requests.Response | None:
        for attempt in range(tries):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, timeout=(15, 90))
                if resp.status_code == 404:
                    return resp
                resp.raise_for_status()
                return resp
            except Exception as e:
                logger.debug(f"GET failed ({url[:90]}) attempt {attempt+1}: {e}")
                time.sleep(min(2 ** attempt, 8))
        return None

    def _search(self, term: str) -> list[str]:
        body = json.dumps({"query": {"unparsed": term}, "count": 100}).encode()
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                resp = self._session.post(
                    SEARCH_API, data=body,
                    headers={"Content-Type": "application/json"},
                    timeout=(15, 60))
                resp.raise_for_status()
                data = resp.json()
                out = []
                for r in data.get("resultset", {}).get("results", []):
                    rid = (r.get("id") or "").replace("Web:lirc:", "").rstrip(":")
                    m = DECISION_URL_RE.search(rid)
                    if m:
                        out.append(m.group(1))
                return out
            except Exception as e:
                logger.debug(f"search '{term}' attempt {attempt+1}: {e}")
                time.sleep(min(2 ** attempt, 8))
        return []

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _program_of(url: str) -> tuple[str, str]:
        for d, meta in PROGRAMS.items():
            if f"/{d}/" in url:
                return meta[0], meta[1]
        return "uc", "Unemployment Insurance"

    @staticmethod
    def _slug(url: str) -> str:
        stem = url.rsplit("/", 1)[-1]
        stem = re.sub(r"(?i)\.(htm|html|pdf)$", "", stem)
        return re.sub(r"[^A-Za-z0-9]+", "-", stem).strip("-").lower()

    @classmethod
    def _iso_from_text(cls, s: str) -> str | None:
        m = LONG_DATE_RE.search(s or "")
        if m:
            mm = MONTHS[m.group(1).lower()]
            dd = int(m.group(2))
            yy = int(m.group(3))
            if 1 <= dd <= 31 and 1970 <= yy <= 2100:
                return f"{yy:04d}-{mm:02d}-{dd:02d}"
        m = NUM_DATE_RE.search(s or "")
        if m:
            mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if yy < 100:
                yy += 1900 if yy >= 50 else 2000
            if 1 <= mm <= 12 and 1 <= dd <= 31 and 1970 <= yy <= 2100:
                return f"{yy:04d}-{mm:02d}-{dd:02d}"
        return None

    @staticmethod
    def _clean_title(raw_title: str) -> str:
        t = _html.unescape(TAG_RE.sub(" ", raw_title or ""))
        t = re.sub(r"\s+", " ", t).strip()
        # Strip the "Wis.LIRC UC Decision:" style prefix noise but keep caption.
        t = re.sub(r"^Wis\.?\s*LIRC\s*", "", t, flags=re.IGNORECASE).strip()
        return t

    def _html_body_text(self, html: str) -> str:
        m = re.search(r"<body[^>]*>(.*?)</body>", html, re.IGNORECASE | re.DOTALL)
        frag = m.group(1) if m else html
        if _HAVE_BS:
            soup = BeautifulSoup(frag, "html.parser")
            for tag in soup(["script", "style", "noscript"]):
                tag.decompose()
            txt = soup.get_text(" ")
        else:
            txt = TAG_RE.sub(" ", frag)
        txt = _html.unescape(txt).replace("\xa0", " ")
        txt = re.sub(r"[ \t\r\f\v]+", " ", txt)
        txt = re.sub(r"\n[ \t]*\n+", "\n\n", txt)
        return txt.strip()

    # --------------------------------------------------------- discovery
    def _resolve_numeric(self, program_dir: str, n: int,
                         seen: set[str]) -> str | None:
        """A numeric decision may be published as .htm or .pdf (the format
        varies even within a program). Try .htm first, then .pdf; return the
        URL that exists (200)."""
        for ext in ("htm", "pdf"):
            url = f"{HOST}/{program_dir}/{n}.{ext}"
            if url.lower() in seen:
                return None
            resp = self._get(url, tries=2)
            if resp is not None and resp.status_code != 404:
                return url
        return None

    def _search_by_program(self, terms) -> dict:
        """Sweep the search API and bucket decision URLs by program code."""
        buckets: dict[str, list[str]] = {"uc": [], "wc": [], "er": []}
        seen: set[str] = set()
        for term in terms:
            for url in self._search(term):
                if url.lower() in seen:
                    continue
                seen.add(url.lower())
                code, _ = self._program_of(url)
                buckets.setdefault(code, []).append(url)
        return buckets

    def discover(self, sample: bool = False) -> Generator[str, None, None]:
        """Yield candidate decision URLs (deduped)."""
        seen: set[str] = set()

        def emit(url: str):
            key = url.lower()
            if key not in seen:
                seen.add(key)
                return url
            return None

        # (1) Mindbreeze sweep, interleaved by program. Round-robin across
        # WC/ER/UC so the head of the stream (and therefore the framework's
        # first-N sample) exercises all three programs -- the HTML WC/ER
        # decisions carry rich <title> captions/dates, the UI ones are mostly
        # PDF. This also captures the non-numeric slugs the numeric probe
        # cannot construct.
        terms = SWEEP_TERMS[:14] if sample else SWEEP_TERMS
        buckets = self._search_by_program(terms)
        while any(buckets.values()):
            for code in ("wc", "er", "uc"):
                if buckets.get(code):
                    u = emit(buckets[code].pop(0))
                    if u:
                        yield u
        if sample:
            logger.info(f"Sample: {len(seen)} pointers from search")
            return
        logger.info(f"Search sweep found {len(seen)} URLs; starting numeric probe")

        # (2) numeric-id range probe per program -- fills the rest of the
        # corpus (trying both .htm and .pdf, since the format varies).
        for d, meta in PROGRAMS.items():
            top = meta[2] if len(meta) > 2 else ER_MAX
            for n in range(1, top + 1):
                url = self._resolve_numeric(d, n, seen)
                if url:
                    u = emit(url)
                    if u:
                        yield u
        logger.info(f"Discovered {len(seen)} LIRC decision pointers total")

    # ------------------------------------------------------- build record
    def _build_raw(self, url: str) -> dict | None:
        code, label = self._program_of(url)
        rid = f"{code}-{self._slug(url)}"
        if rid in self._existing:
            return None

        is_pdf = url.lower().endswith(".pdf")
        resp = self._get(url)
        if resp is None or resp.status_code == 404:
            return None

        title = ""
        if is_pdf:
            text = extract_pdf_markdown(
                "US/WI-LIRC", rid, pdf_bytes=resp.content, table="case_law")
        else:
            html = resp.text
            tm = TITLE_RE.search(html)
            title = self._clean_title(tm.group(1)) if tm else ""
            text = self._html_body_text(html)

        if not text or len(text.strip()) < 300:
            logger.warning(f"No usable text for {url[:90]} "
                           f"({len(text or '')} chars) — skipping")
            return None
        text = text.strip()

        # Decision date: prefer the title, then the "Dated and Mailed" area
        # near the top of the body.
        date = self._iso_from_text(title) or self._iso_from_text(text[:2500])

        if not title:
            # PDF or title-less page: build a caption from the program + slug
            # (the slug is usually the claimant surname or the decision no.).
            stem = self._slug(url).replace("-", " ")
            pretty = stem.title() if not stem.isdigit() else f"No. {stem}"
            title = f"LIRC {label} Decision — {pretty}"

        return {
            "record_id": rid,
            "program": label,
            "program_code": code,
            "title": title[:500],
            "text": text,
            "date": date,
            "url": url,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Wisconsin LIRC decision access...")
        try:
            urls = list(self.discover(sample=True))
            if not urls:
                logger.error("  No decision pointers discovered")
                return False
            logger.info(f"  Discovered {len(urls)} pointers (sample)")
            raw = None
            for u in urls:
                raw = self._build_raw(u)
                if raw:
                    break
            if raw and raw["text"] and len(raw["text"]) > 300:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw['record_id']} [{raw['date']}] {raw['title'][:70]}")
            else:
                logger.error("  Text extraction failed")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"US/WI-LIRC/{raw['record_id']}",
            "_source": "US/WI-LIRC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["record_id"],
            "issuer": "Wisconsin Labor & Industry Review Commission (LIRC)",
            "program": raw.get("program"),
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-WI",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        if not sample:
            try:
                self._existing = preload_existing_ids("US/WI-LIRC", "case_law")
            except Exception as e:
                logger.warning(f"preload_existing_ids failed: {e}")
                self._existing = set()
        emitted = 0
        for url in self.discover(sample=sample):
            raw = self._build_raw(url)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/WI-LIRC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = LIRCScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
