#!/usr/bin/env python3
"""
US/NY-WCB -- New York Workers' Compensation Board — Board Decisions

Fetches the full text of the openly-published decisions of the New York State
Workers' Compensation Board — the quasi-judicial agency that adjudicates
workers'-compensation, disability-benefits and paid-family-leave claims under
the New York Workers' Compensation Law. On administrative appeal from a
Workers' Compensation Law Judge (WCLJ), a three-member Board Panel (and, on
Mandatory or discretionary Full Board Review, the entire Board) issues a
written Memorandum of Decision resolving the contested case. Each such
decision resolves a specific case = case_law, and they are official New York
state-government works in the public domain (government edicts).

BUILD RECIPE (no auth, no CAPTCHA): the Board publishes two openly-browsable
sets of decisions on wcb.ny.gov:

  * "Select Board Panel Decisions"
      https://www.wcb.ny.gov/content/main/Decisions/board-panel-decisions.jsp
    An index linking one server-rendered HTML page per decision at
    /content/main/Decisions/board-panel-decisions/{Matter...}.jsp . The full
    decision body is inside the page's <div id="mainContent">.

  * "Significant Full Board Decisions"
      https://www.wcb.ny.gov/content/main/Decisions/board-decisions.jsp
    An index linking born-digital decision PDFs under
    /content/main/Decisions/{YYYYMon}/*.pdf .

(The Board's complete decision corpus is served through the case-number-keyed
"eCase" system, which is not openly enumerable; only these curated Board Panel
and Full Board decision sets are published as browsable full text.)

The scraper reads both indexes, downloads each HTML decision page or PDF,
extracts the full decision text (mainContent for HTML, common.pdf_extract for
PDF), and parses the matter caption, WCB case number, NY Wrk Comp citation and
decision date.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

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

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NY-WCB")

HOST = "https://www.wcb.ny.gov"
PANEL_INDEX = HOST + "/content/main/Decisions/board-panel-decisions.jsp"
FULLBOARD_INDEX = HOST + "/content/main/Decisions/board-decisions.jsp"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")

TAG_RE = re.compile(r"<[^>]+>")
SCRIPT_STYLE_RE = re.compile(r"<(script|style)[^>]*>.*?</\1>", re.I | re.S)
MAINCONTENT_RE = re.compile(
    r'<div[^>]*id="mainContent"[^>]*>(.*?)</div>\s*(?:<!--\s*/?mainContent|<footer)',
    re.I | re.S)

PANEL_LINK_RE = re.compile(
    r'href="(/content/main/Decisions/board-panel-decisions/[^"?]+\.jsp)"[^>]*>(.*?)</a>',
    re.I | re.S)
FULLBOARD_PDF_RE = re.compile(
    r'href="(/content/main/Decisions/[^"?]+\.pdf)"[^>]*>(.*?)</a>',
    re.I | re.S)

CASE_NO_RE = re.compile(r"Case\s*#\s*([A-Z]?\d[\w-]*)", re.I)
CITATION_RE = re.compile(r"(\d{4})\s+NY\s+Wrk\s+Comp\s+([A-Z]?\d[\w]*)", re.I)
MONTHS = ("January February March April May June July August September "
          "October November December").split()
LONGDATE_RE = re.compile(
    r"\b(" + "|".join(MONTHS) + r")\s+(\d{1,2}),\s+(\d{4})\b")


class NYWCBScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.7
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": UA})
        self._existing: set[str] = set()

    # ---------------------------------------------------------------- http
    def _get_text(self, url: str) -> str | None:
        for attempt in range(5):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, timeout=(15, 90))
                resp.raise_for_status()
                resp.encoding = resp.apparent_encoding or "utf-8"
                return resp.text
            except Exception as e:
                logger.warning(f"GET failed ({url[:90]}) attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    def _get_bytes(self, url: str) -> bytes | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, timeout=(15, 120), stream=True)
                resp.raise_for_status()
                return resp.content
            except Exception as e:
                logger.warning(f"PDF GET failed ({url[:90]}) attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _clean_inline(fragment: str) -> str:
        txt = _html.unescape(TAG_RE.sub(" ", fragment)).replace("\xa0", " ")
        return re.sub(r"\s+", " ", txt).strip()

    @classmethod
    def _maincontent_text(cls, html: str) -> str:
        m = MAINCONTENT_RE.search(html)
        frag = m.group(1) if m else html
        frag = SCRIPT_STYLE_RE.sub(" ", frag)
        txt = _html.unescape(TAG_RE.sub(" ", frag)).replace("\xa0", " ")
        txt = re.sub(r"[ \t]+", " ", txt)
        txt = re.sub(r"\s*\n\s*", "\n", txt)
        txt = re.sub(r"\n{2,}", "\n", txt).strip()
        # Trim the leading breadcrumb/heading duplication: start at the first
        # "Case #" if present, else the first "Matter of".
        idx = txt.find("Case #")
        if idx == -1:
            idx = txt.lower().find("case #")
        if idx > 0:
            txt = txt[idx:].strip()
        return re.sub(r" *\n *", "\n", txt).strip()

    @classmethod
    def _iso_from_body(cls, text: str) -> str | None:
        # Prefer an explicit "meeting on <date>" / "filed <date>" reference,
        # else the most recent long-form date in the body.
        candidates: list[str] = []
        for label in (r"meeting on\s+", r"filed\s+", r"dated\s+"):
            for m in re.finditer(label + LONGDATE_RE.pattern, text, re.I):
                candidates.append(cls._long_to_iso(m.group(1), m.group(2),
                                                    m.group(3)))
        if not candidates:
            alld = [cls._long_to_iso(mo, d, y)
                    for mo, d, y in LONGDATE_RE.findall(text)]
            candidates = [c for c in alld if c]
            return max(candidates) if candidates else None
        candidates = [c for c in candidates if c]
        return max(candidates) if candidates else None

    @staticmethod
    def _long_to_iso(month: str, day: str, year: str) -> str | None:
        try:
            mm = MONTHS.index(month.capitalize()) + 1
        except ValueError:
            return None
        dd, yy = int(day), int(year)
        if 1 <= dd <= 31 and 1970 <= yy <= 2100:
            return f"{yy:04d}-{mm:02d}-{dd:02d}"
        return None

    @staticmethod
    def _slug(s: str) -> str:
        return re.sub(r"[^A-Za-z0-9]+", "-", s).strip("-")[:80]

    # --------------------------------------------------------- discovery
    def discover(self, sample: bool = False) -> Generator[dict, None, None]:
        seen: set[str] = set()

        # 1) Select Board Panel Decisions (per-decision .jsp pages)
        html = self._get_text(PANEL_INDEX)
        if html:
            body = MAINCONTENT_RE.search(html)
            scope = body.group(1) if body else html
            n = 0
            for href, label in PANEL_LINK_RE.findall(scope):
                title = self._clean_inline(label)
                if not title:
                    continue
                key = href.lower()
                if key in seen:
                    continue
                seen.add(key)
                stem = href.rsplit("/", 1)[-1][:-4]  # drop .jsp
                yield {
                    "kind": "jsp",
                    "url": HOST + href,
                    "title": title,
                    "record_id": self._slug(stem),
                    "source_page": PANEL_INDEX,
                }
                n += 1
                if sample and n >= 14:
                    logger.info(f"Sample: stopped after {n} panel pointers")
                    return
            logger.info(f"Board Panel index: {n} decisions")

        # 2) Significant Full Board Decisions (PDFs)
        html = self._get_text(FULLBOARD_INDEX)
        if html:
            body = MAINCONTENT_RE.search(html)
            scope = body.group(1) if body else html
            n = 0
            for href, label in FULLBOARD_PDF_RE.findall(scope):
                if "governor.ny.gov" in href:
                    continue
                key = href.lower()
                if key in seen:
                    continue
                seen.add(key)
                stem = href.rsplit("/", 1)[-1]
                stem = re.sub(r"(?i)\.pdf$", "", stem)
                title = re.sub(r"\s*PDF\s*$", "", self._clean_inline(label)).strip()
                yield {
                    "kind": "pdf",
                    "url": HOST + href,
                    "title": title or ("Matter of " + stem),
                    "record_id": self._slug(stem),
                    "source_page": FULLBOARD_INDEX,
                }
                n += 1
            logger.info(f"Full Board index: {n} decisions")

    # ------------------------------------------------------- build record
    def _build_raw(self, entry: dict) -> dict | None:
        rid = entry["record_id"]
        if rid in self._existing:
            return None

        if entry["kind"] == "jsp":
            html = self._get_text(entry["url"])
            if not html:
                return None
            text = self._maincontent_text(html)
        else:  # pdf
            pdf_bytes = self._get_bytes(entry["url"])
            if not pdf_bytes:
                return None
            text = extract_pdf_markdown(
                "US/NY-WCB", rid, pdf_bytes=pdf_bytes, table="case_law")

        if not text or len(text.strip()) < 400:
            logger.warning(f"No usable text for {entry['url'][:80]} "
                           f"({len(text or '')} chars) — skipping")
            return None
        text = text.strip()

        case_no = None
        m = CASE_NO_RE.search(text)
        if m:
            case_no = m.group(1).upper()
        citation = None
        m = CITATION_RE.search(text)
        if m:
            citation = f"{m.group(1)} NY Wrk Comp {m.group(2).upper()}"
        date = self._iso_from_body(text)
        if not date and citation:
            date = f"{citation[:4]}-01-01"

        return {
            "record_id": rid,
            "title": (entry["title"] or f"WCB Decision {rid}")[:500],
            "text": text,
            "date": date,
            "case_number": case_no,
            "citation": citation,
            "url": entry["url"],
            "source_page": entry.get("source_page"),
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing NY Workers' Compensation Board decision indexes...")
        try:
            entries = list(self.discover(sample=True))
            if not entries:
                logger.error("  No decision pointers discovered")
                return False
            logger.info(f"  Discovered {len(entries)} pointers (sample)")
            raw = None
            for e in entries:
                raw = self._build_raw(e)
                if raw:
                    break
            if raw and raw["text"] and len(raw["text"]) > 400:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw['record_id']} [{raw['date']}] "
                            f"case={raw['case_number']}")
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
            "_id": f"US/NY-WCB/{raw['record_id']}",
            "_source": "US/NY-WCB",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["record_id"],
            "issuer": "New York State Workers' Compensation Board",
            "title": raw["title"],
            "citation": raw.get("citation"),
            "case_number": raw.get("case_number"),
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-NY",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        if not sample:
            try:
                self._existing = preload_existing_ids("US/NY-WCB", "case_law")
            except Exception as e:
                logger.warning(f"preload_existing_ids failed: {e}")
                self._existing = set()
        emitted = 0
        for entry in self.discover(sample=sample):
            raw = self._build_raw(entry)
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

    parser = argparse.ArgumentParser(description="US/NY-WCB bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NYWCBScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
