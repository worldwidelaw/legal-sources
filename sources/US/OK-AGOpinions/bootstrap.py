#!/usr/bin/env python3
"""
US/OK-AGOpinions -- Oklahoma Attorney General Opinions (official + full text).

Fetches the full text of the official written Opinions of the Attorney General
of Oklahoma. Under 74 O.S. § 18b, the Attorney General renders written opinions
upon questions of law submitted by the Legislature, its members, and the officers
of the executive/administrative departments of state government. Those opinions
are binding on the state officers to whom they are directed until superseded by
a court or the AG, and are published and cited "___ OK AG ___" (opinion numbers
run "YY-NNN", e.g. 03-018) = case_law-adjacent official government edicts. The
project classifies binding AG opinions as case_law (a specific-question official
determination), consistent with the other US/*-AGOpinions sources.

Distinct from US/OK-LegalEthics (Oklahoma Bar Association attorney-ethics
advisory opinions), US/OK-Courts, US/OK-Legislation and US/OK-TaxDecisions.

Access (no JavaScript execution, no CAPTCHA, no auth):
  The corpus is the Oklahoma Public Legal Research System (OPLRS), operated by
  the University of Oklahoma Law School at oklegal.onenet.net. It runs a CNIDR
  Isearch-cgi (1.47j) full-text engine over the "okag" database
  (OK Attorney General Opinions, 1948-current).

  * Search endpoint (POST): /oklegal-cgi/isearch
      SEARCH_TYPE=SIMPLE, DATABASE=okag,
      TERM_1=<year>, FIELD_1=HEARING_DATE,   # partition the corpus by year
      ELEMENT_SET=F,                          # return FULL TEXT inline
      MAXHITS=100, START=<offset>             # paginate
    The response is an HTML "Isearch Results" page with one block per opinion,
    each carrying the fields inline:
      Filename: <n>.html
      <ENTRY_DATE>   MMDDYY (data-entry date, not used for `date`)
      <APPELLANT>    the requesting official (e.g. "Representative Dan Boren")
      <JURISDICTION> "Attorney General of Oklahoma - Opinion"
      <HEARING_DATE> the issue date ("April 5, 2002")
      <TEXT_OF_RULE> the FULL opinion text (HTML with <p>/<a> statute links)
      <CITATIONS>    "03-018 (2003) ag"  -> opinion number + year
  * The system's per-document ifetch CGI is server-side broken for okag
    ("Database okag does not exist or is corrupted"), but ELEMENT_SET=F returns
    the full text inline in the search results, so ifetch is not needed.

  Enumeration: sweep HEARING_DATE year by year (1948..current). Each year query
  returns every opinion issued that year with full text; >100/year is paged via
  START. Records are deduped on their internal Filename (unique per document).

  The OSCN mirror (oscn.net, ftdb=STOKAG) datacenter-IP-blocks non-Oklahoma
  vantages; OPLRS (oklegal.onenet.net) is reachable and is the access route here.

Usage:
  python bootstrap.py bootstrap            # Full pull (all years)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.OK-AGOpinions")

SEARCH_URL = "https://oklegal.onenet.net/oklegal-cgi/isearch"
DATABASE = "okag"
FIRST_YEAR = 1948
MAXHITS = 100

MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"], start=1)}
DATE_RE = re.compile(
    r"\b(" + "|".join(MONTHS) + r")\s+(\d{1,2}),\s*(\d{4})", re.I)

# One result block starts at "<HR><b>Filename:</b> <name>".
FILE_RE = re.compile(r"<b>Filename:</b>\s*([^\s<]+)", re.I)
MATCH_COUNT_RE = re.compile(r"Matching Record Count:</i>\s*(\d+)", re.I)
CITE_RE = re.compile(r"CITATIONS</b>:\s*([0-9]{2,4}-[0-9A-Za-z]+)", re.I)
CITE_YEAR_RE = re.compile(r"CITATIONS</b>:[^<]*\((\d{4})\)", re.I)

FIELD_TAGS = ("ENTRY_DATE", "APPELLANT", "JURISDICTION",
              "HEARING_DATE", "TEXT_OF_RULE", "CITATIONS", "PUBLISHED")


class OKAGOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/124.0 Safari/537.36",
            "Accept": "text/html,*/*",
        })

    # ---------------------------------------------------------------- http
    def _search(self, year: int, start: int) -> str | None:
        data = {
            "SEARCH_TYPE": "SIMPLE",
            "DATABASE": DATABASE,
            "ELEMENT_SET": "F",
            "TERM_1": str(year),
            "FIELD_1": "HEARING_DATE",
            "LOGIC": "AND",
            "MAXHITS": str(MAXHITS),
            "START": str(start),
        }
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                r = self._session.post(SEARCH_URL, data=data, timeout=90)
                if r.status_code == 200:
                    return r.text
                logger.warning(f"search {year}@{start} -> HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"search failed (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- parsing
    @staticmethod
    def _split_blocks(html: str) -> list[str]:
        """Split an Isearch results page into per-document blocks."""
        # Each document begins at "<HR><b>Filename:</b>". Cut the trailing
        # "Retrieve Next 30" pagination form off the last block.
        parts = re.split(r"<HR>(?=<b>Filename:</b>)", html, flags=re.I)
        return [p for p in parts if FILE_RE.search(p)]

    @staticmethod
    def _field_html(block: str, tag: str) -> str | None:
        m = re.search(rf"<{tag}><b>{tag}</b>:(.*?)</{tag}>", block,
                      re.S | re.I)
        return m.group(1) if m else None

    @staticmethod
    def _clean(text: str) -> str:
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
        text = text.replace("�", "").replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _parse_block(self, block: str) -> dict | None:
        fm = FILE_RE.search(block)
        if not fm:
            return None
        filename = fm.group(1).strip()

        # The okag database is contaminated with Court of Criminal Appeals
        # decisions (esp. recent years); keep only true AG opinions, whose
        # JURISDICTION field reads "Attorney General of Oklahoma - Opinion".
        jur_html = self._field_html(block, "JURISDICTION")
        jur = (BeautifulSoup(jur_html, "html.parser").get_text(" ", strip=True)
               if jur_html else "")
        if "attorney general" not in jur.lower():
            return None

        body_html = self._field_html(block, "TEXT_OF_RULE")
        if not body_html:
            return None
        text = self._clean(
            BeautifulSoup(body_html, "html.parser").get_text(" ", strip=True))
        if len(text) < 120:
            return None

        # opinion number + issue date
        cm = CITE_RE.search(block)
        number = cm.group(1).upper() if cm else None

        date = None
        hd = self._field_html(block, "HEARING_DATE")
        if hd:
            dm = DATE_RE.search(hd)
            if dm:
                mon = MONTHS[dm.group(1).lower()]
                day, yr = int(dm.group(2)), int(dm.group(3))
                if 1 <= day <= 31 and 1900 <= yr <= 2100:
                    date = f"{yr:04d}-{mon:02d}-{day:02d}"

        appellant = None
        am = self._field_html(block, "APPELLANT")
        if am:
            appellant = self._clean(
                BeautifulSoup(am, "html.parser").get_text(" ", strip=True)) or None

        return {
            "filename": filename,
            "number": number,
            "text": text,
            "date": date,
            "requested_by": appellant,
        }

    # ---------------------------------------------------------- discovery
    def _iter_year(self, year: int) -> Generator[dict, None, None]:
        start = 1
        total = None
        seen_files: set[str] = set()
        while True:
            html = self._search(year, start)
            if html is None:
                return
            if total is None:
                mc = MATCH_COUNT_RE.search(html)
                total = int(mc.group(1)) if mc else 0
                if total:
                    logger.info(f"  {year}: {total} opinions")
            blocks = self._split_blocks(html)
            if not blocks:
                return
            for b in blocks:
                rec = self._parse_block(b)
                if not rec:
                    continue
                if rec["filename"] in seen_files:
                    continue
                seen_files.add(rec["filename"])
                yield rec
            start += MAXHITS
            if total is None or start > total:
                return

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Oklahoma AG Opinions (OPLRS okag)...")
        n = 0
        for rec in self._iter_year(2003):
            if rec["text"] and len(rec["text"]) > 300:
                if n < 3:
                    logger.info(f"  {rec['number']} OK ({len(rec['text'])} "
                                f"chars) date={rec['date']}")
                n += 1
            if n >= 5:
                break
        if n >= 5:
            logger.info(f"API test PASSED ({n}+ full-text opinions in 2003)")
            return True
        logger.error("API test FAILED: insufficient full text")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw.get("number")
        stem = raw["filename"].rsplit(".", 1)[0]
        oid = number if number else stem
        title = (f"Oklahoma Attorney General Opinion No. {number}"
                 if number else f"Oklahoma Attorney General Opinion ({stem})")
        return {
            "_id": f"US/OK-AGOpinions/{oid}",
            "_source": "US/OK-AGOpinions",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "issuer": "Attorney General of Oklahoma",
            "title": title,
            "text": raw["text"],
            "url": "https://oklegal.onenet.net/agopinions.basic.html",
            "date": raw.get("date"),
            "requested_by": raw.get("requested_by"),
            "jurisdiction": "US-OK",
        }

    # ------------------------------------------------------------- fetch
    @staticmethod
    def _dedup_key(rec: dict) -> str:
        # okag stores some opinions twice under different internal filenames;
        # collapse on opinion number when present, else on a text hash, so the
        # same opinion is not emitted twice (which would collide on _id).
        if rec.get("number"):
            return "n:" + rec["number"]
        return "t:" + hashlib.md5(rec["text"].encode("utf-8")).hexdigest()

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        current_year = datetime.now(timezone.utc).year
        # newest-first so a sample pulls recent, dated opinions
        years = range(current_year, FIRST_YEAR - 1, -1)
        if sample:
            years = range(current_year, current_year - 4, -1)
        seen: set[str] = set()
        for yr in years:
            for rec in self._iter_year(yr):
                key = self._dedup_key(rec)
                if key in seen:
                    continue
                seen.add(key)
                yield rec
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

    parser = argparse.ArgumentParser(description="US/OK-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = OKAGOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
