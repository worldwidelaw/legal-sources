#!/usr/bin/env python3
"""
US/GA-LegalEthics -- State Bar of Georgia — Formal Advisory Opinions

Fetches the full text of the legal-ethics advisory opinions published by the
State Bar of Georgia interpreting the Georgia Rules of Professional Conduct
(and, for the older series, the predecessor Standards of Conduct) to advise
LAWYERS. Three co-published series are folded in:

  - FAO   Formal Advisory Opinions (No. YY-N, e.g. 86-2, 05-2, 23-1) — issued
          by the Formal Advisory Opinion Board and approved/issued by the
          Supreme Court of Georgia pursuant to Bar Rule 4-403. SCOG-approved
          FAOs are binding.
  - SDBAO State Disciplinary Board Advisory Opinions (older numbered series,
          "Advisory Opinion N", e.g. 5, 16, 49) — the historical advisory
          opinions of the State Disciplinary Board.
  - UPL   Unlicensed Practice of Law Advisory Opinions (No. YYYY-N) — issued by
          the Standing Committee on the Unlicensed Practice of Law.

All three are the State Bar of Georgia's written interpretations of the rules
governing lawyers = doctrine (advisory; SCOG-approved FAOs binding). The State
Bar of Georgia is the state's integrated (mandatory) bar, so the 17 U.S.C.
§ 105 government-edicts rationale applies -> pd-us.

Distinct from US/GA-EthicsOpinions (the executive Georgia Government
Transparency & Campaign Finance Commission — public officials / campaign
finance) and US/GA-Courts / US/GA-Legislation.

Access (no JavaScript execution, no CAPTCHA, no auth):
  The opinions are published inside the gabar.org online Handbook (Sitefinity
  CMS). The page
    https://www.gabar.org/handbook/?handbook=Formal_Advisory_Opinions
  returns a single ~16 MB HTML document that embeds the ENTIRE handbook as an
  HTML-entity-encoded JSON tree inside <data id="bar-rules-data" value="...">.
  The ?rule=... query fragments are client-side JS navigation only — the server
  always renders the whole handbook and there are NO per-opinion server-side
  URLs. So discovery = fetch the one page ONCE, decode + parse the JSON tree,
  and split it into per-opinion records. Each opinion node carries a Title
  (e.g. "Formal Advisory Opinion No. 86-2") and a Content field holding the
  full born-HTML opinion body (question presented, opinion/analysis, and the
  SCOG approval line). All text; NO PDF, NO OCR.

  Opinions appear multiple times in the tree (cross-referenced from several
  Rules of Professional Conduct); we de-duplicate on (series, number), keeping
  the longest Content.

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
"""

from __future__ import annotations

import sys
import json
import html as htmlmod
import logging
import re
import time
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
logger = logging.getLogger("legal-data-hunter.US.GA-LegalEthics")

BASE = "https://www.gabar.org"
HANDBOOK_URL = BASE + "/handbook/?handbook=Formal_Advisory_Opinions"
# Public JS-navigation permalink for a specific opinion section (informational;
# the server ignores the fragment but this is the canonical human URL).
RULE_URL_TMPL = BASE + "/handbook/?handbook=Formal_Advisory_Opinions#{urlname}"

DATA_RE = re.compile(r'<data id="bar-rules-data" value="(.*?)"\s*>', re.S)

MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"], start=1)}
DATE_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|October|"
    r"November|December)\s+(\d{1,2}),\s+(\d{4})", re.I)

FAO_RE = re.compile(r"Formal Advisory Opinion No\.\s*(.+)", re.I)
UPL_RE = re.compile(r"UPL Advisory Opinion No\.\s*(.+)", re.I)
SDBAO_RE = re.compile(r"Advisory Opinion (\d+[A-Za-z]?)\b", re.I)

# Superseded/withdrawn FAOs are reduced by the CMS to a boilerplate pointer to
# the replacement opinion; they carry no substantive body -> skip them.
REPLACED_STUB_RE = re.compile(
    r"^If you have any questions regarding the replaced opinion", re.I)

SERIES_LABEL = {
    "FAO": "State Bar of Georgia — Formal Advisory Opinion Board / Supreme "
           "Court of Georgia",
    "SDBAO": "State Bar of Georgia — State Disciplinary Board",
    "UPL": "State Bar of Georgia — Standing Committee on the Unlicensed "
           "Practice of Law",
}


class GALegalEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,*/*",
        })
        self._cache: list[dict] | None = None

    # ---------------------------------------------------------------- http
    def _get_handbook(self) -> str | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                r = self._session.get(HANDBOOK_URL, timeout=120)
                if r.status_code == 200 and len(r.text) > 100_000:
                    return r.text
                logger.warning(
                    f"handbook GET -> HTTP {r.status_code} ({len(r.text)} bytes)")
            except Exception as e:
                logger.warning(f"handbook GET failed (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- parse
    @staticmethod
    def _strip(html: str) -> str:
        text = BeautifulSoup(html or "", "html.parser").get_text("\n")
        text = htmlmod.unescape(text)
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
        text = text.replace("​", "").replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        # drop boilerplate "Click here for an explanation..." history preamble
        text = re.sub(
            r"^Click here\s*\n*for an explanation[^\n]*\n+", "", text,
            flags=re.I)
        return text.strip()

    @staticmethod
    def _classify(title: str) -> tuple[str, str] | tuple[None, None]:
        t = (title or "").strip()
        m = FAO_RE.match(t)
        if m:
            return "FAO", m.group(1).strip().rstrip(".").strip()
        m = UPL_RE.match(t)
        if m:
            return "UPL", m.group(1).strip().rstrip(".").strip()
        m = SDBAO_RE.match(t)
        if m:
            return "SDBAO", m.group(1)
        return None, None

    @staticmethod
    def _year_from_number(series: str, num: str) -> int | None:
        m = re.match(r"(\d{2,4})", num)
        if not m:
            return None
        y = int(m.group(1))
        if y >= 1000:          # UPL: already a 4-digit year
            return y
        if y > 99:
            return None
        return 2000 + y if y <= 25 else 1900 + y   # FAO 2-digit year

    def _issue_date(self, series: str, num: str, text: str) -> str | None:
        # The issue/approval date is the first calendar date in the opinion
        # header ("Issued ... On <date>", "Approved And Issued On <date>",
        # "PURSUANT TO BAR RULE 4-403 ON <date>", "State Disciplinary Board
        # <date>"). Body citations to other opinions come later, so the FIRST
        # date is the issue date.
        m = DATE_RE.search(text[:1500]) or DATE_RE.search(text)
        if m:
            mon = MONTHS.get(m.group(1).lower())
            day = int(m.group(2))
            yr = int(m.group(3))
            if mon and 1 <= day <= 31 and 1900 <= yr <= 2100:
                return f"{yr:04d}-{mon:02d}-{day:02d}"
        # fall back to the year encoded in the opinion number
        y = self._year_from_number(series, num)
        return f"{y}-01-01" if y else None

    @staticmethod
    def _make_title(series: str, num: str, text: str) -> str:
        label = {"FAO": "Formal Advisory Opinion",
                 "SDBAO": "Advisory Opinion",
                 "UPL": "UPL Advisory Opinion"}[series]
        base = f"{label} No. {num}" if series != "SDBAO" else f"{label} {num}"
        # append the Question Presented (or first substantive sentence) as a
        # human-readable subject, when present
        qm = re.search(r"QUESTION[S]? PRESENTED:?\s*(.+)", text, re.I)
        subject = None
        if qm:
            subject = qm.group(1).strip().split("\n")[0].strip()
        if subject and len(subject) > 8:
            return f"{base}: {subject}"[:300]
        return base

    def _load(self, html: str) -> list[dict]:
        m = DATA_RE.search(html)
        if not m:
            logger.error("bar-rules-data payload not found in handbook page")
            return []
        try:
            tree = json.loads(htmlmod.unescape(m.group(1)))
        except Exception as e:
            logger.error(f"JSON parse failed: {e}")
            return []

        nodes: list[dict] = []

        def walk(items):
            for it in items or []:
                nodes.append(it)
                walk(it.get("Children"))

        walk(tree)

        by: dict[tuple[str, str], dict] = {}
        for n in nodes:
            series, num = self._classify(n.get("Title", ""))
            if not series:
                continue
            text = self._strip(n.get("Content", ""))
            if len(text) < 120:          # skip index/stub entries
                continue
            if REPLACED_STUB_RE.match(text):   # superseded-opinion pointer
                continue
            key = (series, num)
            prev = by.get(key)
            if prev is None or len(text) > len(prev["text"]):
                by[key] = {
                    "series": series,
                    "number": num,
                    "text": text,
                    "urlname": n.get("UrlName") or "",
                }
        # order: FAO, SDBAO, UPL then by numeric key
        def sortkey(r):
            order = {"FAO": 0, "SDBAO": 1, "UPL": 2}[r["series"]]
            m2 = re.findall(r"\d+", r["number"])
            nums = tuple(int(x) for x in m2) if m2 else (0,)
            return (order,) + nums
        result = sorted(by.values(), key=sortkey)
        logger.info(
            f"  parsed {len(result)} unique opinions "
            f"(FAO={sum(1 for r in result if r['series']=='FAO')}, "
            f"SDBAO={sum(1 for r in result if r['series']=='SDBAO')}, "
            f"UPL={sum(1 for r in result if r['series']=='UPL')})")
        return result

    def _opinions(self) -> list[dict]:
        if self._cache is None:
            html = self._get_handbook()
            self._cache = self._load(html) if html else []
        return self._cache

    def _build(self, op: dict) -> dict:
        series, num = op["series"], op["number"]
        text = op["text"]
        canon = {"FAO": f"FAO-{num}", "SDBAO": f"SDBAO-{num}",
                 "UPL": f"UPL-{num}"}[series]
        url = (RULE_URL_TMPL.format(urlname=op["urlname"])
               if op.get("urlname") else HANDBOOK_URL)
        return {
            "opinion_number": canon,
            "series": series,
            "title": self._make_title(series, num, text),
            "text": text,
            "date": self._issue_date(series, num, text),
            "url": url,
            "issuer": SERIES_LABEL[series],
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing State Bar of Georgia Formal Advisory Opinions...")
        ops = self._opinions()
        if not ops:
            logger.error("API test FAILED: no opinions parsed")
            return False
        ok = 0
        for op in ops[:2] + ops[-1:]:
            rec = self._build(op)
            if rec and len(rec["text"]) > 300:
                logger.info(f"  {rec['opinion_number']} OK "
                            f"({len(rec['text'])} chars) date={rec['date']}")
                ok += 1
            else:
                logger.warning(f"  no text ({op['series']} {op['number']})")
        if ok >= 2:
            logger.info(f"API test PASSED ({len(ops)} opinions available)")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["opinion_number"]
        return {
            "_id": f"US/GA-LegalEthics/{num}",
            "_source": "US/GA-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "series": raw.get("series"),
            "issuer": raw.get("issuer"),
            "title": raw.get("title") or f"Georgia Legal Ethics Opinion {num}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-GA",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for op in self._opinions():
            rec = self._build(op)
            if not rec or len(rec["text"]) < 120:
                continue
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

    parser = argparse.ArgumentParser(description="US/GA-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = GALegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
