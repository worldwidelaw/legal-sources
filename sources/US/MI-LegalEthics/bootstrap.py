#!/usr/bin/env python3
"""
US/MI-LegalEthics -- State Bar of Michigan — Ethics Opinions (Professional &
Judicial)

Fetches the full text of the ethics opinions issued by the State Bar of
Michigan's Standing Committees on Professional Ethics and on Judicial Ethics.
Each opinion interprets the Michigan Rules of Professional Conduct (MRPC) or the
Michigan Code of Judicial Conduct in response to a stated fact situation and
advises lawyers or judges whether the described conduct is proper = doctrine
(the Committees' official written interpretation of the conduct rules).

The corpus is one continuous online archive combining SIX opinion series:
  * R   — formal professional opinions (post-Oct-1988)
  * RI  — informal professional opinions (post-Oct-1988; current series to RI-394)
  * C   — formal professional opinions (pre-1988 legacy)
  * CI  — informal professional opinions (pre-1988 legacy)
  * J   — formal judicial opinions
  * JI  — informal judicial opinions (current series)
spanning C-210 (1972) through the latest RI opinion (2026), ~1,280 opinions.
Distinct from US/MI-MERC (labor board), US/MI-Courts and US/MI-Legislation; this
is the state *bar*'s attorney/judicial ethics advisory-opinion series.

Access (no JavaScript execution needed, no CAPTCHA, no auth, browser UA):
  The michbar.org DNN site renders every opinion detail page at a stable
  internal-primary-key route:
    https://www.michbar.org/opinions/ethics/numbered_opinions?OpinionID={id}
  The OpinionID space is contiguous (1 .. ~1282). Enumerating it in order walks
  the entire corpus without depending on the JavaScript search box. The opinion
  body renders inside
    #dnn_ctr14174_EthicsOpinionsSearchDetail_divEOWholeOpinion
  as born-digital HTML: "{NUMBER} {DATE} SYLLABUS ... References: ... TEXT ...".
  A missing/superseded id renders only the "SBM - State Bar of Michigan" header
  (len ~27) and is skipped. The opinion number & date are parsed from the body
  (both series-prefixed number and the date immediately follow the SBM header).

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
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
logger = logging.getLogger("legal-data-hunter.US.MI-LegalEthics")

BASE = "https://www.michbar.org"
DETAIL_URL = BASE + "/opinions/ethics/numbered_opinions?OpinionID={id}&Type=6&Index=A"
DETAIL_DIV_ID = "dnn_ctr14174_EthicsOpinionsSearchDetail_divEOWholeOpinion"

# Contiguous OpinionID ceiling (RI-394 = Feb 2026 sits at ~1281). Enumerate a
# little past the known tail and stop after a long run of empty ids.
MAX_OPINION_ID = 1400
STOP_AFTER_CONSECUTIVE_EMPTY = 30

SBM_HEADER_RE = re.compile(r"^\s*SBM\s*-\s*State Bar of Michigan\s*", re.I)
# Series-prefixed number immediately followed by its issue date.
NUM_DATE_RE = re.compile(
    r"\b((?:RI|CI|JI|R|C|J)-\d+)\s+"
    r"((?:[A-Z][a-z]+\.?\s*,?\s*)?(?:\d{1,2}\s*,?\s*)?(?:19|20)\d\d)"
)
NUM_ONLY_RE = re.compile(r"\b((?:RI|CI|JI|R|C|J)-\d+)\b")

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7,
    "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}

SERIES_LABEL = {
    "R": "Committee on Professional Ethics (formal)",
    "RI": "Committee on Professional Ethics (informal)",
    "C": "Committee on Professional Ethics (formal, legacy)",
    "CI": "Committee on Professional Ethics (informal, legacy)",
    "J": "Committee on Judicial Ethics (formal)",
    "JI": "Committee on Judicial Ethics (informal)",
}


class MILegalEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                "Version/16.0 Safari/605.1.15"
            ),
            "Accept": "text/html,application/xhtml+xml",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str) -> requests.Response | None:
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                r = self._session.get(url, timeout=60)
                if r.status_code == 200:
                    return r
                if r.status_code == 404:
                    return None
                logger.warning(f"GET {url} -> HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"GET failed {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # -------------------------------------------------------- extraction
    @staticmethod
    def _clean(text: str) -> str:
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _parse_date(raw: str) -> str | None:
        """Parse a Michigan date like 'November 25, 1991' / 'May, 1985' /
        'July 1972'."""
        if not raw:
            return None
        low = raw.lower()
        m = re.search(r"([a-z]+)\.?\s+(\d{1,2})\s*,\s*(\d{4})", low)
        if m and m.group(1) in MONTHS:
            return f"{int(m.group(3)):04d}-{MONTHS[m.group(1)]:02d}-{int(m.group(2)):02d}"
        m = re.search(r"([a-z]+)\.?\s*,?\s*(\d{4})", low)
        if m and m.group(1) in MONTHS:
            return f"{int(m.group(2)):04d}-{MONTHS[m.group(1)]:02d}-01"
        y = re.search(r"\b(19|20)\d{2}\b", raw)
        if y:
            return f"{y.group(0)}-01-01"
        return None

    def _fetch_one(self, opinion_id: int) -> dict | None:
        url = DETAIL_URL.format(id=opinion_id)
        r = self._get(url)
        if not r or not r.text:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        div = soup.find(id=DETAIL_DIV_ID)
        if div is None:
            div = soup.select_one(".ModSBMEthicsOpinionsSearchResultsDetailC")
        if div is None:
            return None
        for t in div(["script", "style"]):
            t.decompose()

        full = div.get_text("\n", strip=True)
        # Drop the leading "SBM - State Bar of Michigan" chrome header.
        body = SBM_HEADER_RE.sub("", full).strip()
        if len(body) < 150:
            # missing / superseded stub with no opinion text
            return None

        # Opinion number + date: the series number followed by its issue date.
        number = None
        date = None
        md = NUM_DATE_RE.search(body)
        if md:
            number = md.group(1).upper()
            date = self._parse_date(md.group(2))
        else:
            mn = NUM_ONLY_RE.search(body)
            if mn:
                number = mn.group(1).upper()
        if not number:
            return None

        text = self._clean(body)
        if len(text) < 150:
            return None

        # A clean canonical URL (SEO slug) for the record.
        prefix, seq = number.split("-", 1)
        canon = f"{BASE}/opinions/ethics/numbered_opinions/{prefix.lower()}-{int(seq):03d}"

        return {
            "opinion_id": opinion_id,
            "opinion_number": number,
            "series": prefix,
            "text": text,
            "date": date,
            "url": canon,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing State Bar of Michigan Ethics Opinions...")
        ok = 0
        for oid in (1, 500, 1000, 1188, 1281):
            rec = self._fetch_one(oid)
            if rec and len(rec["text"]) > 300:
                logger.info(f"  OpinionID {oid} -> {rec['opinion_number']} OK "
                            f"({len(rec['text'])} chars) date={rec['date']}")
                ok += 1
            else:
                logger.warning(f"  no text for OpinionID {oid}")
        if ok >= 3:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: not enough full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["opinion_number"]
        series = raw.get("series", "")
        committee = SERIES_LABEL.get(series, "Standing Committee on Ethics")
        return {
            "_id": f"US/MI-LegalEthics/{num}",
            "_source": "US/MI-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": f"State Bar of Michigan — {committee}",
            "title": f"State Bar of Michigan Ethics Opinion {num}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-MI",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        seen: set[str] = set()
        consecutive_empty = 0
        for oid in range(1, MAX_OPINION_ID + 1):
            rec = self._fetch_one(oid)
            if not rec:
                consecutive_empty += 1
                if consecutive_empty >= STOP_AFTER_CONSECUTIVE_EMPTY:
                    logger.info(f"  {consecutive_empty} consecutive empty ids "
                                f"at OpinionID={oid}; stopping enumeration")
                    return
                continue
            consecutive_empty = 0
            if rec["opinion_number"] in seen:
                continue
            seen.add(rec["opinion_number"])
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

    parser = argparse.ArgumentParser(description="US/MI-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MILegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
