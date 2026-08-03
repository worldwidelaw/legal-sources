#!/usr/bin/env python3
"""
US/MN-LegalEthics -- Minnesota Lawyers Professional Responsibility Board (LPRB)
— Board Opinions.

Fetches the full text of the opinions promulgated by the Minnesota Lawyers
Professional Responsibility Board interpreting the Minnesota Rules of
Professional Conduct to advise LAWYERS = doctrine. Cited as "LPRB Opinion No.
N" (e.g. Opinion No. 21). ~26 born-digital opinions.

The LPRB is established by the Minnesota Supreme Court (Rules on Lawyers
Professional Responsibility) and its opinions bind Minnesota lawyers, so the
17 U.S.C. § 105 government-edicts rationale applies (pd-us). Distinct from the
Director's/OLPR "Advisory Opinions" (ethics-hotline) and from US/MN-CFBOpinions
(Campaign Finance & Public Disclosure Board — public officials).

Access (no JavaScript execution needed, no CAPTCHA, no auth):
  1. A single public page lists every Board opinion:
       https://lprb.mncourts.gov/lawyers-professional-responsibility-board-opinions/
     Each opinion is an anchor whose text is
     "Opinion N - [Month DD, YYYY -] Title", linking to a born-digital PDF
     under /wp-content/uploads/ (path varies by upload month — taken from the
     href verbatim, never constructed).
  2. Each PDF is born-digital (text layer) — extracted with PyMuPDF (fitz),
     NO OCR. Records under 200 chars are skipped.

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
from urllib.parse import urljoin

import requests
import fitz  # PyMuPDF
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.MN-LegalEthics")

BASE = "https://lprb.mncourts.gov"
INDEX_URL = ("https://lprb.mncourts.gov/"
             "lawyers-professional-responsibility-board-opinions/")

# Anchor text e.g. "Opinion 21 - April 24, 2020 - A Lawyer's Duty ..." (a lone
# "Option 15 - ..." typo exists in the source, so match Op\w+).
NUM_RE = re.compile(r"\bOp\w+\s+(\d+)\b", re.I)
MONTHS = ("January|February|March|April|May|June|July|August|September|"
          "October|November|December")
DATE_RE = re.compile(r"(" + MONTHS + r")\s+(\d{1,2}),\s+((?:19|20)\d{2})")
MONTH_NUM = {
    m: i + 1
    for i, m in enumerate(
        "January February March April May June July August September "
        "October November December".split()
    )
}


class MNLegalEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            ),
            "Accept": "text/html,application/pdf,*/*",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str) -> requests.Response | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                r = self._session.get(url, timeout=90, allow_redirects=True)
                if r.status_code == 200:
                    return r
                if r.status_code == 404:
                    return None
                logger.warning(f"GET {url} -> HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"GET failed {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    @staticmethod
    def _iso_date(mo: str, day: str, yr: str) -> str | None:
        m = MONTH_NUM.get(mo)
        if m and 1 <= int(day) <= 31:
            return f"{int(yr):04d}-{m:02d}-{int(day):02d}"
        return None

    def _list_opinions(self) -> list[dict]:
        """Return [{num, url, title, date}], de-duplicated on num."""
        r = self._get(INDEX_URL)
        if not r:
            logger.error("could not fetch the LPRB opinions index page")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        out: dict[str, dict] = {}
        for a in soup.find_all("a", href=True):
            href = a["href"]
            low = href.lower()
            if ".pdf" not in low or "opinion-" not in low:
                continue
            txt = a.get_text(" ", strip=True)
            m = NUM_RE.search(txt)
            if not m:
                continue
            num = m.group(1)
            if num in out:
                continue
            # Everything after "Opinion N -": may lead with a date, then title.
            rest = txt[m.end():].lstrip(" -–").strip()
            date = None
            dm = DATE_RE.match(rest)
            if dm:
                date = self._iso_date(dm.group(1), dm.group(2), dm.group(3))
                rest = rest[dm.end():].lstrip(" -–").strip()
            title = re.sub(r"\s+", " ", rest).strip()
            out[num] = {
                "num": num,
                "url": urljoin(BASE, href),
                "title": title,
                "date": date,
            }
        result = sorted(out.values(), key=lambda x: int(x["num"]))
        logger.info(f"  discovered {len(result)} unique LPRB opinions")
        return result

    # -------------------------------------------------------- extraction
    @staticmethod
    def _clean(text: str) -> str:
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
        text = text.replace("​", "").replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _fetch_one(self, op: dict) -> dict | None:
        r = self._get(op["url"])
        if not r or not r.content:
            return None
        try:
            doc = fitz.open(stream=r.content, filetype="pdf")
            text = self._clean("\n".join(p.get_text() for p in doc))
        except Exception as e:
            logger.warning(f"  Opinion {op['num']}: PDF parse failed: {e}")
            return None
        if len(text) < 200:
            logger.warning(f"  Opinion {op['num']}: insufficient text "
                           f"({len(text)} chars) - skipping")
            return None
        date = op.get("date")
        if not date:
            dm = DATE_RE.search(text[:1500])
            if dm:
                date = self._iso_date(dm.group(1), dm.group(2), dm.group(3))
        title = f"LPRB Opinion No. {op['num']}"
        if op.get("title"):
            title += f" — {op['title']}"
        return {
            "opinion_number": op["num"],
            "title": title,
            "text": text,
            "date": date,
            "url": op["url"],
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing LPRB opinions index...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions listed")
            return False
        ok = 0
        for op in ops[:2] + ops[-1:]:
            rec = self._fetch_one(op)
            if rec and len(rec["text"]) > 400:
                logger.info(f"  Opinion {rec['opinion_number']} OK "
                            f"({len(rec['text'])} chars) date={rec['date']}")
                ok += 1
            else:
                logger.warning(f"  no text ({op['url']})")
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["opinion_number"]
        return {
            "_id": f"US/MN-LegalEthics/{num}",
            "_source": "US/MN-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": "Minnesota Lawyers Professional Responsibility Board",
            "title": raw.get("title") or f"LPRB Opinion No. {num}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-MN",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for op in self._list_opinions():
            rec = self._fetch_one(op)
            if not rec:
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

    parser = argparse.ArgumentParser(description="US/MN-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MNLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
