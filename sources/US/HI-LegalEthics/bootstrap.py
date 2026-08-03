#!/usr/bin/env python3
"""
US/HI-LegalEthics -- Disciplinary Board of the Hawaiʻi Supreme Court —
Formal (Ethics) Opinions.

Fetches the full text of the Formal Opinions issued by the Disciplinary Board
of the Hawaiʻi Supreme Court (the Board and its Office of Disciplinary Counsel
are arms of the Hawaiʻi Supreme Court). Each Formal Opinion interprets the
Hawaiʻi Rules of Professional Conduct as applied to a lawyer's contemplated
conduct to advise LAWYERS = doctrine. Hawaiʻi cites its opinions by number
("Formal Opinion No. 44"). ~19 born-digital PDF opinions.

Access (no JavaScript execution needed, no CAPTCHA, no auth):
  1. A single public page lists every Formal Opinion PDF:
       https://dbhawaii.org/legal-ethics-advice-for-hawaii-lawyers/
     Each opinion is an anchor (topic title) to a born-digital PDF under
     /wp-content/uploads/ (WordPress).
  2. Each PDF is born-digital (text layer) — extracted with PyMuPDF (fitz),
     NO OCR. Records under 200 chars (rare scanned scans) are skipped.

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
from urllib.parse import urljoin, quote, unquote

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
logger = logging.getLogger("legal-data-hunter.US.HI-LegalEthics")

BASE = "https://dbhawaii.org"
INDEX_URL = "https://dbhawaii.org/legal-ethics-advice-for-hawaii-lawyers/"

# Formal-opinion PDFs are named FO_24.pdf, FO_18_and_22.pdf, FO4603-19-15.pdf,
# Formal-Opinion-50.pdf, FO_40_-_E-MAIL_SECURITY.pdf, etc.
FO_FNAME_RE = re.compile(r"(?i)^(?:FO|Formal[-_ ]?Opinion)[-_ ]?\d")

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}
DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2}),?\s+(\d{4})\b"
)


class HILegalEthicsScraper(BaseScraper):

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
    def _list_opinions(self) -> list[dict]:
        """Return [{title, url, fname}] for every Formal Opinion PDF."""
        r = self._get(INDEX_URL)
        if not r:
            logger.error("could not fetch the Hawaiʻi ethics opinions index")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        out: dict[str, dict] = {}
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".pdf" not in href.lower():
                continue
            fname = unquote(href.split("?")[0]).rsplit("/", 1)[-1]
            if not FO_FNAME_RE.match(fname):
                # Skip non-opinion PDFs (procedures memo, journal article).
                continue
            url = urljoin(BASE, quote(href, safe="/:%?=&"))
            title = re.sub(r"\s+", " ", a.get_text(" ", strip=True)).strip()
            # Drop the "(amended <date>)" parenthetical from the topic title.
            title = re.sub(r"\s*\(am[ae]nded[^)]*\)", "", title, flags=re.I).strip()
            if url in out:
                continue
            out[url] = {"title": title, "url": url, "fname": fname}
        result = list(out.values())
        logger.info(f"  discovered {len(result)} Formal Opinion PDFs")
        return result

    # -------------------------------------------------------- extraction
    @staticmethod
    def _clean(text: str) -> str:
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
        text = text.replace("​", "").replace("\xa0", " ")
        # Older PDFs render the Hawaiian ʻokina as "#" (broken ToUnicode).
        text = text.replace("Hawai#i", "Hawaiʻi")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _opinion_number(fname: str, text: str) -> str | None:
        """Derive the Formal Opinion number. Prefer the body header
        ("FORMAL OPINION NO. 24"); fall back to the filename."""
        m = re.search(
            r"FORMAL OPINION\s+NOS?\.?\s*(\d+)(?:\s*(?:,|and|&)\s*(\d+))?",
            text, re.I,
        )
        if m:
            return m.group(1) + (f"-{m.group(2)}" if m.group(2) else "")
        m2 = re.search(r"(\d+)[_-]?and[_-]?(\d+)", fname, re.I)
        if m2:
            return f"{m2.group(1)}-{m2.group(2)}"
        m3 = re.match(r"(?i)(?:FO|Formal[-_ ]?Opinion)[-_ ]?(\d+)", fname)
        if m3:
            return m3.group(1)
        return None

    @staticmethod
    def _issue_date(text: str) -> str | None:
        """First 'Month DD, YYYY' in the body = the original issue date."""
        m = DATE_RE.search(text)
        if not m:
            return None
        month = MONTHS[m.group(1).lower()]
        day = int(m.group(2))
        year = int(m.group(3))
        try:
            return datetime(year, month, day).date().isoformat()
        except ValueError:
            return None

    def _fetch_one(self, op: dict) -> dict | None:
        r = self._get(op["url"])
        if not r or not r.content:
            return None
        try:
            doc = fitz.open(stream=r.content, filetype="pdf")
            text = self._clean("\n".join(p.get_text() for p in doc))
        except Exception as e:
            logger.warning(f"  {op['fname']}: PDF parse failed: {e}")
            return None
        if len(text) < 200:
            logger.warning(f"  {op['fname']}: insufficient text ({len(text)} "
                           f"chars, likely scanned) - skipping")
            return None
        num = self._opinion_number(op["fname"], text)
        if not num:
            logger.warning(f"  {op['fname']}: could not derive opinion number")
            return None
        title = f"Hawaiʻi Formal Opinion No. {num}"
        if op["title"]:
            title += f" — {op['title']}"
        return {
            "opinion_number": num,
            "title": title,
            "text": text,
            "date": self._issue_date(text),
            "url": op["url"],
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Hawaiʻi ethics opinions index...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions listed")
            return False
        ok = 0
        for op in ops[:3]:
            rec = self._fetch_one(op)
            if rec and len(rec["text"]) > 500:
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
            "_id": f"US/HI-LegalEthics/{num}",
            "_source": "US/HI-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": "Disciplinary Board of the Hawaiʻi Supreme Court",
            "title": raw.get("title") or f"Hawaiʻi Formal Opinion No. {num}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-HI",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        seen: set[str] = set()
        emitted = 0
        for op in self._list_opinions():
            rec = self._fetch_one(op)
            if not rec:
                continue
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

    parser = argparse.ArgumentParser(description="US/HI-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = HILegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
