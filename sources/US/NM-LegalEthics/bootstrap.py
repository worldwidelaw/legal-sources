#!/usr/bin/env python3
"""
US/NM-LegalEthics -- State Bar of New Mexico, Ethics Advisory Committee —
Ethics Advisory Opinions.

Fetches the full text of the formal ethics advisory opinions issued by the
State Bar of New Mexico Ethics Advisory Committee. Each opinion interprets the
New Mexico Rules of Professional Conduct as applied to a lawyer's contemplated
conduct to advise LAWYERS = doctrine. New Mexico cites its opinions by a
"{year}-{n}" number (e.g. "Advisory Opinion 1987-5"). ~94 born-digital PDF
opinions, 1983-present.

Access (no JavaScript execution needed, no CAPTCHA, no auth):
  1. A single public page lists every opinion PDF, grouped by year:
       https://www.sbnm.org/Leadership/Committees/Ethics-Advisory-Committee/
       Ethics-Advisory-Opinions
     Each opinion is an anchor to a born-digital PDF under
     /Portals/NMBAR/AboutUs/committees/Ethics/{year-folder}/{YYYY-N}.pdf.
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
logger = logging.getLogger("legal-data-hunter.US.NM-LegalEthics")

BASE = "https://www.sbnm.org"
INDEX_URL = ("https://www.sbnm.org/Leadership/Committees/"
             "Ethics-Advisory-Committee/Ethics-Advisory-Opinions")

# Ethics opinion PDFs live under .../committees/Ethics/{folder}/{YYYY-N}.pdf.
ETHICS_PATH = "/committees/ethics/"
# Filename stem is the opinion number: 4-digit year, dash, sequence (digits,
# optional trailing letter), e.g. 1987-5, 2021-001, 1984-13.
STEM_RE = re.compile(r"^(\d{4})-(\d+)([A-Za-z]?)$")


class NMLegalEthicsScraper(BaseScraper):

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
    @staticmethod
    def _canon(year: str, seq: str, suffix: str) -> str:
        """Canonicalise the opinion number: drop zero-padding on the sequence
        so 2021-001 -> 2021-1, keep an optional trailing letter."""
        return f"{year}-{int(seq)}{suffix.upper()}"

    def _list_opinions(self) -> list[dict]:
        """Return [{num, year, url}], de-duplicated on the canonical number."""
        r = self._get(INDEX_URL)
        if not r:
            logger.error("could not fetch the New Mexico ethics opinions index")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        out: dict[str, dict] = {}
        for a in soup.find_all("a", href=True):
            href = a["href"]
            low = href.lower()
            if ETHICS_PATH not in low or ".pdf" not in low:
                continue
            stem = unquote(href.split("?")[0]).rsplit("/", 1)[-1]
            if stem.lower().endswith(".pdf"):
                stem = stem[:-4]
            m = STEM_RE.match(stem.strip())
            if not m:
                # A few non-opinion PDFs (guidelines, mission) live in the same
                # folder tree; skip anything not matching the YYYY-N pattern.
                continue
            year, seq, suffix = m.group(1), m.group(2), m.group(3)
            num = self._canon(year, seq, suffix)
            url = urljoin(BASE, quote(href, safe="/:%?=&"))
            if num in out:
                continue
            out[num] = {"num": num, "year": year, "url": url}
        result = sorted(out.values(),
                        key=lambda x: (int(x["year"]), x["num"]))
        logger.info(f"  discovered {len(result)} unique ethics opinions")
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
            logger.warning(f"  {op['num']}: PDF parse failed: {e}")
            return None
        if len(text) < 200:
            logger.warning(f"  {op['num']}: insufficient text ({len(text)} chars, "
                           f"likely scanned) - skipping")
            return None
        title = f"State Bar of New Mexico Ethics Advisory Opinion {op['num']}"
        return {
            "opinion_number": op["num"],
            "title": title,
            "text": text,
            "date": f"{op['year']}-01-01",
            "url": op["url"],
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing New Mexico ethics opinions index...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions listed")
            return False
        ok = 0
        for op in ops[:2] + ops[-1:]:
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
            "_id": f"US/NM-LegalEthics/{num}",
            "_source": "US/NM-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": "State Bar of New Mexico — Ethics Advisory Committee",
            "title": raw.get("title") or f"New Mexico Ethics Advisory Opinion {num}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-NM",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        # Newest opinion first so a sample spans the recent, richer opinions.
        ops = list(reversed(self._list_opinions()))
        emitted = 0
        for op in ops:
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

    parser = argparse.ArgumentParser(description="US/NM-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NMLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
