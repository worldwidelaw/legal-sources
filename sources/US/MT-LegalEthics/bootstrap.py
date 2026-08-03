#!/usr/bin/env python3
"""
US/MT-LegalEthics -- State Bar of Montana, Ethics Committee — Ethics Opinions.

Fetches the full text of the ethics opinions issued by the State Bar of
Montana Ethics Committee. Each opinion interprets the Montana Rules of
Professional Conduct as applied to a member's contemplated conduct to advise
LAWYERS = doctrine. Montana cites its opinions by a six-digit code (e.g.
"Ethics Opinion 970717"). ~106 born-digital opinions, 1985-present.

Access (no JavaScript execution needed, no CAPTCHA, no auth):
  1. A single public page lists every opinion, grouped by Rule of Professional
     Conduct:
       https://www.montanabar.org/For-Attorneys/State-Bar-Resources/Ethics-Opinions
     Each opinion is an anchor to a born-digital PDF under
     /Portals/MONTANA/ (some filenames carry a descriptive title after the
     six-digit code; the SAME opinion is often linked under several rules, so
     records are de-duplicated on the six-digit code).
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
logger = logging.getLogger("legal-data-hunter.US.MT-LegalEthics")

BASE = "https://www.montanabar.org"
INDEX_URL = ("https://www.montanabar.org/For-Attorneys/"
             "State-Bar-Resources/Ethics-Opinions")

# Opinion filenames lead with a six-digit code, optionally a "_N" variant and
# an optional descriptive title, e.g. "870522 Candor Toward the Tribunal.pdf",
# "900517_1.pdf", "970717.pdf".
NUM_RE = re.compile(r"^\s*(\d{6})(_\d+)?")


class MTLegalEthicsScraper(BaseScraper):

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
    def _decode_date(num6: str) -> str | None:
        """Best-effort decode of the six-digit code to an ISO date. Montana
        codes are mostly YYMMDD (e.g. 970717 -> 1997-07-17) but a few are
        MMDDYY (e.g. 112314 -> 2014-11-23); try YYMMDD first, then MMDDYY."""
        a, b, c = int(num6[0:2]), int(num6[2:4]), int(num6[4:6])
        for yy, mm, dd in ((a, b, c), (c, a, b)):
            if 1 <= mm <= 12 and 1 <= dd <= 31:
                year = 1900 + yy if yy >= 50 else 2000 + yy
                return f"{year:04d}-{mm:02d}-{dd:02d}"
        return None

    def _list_opinions(self) -> list[dict]:
        """Return [{num, url, title}], de-duplicated on the six-digit code."""
        r = self._get(INDEX_URL)
        if not r:
            logger.error("could not fetch the Montana ethics opinions index page")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        out: dict[str, dict] = {}
        for a in soup.find_all("a", href=True):
            href = a["href"]
            low = href.lower()
            if ".pdf" not in low or "montana" not in low:
                continue
            stem = unquote(href.split("?")[0]).rsplit("/", 1)[-1]
            if stem.lower().endswith(".pdf"):
                stem = stem[:-4]
            m = NUM_RE.match(stem)
            if not m:
                # A few entries have no six-digit code (descriptive-only slugs);
                # these duplicate numbered opinions, so skip them.
                continue
            # Dedup on the BASE six-digit code: the "_N" filename variants are
            # byte-identical copies of the same opinion filed under several
            # rules, so they must collapse to one record.
            num = m.group(1)
            # Descriptive title after the code, if any.
            desc = re.sub(r"\s+", " ", stem[m.end():].strip(" -_").strip())
            url = urljoin(BASE, quote(href, safe="/:%?=&"))
            if num in out:
                # Prefer the copy that carries a descriptive title / no suffix.
                if desc and not out[num]["title"]:
                    out[num] = {"num": num, "url": url, "title": desc}
                continue
            out[num] = {"num": num, "url": url, "title": desc}
        result = sorted(out.values(), key=lambda x: x["num"])
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
        base = op["num"].split("_")[0]
        title = f"State Bar of Montana Ethics Opinion {op['num']}"
        if op.get("title"):
            title += f" — {op['title']}"
        return {
            "opinion_number": op["num"],
            "title": title,
            "text": text,
            "date": self._decode_date(base),
            "url": op["url"],
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Montana ethics opinions index...")
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
            "_id": f"US/MT-LegalEthics/{num}",
            "_source": "US/MT-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": "State Bar of Montana — Ethics Committee",
            "title": raw.get("title") or f"Montana Ethics Opinion {num}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-MT",
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

    parser = argparse.ArgumentParser(description="US/MT-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MTLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
