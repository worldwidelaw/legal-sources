#!/usr/bin/env python3
"""
US/LA-LegalEthics -- Louisiana State Bar Association, Rules of Professional
Conduct Committee — PUBLIC Ethics Advisory Opinions.

Fetches the full text of the PUBLIC ethics advisory opinions issued by the
Louisiana State Bar Association (LSBA) Rules of Professional Conduct Committee
(via its Publications Subcommittee). Each opinion answers, on the basis of an
actual advisory-service inquiry from a member, how the Louisiana Rules of
Professional Conduct apply to contemplated attorney conduct = doctrine. Only
opinions the Committee has expressly designated "PUBLIC" are published and may
be cited; the recommended citation is e.g. "LSBA-RPCC PUBLIC Opinion
05-RPCC-001 (04/04/2005)". ~22 public opinions 2005-present.

The LSBA is Louisiana's INTEGRATED (mandatory) bar, established by the
Louisiana Supreme Court, so the 17 U.S.C. § 105 government-edicts rationale
applies fairly directly (like US/SC-LegalEthics). Distinct from US/LA-Courts
and US/LA-Legislation. This is the attorney professional-conduct
advisory-opinion series that in other states we build as US/{ST}-LegalEthics
(lawyers).

Access (no JavaScript execution needed, no CAPTCHA, no auth):
  1. A single public page lists every PUBLIC opinion:
       https://www.lsba.org/members/EthicsAdvisory.aspx
     Each opinion is an anchor whose visible text starts with the number and
     carries the date, e.g. "05-LSBA-RPCC-001 PUBLIC Opinion (04/04/2005)",
     linking to a born-digital PDF under /documents/Ethics/ (filenames are
     irregular, taken from the href verbatim).
  2. Each PDF is born-digital (text layer) — extracted with PyMuPDF (fitz),
     NO OCR needed. The number/date are read from the authoritative index
     anchor (the PDF body occasionally carries a typo, e.g. "21-RPCC-221").

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
from urllib.parse import urljoin, quote

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
logger = logging.getLogger("legal-data-hunter.US.LA-LegalEthics")

BASE = "https://www.lsba.org"
INDEX_URL = "https://www.lsba.org/members/EthicsAdvisory.aspx"

# Anchor text, e.g. "05-LSBA-RPCC-001 PUBLIC Opinion (04/04/2005)"
NUM_RE = re.compile(r"(\d{2})\s*[-–]\s*(?:LSBA[-–])?RPCC\s*[-–]\s*(\d{1,3})", re.I)
DATE_RE = re.compile(r"\((\d{1,2})/(\d{1,2})/((?:19|20)?\d\d)\)")


class LALegalEthicsScraper(BaseScraper):

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
                r = self._session.get(url, timeout=90)
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
    def _norm_num(yy: str, seq: str) -> str:
        """'YY-RPCC-NNN' -> canonical 'YYYY-RPCC-NNN' (2000s corpus,
        sequence zero-padded to 3)."""
        year = 2000 + int(yy)
        return f"{year}-RPCC-{int(seq):03d}"

    def _list_opinions(self) -> list[dict]:
        """Return [{num, url, date}], de-duplicated on num, oldest-first."""
        r = self._get(INDEX_URL)
        if not r:
            logger.error("could not fetch the LSBA ethics opinions index page")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        out: dict[str, dict] = {}
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".pdf" not in href.lower() or "Ethics/" not in href:
                continue
            txt = a.get_text(" ", strip=True)
            m = NUM_RE.search(txt)
            if not m:
                continue
            num = self._norm_num(m.group(1), m.group(2))
            if num in out:
                continue
            # Preserve %-encoding for any spaces in the filename.
            url = urljoin(INDEX_URL, quote(href, safe="/:%?=&"))
            date = None
            dm = DATE_RE.search(txt)
            if dm:
                mo, day, yr = int(dm.group(1)), int(dm.group(2)), int(dm.group(3))
                if yr < 100:
                    yr += 2000
                if 1 <= mo <= 12 and 1 <= day <= 31:
                    date = f"{yr:04d}-{mo:02d}-{day:02d}"
            if date is None:
                date = f"{num[:4]}-01-01"
            out[num] = {"num": num, "url": url, "date": date}
        result = sorted(out.values(), key=lambda x: x["num"])
        logger.info(f"  discovered {len(result)} unique PUBLIC ethics opinions")
        return result

    # -------------------------------------------------------- extraction
    @staticmethod
    def _clean(text: str) -> str:
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
        text = text.replace("​", "").replace("\xa0", " ")
        # Drop the recurring "– Page (N) –" footer markers.
        text = re.sub(r"[–-]\s*Page\s*\(\d+\)\s*[–-]", "", text)
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
            logger.warning(f"  {op['num']}: insufficient text ({len(text)} chars)")
            return None
        return {
            "opinion_number": op["num"],
            "title": f"Louisiana State Bar Association PUBLIC Opinion {op['num']}",
            "text": text,
            "date": op["date"],
            "url": op["url"],
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing LSBA public ethics advisory opinions...")
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
            "_id": f"US/LA-LegalEthics/{num}",
            "_source": "US/LA-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": "Louisiana State Bar Association — Rules of Professional Conduct Committee",
            "title": raw.get("title") or f"LSBA PUBLIC Opinion {num}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-LA",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for op in self._list_opinions():
            rec = self._fetch_one(op)
            if not rec:
                logger.warning(f"  no text for {op['num']}, skipping")
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

    parser = argparse.ArgumentParser(description="US/LA-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = LALegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
