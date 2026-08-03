#!/usr/bin/env python3
"""
US/DE-LegalEthics -- Delaware State Bar Association, Committee on Professional
Ethics — Legal Ethics Opinions.

Fetches the full text of the legal ethics opinions issued by the Delaware
State Bar Association (DSBA) Committee on Professional Ethics. Each opinion
answers, on the basis of an actual member inquiry, how the Delaware Lawyers'
Rules of Professional Conduct apply to contemplated attorney conduct =
doctrine. The recommended citation is e.g. "DSBA Comm. on Prof'l Ethics,
Op. 2009-1". ~55 born-digital opinions 1989-present (the Committee does not
issue opinions every year).

Distinct from US/DE-EthicsOpinions (Delaware Public Integrity Commission —
advisory opinions to state officials/employees under the State Employees',
Officers' and Officials' Code of Conduct). This is the attorney
professional-conduct advisory-opinion series that in other states we build as
US/{ST}-LegalEthics (lawyers).

Access (no JavaScript execution needed, no CAPTCHA, no auth):
  1. A single public index page lists every opinion:
       https://www.dsba.org/publications/ethics-opinions-index/
     Each opinion is an anchor whose visible text is the number (e.g.
     "2009-1") linking to a born-digital PDF on media.dsba.org /
     media1.dsba.org (filenames irregular, taken from the href verbatim);
     the surrounding table row carries a one-line "Rules Discussed" summary.
  2. Each modern PDF (1989-present) is born-digital (text layer) — extracted
     with PyMuPDF (fitz), NO OCR needed. Older opinions (1979-1988) are
     scanned images with no text layer and are skipped (no OCR available).

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
logger = logging.getLogger("legal-data-hunter.US.DE-LegalEthics")

BASE = "https://www.dsba.org"
INDEX_URL = "https://www.dsba.org/publications/ethics-opinions-index/"

# Opinion PDF filenames look like: 1989-1.pdf, 2009-01.pdf, 1987.pdf,
# 2003-2Dissent.pdf, "DSBA PEC Opinion 2021-1.pdf". Pull the YEAR and the
# optional sequence from the filename stem.
FNUM_RE = re.compile(r"((?:19|20)\d{2})(?:\s*[-–]\s*0*(\d{1,3}))?")
MONTHS = (
    "January|February|March|April|May|June|July|August|September|"
    "October|November|December"
)
DATE_RE = re.compile(r"(" + MONTHS + r")\s+(\d{1,2}),\s+((?:19|20)\d{2})")
MONTH_NUM = {
    m: i + 1
    for i, m in enumerate(
        "January February March April May June July August September "
        "October November December".split()
    )
}


class DELegalEthicsScraper(BaseScraper):

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
                # media.dsba.org serves http->https / CloudFront 30x redirects.
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
    def _num_from_filename(href: str) -> str | None:
        """'.../1989-1.pdf' -> '1989-1'; '.../2009-01.pdf' -> '2009-1';
        '.../1987.pdf' -> '1987-1'; '2003-2Dissent.pdf' -> '2003-2-dissent'."""
        stem = unquote(href).rsplit("/", 1)[-1]
        low = stem.lower()
        m = FNUM_RE.search(stem)
        if not m:
            return None
        year = m.group(1)
        seq = m.group(2) or "1"
        num = f"{year}-{int(seq)}"
        if "dissent" in low:
            num += "-dissent"
        return num

    def _list_opinions(self) -> list[dict]:
        """Return [{num, url, summary}], de-duplicated on num, oldest-first."""
        r = self._get(INDEX_URL)
        if not r:
            logger.error("could not fetch the DSBA ethics opinions index page")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        out: dict[str, dict] = {}
        for a in soup.find_all("a", href=True):
            href = a["href"]
            low = href.lower()
            if ".pdf" not in low:
                continue
            if "dsba.org" not in low:
                continue
            # Only the ethics-opinion PDFs (skip request forms, bylaws, etc.).
            if not ("ethics" in low or "committees/ethics" in low):
                continue
            if any(x in low for x in (
                "requestforleo", "bylaws", "administrative", "outline",
            )):
                continue
            num = self._num_from_filename(href)
            if not num or num in out:
                continue
            # Preserve %-encoding for any spaces in the filename; upgrade to
            # https so the first hop doesn't 30x through http.
            url = urljoin(INDEX_URL, quote(href, safe="/:%?=&"))
            if url.startswith("http://"):
                url = "https://" + url[len("http://"):]
            row = a.find_parent("tr") or a.parent
            summary = ""
            if row is not None:
                rt = row.get_text(" ", strip=True)
                # Drop a leading "Opinion NNNN-N ;" / ":" label if present.
                rt = re.sub(r"^\s*Opinion\s+\S+\s*[;:]\s*", "", rt, flags=re.I)
                summary = re.sub(r"\s+", " ", rt).strip()[:400]
            out[num] = {"num": num, "url": url, "summary": summary}
        result = sorted(out.values(), key=lambda x: self._sort_key(x["num"]))
        logger.info(f"  discovered {len(result)} unique ethics opinions")
        return result

    @staticmethod
    def _sort_key(num: str):
        m = re.match(r"(\d{4})-(\d+)", num)
        if m:
            return (int(m.group(1)), int(m.group(2)), num)
        return (9999, 9999, num)

    # -------------------------------------------------------- extraction
    @staticmethod
    def _clean(text: str) -> str:
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
        text = text.replace("​", "").replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _parse_date(text: str, num: str) -> str:
        dm = DATE_RE.search(text[:3000])
        if dm:
            mo = MONTH_NUM.get(dm.group(1))
            day = int(dm.group(2))
            yr = int(dm.group(3))
            if mo and 1 <= day <= 31:
                return f"{yr:04d}-{mo:02d}-{day:02d}"
        return f"{num[:4]}-01-01"

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
            # Older opinions (1979-1988) are scanned images (no text layer).
            logger.warning(f"  {op['num']}: insufficient text ({len(text)} chars, "
                           f"likely scanned) - skipping")
            return None
        date = self._parse_date(text, op["num"])
        return {
            "opinion_number": op["num"],
            "title": (f"Delaware State Bar Association Committee on Professional "
                      f"Ethics — Opinion {op['num']}"),
            "summary": op.get("summary") or "",
            "text": text,
            "date": date,
            "url": op["url"],
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing DSBA ethics opinions index...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions listed")
            return False
        ok = 0
        for op in ops[-3:]:
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
            "_id": f"US/DE-LegalEthics/{num}",
            "_source": "US/DE-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": "Delaware State Bar Association — Committee on Professional Ethics",
            "title": raw.get("title") or f"DSBA Ethics Opinion {num}",
            "summary": raw.get("summary") or "",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-DE",
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

    parser = argparse.ArgumentParser(description="US/DE-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = DELegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
