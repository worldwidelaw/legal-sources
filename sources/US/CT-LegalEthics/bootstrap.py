#!/usr/bin/env python3
"""
US/CT-LegalEthics -- Connecticut Bar Association — Informal Ethics Opinions

Fetches the full text of the Informal Opinions issued by the Connecticut Bar
Association's Standing Committee on Professional Ethics (now the Committee on
Professional Ethics and the Unauthorized Practice of Law). Each Informal
Opinion is the Committee's written response to a member's inquiry, interpreting
the Connecticut Rules of Professional Conduct = doctrine (advisory, no weight
of law). The corpus published free on ctbar.org runs from 2011 to present
(one numbered series "YY-NN", ~74 opinions); older Formal & Informal Opinions
back to 1986 are only on the paywalled Casemaker/vLex service and are excluded.

Distinct from US/CT-EthicsOpinions (the executive Connecticut Office of State
Ethics — public officials) and US/CT-Courts. This is the attorney
professional-conduct advisory-opinion series that in other states we build as
US/{ST}-LegalEthics (lawyers).

Access (no JavaScript execution needed, no CAPTCHA, no auth):
  1. The opinions are listed on the CBA's public page
       https://www.ctbar.org/news/CBAPublications/informal-ethics-opinions
     Each opinion is a direct born-digital PDF link whose anchor text is
     "Informal Opinion {YY-NN} | {Title}", so the number and title come
     straight from the index; the year folder in the PDF path
     (/…/{YYYY}-opinions/…) gives the canonical 4-digit year.
  2. Each opinion PDF is born-digital (text layer) — extracted with PyMuPDF,
     NO OCR needed. Body carries the CBA letterhead, the opinion number,
     the approved/issue date and full discussion.

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

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CT-LegalEthics")

BASE = "https://www.ctbar.org"
INDEX_URL = BASE + "/news/CBAPublications/informal-ethics-opinions"
PATH_MARK = "ethics-opinions-informal-opinions"
# Anchor text like "Informal Opinion 25-01 | Virtual Unbundled Services ..."
ANCHOR_RE = re.compile(
    r"Informal\s+Opinion\s+(\d{2,4})\s*[-–—]\s*(\d{1,3})\s*[|:–—-]?\s*(.*)",
    re.I,
)
# Canonical year folder in the PDF path, e.g. /2011-opinions/ or /2012/
FOLDER_YEAR_RE = re.compile(r"/((?:19|20)\d\d)(?:-opinions)?/[^/]+\.pdf", re.I)
MONTH_DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+((?:19|20)\d\d)\b",
    re.I,
)
_MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}


class CTLegalEthicsScraper(BaseScraper):

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
    def _norm_num(seq_prefix: str, seq: str, folder_year: int | None) -> str:
        """Normalise an opinion number to canonical 'YYYY-NN'.
        seq_prefix is the digits before the dash (2 or 4 digits)."""
        if len(seq_prefix) == 4:
            year = int(seq_prefix)
        else:
            yy = int(seq_prefix)
            year = 1900 + yy if yy >= 80 else 2000 + yy
        # Prefer the year folder from the PDF path when it agrees (±1yr);
        # it is the authoritative publication year.
        if folder_year and abs(folder_year - year) <= 1:
            year = folder_year
        return f"{year}-{int(seq):02d}"

    def _list_opinions(self) -> list[tuple[str, str, str, int]]:
        """Return [(opinion_number 'YYYY-NN', title, pdf_url, year)],
        de-duplicated on opinion_number, ordered oldest-first."""
        r = self._get(INDEX_URL)
        if not r:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        out: dict[str, tuple[str, str, str, int]] = {}
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if PATH_MARK not in href.lower() or ".pdf" not in href.lower():
                continue
            if href.startswith("/"):
                href = BASE + href
            fy = FOLDER_YEAR_RE.search(href)
            folder_year = int(fy.group(1)) if fy else None
            label = a.get_text(" ", strip=True)
            m = ANCHOR_RE.search(label)
            if not m:
                continue
            num = self._norm_num(m.group(1), m.group(2), folder_year)
            if num in out:
                continue
            title = re.sub(r"\s+", " ", m.group(3) or "").strip(" |-–—:").strip()
            year = int(num[:4])
            out[num] = (num, title, href, year)
        result = sorted(out.values(), key=lambda x: x[0])
        logger.info(f"  discovered {len(result)} unique informal opinions")
        return result

    # -------------------------------------------------------- extraction
    @staticmethod
    def _clean(text: str) -> str:
        # drop PDF control-char artifacts (e.g. \x01 markers) and zero-widths
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
        text = text.replace("​", "").replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _pdf_text(content: bytes) -> str:
        if fitz is None:
            raise RuntimeError("PyMuPDF (fitz) is required for PDF extraction")
        doc = fitz.open(stream=content, filetype="pdf")
        try:
            return "".join(page.get_text() for page in doc)
        finally:
            doc.close()

    def _fetch_one(self, num: str, title: str, url: str, year: int) -> dict | None:
        r = self._get(url)
        if not r or not r.content:
            return None
        ctype = r.headers.get("content-type", "").lower()
        if "pdf" not in ctype and r.content[:4] != b"%PDF":
            logger.warning(f"  {num}: not a PDF ({ctype})")
            return None
        try:
            raw = self._pdf_text(r.content)
        except Exception as e:
            logger.warning(f"  {num}: PDF extract failed: {e}")
            return None
        text = self._clean(raw)
        if len(text) < 150:
            return None

        # Date: the "Approved Month DD, YYYY" / issue-date line in the header.
        # Prefer a match in the header region (first ~700 chars); else the
        # first in-range date; else year-01-01.
        date = f"{year}-01-01"
        best = None
        for scope in (text[:700], text):
            for mm in MONTH_DATE_RE.finditer(scope):
                mon, day, yr = mm.group(1).lower(), int(mm.group(2)), int(mm.group(3))
                if abs(yr - year) <= 1:
                    best = f"{yr:04d}-{_MONTHS[mon]:02d}-{day:02d}"
                    break
            if best:
                break
        if best:
            date = best

        return {
            "opinion_number": num,
            "title": title,
            "text": text,
            "date": date,
            "url": url,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Connecticut Bar Association informal ethics opinions...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions listed")
            return False
        ok = 0
        for num, title, url, year in ops[:2] + ops[-1:]:
            rec = self._fetch_one(num, title, url, year)
            if rec and len(rec["text"]) > 300:
                logger.info(f"  Opinion {rec['opinion_number']} OK "
                            f"({len(rec['text'])} chars) date={rec['date']}")
                ok += 1
            else:
                logger.warning(f"  no text ({url})")
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["opinion_number"]
        return {
            "_id": f"US/CT-LegalEthics/{num}",
            "_source": "US/CT-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": ("Connecticut Bar Association — Committee on "
                       "Professional Ethics"),
            "title": raw.get("title") or f"CBA Informal Opinion {num}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-CT",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for num, title, url, year in self._list_opinions():
            rec = self._fetch_one(num, title, url, year)
            if not rec:
                logger.warning(f"  no text for {num}, skipping")
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

    parser = argparse.ArgumentParser(description="US/CT-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CTLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
