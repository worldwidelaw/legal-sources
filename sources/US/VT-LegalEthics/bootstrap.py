#!/usr/bin/env python3
"""
US/VT-LegalEthics -- Vermont Bar Association — Advisory Ethics Opinions

Fetches the full text of the Advisory Ethics Opinions issued by the Vermont
Bar Association's Professional Responsibility Committee (formerly the Committee
on Professional Ethics). Each opinion is the Committee's advisory answer to a
member's inquiry, interpreting the Vermont Rules of Professional Conduct
(formerly the Code of Professional Responsibility) to advise LAWYERS =
doctrine (advisory, no weight of law). One continuous per-year numbered series
("YY-NN"), ~312 opinions 1978-present.

Distinct from US/VT-Legislation and any Vermont judicial/AG sources. This is
the attorney professional-conduct advisory-opinion series that in other states
we build as US/{ST}-LegalEthics (lawyers).

Access (no JavaScript execution needed, no CAPTCHA, no auth):
  1. The opinions are published on the VBA site under /advisory_ethics/, which
     is a WordPress custom-post-type archive organised by TOPIC (category)
     pages. The archive index paginates at /advisory_ethics/page/{n}/; each
     entry links to a topic page /advisory_ethics/{topic}/.
  2. Each topic page lists that topic's opinions as direct born-digital PDF
     links /wp-content/uploads/{YYYY}/{MM}/{YY-NN}[-v].pdf. The same opinion
     can appear under several topics, so opinions are de-duplicated on the
     canonical number. (The site's wp-json REST API is WAF-403, so the archive
     HTML is scraped directly with a browser UA.)
  3. Each opinion PDF is born-digital (text layer) — extracted with PyMuPDF,
     NO OCR needed. Body carries "ADVISORY ETHICS OPINION {num}" / "OPINION
     {num}", a SYNOPSIS summary and full discussion.

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
logger = logging.getLogger("legal-data-hunter.US.VT-LegalEthics")

BASE = "https://www.vtbar.org"
ARCHIVE = BASE + "/advisory_ethics/"
# PDF opinion files, e.g. /wp-content/uploads/2021/03/79-03.pdf, 15-03-2.pdf
PDF_RE = re.compile(
    r'(/wp-content/uploads/\d{4}/\d{2}/(\d{2})-(\d{1,2})(?:-\d+)?\.pdf)', re.I)
# Opinion number as stated in the PDF body, e.g. "OPINION 2006-1",
# "ADVISORY ETHICS OPINION 79-03"
BODY_NUM_RE = re.compile(
    r'(?:ADVISORY\s+ETHICS\s+)?OPINION\s+((?:19|20)?\d\d)\s*[-–]\s*(\d{1,3})', re.I)
MONTH_DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+((?:19|20)\d\d)\b",
    re.I,
)
_MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}


class VTLegalEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.6
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            ),
            "Accept": "text/html,application/pdf,*/*",
            "Accept-Language": "en-US,en;q=0.9",
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
    def _canon(yy: str, nn: str) -> str:
        y = int(yy)
        year = 1900 + y if y >= 78 else 2000 + y
        return f"{year}-{int(nn):02d}"

    def _topic_pages(self) -> list[str]:
        """Enumerate every topic (category) page URL under /advisory_ethics/."""
        slugs: dict[str, None] = {}
        for pg in range(1, 15):
            url = ARCHIVE if pg == 1 else f"{ARCHIVE}page/{pg}/"
            r = self._get(url)
            if not r:
                break
            soup = BeautifulSoup(r.text, "html.parser")
            arts = soup.find_all("article")
            if not arts:
                break
            found = 0
            for a in arts:
                link = a.find("a", href=True)
                if not link or "/advisory_ethics/" not in link["href"]:
                    continue
                sl = link["href"].split("/advisory_ethics/")[-1].strip("/")
                if sl and "page/" not in sl and sl not in slugs:
                    slugs[sl] = None
                    found += 1
            logger.info(f"  index page {pg}: {found} new topics")
        return [f"{ARCHIVE}{sl}/" for sl in slugs]

    def _list_opinions(self) -> list[tuple[str, str]]:
        """Return [(canonical_number 'YYYY-NN', pdf_url)], de-duplicated,
        preferring the /wp-content/ upload path, ordered oldest-first."""
        out: dict[str, str] = {}
        for turl in self._topic_pages():
            r = self._get(turl)
            if not r:
                continue
            for m in PDF_RE.finditer(r.text):
                path, yy, nn = m.group(1), m.group(2), m.group(3)
                num = self._canon(yy, nn)
                if num not in out:
                    out[num] = BASE + path
        result = sorted(out.items(), key=lambda x: x[0])
        logger.info(f"  discovered {len(result)} unique advisory opinions")
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

    @staticmethod
    def _pdf_text(content: bytes) -> str:
        if fitz is None:
            raise RuntimeError("PyMuPDF (fitz) is required for PDF extraction")
        doc = fitz.open(stream=content, filetype="pdf")
        try:
            return "".join(page.get_text() for page in doc)
        finally:
            doc.close()

    def _fetch_one(self, num: str, url: str) -> dict | None:
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

        year = int(num[:4])
        # Date: best-effort — VBA opinions usually carry only a number, so
        # search the body for a "Month DD, YYYY" in range; else YYYY-01-01.
        date = f"{year}-01-01"
        for mm in MONTH_DATE_RE.finditer(text):
            mon, day, yr = mm.group(1).lower(), int(mm.group(2)), int(mm.group(3))
            if abs(yr - year) <= 1:
                date = f"{yr:04d}-{_MONTHS[mon]:02d}-{day:02d}"
                break

        # Title: first sentence of the SYNOPSIS summary (the Committee's own
        # topical summary); else fall back to a generic label.
        title = ""
        ms = re.search(r"SYNOPSIS\s*:?\s*(.+?)(?:\n\n|\Z)", text, re.S | re.I)
        if ms:
            syl = re.sub(r"\s+", " ", ms.group(1)).strip()
            syl = re.split(r"(?<=[.;])\s", syl, 1)[0]
            title = syl[:200].strip()
            # reject a synopsis that begins with a bare section marker
            # (e.g. "I.", "1.", "A.") — not a meaningful title
            if len(title) < 10 or re.fullmatch(r"[IVXLC0-9A-Z]{1,4}\.?", title):
                title = ""

        return {
            "opinion_number": num,
            "title": title or f"VBA Advisory Ethics Opinion {num}",
            "text": text,
            "date": date,
            "url": url,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Vermont Bar Association advisory ethics opinions...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions listed")
            return False
        ok = 0
        for num, url in ops[:2] + ops[-1:]:
            rec = self._fetch_one(num, url)
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
            "_id": f"US/VT-LegalEthics/{num}",
            "_source": "US/VT-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": ("Vermont Bar Association — Professional "
                       "Responsibility Committee"),
            "title": raw.get("title") or f"VBA Advisory Ethics Opinion {num}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-VT",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for num, url in self._list_opinions():
            rec = self._fetch_one(num, url)
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

    parser = argparse.ArgumentParser(description="US/VT-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = VTLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
