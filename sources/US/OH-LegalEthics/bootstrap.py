#!/usr/bin/env python3
"""
US/OH-LegalEthics -- Ohio Board of Professional Conduct — Advisory Opinions

Fetches the full text of the Advisory Opinions issued by the Ohio Board of
Professional Conduct (formerly the Board of Commissioners on Grievances &
Discipline). Each opinion is the Board's nonbinding written interpretation,
issued in response to a prospective or hypothetical question, of the ethics
rules applicable to Ohio judges AND lawyers -- the Ohio Rules of Professional
Conduct, the Ohio Code of Judicial Conduct, the Rules for the Government of
the Bar and (for pre-2007 opinions) the former Code of Professional
Responsibility (CPR) = doctrine (the state's official written interpretation
of the attorney/judicial-conduct rules).

The corpus is one continuous per-year numbered series ("{YY}-{NNN}",
e.g. 86-001 .. 90-001 .. 22-01 .. present), ~458 opinions 1986-present.
Distinct from US/OH-EthicsOpinions class (executive Ohio Ethics Commission —
public officials) and US/OH-AGOpinions (Attorney General); this is the
attorney/judge professional-conduct advisory-opinion series that in other
states we split into US/{ST}-LegalEthics (lawyers) and US/{ST}-JudicialEthics
(judges) — Ohio issues both from a single Board, so they are combined here.

Access (no JavaScript execution needed, no CAPTCHA, no auth):
  1. The opinions are published on the Board's dedicated public site
     ohioadvop.org (WordPress). Its REST API enumerates one page per year:
       https://ohioadvop.org/wp-json/wp/v2/pages?per_page=100&_fields=id,title,content
     Each page whose title is a 4-digit year carries, in content.rendered,
     an <a href="...pdf">Op. YY-NNN</a> link per opinion issued that year.
  2. Each opinion PDF is born-digital (text layer) — extracted with PyMuPDF,
     NO OCR needed, back to 1986. Body carries the Board letterhead, the
     opinion number, syllabus and full discussion.

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
logger = logging.getLogger("legal-data-hunter.US.OH-LegalEthics")

BASE = "https://ohioadvop.org"
PAGES_API = BASE + "/wp-json/wp/v2/pages?per_page=100&_fields=id,title,content&page={page}"
YEAR_RE = re.compile(r"^(19|20)\d\d$")
# Opinion number in the anchor text, e.g. "Op. 22-01", "Op 90-001", "Opinion 2016-5"
NUM_RE = re.compile(r"\b(\d{2,4})\s*-\s*(\d{1,3})\b")
MONTH_DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2}),\s+((?:19|20)\d\d)\b"
)
_MONTHS = {m: i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}


class OHLegalEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            ),
            "Accept": "application/json,text/html,application/pdf,*/*",
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
    def _norm_num(raw_num: str, page_year: int) -> str | None:
        """Normalise an anchor-text opinion number to canonical 'YYYY-NNN'.
        raw_num is the digits before the dash (2 or 4 digits)."""
        seq_prefix, seq = raw_num
        if len(seq_prefix) == 4:
            year = int(seq_prefix)
        else:
            yy = int(seq_prefix)
            year = 1900 + yy if yy >= 80 else 2000 + yy
        # sanity: opinion year should be within a couple years of its page
        if not (1985 <= year <= page_year + 1):
            year = page_year
        return f"{year}-{int(seq):03d}"

    def _list_opinions(self) -> list[tuple[str, str, str, int]]:
        """Return [(opinion_number 'YYYY-NNN', title, pdf_url, year)],
        de-duplicated on opinion_number, ordered oldest-first."""
        pages: list[dict] = []
        for page in range(1, 4):
            r = self._get(PAGES_API.format(page=page))
            if not r:
                break
            try:
                batch = r.json()
            except Exception:
                break
            if not isinstance(batch, list) or not batch:
                break
            pages.extend(batch)
            if len(batch) < 100:
                break

        out: dict[str, tuple[str, str, str, int]] = {}
        for p in pages:
            title = (p.get("title", {}) or {}).get("rendered", "").strip()
            if not YEAR_RE.match(title):
                continue
            year = int(title)
            html = (p.get("content", {}) or {}).get("rendered", "")
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if ".pdf" not in href.lower():
                    continue
                label = a.get_text(" ", strip=True)
                m = NUM_RE.search(label) or NUM_RE.search(href.rsplit("/", 1)[-1])
                if not m:
                    continue
                num = self._norm_num(m.groups(), year)
                if not num or num in out:
                    continue
                # title: strip a leading "Op. / Opinion {num}" prefix if present
                t = re.sub(r"^\s*(?:Adv\.?\s*)?Op(?:inion)?\.?\s*[\d\-]+\s*[:.\-–]?\s*",
                           "", label).strip()
                out[num] = (num, t, href, year)
        result = sorted(out.values(), key=lambda x: x[0])
        logger.info(f"  discovered {len(result)} unique advisory opinions")
        return result

    # -------------------------------------------------------- extraction
    @staticmethod
    def _clean(text: str) -> str:
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

        # Date: the issue date is the "Month DD, YYYY" line right under the
        # "OPINION {num}" header. Prefer a match in the header region
        # (first ~700 chars) to avoid picking up a date cited deep in the
        # body; else fall back to the first in-range date; else year-01-01.
        date = f"{year}-01-01"
        header = text[:700]
        best = None
        for scope in (header, text):
            for mm in MONTH_DATE_RE.finditer(scope):
                mon, day, yr = mm.group(1), int(mm.group(2)), int(mm.group(3))
                if abs(yr - year) <= 1:
                    best = f"{yr:04d}-{_MONTHS[mon]:02d}-{day:02d}"
                    break
            if best:
                break
        if best:
            date = best

        # Title: the SYLLABUS line is the Board's topical summary of the
        # opinion; use its first sentence (capped) as the title.
        if not title:
            ms = re.search(r"SYLLABUS:\s*(.+?)(?:\n\n|\Z)", text, re.S)
            if ms:
                syl = re.sub(r"\s+", " ", ms.group(1)).strip()
                syl = re.split(r"(?<=[.;])\s", syl, 1)[0]
                title = syl[:200].strip()

        return {
            "opinion_number": num,
            "title": title or f"Ohio Advisory Opinion {num}",
            "text": text,
            "date": date,
            "url": url,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Ohio Board of Professional Conduct advisory opinions...")
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
            "_id": f"US/OH-LegalEthics/{num}",
            "_source": "US/OH-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": "Ohio Board of Professional Conduct",
            "title": raw.get("title"),
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-OH",
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

    parser = argparse.ArgumentParser(description="US/OH-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = OHLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
