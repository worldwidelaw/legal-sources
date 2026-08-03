#!/usr/bin/env python3
"""
US/VA-JudicialEthics -- Virginia Judicial Ethics Advisory Committee (JEAC) — Opinions

Fetches the full text of the advisory opinions issued by the Virginia Judicial
Ethics Advisory Committee (JEAC), a committee established by Order of the Supreme
Court of Virginia that issues advisory opinions on the compliance of proposed
future conduct with the Canons of Judicial Conduct for the State of Virginia =
doctrine (official written interpretation of the judicial-conduct rules). The
opinions are advisory only and become public records; the inquiring judge's
identity is kept confidential.

Access (no JavaScript, no CAPTCHA, no auth):
  A single public index lists every opinion; the corpus is served in two formats:

      https://www.vacourts.gov/programs/jeac/opinions/home       (index)
      - older (1999-2008): HTML pages  /programs/jeac/opinions/{YYYY}/{num}
      - newer (2016-present): born-digital PDFs
                              /static/programs/jeac/opinions/{YYYY}/{num}.pdf

  Each index anchor's TEXT is the opinion number (e.g. "99-7", "24-2"); the href
  is taken verbatim. HTML pages carry the full text in <main>; PDFs are
  born-digital (text layer) and extracted with PyMuPDF (fitz), no OCR.

Strategy:
  GET the index, collect (number, href) for every opinion, then fetch each: parse
  the <main> body for HTML pages or extract the PDF text for .pdf links.

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
import subprocess
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.VA-JudicialEthics")

BASE = "https://www.vacourts.gov"
INDEX_URL = BASE + "/programs/jeac/opinions/home"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}
# "Date Issued:  May 8, 2000"
_DATE_ISSUED_RE = re.compile(
    r"Date\s+Issued:?\s*(January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+(\d{1,2}),\s*((?:19|20)\d{2})",
    re.I,
)
# "on Monday the 16th day of September, 2024"
_DATE_DAY_OF_RE = re.compile(
    r"the\s+(\d{1,2})(?:st|nd|rd|th)\s+day\s+of\s+(January|February|March|April|"
    r"May|June|July|August|September|October|November|December),?\s*((?:19|20)\d{2})",
    re.I,
)


class VAJudicialEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0

    # ---------------------------------------------------------------- http
    def _curl(self, url: str, binary: bool = False):
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--compressed", "--max-time", "90",
                     "-A", UA, url],
                    capture_output=True, timeout=120,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout if binary else out.stdout.decode("utf-8", "replace")
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    @staticmethod
    def _canon_number(num: str) -> str:
        """Normalize the anchor number to 'YY-N' (strip zero-pad on the seq)."""
        m = re.match(r"\s*(\d{2})\D+(\d{1,2})", num or "")
        if m:
            return f"{m.group(1)}-{int(m.group(2))}"
        return (num or "").strip()

    @classmethod
    def _parse_index(cls, html: str) -> list[dict]:
        """Return [{number, url, is_pdf}] for every opinion on the index."""
        soup = BeautifulSoup(html, "html.parser")
        out: list[dict] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if "jeac/opinions/" not in href:
                continue
            if href.rstrip("/").endswith("/home"):
                continue
            if not re.search(r"/opinions/\d{4}/", href):
                continue
            number = cls._canon_number(a.get_text(strip=True))
            if not re.match(r"^\d{2}-\d{1,2}$", number) or number in seen:
                continue
            seen.add(number)
            url = href if href.startswith("http") else BASE + href
            out.append({
                "number": number,
                "url": url,
                "is_pdf": href.lower().endswith(".pdf"),
            })
        return out

    def _list_opinions(self, sample: bool = False) -> list[dict]:
        html = self._curl(INDEX_URL)
        if not html:
            return []
        ops = self._parse_index(html)
        logger.info(f"  discovered {len(ops)} opinions on the index")
        # newest first (year prefix descending, 99 -> 1999)
        ops.sort(key=lambda o: (0 if o["number"][:2] >= "90" else 100)
                 + int(o["number"][:2]), reverse=True)
        return ops

    # -------------------------------------------------------- extraction
    @staticmethod
    def _extract_pdf(pdf_bytes: bytes) -> str:
        import fitz  # PyMuPDF
        parts = []
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        try:
            for page in doc:
                parts.append(page.get_text())
        finally:
            doc.close()
        text = "".join(parts)
        text = re.sub(r"[ \t]+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _extract_html(html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        main = soup.find("main") or soup.find("article") or soup.find(id="content")
        if not main:
            return ""
        text = main.get_text("\n", strip=True)
        text = text.replace("\xa0", " ")
        text = re.sub(r"[ \t]+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @classmethod
    def _parse_date(cls, text: str, number: str) -> str | None:
        yy = int(number[:2])
        op_year = 1900 + yy if yy >= 90 else 2000 + yy

        def _ok(y: int) -> bool:
            return abs(y - op_year) <= 1

        for rx in (_DATE_ISSUED_RE, _DATE_DAY_OF_RE):
            for m in rx.finditer(text):
                if rx is _DATE_ISSUED_RE:
                    mon, dd, yr = m.group(1), m.group(2), m.group(3)
                else:
                    dd, mon, yr = m.group(1), m.group(2), m.group(3)
                mo = _MONTHS.get(mon.lower())
                y = int(yr)
                if mo and _ok(y):
                    return f"{y:04d}-{mo:02d}-{int(dd):02d}"
        return f"{op_year:04d}-01-01"

    @staticmethod
    def _derive_subject(text: str, number: str) -> str:
        """The subject sits between the 'Opinion NN-N' header and the
        'All opinions shall be advisory' boilerplate (HTML) / the body."""
        # Everything after the "Date Issued" line up to the advisory disclaimer
        m = re.search(
            r"(?:Date\s+Issued:?[^\n]*\n)(.+?)(?:\n\s*All opinions shall be advisory)",
            text, re.S | re.I,
        )
        if m:
            subj = re.sub(r"\s+", " ", m.group(1)).strip()
            if 3 <= len(subj) <= 300:
                return subj
        return ""

    def _fetch_one(self, op: dict) -> dict | None:
        if op["is_pdf"]:
            pdf_bytes = self._curl(op["url"], binary=True)
            if not pdf_bytes or not pdf_bytes[:5].startswith(b"%PDF"):
                return None
            try:
                text = self._extract_pdf(pdf_bytes)
            except Exception as e:
                logger.warning(f"pdf extract failed for {op['number']}: {e}")
                return None
        else:
            html = self._curl(op["url"])
            if not html:
                return None
            text = self._extract_html(html)
        if not text or len(text) < 200:
            return None
        return {
            "number": op["number"],
            "subject": self._derive_subject(text, op["number"]),
            "text": text,
            "date": self._parse_date(text, op["number"]),
            "url": op["url"],
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Virginia Judicial Ethics Advisory Committee index...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions found on index")
            return False
        logger.info(f"  discovered {len(ops)} opinions")
        ok = 0
        for op in ops[:8]:
            rec = self._fetch_one(op)
            if rec and len(rec["text"]) > 200:
                logger.info(f"  Opinion {rec['number']} OK "
                            f"({len(rec['text'])} chars) date={rec['date']}")
                ok += 1
            if ok >= 3:
                break
        if ok:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw["number"]
        subject = (raw.get("subject") or "").strip()
        title = f"Judicial Ethics Advisory Committee Opinion {number}"
        if subject:
            title += f": {subject}"
        return {
            "_id": f"US/VA-JudicialEthics/{number}",
            "_source": "US/VA-JudicialEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "issuer": "Virginia Judicial Ethics Advisory Committee "
                      "(Supreme Court of Virginia)",
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-VA",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        ops = self._list_opinions(sample=sample)
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

    parser = argparse.ArgumentParser(description="US/VA-JudicialEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = VAJudicialEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
