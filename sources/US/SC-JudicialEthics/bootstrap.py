#!/usr/bin/env python3
"""
US/SC-JudicialEthics -- South Carolina Advisory Committee on Standards of
                        Judicial Conduct — Advisory Opinions

Fetches the full text of the advisory opinions issued by the South Carolina
Advisory Committee on Standards of Judicial Conduct, a committee created by the
Supreme Court of South Carolina (Rule 503, SCACR) that renders written advisory
opinions to inquiring judges on the propriety of contemplated judicial and
nonjudicial conduct under the South Carolina Code of Judicial Conduct = doctrine
(official written interpretation of the judicial-conduct rules). Upon filing
with the Clerk of the Supreme Court the opinions become a matter of public
record; the requesting judge's identity is kept confidential.

Access (no JavaScript, no CAPTCHA, no auth):
  The Committee publishes a per-year index that links every opinion, and each
  opinion is a born-digital PDF (text layer, no OCR needed):

      https://www.sccourts.org/opinions-orders/judicial-advisory-opinions/?year=YYYY
      https://www.sccourts.org/media/advisoryOpinions/html/{NN}-{YYYY}.pdf

  The index page for a given year lists each opinion as a `.result` block with a
  link to the opinion PDF (anchor text = "NN-YYYY") and a `p.subtitle` holding
  the subject ("RE:" line). We iterate the years 1989..present, collect the
  (number, subject, pdf-url) triples, download each PDF and extract the full
  text with PyMuPDF (fitz).

Strategy:
  For each year 1989..current, GET the year index, collect every opinion PDF
  link + its subtitle, then download each PDF and extract the body text.

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
logger = logging.getLogger("legal-data-hunter.US.SC-JudicialEthics")

BASE = "https://www.sccourts.org"
INDEX_URL = BASE + "/opinions-orders/judicial-advisory-opinions/?year={year}"
PDF_URL = BASE + "/media/advisoryOpinions/html/{number}.pdf"
FIRST_YEAR = 1989
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}
# "Sep. 2, 2025" / "September 2, 2025" / "Sept. 2, 2025"
_DATE_MONTH_RE = re.compile(
    r"\b([A-Z][a-z]{2,8})\.?\s+(\d{1,2}),\s*((?:19|20)\d{2})\b"
)
_DATE_SLASH_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/((?:19|20)\d{2})\b")


class SCJudicialEthicsScraper(BaseScraper):

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
    def _parse_index(html: str) -> list[dict]:
        """Return [{number, subject}] for every opinion PDF link on a year page."""
        soup = BeautifulSoup(html, "html.parser")
        out: list[dict] = []
        seen: set[str] = set()
        for a in soup.select('a[href*="/media/advisoryOpinions/html/"]'):
            href = a.get("href", "")
            m = re.search(r"/([0-9]{1,3}-[0-9]{4})\.pdf", href)
            if not m:
                continue
            number = m.group(1)
            if number in seen:
                continue
            seen.add(number)
            # subject = the sibling p.subtitle within the same result block
            subject = ""
            result = a.find_parent(class_="result")
            if result:
                sub = result.select_one("p.subtitle")
                if sub:
                    subject = sub.get_text(" ", strip=True)
            out.append({"number": number, "subject": subject})
        return out

    def _list_opinions(self, sample: bool = False) -> list[dict]:
        """Walk every year index (newest first) and collect all opinions."""
        current_year = datetime.now(timezone.utc).year
        out: list[dict] = []
        seen: set[str] = set()
        for year in range(current_year, FIRST_YEAR - 1, -1):
            html = self._curl(INDEX_URL.format(year=year))
            if not html:
                continue
            ops = self._parse_index(html)
            new = [o for o in ops if o["number"] not in seen]
            for o in new:
                seen.add(o["number"])
            out.extend(new)
            if new:
                logger.info(f"  year {year}: {len(new)} opinions")
            if sample and len(out) >= 15:
                break
        return out

    # -------------------------------------------------------- extraction
    @staticmethod
    def _extract_pdf(pdf_bytes: bytes) -> str:
        import fitz  # PyMuPDF
        text_parts = []
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        try:
            for page in doc:
                text_parts.append(page.get_text())
        finally:
            doc.close()
        text = "".join(text_parts)
        # tidy whitespace
        text = re.sub(r"[ \t]+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _parse_date(text: str, number: str) -> str | None:
        """Best-effort issue date; fall back to the opinion-number year.

        The opinion-number year (second half of "NN-YYYY") is authoritative, so a
        precise date parsed from the body is only accepted when its year is within
        one year of it — this rejects dates of *cited* older opinions that appear
        in the text (e.g. a 2026 opinion referencing a 2004 opinion).
        """
        ym = re.search(r"-((?:19|20)\d{2})$", number)
        op_year = int(ym.group(1)) if ym else None

        def _ok(y: int) -> bool:
            return op_year is None or abs(y - op_year) <= 1

        # Precise date near the signatures (search the tail first).
        tail = text[-1500:]
        for chunk in (tail, text):
            for m in _DATE_MONTH_RE.finditer(chunk):
                mon = _MONTHS.get(m.group(1).lower()[:4]) or _MONTHS.get(m.group(1).lower()[:3])
                y = int(m.group(3))
                if mon and _ok(y):
                    return f"{y:04d}-{mon:02d}-{int(m.group(2)):02d}"
            for m in _DATE_SLASH_RE.finditer(chunk):
                y = int(m.group(3))
                if _ok(y):
                    return f"{y:04d}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"
        # Fallback: the year is always the second half of the opinion number.
        if op_year is not None:
            return f"{op_year:04d}-01-01"
        return None

    def _fetch_one(self, op: dict) -> dict | None:
        pdf_bytes = self._curl(PDF_URL.format(number=op["number"]), binary=True)
        if not pdf_bytes or not pdf_bytes[:5].startswith(b"%PDF"):
            return None
        try:
            text = self._extract_pdf(pdf_bytes)
        except Exception as e:
            logger.warning(f"extract failed for {op['number']}: {e}")
            return None
        if not text or len(text) < 200:
            return None
        subject = op.get("subject") or ""
        if not subject:
            # derive subject from the "RE:" line of the opinion body
            rm = re.search(r"RE:\s*(.+?)(?:\n\s*\n|FACTS|CONCLUSION)", text, re.S | re.I)
            if rm:
                subject = re.sub(r"\s+", " ", rm.group(1)).strip()
        date_iso = self._parse_date(text, op["number"])
        return {
            "number": op["number"],
            "subject": subject,
            "text": text,
            "date": date_iso,
            "url": PDF_URL.format(number=op["number"]),
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing SC Advisory Committee on Standards of Judicial Conduct...")
        html = self._curl(INDEX_URL.format(year=datetime.now(timezone.utc).year))
        if not html:
            # fall back to a known-populated year
            html = self._curl(INDEX_URL.format(year=2025))
        ops = self._parse_index(html) if html else []
        if not ops:
            logger.error("API test FAILED: no opinions found on index")
            return False
        logger.info(f"  discovered {len(ops)} opinions on the sampled year")
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
        title = f"Advisory Opinion No. {number}"
        if subject:
            title += f": {subject}"
        return {
            "_id": f"US/SC-JudicialEthics/{number}",
            "_source": "US/SC-JudicialEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "issuer": "South Carolina Advisory Committee on Standards of "
                      "Judicial Conduct (Supreme Court of South Carolina)",
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-SC",
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

    parser = argparse.ArgumentParser(description="US/SC-JudicialEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SCJudicialEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
