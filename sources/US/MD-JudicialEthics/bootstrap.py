#!/usr/bin/env python3
"""
US/MD-JudicialEthics -- Maryland Judicial Ethics Committee — Opinions

Fetches the full text of the advisory opinions issued by the Maryland Judicial
Ethics Committee, a committee of the Maryland Judiciary (established under the
Maryland Rules, Title 18) that renders written advisory opinions to inquiring
Maryland judges and judicial appointees on the propriety of contemplated conduct
under the Maryland Code of Judicial Conduct / Code of Conduct for Judicial
Appointees = doctrine (official written interpretation of the judicial-conduct
rules). Published opinions are a matter of public record; the requesting judge's
identity is kept confidential.

Access (no JavaScript, no CAPTCHA, no auth):
  A single public index page lists every published opinion as an HTML table row:

      https://www.mdcourts.gov/ethics/opinions

  Each `<tr>` is:
      <td><a href="/sites/default/files/import/ethics/pdfs/{file}.pdf">{number}</a></td>
      <td>{MM-DD-YY issue date}</td>
      <td>{subject / question}</td>

  The anchor TEXT is the authoritative opinion number (e.g. "2025-32"); the
  PDF filename is sometimes typo'd or uses underscores ("2104-14.pdf",
  "2009_13.pdf", "2008_31and32.pdf") so we always take the href verbatim for the
  download and the anchor text for the number. Each opinion is a born-digital
  PDF (text layer, no OCR needed) whose full text we extract with PyMuPDF (fitz).

Strategy:
  GET the single index, parse (number, date, subject, pdf-url) for every row,
  download each PDF and extract the body text.

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
logger = logging.getLogger("legal-data-hunter.US.MD-JudicialEthics")

BASE = "https://www.mdcourts.gov"
INDEX_URL = BASE + "/ethics/opinions"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

# "11-07-25" (MM-DD-YY) in the index table
_INDEX_DATE_RE = re.compile(r"^\s*(\d{1,2})-(\d{1,2})-(\d{2})\s*$")


class MDJudicialEthicsScraper(BaseScraper):

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
    def _index_date(raw: str) -> str | None:
        """Parse the index table's MM-DD-YY issue date to ISO 8601."""
        m = _INDEX_DATE_RE.match(raw or "")
        if not m:
            return None
        mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        year = 1900 + yy if yy >= 60 else 2000 + yy
        try:
            return f"{year:04d}-{mm:02d}-{dd:02d}"
        except Exception:
            return None

    @classmethod
    def _parse_index(cls, html: str) -> list[dict]:
        """Return [{number, subject, date, url}] for every opinion PDF row."""
        soup = BeautifulSoup(html, "html.parser")
        out: list[dict] = []
        seen: set[str] = set()
        for a in soup.select('a[href*="/ethics/pdfs/"]'):
            href = a.get("href", "").strip()
            if not href.lower().endswith(".pdf"):
                continue
            number = a.get_text(strip=True)
            if not number or number in seen:
                continue
            # skip aggregate/non-opinion links (e.g. "rulesorders")
            if not re.match(r"^\d{4}-\d{1,3}", number):
                continue
            seen.add(number)
            date_iso = None
            subject = ""
            tr = a.find_parent("tr")
            if tr:
                tds = tr.find_all("td")
                if len(tds) > 1:
                    date_iso = cls._index_date(tds[1].get_text(strip=True))
                if len(tds) > 2:
                    subject = tds[2].get_text(" ", strip=True)
            # resolve href to absolute
            url = href if href.startswith("http") else BASE + href
            out.append({
                "number": number,
                "subject": subject,
                "date": date_iso,
                "url": url,
            })
        return out

    def _list_opinions(self, sample: bool = False) -> list[dict]:
        html = self._curl(INDEX_URL)
        if not html:
            return []
        ops = self._parse_index(html)
        logger.info(f"  discovered {len(ops)} opinions on the index")
        return ops

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

    def _fetch_one(self, op: dict) -> dict | None:
        pdf_bytes = self._curl(op["url"], binary=True)
        if not pdf_bytes or not pdf_bytes[:5].startswith(b"%PDF"):
            return None
        try:
            text = self._extract_pdf(pdf_bytes)
        except Exception as e:
            logger.warning(f"extract failed for {op['number']}: {e}")
            return None
        if not text or len(text) < 150:
            return None
        return {
            "number": op["number"],
            "subject": op.get("subject") or "",
            "text": text,
            "date": op.get("date"),
            "url": op["url"],
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Maryland Judicial Ethics Committee index...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions found on index")
            return False
        logger.info(f"  discovered {len(ops)} opinions")
        ok = 0
        for op in ops[:8]:
            rec = self._fetch_one(op)
            if rec and len(rec["text"]) > 150:
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
        title = f"Judicial Ethics Opinion {number}"
        if subject:
            title += f": {subject}"
        return {
            "_id": f"US/MD-JudicialEthics/{number}",
            "_source": "US/MD-JudicialEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "issuer": "Maryland Judicial Ethics Committee (Maryland Judiciary)",
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-MD",
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

    parser = argparse.ArgumentParser(description="US/MD-JudicialEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MDJudicialEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
