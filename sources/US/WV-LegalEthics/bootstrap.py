#!/usr/bin/env python3
"""
US/WV-LegalEthics -- West Virginia Legal Ethics Opinions (L.E.O.)

Official attorney-ethics advisory opinions ("Legal Ethics Opinions", L.E.O.)
issued by the Lawyer Disciplinary Board of the West Virginia Office of
Disciplinary Counsel (an arm of the Supreme Court of Appeals of West Virginia).
The opinions interpret the West Virginia Rules of Professional Conduct and
advise members of the Bar on the ethical propriety of contemplated conduct.
Series runs from the mid-1970s "Legal Ethics Inquiry" numbers (76-1 ...) through
the modern "L.E.O. YYYY-NN" numbering (e.g. L.E.O. 2024-01). = doctrine
(official government advisory opinion), public domain (US state edict).

Distinct from US/WV-EthicsOpinions (West Virginia Ethics Commission advisory
opinions for public officials/employees), US/WV-Courts, US/WV-Legislation and
US/WV-COMAR.

Access (no JavaScript execution, no CAPTCHA, no auth):
  The opinions are listed on a single public page at
  https://wvodc.org/Legal-Ethics-Opinion (a GoHighLevel-hosted site). Each
  opinion is an anchor whose visible text carries the number and title, e.g.
  "L.E.O. 2024-01 Artificial Intelligence" or "76-1 EMPLOYMENT - DUAL PRACTICE
  OF LAW ...". The href points to the born-scanned PDF hosted on
  storage.googleapis.com/msgsndr/<acct>/media/<id>.pdf. The PDFs are image-only
  (no text layer) so full text is recovered by OCR via common.pdf_extract
  (opendataloader/pdfplumber/pypdf text-layer backends fall through to tesseract
  OCR automatically).

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
"""

from __future__ import annotations

import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

import requests
from bs4 import BeautifulSoup

ROOT_DIR = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

from common.base_scraper import BaseScraper  # noqa: E402
from common.pdf_extract import extract_pdf_markdown  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.WV-LegalEthics")

LISTING_URL = "https://wvodc.org/Legal-Ethics-Opinion"
PDF_HOST = "storage.googleapis.com"

# Number at the start of the anchor text: "2024-01", "99-03", "86-3", "76-1".
NUMBER_RE = re.compile(r"^\s*(?:L\.?\s*E\.?\s*O\.?\s*)?(\d{2,4})-(\d+[A-Za-z]?)\b", re.I)
# Fallback: a bare 4-digit year (board orders with no NN-NN number).
YEAR_ONLY_RE = re.compile(r"^\s*(?:L\.?\s*E\.?\s*O\.?\s*)?(\d{4})\b")
LEO_PREFIX_RE = re.compile(r"^\s*L\.?\s*E\.?\s*O\.?\s*", re.I)


class WVLegalEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.5
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                          "Version/17.0 Safari/605.1.15",
            "Accept": "text/html,application/xhtml+xml,*/*",
        })

    # ---------------------------------------------------------------- http
    def _fetch_listing(self) -> str | None:
        for attempt in range(4):
            try:
                r = self._session.get(LISTING_URL, timeout=60)
                if r.status_code == 200:
                    return r.text
                logger.warning(f"listing -> HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"listing fetch failed (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- parsing
    @staticmethod
    def _two_digit_year(y: int) -> int:
        """Expand a 2-digit L.E.I./L.E.O. year: 76-99 -> 19xx, 00-25 -> 20xx."""
        return 1900 + y if y >= 50 else 2000 + y

    @classmethod
    def _parse_anchor(cls, text: str, href: str) -> dict | None:
        """Return {number, title, year} parsed from an anchor's text, or None."""
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            return None

        m = NUMBER_RE.search(text)
        if m:
            raw_year, seq = m.group(1), m.group(2)
            yr = int(raw_year)
            if len(raw_year) <= 2:
                yr = cls._two_digit_year(yr)
            number = f"{yr:04d}-{seq}"
            title = text[m.end():].strip(" -–—:.")
            return {"number": number, "title": title or number, "year": yr}

        ym = YEAR_ONLY_RE.search(text)
        if ym:
            yr = int(ym.group(1))
            body = LEO_PREFIX_RE.sub("", text).strip()
            # Slug for a board order / untitled entry: year + short href stem.
            stem = href.rsplit("/", 1)[-1].split(".")[0][:8]
            number = f"{yr:04d}-order-{stem}"
            return {"number": number, "title": body[:200] or number, "year": yr}

        return None

    def _iter_entries(self) -> Generator[dict, None, None]:
        html = self._fetch_listing()
        if not html:
            return
        soup = BeautifulSoup(html, "html.parser")
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if PDF_HOST not in href or not href.lower().endswith(".pdf"):
                continue
            meta = self._parse_anchor(a.get_text(" ", strip=True), href)
            if not meta:
                continue
            if meta["number"] in seen:
                continue
            seen.add(meta["number"])
            meta["url"] = href
            yield meta

    # ------------------------------------------------------------- extract
    def _extract_text(self, url: str, doc_id: str) -> str | None:
        try:
            return extract_pdf_markdown(
                "US/WV-LegalEthics", doc_id, pdf_url=url, table="doctrine"
            )
        except Exception as e:
            logger.warning(f"extract failed for {doc_id}: {e}")
            return None

    # ---------------------------------------------------------- discovery
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for meta in self._iter_entries():
            time.sleep(self.delay)
            text = self._extract_text(meta["url"], meta["number"])
            if not text or len(text) < 200:
                logger.info(f"  skip {meta['number']} (no/short text)")
                continue
            yield {
                "number": meta["number"],
                "title": meta["title"],
                "year": meta["year"],
                "url": meta["url"],
                "text": text,
            }
            emitted += 1
            if sample and emitted >= 12:
                return

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing West Virginia Legal Ethics Opinions (wvodc.org)...")
        n = 0
        for rec in self._iter_raw(sample=True):
            if n < 3:
                logger.info(f"  {rec['number']} OK ({len(rec['text'])} chars) "
                            f"- {rec['title'][:50]}")
            n += 1
            if n >= 5:
                break
        if n >= 5:
            logger.info(f"API test PASSED ({n}+ full-text opinions)")
            return True
        logger.error("API test FAILED: insufficient full text")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw["number"]
        return {
            "_id": f"US/WV-LegalEthics/{number}",
            "_source": "US/WV-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "issuer": "Lawyer Disciplinary Board of West Virginia",
            "title": f"West Virginia Legal Ethics Opinion {number}"
                     + (f": {raw['title']}" if raw.get("title") else ""),
            "text": raw["text"],
            "url": raw["url"],
            "date": f"{raw['year']:04d}-01-01",
            "jurisdiction": "US-WV",
        }

    # ------------------------------------------------------------- fetch
    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("year")
                             and f"{raw['year']:04d}-01-01" >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/WV-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WVLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
