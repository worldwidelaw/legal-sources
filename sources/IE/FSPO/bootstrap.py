#!/usr/bin/env python3
"""
IE/FSPO -- Financial Services and Pensions Ombudsman of Ireland (Legally Binding Decisions)

The FSPO is a statutory independent officer (Financial Services and Pensions
Ombudsman Act 2017) who investigates and issues **legally binding decisions** on
complaints against financial service and pension providers in Ireland. Decisions
are binding on both parties (subject only to a statutory appeal to the High
Court) — i.e. adjudicative case law. The FSPO publishes anonymised decisions in
full as born-digital PDFs, searchable by year/sector/product/conduct/outcome.

Strategy:
  - Enumerate decision references per year via the searchable database:
    GET display.asp?dyear=YYYY&...&mypage=N  (25 per page)
  - Decision PDFs live at documents/{YYYY}-{NNNN}.pdf (born-digital, text layer)
  - Extract full text with PyMuPDF (fitz); parse the structured header
    (Decision Ref / Sector / Product / Conduct / Outcome) from the text.

Data:
  - ~2,000+ decisions, 2018-present
  - Language: English
  - Auth: None (free public access)

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental (recent years)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IE.FSPO")

BASE_URL = "https://www.fspo.ie"
DECISIONS_PATH = "/complaint-outcomes/investigation-services/legally-binding-decisions"
FIRST_YEAR = 2018
REF_RE = re.compile(r"documents/(\d{4}-\d{4})\.pdf", re.IGNORECASE)
LAST_PAGE_RE = re.compile(r'mypage=(\d+)"\s+title="Last Page"', re.IGNORECASE)


def _pdf_text(pdf_bytes: bytes) -> str:
    if not fitz:
        raise RuntimeError("PyMuPDF (fitz) is required to extract FSPO PDFs")
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        return "\n".join(page.get_text() for page in doc)
    finally:
        doc.close()


def _clean(text: str) -> str:
    # Collapse the runs of blank lines PyMuPDF emits from the header layout.
    lines = [ln.rstrip() for ln in text.split("\n")]
    out, blanks = [], 0
    for ln in lines:
        if ln.strip():
            blanks = 0
            out.append(ln.strip())
        else:
            blanks += 1
            if blanks <= 1:
                out.append("")
    return "\n".join(out).strip()


class FSPOScraper(BaseScraper):
    """Scraper for the FSPO legally binding decisions database."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-IE,en;q=0.9",
            },
            timeout=90,
        )

    def _list_page(self, year: int, page: int) -> Optional[str]:
        url = (f"{DECISIONS_PATH}/display.asp?dyear={year}&product=0&conduct=0"
               f"&sector=0&outcome=0&decisionref=&mypage={page}")
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            if resp.status_code != 200:
                logger.warning(f"list {year} page {page}: HTTP {resp.status_code}")
                return None
            return resp.content.decode("utf-8", errors="replace")
        except Exception as e:
            logger.warning(f"Error listing {year} page {page}: {e}")
            return None

    def _refs_for_year(self, year: int) -> List[str]:
        """Ordered, de-duplicated list of decision refs for a year."""
        html = self._list_page(year, 1)
        if not html:
            return []
        last_page = 1
        m = LAST_PAGE_RE.search(html)
        if m:
            last_page = int(m.group(1))
        refs: List[str] = []
        seen = set()

        def collect(page_html: str):
            for r in REF_RE.findall(page_html):
                if r not in seen:
                    seen.add(r)
                    refs.append(r)

        collect(html)
        for page in range(2, last_page + 1):
            ph = self._list_page(year, page)
            if not ph:
                continue
            before = len(refs)
            collect(ph)
            if len(refs) == before:
                # No new refs — stop paginating this year defensively.
                break
        return refs

    def _fetch_pdf(self, ref: str) -> Optional[bytes]:
        url = f"{DECISIONS_PATH}/documents/{ref}.pdf"
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            if resp.status_code != 200:
                logger.warning(f"pdf {ref}: HTTP {resp.status_code}")
                return None
            data = resp.content
            if not data[:5].startswith(b"%PDF"):
                logger.warning(f"pdf {ref}: not a PDF")
                return None
            return data
        except Exception as e:
            logger.warning(f"Error fetching pdf {ref}: {e}")
            return None

    @staticmethod
    def _header_field(text: str, label: str) -> str:
        """The FSPO header prints 'Label:' then the value on following line(s)."""
        m = re.search(label + r"\s*:?\s*\n\s*([^\n]+)", text)
        return m.group(1).strip() if m else ""

    def _years(self) -> List[int]:
        current = datetime.now(timezone.utc).year
        return list(range(FIRST_YEAR, current + 1))

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        any_ref = False
        for year in self._years():
            refs = self._refs_for_year(year)
            logger.info(f"{year}: {len(refs)} decisions")
            for ref in refs:
                any_ref = True
                pdf = self._fetch_pdf(ref)
                if not pdf:
                    continue
                try:
                    raw_text = _pdf_text(pdf)
                except Exception as e:
                    logger.warning(f"extract {ref} failed: {e}")
                    continue
                yield {"ref": ref, "year": year, "text": raw_text}
        if not any_ref:
            raise RuntimeError(
                "FSPO listing returned 0 decision references — site blocked or markup changed"
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        start_year = max(FIRST_YEAR, since.year)
        for year in range(start_year, datetime.now(timezone.utc).year + 1):
            for ref in self._refs_for_year(year):
                pdf = self._fetch_pdf(ref)
                if not pdf:
                    continue
                try:
                    raw_text = _pdf_text(pdf)
                except Exception:
                    continue
                yield {"ref": ref, "year": year, "text": raw_text}

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        ref = raw.get("ref", "")
        raw_text = raw.get("text", "") or ""
        text = _clean(raw_text)
        if len(text) < 200:
            return None

        sector = self._header_field(raw_text, "Sector")
        product = self._header_field(raw_text, r"Product\s*/\s*Service")
        conduct = self._header_field(raw_text, r"Conduct\(s\) complained of")
        outcome = self._header_field(raw_text, "Outcome")

        year = raw.get("year") or (int(ref[:4]) if ref[:4].isdigit() else None)
        iso_date = f"{year}-01-01" if year else None

        title_bits = [f"FSPO Decision {ref}"]
        if sector:
            title_bits.append(sector)
        if outcome:
            title_bits.append(outcome)
        title = " — ".join(title_bits)

        url = f"{BASE_URL}{DECISIONS_PATH}/documents/{ref}.pdf"

        return {
            "_id": f"FSPO-{ref}",
            "_source": "IE/FSPO",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": iso_date,
            "url": url,
            "decision_ref": ref,
            "sector": sector,
            "product": product,
            "conduct": conduct,
            "outcome": outcome,
            "court": "Financial Services and Pensions Ombudsman",
            "jurisdiction": "IE",
            "language": "en",
        }

    def test_connection(self):
        print("Testing FSPO decisions database...")
        refs = self._refs_for_year(2023)
        print(f"  2023: {len(refs)} refs; first={refs[0] if refs else None}")
        if refs:
            pdf = self._fetch_pdf(refs[0])
            if pdf:
                t = _pdf_text(pdf)
                print(f"  {refs[0]}: text {len(t)} chars - OK")
            else:
                print("  PDF fetch FAILED")


def main():
    scraper = FSPOScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)
    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command in ("bootstrap", "bootstrap-fast"):
        if sample_mode:
            logger.info("Running bootstrap in sample mode")
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        else:
            logger.info("Running full bootstrap")
            stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Bootstrap complete: {stats}")
    elif command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
