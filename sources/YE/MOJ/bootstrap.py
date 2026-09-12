#!/usr/bin/env python3
"""
YE/MOJ -- Yemen Ministry of Justice Legislation Database

Fetches laws and regulations from the Yemen Ministry of Justice website.
Laws are listed as HTML table entries with PDF download links.

Approach:
  1. Scrape list pages at /LawsM?page={N} (5 pages, 8 items each)
  2. Extract title and PDF link for each law
  3. Download PDF and extract text via common.pdf_extract.extract_pdf_markdown,
     which repairs the visual-order (character-reversed) Arabic these PDFs
     carry in their text layer — see issue #1560.

Data:
  - ~38 laws (presidential decrees, legislation, regulations)
  - Full text extracted from PDFs
  - Arabic language

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.YE.MOJ")

BASE_URL = "https://www.moj.gov.ye"
LIST_URL = "/LawsM"
TOTAL_PAGES = 5

def _letters(text: str) -> int:
    """Characters that carry meaning — excludes spaces and unmapped glyphs."""
    return sum(1 for c in text if c.isalpha())


def _unmapped_glyphs(text: str) -> int:
    """Private Use Area characters.

    A font embedded without a usable ToUnicode table extracts as a run of
    U+E000..U+F8FF code points. They are visually a document and numerically a
    lot of text, but they match nothing — no keyword, no tokenizer, no reader.
    """
    return sum(1 for c in text if "" <= c <= "")


def parse_year_from_title(title: str) -> Optional[str]:
    """Extract year from Arabic law title (e.g., 'لسنة 2012م')."""
    match = re.search(r'لسنة\s*(\d{4})', title)
    if match:
        return match.group(1)
    match = re.search(r'(\d{4})\s*م', title)
    if match:
        return match.group(1)
    return None


class YEMOJScraper(BaseScraper):
    """Scraper for YE/MOJ -- Yemen Ministry of Justice Legislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=120,
        )

    def _parse_list_page(self, page: int) -> list:
        """Parse a list page and return law entries."""
        from bs4 import BeautifulSoup

        url = f"/Home/LawsM?page={page}" if page > 1 else LIST_URL
        self.rate_limiter.wait()
        resp = self.client.get(url)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", id="table_id")
        if not table:
            return []

        entries = []
        for row in table.select("tbody tr"):
            tds = row.find_all("td")
            if len(tds) < 4:
                continue
            seq = tds[0].get_text(strip=True)
            title = tds[1].get_text(strip=True)
            downloads = tds[2].get_text(strip=True)
            link_a = tds[3].find("a", href=True)
            if not link_a:
                continue
            pdf_path = link_a["href"]
            entries.append({
                "seq": seq,
                "title": title,
                "downloads": downloads,
                "pdf_path": pdf_path,
            })
        return entries

    def _download_pdf_text(self, pdf_path: str) -> str:
        """Download a PDF and extract text in logical (reading) order."""
        self.rate_limiter.wait()
        resp = self.client.get(pdf_path)
        resp.raise_for_status()

        if not resp.content or len(resp.content) < 100:
            return ""

        try:
            # These PDFs store their Arabic text layer in visual order, so the
            # old pdfplumber/PyPDF2 path emitted every line character-reversed
            # (issue #1560). extract_pdf_markdown runs every backend, OCRs a
            # scan, folds shaped presentation forms back to base letters and
            # reorders glyph clusters by x-geometry.
            #
            # force=True because the rows already in Neon hold this corpus
            # reversed with non-empty text, and the helper skips any document it
            # finds stored that way — without it the re-crawl meant to replace
            # them emits nothing. table= must be "legislation"; the default
            # "case_law" would point the idempotency preload at the wrong table.
            text = extract_pdf_markdown(
                source="YE/MOJ",
                source_id=f"YE/MOJ/{self._law_id(pdf_path)}",
                pdf_bytes=resp.content,
                table="legislation",
                force=True,
            ) or ""
            return self._force_logical_order(text, resp.content)
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_path}: {e}")
            return ""

    @staticmethod
    def _force_logical_order(text: str, pdf_bytes: bytes) -> str:
        """Reorder even when the shared helper's Arabic-ratio gate declines.

        `extract_pdf_markdown` only reorders a document that passes `looks_rtl`
        (>=20% RTL characters). Several of these decrees are set in a legacy
        font with no usable ToUnicode table, so most of the extracted string is
        mojibake — ``< <Jð]…‡çÖ]<‹×¥`` — which dilutes the Arabic below the
        threshold and leaves the *real* Arabic reversed (LawsMD/2 is the case
        that survived the first pass of #1560). Every document in this corpus is
        Arabic-language Yemeni legislation, so there is no genuine Latin body
        text the reorder could damage — the concern that made the gate
        load-bearing for the bilingual QA/FreeZonesLaws corpus does not apply.

        The shared helper's own length guard is kept in spirit: geometry
        reordering runs on PyMuPDF alone, so a PDF that another backend read
        more completely should not lose most of its body to gain reading order.
        It is applied to the *letter* count rather than the raw length, because
        these documents are padded with runs of spaces and unmapped glyphs that
        make raw length a poor proxy for how much text was actually recovered.
        """
        from common.arabic_pdf import extract_rtl_pdf_text, looks_rtl

        if not text or looks_rtl(text):
            return text  # empty, or the helper already reordered it
        try:
            repaired = extract_rtl_pdf_text(pdf_bytes)
        except Exception as e:  # noqa: BLE001 - never fail a crawl over reading order
            logger.warning(f"RTL reorder failed, keeping original: {e}")
            return text
        if not repaired or _letters(repaired) < 0.6 * _letters(text):
            return text
        return repaired

    @staticmethod
    def _law_id(pdf_path: str) -> str:
        return pdf_path.replace("/LawsMD/", "")

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        law_id = self._law_id(raw.get("pdf_path", ""))
        title = raw.get("title", "")
        text = raw.get("text", "")
        year = parse_year_from_title(title)
        date = f"{year}-01-01" if year else None

        return {
            "_id": f"YE/MOJ/{law_id}",
            "_source": "YE/MOJ",
            "_type": "legislation",
            "_fetched_at": now,
            "title": title,
            "text": text,
            "date": date,
            "url": f"{BASE_URL}/LawsMD/{law_id}",
            "doc_id": law_id,
            "year": year,
            "downloads": raw.get("downloads", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        limit = 12 if sample else None
        count = 0

        for page in range(1, TOTAL_PAGES + 1):
            if limit and count >= limit:
                break

            logger.info(f"Fetching list page {page}/{TOTAL_PAGES}...")
            try:
                entries = self._parse_list_page(page)
            except Exception as e:
                logger.error(f"Failed to fetch list page {page}: {e}")
                break

            logger.info(f"  Found {len(entries)} entries")

            for entry in entries:
                if limit and count >= limit:
                    break

                title = entry["title"]
                if not title:
                    logger.warning(f"  Skipping entry with empty title")
                    continue

                logger.info(f"  [{count + 1}] Downloading PDF for: {title[:60]}...")
                try:
                    text = self._download_pdf_text(entry["pdf_path"])
                except Exception as e:
                    logger.error(f"  Failed to download PDF: {e}")
                    continue

                # Judge the text layer on readable letters, not raw length. A
                # few of these decrees (LawsMD/5, LawsMD/10) are set in a font
                # with no ToUnicode table, so they extract as thousands of
                # Private Use Area code points padded with spaces — long enough
                # to clear a length check, unsearchable to everything
                # downstream. Those need OCR; emitting them would put a
                # title-and-date-only record into the index.
                letters, unmapped = _letters(text), _unmapped_glyphs(text)
                if letters < 50 or unmapped > letters:
                    logger.warning(
                        f"  Skipping - no usable text layer ({letters} letters, "
                        f"{unmapped} unmapped glyphs in {len(text)} chars); needs OCR"
                    )
                    continue

                entry["text"] = text
                yield entry
                count += 1
                logger.info(f"  [{count}] OK: {len(text)} chars extracted")

        logger.info(f"Fetched {count} laws total")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all(sample=False)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = YEMOJScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
