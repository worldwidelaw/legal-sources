#!/usr/bin/env python3
"""
BM/CourtJudgments -- Bermuda Court Judgments (gov.bm)

Fetches Supreme Court and Court of Appeal judgments from the official
Bermuda Government website. Judgments are organized by year (2007-2026),
with ~1,600-2,000 PDF decisions freely downloadable.

Strategy:
  - Fetch 20 yearly HTML pages (current year + 2007-2025 archives)
  - Parse <li> elements containing case title, citation, date, and PDF link
  - Download each PDF and extract full text via pdf_extract
  - Current year: /court-judgments
  - Archive years: /court-judgments-YYYY

Citation format: TITLE [YYYY] CA|SC (BDA) NN Div (DD Month YYYY)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Re-fetch all
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BM.CourtJudgments")

BASE_URL = "https://www.gov.bm"
CURRENT_YEAR = datetime.now().year
START_YEAR = 2007

MONTHS = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}


def _clean_html(text: str) -> str:
    """Strip HTML tags and clean whitespace."""
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'&lt;', '<', text)
    text = re.sub(r'&gt;', '>', text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'&#?\w+;', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def _parse_date(date_str: str) -> Optional[str]:
    """Parse date from '20 March 2026' or '(20 March 2026)' to ISO 8601."""
    date_str = date_str.strip().strip('()')
    for fmt in ("%d %B %Y", "%d %b %Y", "%B %d, %Y"):
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _detect_court(text: str) -> str:
    """Detect court from citation text."""
    if re.search(r'\bCA\b', text):
        return "Court of Appeal"
    if re.search(r'\bSC\b', text):
        return "Supreme Court"
    return "Unknown"


def _parse_year_page(html: str, year: int) -> List[Dict[str, Any]]:
    """Parse all judgment entries from a yearly page."""
    records = []

    # Find all list items containing PDF links
    # Pattern: <li> ... <a href="...pdf">TITLE</a> ... (DATE) ... </li>
    li_pattern = re.compile(
        r'<li[^>]*>(.*?)</li>',
        re.DOTALL | re.IGNORECASE
    )

    current_court = "Unknown"

    # Track court sections via headings
    sections = re.split(r'(<h[23][^>]*>.*?</h[23]>)', html, flags=re.DOTALL | re.IGNORECASE)

    for section in sections:
        # Check if this is a heading that indicates court type
        heading_match = re.search(r'<h[23][^>]*>(.*?)</h[23]>', section, re.DOTALL | re.IGNORECASE)
        if heading_match:
            heading_text = _clean_html(heading_match.group(1)).lower()
            if 'court of appeal' in heading_text or 'appeal' in heading_text:
                current_court = "Court of Appeal"
            elif 'supreme court' in heading_text or 'supreme' in heading_text:
                current_court = "Supreme Court"
            continue

        # Find list items with PDF links
        for li_match in li_pattern.finditer(section):
            li_content = li_match.group(1)

            # Find PDF link
            pdf_match = re.search(
                r'<a[^>]+href="([^"]*\.pdf)"[^>]*>(.*?)</a>',
                li_content, re.DOTALL | re.IGNORECASE
            )
            if not pdf_match:
                continue

            pdf_url = pdf_match.group(1)
            link_text = _clean_html(pdf_match.group(2))

            # Make URL absolute
            if pdf_url.startswith('/'):
                pdf_url = BASE_URL + pdf_url
            elif not pdf_url.startswith('http'):
                pdf_url = BASE_URL + '/' + pdf_url

            # Extract date from parenthetical at end of li or link text
            date_match = re.search(r'\((\d{1,2}\s+\w+\s+\d{4})\)', li_content)
            date_iso = _parse_date(date_match.group(1)) if date_match else None

            # Extract citation pattern: [YYYY] CA/SC (BDA) NN Div
            citation_match = re.search(
                r'\[\d{4}\]\s+(?:CA|SC)\s*\(BDA\)\s*\d+\s*\w*',
                link_text
            )
            citation = citation_match.group(0) if citation_match else ""

            # Detect court from citation or section heading
            court = _detect_court(link_text) if citation else current_court

            # Build title: use full link text, clean up
            title = link_text.strip()
            if not title:
                title = f"Bermuda Judgment {year}"

            # Generate doc_id from PDF filename
            pdf_filename = pdf_url.rsplit('/', 1)[-1]
            doc_id = re.sub(r'\.pdf$', '', pdf_filename, flags=re.IGNORECASE)
            doc_id = re.sub(r'%20', '_', doc_id)
            doc_id = re.sub(r'[^\w\-.]', '_', doc_id)

            records.append({
                "doc_id": doc_id,
                "title": title,
                "citation": citation,
                "court": court,
                "date": date_iso,
                "year": year,
                "pdf_url": pdf_url,
            })

    return records


class BermudaCourtJudgmentsScraper(BaseScraper):
    """Scraper for BM/CourtJudgments -- Bermuda Court Judgments."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,*/*",
            },
            timeout=120,
        )

    def _extract_pdf_text(self, pdf_bytes: bytes, doc_id: str) -> str:
        return extract_pdf_markdown(
            source="BM/CourtJudgments",
            source_id=doc_id,
            pdf_bytes=pdf_bytes,
            table="case_law",
        ) or ""

    def _get_year_urls(self) -> List[tuple]:
        """Return (year, url_path) pairs for all years."""
        urls = [(CURRENT_YEAR, "/court-judgments")]
        for y in range(CURRENT_YEAR - 1, START_YEAR - 1, -1):
            urls.append((y, f"/court-judgments-{y}"))
        return urls

    def _fetch_year(self, year: int, path: str) -> List[Dict[str, Any]]:
        """Fetch and parse all judgments for a given year."""
        logger.info(f"Fetching year {year}: {path}")
        try:
            self.rate_limiter.wait()
            resp = self.client.get(path)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch year {year}: {e}")
            return []

        records = _parse_year_page(resp.text, year)
        logger.info(f"Year {year}: {len(records)} judgments found")
        return records

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        doc_id = raw.get("doc_id", "unknown")

        return {
            "_id": f"BM/CourtJudgments/{doc_id}",
            "_source": "BM/CourtJudgments",
            "_type": "case_law",
            "_fetched_at": now,
            "doc_id": doc_id,
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("pdf_url", ""),
            "court": raw.get("court", ""),
            "citation": raw.get("citation", ""),
            "year": raw.get("year"),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        year_urls = self._get_year_urls()
        count = 0
        errors = 0

        for year, path in year_urls:
            records = self._fetch_year(year, path)

            for record in records:
                pdf_url = record["pdf_url"]
                doc_id = record["doc_id"]

                logger.info(f"  [{count + errors + 1}] Downloading: {record['title'][:80]}")

                try:
                    self.rate_limiter.wait()
                    resp = self.client.get(pdf_url)
                    resp.raise_for_status()
                except Exception as e:
                    logger.warning(f"  Failed to download PDF {doc_id}: {e}")
                    errors += 1
                    continue

                if not resp.content or resp.content[:5] != b"%PDF-":
                    logger.warning(f"  Not a valid PDF: {doc_id}")
                    errors += 1
                    continue

                text = self._extract_pdf_text(resp.content, doc_id)
                if not text or len(text.strip()) < 50:
                    logger.warning(f"  Insufficient text from {doc_id}: {len(text) if text else 0} chars")
                    errors += 1
                    continue

                record["text"] = text
                yield record
                count += 1

            logger.info(f"Year {year} done. Running total: {count} records, {errors} errors")

        logger.info(f"Complete: {count} judgments fetched ({errors} errors)")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = BermudaCourtJudgmentsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing year page fetch...")
        records = scraper._fetch_year(CURRENT_YEAR, "/court-judgments")
        if not records:
            # Try the most recent archive year
            records = scraper._fetch_year(CURRENT_YEAR - 1, f"/court-judgments-{CURRENT_YEAR - 1}")
        if not records:
            logger.error("FAILED - no records found")
            sys.exit(1)
        logger.info(f"OK - {len(records)} records found")

        logger.info("Testing PDF download...")
        first = records[0]
        import requests
        resp = requests.get(first['pdf_url'], timeout=60,
                            headers={"User-Agent": "LegalDataHunter/1.0"})
        if resp.status_code == 200 and resp.content[:5] == b"%PDF-":
            text = scraper._extract_pdf_text(resp.content, first['doc_id'])
            logger.info(f"OK - PDF download works, {len(text)} chars extracted")
        else:
            logger.error(f"FAILED - status {resp.status_code}")
            sys.exit(1)

    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
