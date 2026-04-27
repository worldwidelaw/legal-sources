#!/usr/bin/env python3
"""
GI/SupremeCourt -- Gibraltar Supreme Court Judgments

Fetches court judgments from gibraltarlaws.gov.gi. The site has year-filtered
listing pages with clickable rows linking to detail pages. Each detail page
embeds a PDF viewer with the full judgment text.

Strategy:
  - Scrape listing pages by year to get detail page URLs + metadata
  - Visit each detail page to extract judge, court, case number, and PDF URL
  - Download PDFs and extract full text

Listing HTML structure:
  <div class="tr" data-href="https://.../{slug}-{id}">
    <div class="td"><p>TITLE</p></div>
    <div class="td"><p>2024GibLR605</p></div>
    <div class="td"><p>11 Dec 2024</p></div>
  </div>

Detail page structure:
  <p class="date mb-0"><span>Date of Judgment:</span> 11 Dec 2024</p>
  <p class="date mb-0"><span>Judge:</span> Happold, J.</p>
  <p class="date mb-0"><span>Court:</span> Supreme Court</p>
  <p class="date mb-0"><span>Case Number:</span> 2024GibLR605</p>
  <iframe src="...uploads/judgments/YYYY/REF.pdf" ...></iframe>

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GI.SupremeCourt")

BASE_URL = "https://www.gibraltarlaws.gov.gi"

# Match listing rows: <div class="tr" data-href="URL">
ROW_RE = re.compile(
    r'<div\s+class="tr"\s+data-href="([^"]+)">\s*'
    r'<div\s+class="td">\s*(?:<h6[^>]*>[^<]*</h6>\s*)?<p>([^<]*)</p>\s*</div>\s*'
    r'<div\s+class="td">\s*(?:<h6[^>]*>[^<]*</h6>\s*)?<p>([^<]*)</p>\s*</div>\s*'
    r'<div\s+class="td">\s*(?:<h6[^>]*>[^<]*</h6>\s*)?<p>([^<]*)</p>\s*</div>',
    re.DOTALL,
)

# Extract metadata from detail page
JUDGE_RE = re.compile(r'<span[^>]*>Judge:</span>\s*([^<]+)', re.IGNORECASE)
COURT_RE = re.compile(r'<span[^>]*>Court:</span>\s*([^<]+)', re.IGNORECASE)
CASE_NUM_RE = re.compile(r'<span[^>]*>Case Number:</span>\s*([^<]+)', re.IGNORECASE)

# Extract PDF URL from iframe
PDF_IFRAME_RE = re.compile(
    r'<iframe[^>]+src="[^"]*?#\.\./\.\./\.\./([^"]+\.pdf)"',
    re.IGNORECASE,
)

# Fallback: direct link to uploads/judgments
PDF_UPLOAD_RE = re.compile(
    r'uploads/judgments/\d{4}/[^"\'<>\s]+\.pdf',
    re.IGNORECASE,
)

MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def parse_date(date_str: str) -> Optional[str]:
    """Parse 'DD Mon YYYY' or 'D Mon YYYY' to ISO date."""
    date_str = date_str.strip()
    match = re.match(r'(\d{1,2})\s+(\w{3,})\s+(\d{4})', date_str)
    if match:
        day, month_name, year = match.groups()
        month = MONTH_MAP.get(month_name[:3].lower())
        if month:
            return f"{year}-{month:02d}-{int(day):02d}"
    return None


class GISupremeCourtScraper(BaseScraper):
    """Scraper for GI/SupremeCourt -- Gibraltar Supreme Court Judgments."""

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

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        return extract_pdf_markdown(
            source="GI/SupremeCourt",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="case_law",
        ) or ""

    def _get_year_list(self) -> List[int]:
        """Fetch the list of available years from the dropdown."""
        try:
            resp = self.client.get("/judgments")
            resp.raise_for_status()
            years = re.findall(r'<option\s+value="(\d{4})"', resp.text)
            return sorted([int(y) for y in years], reverse=True)
        except Exception as e:
            logger.warning(f"Failed to get year list: {e}")
            return list(range(2024, 1811, -1))

    def _parse_year_page(self, year: int) -> List[Dict[str, Any]]:
        """Fetch and parse a year's judgment listing."""
        url = f"/judgments?year={year}"
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch year {year}: {e}")
            return []

        results = []
        for match in ROW_RE.finditer(resp.text):
            detail_url, title, ref_number, date_str = match.groups()
            entry = {
                "detail_url": detail_url.strip(),
                "title": title.strip(),
                "ref_number": ref_number.strip(),
                "date": parse_date(date_str),
                "raw_date": date_str.strip(),
                "year": year,
            }
            results.append(entry)

        return results

    def _fetch_detail(self, detail_url: str) -> Dict[str, Any]:
        """Fetch a detail page and extract judge, court, case number, PDF URL."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(detail_url)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch detail {detail_url}: {e}")
            return {}

        html = resp.text
        info: Dict[str, Any] = {}

        judge_match = JUDGE_RE.search(html)
        if judge_match:
            info["judge"] = judge_match.group(1).strip()

        court_match = COURT_RE.search(html)
        if court_match:
            info["court"] = court_match.group(1).strip()

        case_match = CASE_NUM_RE.search(html)
        if case_match:
            info["case_number"] = case_match.group(1).strip()

        # Try iframe src first
        pdf_match = PDF_IFRAME_RE.search(html)
        if pdf_match:
            info["pdf_url"] = f"{BASE_URL}/{pdf_match.group(1)}"
        else:
            # Fallback: search for uploads/judgments path
            pdf_match2 = PDF_UPLOAD_RE.search(html)
            if pdf_match2:
                info["pdf_url"] = f"{BASE_URL}/{pdf_match2.group(0)}"

        return info

    def _make_doc_id(self, ref_number: str, detail_url: str) -> str:
        key = ref_number if ref_number else detail_url
        return hashlib.sha256(key.encode()).hexdigest()[:16]

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        ref = raw.get("ref_number", "")
        detail_url = raw.get("detail_url", "")
        doc_id = self._make_doc_id(ref, detail_url)
        pdf_url = raw.get("pdf_url", "")

        return {
            "_id": f"GI/SupremeCourt/{doc_id}",
            "_source": "GI/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": now,
            "title": raw.get("title", "Unknown"),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": detail_url or pdf_url,
            "doc_id": doc_id,
            "judge": raw.get("judge"),
            "court": raw.get("court"),
            "decision_date": raw.get("date"),
            "case_number": raw.get("case_number", ref),
            "file_url": pdf_url,
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        years = self._get_year_list()
        limit = 15 if sample else None
        count = 0

        for year in years:
            if limit and count >= limit:
                break

            logger.info(f"Fetching judgments for {year}...")
            entries = self._parse_year_page(year)
            logger.info(f"  Found {len(entries)} entries for {year}")

            for entry in entries:
                if limit and count >= limit:
                    break

                title = entry.get("title", "?")
                detail_url = entry["detail_url"]
                logger.info(f"  [{count + 1}] Detail: {title[:60]}")

                # Fetch detail page for judge, court, PDF URL
                detail_info = self._fetch_detail(detail_url)
                entry.update(detail_info)

                pdf_url = entry.get("pdf_url")
                if not pdf_url:
                    logger.warning(f"  No PDF found for: {title[:60]}")
                    continue

                logger.info(f"  Downloading PDF: {pdf_url}")
                try:
                    self.rate_limiter.wait()
                    resp = self.client.get(pdf_url)
                    resp.raise_for_status()
                except Exception as e:
                    logger.warning(f"  Failed to download PDF: {e}")
                    continue

                if resp.content[:5] != b"%PDF-":
                    logger.warning(f"  Not a PDF: {pdf_url}")
                    continue

                text = self._extract_pdf_text(resp.content)
                if not text or len(text.strip()) < 50:
                    logger.warning(f"  No meaningful text from {title[:40]}")
                    continue

                entry["text"] = text
                yield entry
                count += 1

        logger.info(f"Fetched {count} judgments total")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        current_year = datetime.now().year
        logger.info(f"Fetching updates for {current_year}...")
        entries = self._parse_year_page(current_year)

        for entry in entries:
            detail_url = entry["detail_url"]
            detail_info = self._fetch_detail(detail_url)
            entry.update(detail_info)

            pdf_url = entry.get("pdf_url")
            if not pdf_url:
                continue

            try:
                self.rate_limiter.wait()
                resp = self.client.get(pdf_url)
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"  Failed: {e}")
                continue

            if resp.content[:5] != b"%PDF-":
                continue

            text = self._extract_pdf_text(resp.content)
            if not text or len(text.strip()) < 50:
                continue

            entry["text"] = text
            yield entry


if __name__ == "__main__":
    scraper = GISupremeCourtScraper()

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
