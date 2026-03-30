#!/usr/bin/env python3
"""
INTL/WIPO-UDRP -- WIPO Domain Name Dispute Decisions (UDRP)

Fetches panel decisions from the WIPO Arbitration and Mediation Center.

Strategy:
  - Parse list.jsp pages per year/range for case metadata (parties, domains, result)
  - Follow text.jsp redirect to get decision full text
  - 1999-2021: HTML decisions parsed directly
  - 2022+: PDF decisions extracted via PyMuPDF
  - ~75,000+ decisions since 1999

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.WIPO-UDRP")

BASE_URL = "https://www.wipo.int"
INDEX_URL = f"{BASE_URL}/amc/en/domains/decisionsx/index.html"
LIST_URL = f"{BASE_URL}/amc/en/domains/decisionsx/list.jsp"
TEXT_URL = f"{BASE_URL}/amc/en/domains/search/text.jsp"

# Years where decisions are in HTML format (vs PDF for 2022+)
HTML_CUTOFF_YEAR = 2022


class WIPOUDRPScraper(BaseScraper):
    """
    Scraper for INTL/WIPO-UDRP -- WIPO Domain Name Dispute Decisions.
    Country: INTL
    URL: https://www.wipo.int/amc/en/domains/search/

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })

    def _get_year_ranges(self) -> list[tuple[int, int, int]]:
        """Parse the index page to get (year, seq_min, seq_max) tuples."""
        r = self.session.get(INDEX_URL, timeout=30)
        r.raise_for_status()
        matches = re.findall(
            r'list\.jsp\?prefix=D&year=(\d+)&seq_min=(\d+)&seq_max=(\d+)',
            r.text,
        )
        ranges = [(int(y), int(mn), int(mx)) for y, mn, mx in matches]
        ranges.sort(key=lambda x: (x[0], x[1]))
        return ranges

    def _parse_list_page(self, year: int, seq_min: int, seq_max: int) -> list[dict]:
        """Parse a list.jsp page for case metadata."""
        params = {
            "prefix": "D",
            "year": str(year),
            "seq_min": str(seq_min),
            "seq_max": str(seq_max),
        }
        r = self.session.get(LIST_URL, params=params, timeout=30)
        r.raise_for_status()

        cases = []
        # Match rows with a link (have a published decision)
        # Links go to text.jsp?case=... (newer) or directly to decisions/html/... (older)
        pattern = re.compile(
            r'<tr><td[^>]*><a\s+href="([^"]*)"[^>]*>'
            r'(D\d{4}-\d{4})</a></td>'
            r'<td[^>]*>(.*?)</td>'
            r'<td[^>]*>(.*?)</td>'
            r'<td[^>]*>(.*?)</td>'
            r'<td[^>]*>(.*?)</td></tr>',
            re.S,
        )
        for m in pattern.finditer(r.text):
            href, case_id, complainant, respondent, domains, result = m.groups()
            cases.append({
                "case_number": case_id.strip(),
                "decision_url": href.strip(),
                "complainant": self._clean_field(complainant),
                "respondent": self._clean_field(respondent),
                "domain_names": self._clean_field(domains),
                "decision_result": self._clean_field(result),
            })
        return cases

    @staticmethod
    def _clean_field(html_str: str) -> str:
        """Clean a table cell value: strip tags, decode entities."""
        text = re.sub(r'<br\s*/?>', ', ', html_str, flags=re.I)
        text = re.sub(r'<[^>]+>', '', text)
        text = unescape(text).strip()
        return re.sub(r'\s+', ' ', text)

    def _strip_html(self, html: str) -> str:
        """Remove HTML tags and decode entities, preserving paragraph breaks."""
        text = re.sub(r'<br\s*/?>', '\n', html, flags=re.I)
        text = re.sub(r'</(p|div|h[1-6]|li|tr)>', '\n', text, flags=re.I)
        text = re.sub(r'<[^>]+>', '', text)
        text = unescape(text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _extract_html_decision(self, html: str) -> dict:
        """Extract metadata and full text from an HTML decision page."""
        meta = {}
        for m in re.finditer(r'<meta\s+name="([^"]+)"\s+content="([^"]*)"', html, re.I):
            meta[m.group(1).lower()] = unescape(m.group(2))

        title_m = re.search(r'<title>(.*?)</title>', html, re.S | re.I)
        title = unescape(title_m.group(1).strip()) if title_m else ""

        # Extract body content
        body_m = re.search(r'<body[^>]*>(.*)</body>', html, re.S | re.I)
        body_html = body_m.group(1) if body_m else html
        text = self._strip_html(body_html)

        return {
            "title": title,
            "text": text,
            "date": meta.get("date", ""),
            "meta_domains": meta.get("domains", ""),
            "meta_complainants": meta.get("complainants", ""),
            "meta_keywords": meta.get("keywords", ""),
        }

    def _extract_pdf_decision(self, pdf_bytes: bytes) -> dict:
        """Extract text from a PDF decision using PyMuPDF."""
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages = []
        for page in doc:
            pages.append(page.get_text())
        doc.close()
        text = "\n".join(pages).strip()
        return {"text": text, "title": "", "date": "", "meta_domains": "",
                "meta_complainants": "", "meta_keywords": ""}

    def _fetch_decision_text(self, case_number: str, decision_url: str = "") -> Optional[dict]:
        """Fetch full text for a single decision.

        Args:
            case_number: e.g. "D2020-0001"
            decision_url: href from list page (may be text.jsp or direct HTML/PDF link)
        """
        if decision_url and decision_url.startswith("/"):
            url = f"{BASE_URL}{decision_url}"
        elif decision_url and decision_url.startswith("http"):
            url = decision_url
        else:
            url = f"{TEXT_URL}?case={case_number}"

        try:
            r = self.session.get(url, timeout=60, allow_redirects=True)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch {case_number}: {e}")
            return None

        final_url = r.url
        content_type = r.headers.get("content-type", "")

        if "application/pdf" in content_type or final_url.endswith(".pdf"):
            try:
                return self._extract_pdf_decision(r.content)
            except Exception as e:
                logger.warning(f"PDF extraction failed for {case_number}: {e}")
                return None
        else:
            # Fix double-encoded UTF-8 on older pages
            r.encoding = r.apparent_encoding or "utf-8"
            return self._extract_html_decision(r.text)

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw UDRP decision record."""
        case_number = raw.get("case_number", "")
        date_str = raw.get("date", "") or raw.get("meta_date", "")
        if not date_str and raw.get("text_data", {}).get("date"):
            date_str = raw["text_data"]["date"]

        text = raw.get("text", "")
        title = raw.get("title", "") or f"WIPO UDRP Decision: {case_number}"

        return {
            "_id": case_number,
            "_source": "INTL/WIPO-UDRP",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str if date_str else None,
            "url": f"{BASE_URL}/amc/en/domains/search/text.jsp?case={case_number}",
            "case_number": case_number,
            "complainant": raw.get("complainant", ""),
            "respondent": raw.get("respondent", ""),
            "domain_names": raw.get("domain_names", ""),
            "decision_result": raw.get("decision_result", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Yield all UDRP decisions with full text."""
        logger.info("Fetching year/range index...")
        ranges = self._get_year_ranges()
        logger.info(f"Found {len(ranges)} year/range blocks")

        if sample:
            # For sample: pick a few ranges from different eras
            sample_ranges = [
                r for r in ranges
                if (r[0] == 2005 and r[1] == 1) or
                   (r[0] == 2015 and r[1] == 1) or
                   (r[0] == 2024 and r[1] == 1)
            ]
            if not sample_ranges:
                sample_ranges = ranges[:3]
            ranges = sample_ranges

        count = 0
        for year, seq_min, seq_max in ranges:
            logger.info(f"Processing D{year} seq {seq_min}-{seq_max}...")
            try:
                cases = self._parse_list_page(year, seq_min, seq_max)
            except Exception as e:
                logger.error(f"Failed to parse list page {year}/{seq_min}-{seq_max}: {e}")
                continue

            logger.info(f"  Found {len(cases)} cases with decisions")

            for case in cases:
                case_num = case["case_number"]
                time.sleep(1.0)  # Rate limit

                text_data = self._fetch_decision_text(
                    case_num, case.get("decision_url", ""))
                if not text_data or not text_data.get("text"):
                    logger.warning(f"  No text for {case_num}, skipping")
                    continue

                # Merge list metadata with decision text
                record = {
                    **case,
                    "text": text_data["text"],
                    "title": text_data.get("title") or f"WIPO UDRP Decision: {case_num}",
                    "date": text_data.get("date", ""),
                }

                normalized = self.normalize(record)
                yield normalized
                count += 1

                if sample and count >= 15:
                    logger.info(f"Sample complete: {count} records")
                    return

        logger.info(f"Fetch complete: {count} records")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch decisions from the current year."""
        current_year = datetime.now().year
        ranges = self._get_year_ranges()
        current_ranges = [(y, mn, mx) for y, mn, mx in ranges if y >= current_year]

        count = 0
        for year, seq_min, seq_max in current_ranges:
            logger.info(f"Processing D{year} seq {seq_min}-{seq_max}...")
            try:
                cases = self._parse_list_page(year, seq_min, seq_max)
            except Exception as e:
                logger.error(f"Failed: {e}")
                continue

            for case in cases:
                time.sleep(1.0)
                text_data = self._fetch_decision_text(
                    case["case_number"], case.get("decision_url", ""))
                if not text_data or not text_data.get("text"):
                    continue

                record = {**case, "text": text_data["text"],
                          "title": text_data.get("title", ""),
                          "date": text_data.get("date", "")}
                normalized = self.normalize(record)

                if normalized.get("date") and normalized["date"] >= since:
                    yield normalized
                    count += 1

        logger.info(f"Update complete: {count} new records since {since}")

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            r = self.session.get(INDEX_URL, timeout=15)
            r.raise_for_status()
            assert "list.jsp" in r.text
            logger.info("Connectivity test passed")
            return True
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            return False


if __name__ == "__main__":
    scraper = WIPOUDRPScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    sample = "--sample" in sys.argv

    if cmd == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)
    elif cmd == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_all(sample=sample):
            out_path = sample_dir / f"{record['_id'].replace('/', '_')}.json"
            out_path.write_text(json.dumps(record, ensure_ascii=False, indent=2))
            count += 1
            logger.info(f"  Saved {record['_id']} ({len(record.get('text', ''))} chars)")
        logger.info(f"Done: {count} records saved to {sample_dir}")
    elif cmd == "update":
        since = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith("-") else "2026-01-01"
        for record in scraper.fetch_updates(since):
            logger.info(f"  Updated: {record['_id']}")
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
