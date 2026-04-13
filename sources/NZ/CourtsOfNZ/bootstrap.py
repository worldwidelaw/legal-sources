#!/usr/bin/env python3
"""
NZ/CourtsOfNZ -- Courts of New Zealand Official Judgments

Fetches judgments of public interest from the Supreme Court, Court of Appeal,
and High Court of New Zealand via official RSS feeds.

Strategy:
  - Parse RSS feeds for 3 courts (Supreme, Court of Appeal, High Court)
  - Each RSS item has title, description (summary), date, and case page URL
  - Visit case pages to extract PDF links
  - Download PDFs and extract full text with PyPDF2
  - ~240 judgments from 2019 to present

Source: https://www.courtsofnz.govt.nz/judgments (Government of NZ, open access)
Rate limit: 1 req/sec

Usage:
  python bootstrap.py bootstrap            # Full pull (all judgments)
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import io
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from email.utils import parsedate_to_datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NZ.CourtsOfNZ")

BASE_URL = "https://www.courtsofnz.govt.nz"

COURT_FEEDS = {
    "supreme-court": "Supreme Court",
    "court-of-appeal": "Court of Appeal",
    "high-court": "High Court",
}


class CourtsOfNZScraper(BaseScraper):
    """
    Scraper for NZ/CourtsOfNZ -- Courts of New Zealand.
    Country: NZ
    URL: https://www.courtsofnz.govt.nz/judgments

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "*/*",
            },
            timeout=60,
        )

    # -- RSS parsing ---------------------------------------------------------

    def _parse_rss_feed(self, court_slug: str) -> list[dict]:
        """Parse an RSS feed and return list of case entries."""
        url = f"{BASE_URL}/judgments/{court_slug}/RSS"
        try:
            resp = self.client.get(url, timeout=30)
            if resp is None or resp.status_code != 200:
                logger.error(f"RSS feed failed for {court_slug}: {resp.status_code if resp else 'None'}")
                return []

            root = ET.fromstring(resp.content)
            entries = []
            for item in root.findall('.//item'):
                title = item.findtext('title', '').strip()
                link = item.findtext('link', '').strip()
                description = item.findtext('description', '').strip()
                pub_date = item.findtext('pubDate', '').strip()
                guid = item.findtext('guid', '').strip()

                # Parse date
                date_iso = None
                if pub_date:
                    try:
                        dt = parsedate_to_datetime(pub_date)
                        date_iso = dt.strftime('%Y-%m-%d')
                    except Exception:
                        pass

                # Clean HTML from description
                if description:
                    description = re.sub(r'<[^>]+>', ' ', description)
                    description = re.sub(r'\s+', ' ', description).strip()

                # Generate a stable ID from the URL slug
                slug = link.rstrip('/').split('/')[-1] if link else guid.rstrip('/').split('/')[-1]

                entries.append({
                    'title': title,
                    'case_url': link,
                    'description': description,
                    'date': date_iso,
                    'slug': slug,
                    'court': COURT_FEEDS[court_slug],
                    'court_slug': court_slug,
                })

            logger.info(f"RSS {court_slug}: {len(entries)} items")
            return entries

        except Exception as e:
            logger.error(f"Error parsing RSS for {court_slug}: {e}")
            return []

    # -- Case page and PDF extraction ----------------------------------------

    def _get_pdf_url(self, case_url: str) -> Optional[str]:
        """Visit a case page and extract the main judgment PDF URL."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(case_url, timeout=30)
            if resp is None or resp.status_code != 200:
                return None

            # Find PDF links - prefer non-MR (media release) PDFs
            pdfs = re.findall(r'href="([^"]*\.pdf)"', resp.text)
            main_pdf = None
            for pdf in pdfs:
                if '/assets/cases/' in pdf:
                    if 'MR-' not in pdf and 'MR_' not in pdf:
                        main_pdf = pdf
                        break
            # Fallback to any case PDF
            if not main_pdf:
                for pdf in pdfs:
                    if '/assets/cases/' in pdf:
                        main_pdf = pdf
                        break

            if main_pdf and not main_pdf.startswith('http'):
                main_pdf = f"{BASE_URL}{main_pdf}"

            # Also extract citation from the page
            return main_pdf

        except Exception as e:
            logger.debug(f"Error fetching case page {case_url}: {e}")
            return None

    def _extract_citation(self, case_url: str, html: str = None) -> Optional[str]:
        """Extract the citation (e.g., [2026] NZHC 544) from a case page."""
        if html is None:
            return None
        match = re.search(r'\[(\d{4})\]\s*(NZSC|NZCA|NZHC)\s+(\d+)', html)
        if match:
            return f"[{match.group(1)}] {match.group(2)} {match.group(3)}"
        return None

    def _download_and_extract_pdf(self, pdf_url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="NZ/CourtsOfNZ",
            source_id="",
            pdf_url=pdf_url,
            table="case_law",
        ) or ""

    def _fetch_case(self, entry: dict) -> Optional[dict]:
        """Fetch full text for a single case."""
        case_url = entry.get('case_url', '')

        # Get case page and extract PDF URL + citation
        self.rate_limiter.wait()
        try:
            resp = self.client.get(case_url, timeout=30)
            if resp and resp.status_code == 200:
                html = resp.text
                entry['citation'] = self._extract_citation(case_url, html)

                # Extract PDF URL
                pdfs = re.findall(r'href="([^"]*\.pdf)"', html)
                main_pdf = None
                for pdf in pdfs:
                    if '/assets/cases/' in pdf:
                        if 'MR-' not in pdf and 'MR_' not in pdf:
                            main_pdf = pdf
                            break
                if not main_pdf:
                    for pdf in pdfs:
                        if '/assets/cases/' in pdf:
                            main_pdf = pdf
                            break

                if main_pdf and not main_pdf.startswith('http'):
                    main_pdf = f"{BASE_URL}{main_pdf}"
                entry['pdf_url'] = main_pdf
        except Exception as e:
            logger.debug(f"Error fetching case page: {e}")

        pdf_url = entry.get('pdf_url')
        if not pdf_url:
            # Fall back to description as text
            if entry.get('description') and len(entry['description']) > 100:
                entry['text'] = entry['description']
                return entry
            logger.debug(f"No PDF for {entry.get('slug')}")
            return None

        text = self._download_and_extract_pdf(pdf_url)
        if not text or len(text) < 50:
            # Fall back to description
            if entry.get('description') and len(entry['description']) > 100:
                entry['text'] = entry['description']
                return entry
            return None

        entry['text'] = text
        return entry

    # -- Core scraper methods ------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all judgments from all 3 court RSS feeds."""
        all_entries = []
        for slug in COURT_FEEDS:
            self.rate_limiter.wait()
            entries = self._parse_rss_feed(slug)
            all_entries.extend(entries)

        logger.info(f"Total entries from RSS: {len(all_entries)}")

        found = 0
        for entry in all_entries:
            result = self._fetch_case(entry)
            if result:
                found += 1
                if found % 20 == 0:
                    logger.info(f"Progress: {found}/{len(all_entries)} cases with text")
                yield result

        logger.info(f"Fetch complete: {found} cases with text out of {len(all_entries)} from RSS")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch cases published since a given date."""
        for slug in COURT_FEEDS:
            self.rate_limiter.wait()
            entries = self._parse_rss_feed(slug)
            for entry in entries:
                if entry.get('date') and entry['date'] >= since.strftime('%Y-%m-%d'):
                    result = self._fetch_case(entry)
                    if result:
                        yield result

    def fetch_sample(self, count: int = 15) -> Generator[dict, None, None]:
        """Fetch sample cases — 5 from each court."""
        found = 0
        per_court = max(count // 3, 5)

        for slug, court_name in COURT_FEEDS.items():
            if found >= count:
                break

            self.rate_limiter.wait()
            entries = self._parse_rss_feed(slug)
            court_found = 0

            for entry in entries:
                if found >= count or court_found >= per_court:
                    break

                result = self._fetch_case(entry)
                if result:
                    found += 1
                    court_found += 1
                    title = result.get('title', 'N/A')[:60]
                    text_len = len(result.get('text', ''))
                    logger.info(f"Sample {found}/{count}: [{court_name}] {title} ({text_len} chars)")
                    yield result

        logger.info(f"Sample complete: {found} records")

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw case record to standard schema."""
        slug = raw.get('slug', 'unknown')
        title = raw.get('title', 'Unknown Case')
        text = raw.get('text', '')
        court = raw.get('court', '')
        citation = raw.get('citation', '')

        # Build ID from citation if available, otherwise slug
        if citation:
            safe_id = citation.replace('[', '').replace(']', '').replace(' ', '-')
        else:
            safe_id = slug

        return {
            "_id": f"NZ-CourtsOfNZ-{safe_id}",
            "_source": "NZ/CourtsOfNZ",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get('date'),
            "url": raw.get('case_url', ''),
            "court": court,
            "citation": citation,
            "pdf_url": raw.get('pdf_url', ''),
            "language": "en",
        }

    def test_api(self) -> bool:
        """Test connectivity to Courts of NZ website and RSS feeds."""
        logger.info("Testing Courts of New Zealand access...")

        # Test main page
        resp = self.client.get(f"{BASE_URL}/judgments", timeout=15)
        if not resp or resp.status_code != 200:
            logger.error(f"Main page failed: {resp.status_code if resp else 'None'}")
            return False
        logger.info("Main page: OK")

        # Test RSS feed
        self.rate_limiter.wait()
        entries = self._parse_rss_feed("high-court")
        if not entries:
            logger.error("RSS feed returned no entries")
            return False
        logger.info(f"RSS feed: {len(entries)} entries")

        # Test case page + PDF
        if entries:
            self.rate_limiter.wait()
            result = self._fetch_case(entries[0])
            if result and len(result.get('text', '')) > 100:
                logger.info(f"PDF extraction: OK ({len(result['text'])} chars)")
            else:
                logger.error("PDF extraction failed")
                return False

        logger.info("All tests passed!")
        return True


# ── CLI ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    scraper = CourtsOfNZScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample] [--count N]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        count = 15
        for i, arg in enumerate(sys.argv):
            if arg == "--count" and i + 1 < len(sys.argv):
                count = int(sys.argv[i + 1])

        if sample_mode:
            gen = scraper.fetch_sample(count=count)
        else:
            gen = scraper.fetch_all()

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in gen:
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1
            logger.info(f"Saved: {out_path.name}")

        logger.info(f"Bootstrap complete: {saved} records saved to {sample_dir}")

    elif command == "update":
        logger.info("Running full fetch")
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in scraper.fetch_all():
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1

        logger.info(f"Update complete: {saved} records saved")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
