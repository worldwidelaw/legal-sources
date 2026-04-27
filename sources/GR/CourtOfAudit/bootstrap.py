#!/usr/bin/env python3
"""
GR/CourtOfAudit -- Greek Court of Audit (Ελεγκτικό Συνέδριο) Data Fetcher

Fetches case law from the Hellenic Court of Audit, Greece's supreme audit
institution responsible for public finance, procurement, and pension matters.

Strategy:
  - Scrapes the case law listing pages at http://www.elsyn.gr/el/νομολογία
  - Full text is embedded directly in HTML (no PDF extraction needed)
  - Paginated results (page=0 to page=N)
  - Uses HTTP (site has SSL certificate issues)

Document types:
  - Απόφαση (Decision) - Court decisions on specific cases
  - Πράξη (Act) - Administrative acts by court chambers
  - Γνωμοδότηση (Opinion) - Advisory opinions on legal matters

Data:
  - Decisions on public contracts, state procurement, financial audits
  - Pension and retirement fund disputes
  - Covers 2016 to present (Drupal 10 site)

License: Open public data

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GR.CourtOfAudit")

# Base URL - using HTTP due to SSL certificate issues
BASE_URL = "http://www.elsyn.gr"
CASE_LAW_URL = f"{BASE_URL}/el/%CE%BD%CE%BF%CE%BC%CE%BF%CE%BB%CE%BF%CE%B3%CE%AF%CE%B1"  # /el/νομολογία

# Decision type mapping (Greek -> English)
DECISION_TYPES = {
    "Απόφαση": "decision",
    "Πράξη": "act",
    "Γνωμοδότηση": "opinion",
}


class CourtOfAuditScraper(BaseScraper):
    """
    Scraper for GR/CourtOfAudit -- Greek Court of Audit case law.
    Country: GR
    URL: https://www.elsyn.gr

    Data types: case_law
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "el,en;q=0.9",
            },
            timeout=60,
        )

    def _clean_text(self, text: str) -> str:
        """Clean HTML entities and excessive whitespace from text."""
        if not text:
            return ""
        # Decode HTML entities
        text = html.unescape(text)
        # Remove HTML tags if any remain
        text = re.sub(r'<[^>]+>', '', text)
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'\n\s*\n', '\n\n', text)
        return text.strip()

    def _parse_header(self, header: str) -> Tuple[str, str, str, str]:
        """
        Parse decision header to extract type, number, year, and chamber.

        Examples:
          "Απόφαση   62/2026  Έβδομο Τμήμα" -> ("Απόφαση", "62", "2026", "Έβδομο Τμήμα")
          "Πράξη   51/2026  Ζ' Κλιμάκιο" -> ("Πράξη", "51", "2026", "Ζ' Κλιμάκιο")
          "Γνωμοδότηση Γενική Συνεδρίαση  1η/2026 Πλήρης Ολομέλεια" -> ("Γνωμοδότηση", "1", "2026", "Πλήρης Ολομέλεια")
        """
        header = self._clean_text(header)

        # Extract decision type
        decision_type = ""
        for dtype in DECISION_TYPES.keys():
            if header.startswith(dtype):
                decision_type = dtype
                break

        # Extract number and year (format: NUMBER/YEAR or NUMBERη/YEAR)
        number_match = re.search(r'(\d+)[ηή]?/(\d{4})', header)
        if number_match:
            number = number_match.group(1)
            year = number_match.group(2)
        else:
            # Try alternative format like "3η, 4η, 5η, & 6η  Ε' Κλιμάκιο"
            number_match = re.search(r'(\d+)[ηή]?', header)
            number = number_match.group(1) if number_match else ""
            year_match = re.search(r'/(\d{4})', header)
            year = year_match.group(1) if year_match else ""

        # Extract chamber/division (everything after the number/year)
        chamber = ""
        if number_match:
            remainder = header[number_match.end():].strip()
            # Remove leading slashes, numbers, and punctuation
            chamber = re.sub(r'^[\s\d/,&]+', '', remainder).strip()

        return decision_type, number, year, chamber

    def _extract_pdf_urls(self, html_content: str) -> List[str]:
        """Extract PDF download URLs from the decision entry."""
        pdf_pattern = r'href="(/sites/default/files/Law%20cases/[^"]+\.pdf)"'
        matches = re.findall(pdf_pattern, html_content)
        return [f"{BASE_URL}{url}" for url in matches]

    def _parse_listing_page(self, html_content: str) -> List[Dict[str, Any]]:
        """
        Parse a listing page and extract all decisions with full text.

        Returns list of raw decision dicts with:
          - header, decision_type, number, year, chamber
          - full_text (from legal-page-hidden div)
          - pdf_urls
        """
        decisions = []

        # Find all decision entries
        # Pattern: <h2 class="decision-page-header">...</h2> followed by
        # <div class="legal-page">...<div class="legal-page-hidden">FULL TEXT</div>...</div>

        # Split by decision headers
        header_pattern = r'<h2 class="decision-page-header">(.*?)</h2>'
        headers = re.findall(header_pattern, html_content, re.DOTALL)

        # Split content by headers to get text blocks
        parts = re.split(header_pattern, html_content, flags=re.DOTALL)

        # parts[0] is before first header, parts[1] is header1, parts[2] is content1, etc.
        for i in range(1, len(parts) - 1, 2):
            header = parts[i]
            content = parts[i + 1] if i + 1 < len(parts) else ""

            # Extract full text from legal-page-hidden div
            text_match = re.search(
                r'<div class="legal-page-hidden">(.*?)</div>',
                content,
                re.DOTALL
            )

            if not text_match:
                continue

            full_text = self._clean_text(text_match.group(1))

            # Skip if text is too short (likely just navigation elements)
            if len(full_text) < 100:
                continue

            # Parse header
            decision_type, number, year, chamber = self._parse_header(header)

            # Extract PDF URLs
            pdf_urls = self._extract_pdf_urls(content)

            # Generate unique ID
            decision_id = f"ELSYN-{decision_type}-{number}-{year}" if number and year else None
            if not decision_id:
                # Fallback: hash first 100 chars of text
                import hashlib
                text_hash = hashlib.md5(full_text[:100].encode()).hexdigest()[:8]
                decision_id = f"ELSYN-{text_hash}"

            decisions.append({
                "id": decision_id,
                "header": self._clean_text(header),
                "decision_type": decision_type,
                "number": number,
                "year": year,
                "chamber": chamber,
                "full_text": full_text,
                "pdf_urls": pdf_urls,
            })

        return decisions

    def _fetch_page(self, page: int = 0) -> Optional[str]:
        """Fetch a single listing page."""
        try:
            self.rate_limiter.wait()
            url = f"/el/%CE%BD%CE%BF%CE%BC%CE%BF%CE%BB%CE%BF%CE%B3%CE%AF%CE%B1?page={page}"
            resp = self.client.get(url)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.error(f"Failed to fetch page {page}: {e}")
            return None

    def _get_total_pages(self, html_content: str) -> int:
        """Extract total number of pages from pagination."""
        # Look for "Τελευταία σελίδα" (Last page) link
        match = re.search(r'href="\?page=(\d+)"[^>]*title="Τελευταία σελίδα"', html_content)
        if match:
            return int(match.group(1)) + 1  # pages are 0-indexed
        return 1

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all case law documents from the Court of Audit.
        """
        logger.info("Starting full fetch from Court of Audit...")

        # First, get page 0 to determine total pages
        first_page = self._fetch_page(0)
        if not first_page:
            logger.error("Failed to fetch first page")
            return

        total_pages = self._get_total_pages(first_page)
        logger.info(f"Found {total_pages} pages to fetch")

        # Parse first page
        decisions = self._parse_listing_page(first_page)
        logger.info(f"Page 0: Found {len(decisions)} decisions")
        for decision in decisions:
            yield decision

        # Fetch remaining pages
        for page in range(1, total_pages):
            html_content = self._fetch_page(page)
            if not html_content:
                continue

            decisions = self._parse_listing_page(html_content)
            logger.info(f"Page {page}: Found {len(decisions)} decisions")

            for decision in decisions:
                yield decision

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents published since the given date.

        Since the site doesn't have date filtering, we fetch recent pages
        and filter by year from the decision headers.
        """
        since_year = since.year
        logger.info(f"Fetching updates since {since.isoformat()} (year >= {since_year})...")

        page = 0
        found_old = False

        while not found_old:
            html_content = self._fetch_page(page)
            if not html_content:
                break

            decisions = self._parse_listing_page(html_content)

            for decision in decisions:
                year = decision.get("year", "")
                if year and int(year) >= since_year:
                    yield decision
                elif year and int(year) < since_year:
                    # Found an old decision, but continue checking this page
                    found_old = True

            page += 1

            # Safety limit - don't fetch more than 10 pages for updates
            if page >= 10:
                break

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw scraped data to standard schema.
        """
        # Build title from header components
        decision_type = raw.get("decision_type", "")
        number = raw.get("number", "")
        year = raw.get("year", "")
        chamber = raw.get("chamber", "")

        if decision_type and number and year:
            title = f"{decision_type} {number}/{year}"
            if chamber:
                title += f" - {chamber}"
        else:
            title = raw.get("header", "") or f"Court of Audit Decision {raw.get('id')}"

        # Construct URL - the site doesn't have individual decision pages
        # so we link to the listing page
        url = CASE_LAW_URL

        # Determine date from year
        date = None
        if year:
            date = f"{year}-01-01"  # Use January 1st as placeholder

        return {
            "_id": raw.get("id"),
            "_source": "GR/CourtOfAudit",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("full_text", ""),
            "date": date,
            "year": year,
            "url": url,
            "decision_type": decision_type,
            "decision_type_en": DECISION_TYPES.get(decision_type, ""),
            "number": number,
            "chamber": chamber,
            "pdf_urls": raw.get("pdf_urls", []),
            "header_raw": raw.get("header", ""),
        }

    def _fetch_sample(self, sample_size: int = 12) -> list:
        """Fetch sample records for validation."""
        samples = []

        # Fetch first page only for samples
        html_content = self._fetch_page(0)
        if not html_content:
            logger.error("Failed to fetch page for samples")
            return samples

        decisions = self._parse_listing_page(html_content)

        for decision in decisions[:sample_size]:
            normalized = self.normalize(decision)
            samples.append(normalized)
            logger.info(
                f"Sample {len(samples)}/{sample_size}: {normalized['_id']} "
                f"({len(normalized.get('text', ''))} chars)"
            )

        return samples


def main():
    import argparse

    parser = argparse.ArgumentParser(description="GR/CourtOfAudit Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch sample records for validation")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CourtOfAuditScraper()

    if args.command == "test":
        print("Testing Court of Audit connection...")
        html_content = scraper._fetch_page(0)
        if html_content:
            decisions = scraper._parse_listing_page(html_content)
            total_pages = scraper._get_total_pages(html_content)
            print(f"SUCCESS: Found {len(decisions)} decisions on first page")
            print(f"Total pages: {total_pages}")
            if decisions:
                d = decisions[0]
                print(f"Sample ID: {d.get('id')}")
                print(f"Sample type: {d.get('decision_type')}")
                print(f"Sample text preview: {d.get('full_text', '')[:200]}...")
        else:
            print("FAILED: Could not fetch page")
            sys.exit(1)

    elif args.command == "bootstrap":
        if args.sample:
            print("Fetching sample records...")
            samples = scraper._fetch_sample(sample_size=12)

            # Save samples
            sample_dir = scraper.source_dir / "sample"
            sample_dir.mkdir(exist_ok=True)

            for record in samples:
                # Use sanitized filename
                safe_id = re.sub(r'[^\w\-]', '_', record['_id'])
                filepath = sample_dir / f"{safe_id}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"\nSaved {len(samples)} sample records to {sample_dir}/")

            # Print summary
            if samples:
                text_lengths = [len(s.get("text", "")) for s in samples]
                avg_len = sum(text_lengths) / len(text_lengths)
                print(f"Average text length: {avg_len:.0f} characters")
                print(f"Min text length: {min(text_lengths)} chars")
                print(f"Max text length: {max(text_lengths)} chars")

                # Verify all have text
                with_text = sum(1 for s in samples if len(s.get("text", "")) > 100)
                print(f"Records with substantial text: {with_text}/{len(samples)}")
        else:
            print("Running full bootstrap...")
            count = 0
            for record in scraper.fetch_all():
                normalized = scraper.normalize(record)
                print(f"  {normalized['_id']}: {len(normalized.get('text', ''))} chars")
                count += 1

            print(f"\nFetched {count} Court of Audit decisions")

    elif args.command == "update":
        # Default to last 30 days
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=30)
        print(f"Fetching updates since {since.isoformat()}...")

        count = 0
        for record in scraper.fetch_updates(since):
            normalized = scraper.normalize(record)
            print(f"  {normalized['_id']}: {len(normalized.get('text', ''))} chars")
            count += 1

        print(f"\nFetched {count} new Court of Audit decisions")


if __name__ == "__main__":
    main()
