#!/usr/bin/env python3
"""
AE/AbuDhabi-Legislation -- Abu Dhabi Official Gazette Fetcher

Fetches legislation from the Abu Dhabi Official Gazette portal. The gazette
is published monthly and contains laws, decrees, executive council resolutions,
circulars and regulations for the Emirate of Abu Dhabi.

English-language gazette editions from 2009-2023 are available as direct PDF
downloads. Text is extracted via pdfplumber/pypdf.

Each gazette edition becomes one record. The full text includes all laws and
decrees within that edition.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records
  python bootstrap.py test-api           # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

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
logger = logging.getLogger("legal-data-hunter.AE.AbuDhabi-Legislation")

GAZETTE_PAGE = "https://www.abudhabi.gov.ae/en/policies-and-legislations"
BASE_URL = "https://www.abudhabi.gov.ae"

# Months for date extraction from edition names
MONTH_MAP = {
    "first": "01", "1st": "01", "january": "01",
    "second": "02", "2nd": "02", "february": "02",
    "third": "03", "3rd": "03", "march": "03",
    "fourth": "04", "4th": "04", "april": "04",
    "fifth": "05", "5th": "05", "may": "05",
    "sixth": "06", "6th": "06", "june": "06",
    "seventh": "07", "7th": "07", "july": "07",
    "eighth": "08", "8th": "08", "august": "08",
    "ninth": "09", "9th": "09", "september": "09",
    "tenth": "10", "10th": "10", "october": "10",
    "eleventh": "11", "11th": "11", "november": "11",
    "twelfth": "12", "12th": "12", "december": "12",
}


def _parse_edition_info(pdf_path: str) -> dict:
    """Extract year, edition number, and approximate date from gazette PDF path."""
    year_match = re.search(r"/(\d{4})/", pdf_path)
    year = year_match.group(1) if year_match else "unknown"

    # Extract edition number from filename
    filename = pdf_path.split("/")[-1].lower().replace(".ashx", "")

    # Try to find edition number (e.g., "first", "2-nd", "3-rd", "5th", "10th")
    edition_num = None
    # Numeric prefix pattern: "10english2019" or "3-rd-edition"
    num_match = re.match(r"(\d+)", filename)
    if num_match:
        edition_num = int(num_match.group(1))

    # Ordinal word pattern: "first-edition", "second-edition"
    ordinal_map = {
        "first": 1, "second": 2, "third": 3, "fourth": 4, "fifth": 5,
        "sixth": 6, "seventh": 7, "eighth": 8, "ninth": 9, "tenth": 10,
        "eleventh": 11, "twelfth": 12,
    }
    for word, num in ordinal_map.items():
        if word in filename:
            edition_num = num
            break

    if edition_num is None:
        edition_num = 1

    # Approximate date: edition N ≈ month N of that year
    month = str(min(edition_num, 12)).zfill(2)
    date_str = f"{year}-{month}-01"

    edition_id = f"ADGAZ-{year}-{str(edition_num).zfill(2)}"
    title = f"Abu Dhabi Official Gazette - {year} Edition {edition_num}"

    return {
        "edition_id": edition_id,
        "title": title,
        "year": year,
        "edition_num": edition_num,
        "date": date_str,
    }


class AbuDhabiLegislationScraper(BaseScraper):
    """
    Scraper for AE/AbuDhabi-Legislation -- Abu Dhabi Official Gazette.
    Country: AE
    URL: https://www.abudhabi.gov.ae/en/policies-and-legislations

    Data types: legislation
    Auth: none (public PDF downloads)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=60,
        )

    def _get_gazette_urls(self) -> list[dict]:
        """Fetch the gazette page and extract all English PDF URLs."""
        self.rate_limiter.wait()
        resp = self.client.get(GAZETTE_PAGE)
        resp.raise_for_status()
        html = resp.text

        # Find all gazette PDF links under /gazettes/ path
        pattern = re.compile(
            r'href="(/-/media/sites/adgov/gazettes/[^"]+\.ashx)"',
            re.IGNORECASE,
        )
        raw_urls = pattern.findall(html)

        # Deduplicate preserving order
        seen = set()
        results = []
        for path in raw_urls:
            if path in seen:
                continue
            seen.add(path)

            full_url = BASE_URL + path
            info = _parse_edition_info(path)
            info["pdf_url"] = full_url
            info["pdf_path"] = path
            results.append(info)

        # Sort by year desc, edition desc (newest first)
        results.sort(key=lambda x: (x["year"], x["edition_num"]), reverse=True)
        return results

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all gazette edition metadata."""
        logger.info("Fetching gazette PDF list from %s", GAZETTE_PAGE)
        editions = self._get_gazette_urls()
        logger.info("Found %d gazette editions", len(editions))
        for edition in editions:
            yield edition

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield editions published after `since`."""
        since_year = since.year
        for edition in self.fetch_all():
            try:
                if int(edition.get("year", 0)) >= since_year:
                    yield edition
            except (ValueError, TypeError):
                yield edition

    def normalize(self, raw: dict) -> dict:
        """Transform raw gazette metadata into standard schema with full text."""
        pdf_url = raw.get("pdf_url", "")
        edition_id = raw.get("edition_id", "")

        # Extract full text from PDF
        logger.info("Extracting text from %s (%s)...", edition_id, raw.get("title", ""))
        full_text = extract_pdf_markdown(
            source="AE/AbuDhabi-Legislation",
            source_id=edition_id,
            pdf_url=pdf_url,
            table="legislation",
        ) or ""

        if full_text:
            logger.info("  Extracted %d chars from %s", len(full_text), edition_id)
        else:
            logger.warning("  No text extracted from %s", edition_id)

        return {
            "_id": edition_id,
            "_source": "AE/AbuDhabi-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "edition_id": edition_id,
            "title": raw.get("title", ""),
            "text": full_text,
            "date": raw.get("date", ""),
            "url": pdf_url,
            "year": raw.get("year", ""),
            "edition_num": raw.get("edition_num"),
            "country": "AE",
            "jurisdiction": "AE-AZ",
            "language": "en",
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity test."""
        print("Testing Abu Dhabi Gazette page...")
        editions = self._get_gazette_urls()
        print(f"  Found {len(editions)} gazette editions")

        years = {}
        for e in editions:
            y = e.get("year", "?")
            years[y] = years.get(y, 0) + 1
        for y in sorted(years.keys(), reverse=True):
            print(f"  {y}: {years[y]} editions")

        if editions:
            # Test PDF download and extraction on the smallest/newest edition
            test_ed = editions[0]
            print(f"\n  Testing PDF extraction: {test_ed['title']}")
            print(f"  URL: {test_ed['pdf_url']}")
            text = extract_pdf_markdown(
                source="AE/AbuDhabi-Legislation",
                source_id=test_ed["edition_id"],
                pdf_url=test_ed["pdf_url"],
                table="legislation",
                force=True,
            )
            if text:
                print(f"  PDF extraction: SUCCESS ({len(text)} chars)")
                print(f"  First 300 chars: {text[:300]}...")
            else:
                print("  PDF extraction: FAILED")

        print("\nTest completed!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = AbuDhabiLegislationScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
