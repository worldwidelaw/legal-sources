#!/usr/bin/env python3
"""
US/eCFR -- Electronic Code of Federal Regulations

Fetches all 50 titles of the US Code of Federal Regulations with full text
using the official eCFR versioner API.

Strategy:
  - List all 50 CFR titles via /api/versioner/v1/titles
  - For each title, get the structure to discover parts
  - For each part, fetch full XML text and extract clean content
  - Normalize into standard schema with full text

Data: Public domain (US government works). Updated daily.
Rate limit: 2 req/sec (self-imposed).

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample parts
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.eCFR")

BASE_URL = "https://www.ecfr.gov/api/versioner/v1"


def xml_to_text(xml_content: str) -> str:
    """Extract clean text from eCFR XML, preserving section structure."""
    if not xml_content:
        return ""
    # Replace section/heading tags with newlines
    text = re.sub(r'<HEAD[^>]*>', '\n## ', xml_content)
    text = re.sub(r'</HEAD>', '\n', text)
    # Replace paragraph tags
    text = re.sub(r'<P[^>]*>', '\n', text)
    text = re.sub(r'</P>', '', text)
    # Replace list items
    text = re.sub(r'<FP[^>]*>', '\n  ', text)
    text = re.sub(r'</FP>', '', text)
    # Authority and source
    text = re.sub(r'<AUTH>', '\nAuthority: ', text)
    text = re.sub(r'<SOURCE>', '\nSource: ', text)
    text = re.sub(r'<CITA[^>]*>', '[', text)
    text = re.sub(r'</CITA>', ']', text)
    # Remove all remaining XML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Decode XML entities
    text = html_module.unescape(text)
    text = text.replace('&amp;', '&')
    # Clean up whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+\n', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class ECFRScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
                "Accept": "application/xml, application/json",
            },
            timeout=120,
        )

    def test_api(self):
        """Test connectivity to the eCFR API."""
        logger.info("Testing eCFR API...")
        try:
            resp = self.http.get(f"{BASE_URL}/titles")
            data = resp.json()
            titles = data.get("titles", [])
            logger.info(f"  Status: {resp.status_code}")
            logger.info(f"  Titles found: {len(titles)}")
            if titles:
                logger.info(f"  First: Title {titles[0]['number']} - {titles[0]['name']}")
                logger.info("API test PASSED")
                return True
            else:
                logger.error("API test FAILED: no titles returned")
                return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def get_titles(self) -> list:
        """Get list of all CFR titles with current dates."""
        resp = self.http.get(f"{BASE_URL}/titles")
        data = resp.json()
        return data.get("titles", [])

    def get_parts_for_title(self, title_number: int, as_of_date: str) -> list:
        """Get all part numbers for a given title."""
        url = f"{BASE_URL}/structure/{as_of_date}/title-{title_number}.json"
        try:
            resp = self.http.get(url)
            data = resp.json()
        except Exception as e:
            logger.warning(f"Failed to get structure for title {title_number}: {e}")
            return []

        parts = []

        def find_parts(node):
            if node.get("type") == "part":
                identifier = node.get("identifier", "")
                # Skip reserved/range parts like "23-49"
                if identifier and "-" not in identifier:
                    parts.append({
                        "number": identifier,
                        "label": node.get("label", ""),
                        "label_description": node.get("label_description", ""),
                    })
            for child in node.get("children", []):
                find_parts(child)

        find_parts(data)
        return parts

    def fetch_part_text(self, title_number: int, part_number: str, as_of_date: str) -> Optional[str]:
        """Fetch full XML text for a specific CFR part and convert to text."""
        url = f"{BASE_URL}/full/{as_of_date}/title-{title_number}.xml?part={part_number}"
        try:
            resp = self.http.get(url)
            if resp.status_code == 200:
                return xml_to_text(resp.text)
            else:
                logger.warning(f"HTTP {resp.status_code} for {title_number} CFR Part {part_number}")
                return None
        except Exception as e:
            logger.warning(f"Failed to fetch {title_number} CFR Part {part_number}: {e}")
            return None

    def normalize(self, title_number: int, part: dict, text: str, as_of_date: str) -> Optional[dict]:
        """Transform eCFR part data into standard schema."""
        if not text or len(text) < 20:
            return None

        part_number = part["number"]
        cfr_citation = f"{title_number} CFR Part {part_number}"
        title = part.get("label_description") or part.get("label") or cfr_citation

        return {
            "_id": cfr_citation.replace(" ", "_"),
            "_source": "US/eCFR",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"{cfr_citation} — {title}",
            "text": text,
            "date": as_of_date,
            "url": f"https://www.ecfr.gov/current/title-{title_number}/part-{part_number}",
            "cfr_citation": cfr_citation,
            "title_number": title_number,
            "part_number": part_number,
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Fetch all CFR parts with full text."""
        sample_limit = 15 if sample else None
        count = 0

        titles = self.get_titles()
        logger.info(f"Found {len(titles)} CFR titles")

        for title_info in titles:
            title_num = title_info["number"]
            as_of_date = title_info.get("up_to_date_as_of")
            if not as_of_date:
                logger.info(f"  Title {title_num}: reserved, skipping")
                continue

            logger.info(f"Processing Title {title_num}: {title_info['name'][:50]}")
            parts = self.get_parts_for_title(title_num, as_of_date)
            logger.info(f"  Found {len(parts)} parts")
            time.sleep(0.5)

            for part in parts:
                if sample_limit and count >= sample_limit:
                    return

                text = self.fetch_part_text(title_num, part["number"], as_of_date)
                time.sleep(0.5)

                if text:
                    record = self.normalize(title_num, part, text, as_of_date)
                    if record:
                        yield record
                        count += 1
                        logger.info(f"  [{count}] {record['cfr_citation']} — {len(text):,} chars")

        logger.info(f"Fetch complete: {count} parts yielded")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch all parts (eCFR is always current, no incremental updates)."""
        yield from self.fetch_all()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/eCFR data fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (15 parts)")
    args = parser.parse_args()

    scraper = ECFRScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            safe_id = re.sub(r'[^\w\-.]', '_', record["_id"])[:80]
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1

        logger.info(f"Saved {count} records to {sample_dir}")
        if count == 0:
            logger.error("No records fetched!")
            sys.exit(1)


if __name__ == "__main__":
    main()
