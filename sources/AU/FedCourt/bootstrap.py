#!/usr/bin/env python3
"""
AU/FedCourt -- Federal Court of Australia Judgments Fetcher

Fetches Federal Court of Australia judgments from the Open Australian
Legal Corpus on HuggingFace (isaacus/open-australian-legal-corpus).

Strategy:
  - Streams the HuggingFace dataset to avoid downloading 5GB+ full corpus
  - Filters for source == "federal_court_of_australia"
  - Normalizes records to standard schema with full text
  - For updates, uses the official RSS feed at judgments.fedcourt.gov.au

Data:
  - 50K+ judgments from 1977 to present
  - Full text already extracted from HTML/DOCX
  - Citations, dates, URLs included
  - Language: English

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Check RSS for new judgments
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AU.FedCourt")

DATASET_ID = "isaacus/open-australian-legal-corpus"
SOURCE_FILTER = "federal_court_of_australia"
RSS_URL = "https://www.judgments.fedcourt.gov.au/rss/fca-judgments"


class AustraliaFedCourtScraper(BaseScraper):
    """
    Scraper for AU/FedCourt -- Federal Court of Australia Judgments.
    Country: AU
    URL: https://www.fedcourt.gov.au/digital-law-library/judgments

    Data types: case_law
    Auth: none (Open Data via HuggingFace)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _extract_citation(self, text: str, version_id: str) -> str:
        """Extract citation from text or version_id."""
        # Try to extract from text first line
        match = re.search(r'\[\d{4}\]\s+(?:FCA|FCAFC)\s+\d+', text[:500] if text else "")
        if match:
            return match.group(0)
        # Fall back to version_id
        # version_id format: federal_court_of_australia:fca/single/2020/2020fca1492
        parts = version_id.split(":")
        if len(parts) > 1:
            path = parts[1]
            match = re.search(r'(\d{4})(?:fca|fcafc)(\d+)', path)
            if match:
                year, num = match.groups()
                court = "FCAFC" if "fcafc" in path else "FCA"
                return f"[{year}] {court} {int(num)}"
        return version_id

    def _extract_case_name(self, text: str, citation: str) -> str:
        """Extract case name from the beginning of text."""
        if not text:
            return citation
        # Look for case name pattern before the citation
        lines = text.strip().split('\n')
        # Skip header lines like "FEDERAL COURT OF AUSTRALIA"
        for line in lines[:10]:
            line = line.strip()
            if not line:
                continue
            if line.upper() == line and len(line) > 10:
                continue  # Skip all-caps headers
            if ' v ' in line or ' V ' in line:
                # Clean up citation from the name
                name = re.sub(r'\s*\[\d{4}\]\s+(?:FCA|FCAFC)\s+\d+\s*', '', line).strip()
                if name:
                    return name
        # Fall back to first meaningful line
        for line in lines[:5]:
            line = line.strip()
            if line and len(line) > 10 and not line.upper() == line:
                return line[:200]
        return citation

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string to ISO 8601 format."""
        if not date_str:
            return None
        try:
            # Format: "2020-10-16 00:00:00"
            dt = datetime.strptime(date_str.strip(), "%Y-%m-%d %H:%M:%S")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            try:
                dt = datetime.strptime(date_str.strip()[:10], "%Y-%m-%d")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a HuggingFace record to standard schema."""
        version_id = raw.get("version_id", "")
        text = raw.get("text", "")
        citation_str = raw.get("citation", "")
        date_str = raw.get("date", "")
        url = raw.get("url", "")

        # Extract structured citation
        citation = self._extract_citation(citation_str or text, version_id)

        # Extract case name
        if citation_str and (' v ' in citation_str or ' V ' in citation_str):
            title = re.sub(r'\s*\[\d{4}\]\s+(?:FCA|FCAFC)\s+\d+\s*', '', citation_str).strip()
            if not title:
                title = citation_str
        else:
            title = self._extract_case_name(text, citation)

        return {
            "_id": version_id,
            "_source": "AU/FedCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title or citation_str,
            "text": text,
            "date": self._parse_date(date_str),
            "url": url,
            "citation": citation_str or citation,
            "jurisdiction": "commonwealth",
            "court": "FCAFC" if "fcafc" in version_id.lower() else "FCA",
            "version_id": version_id,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Stream all Federal Court judgments from HuggingFace dataset."""
        try:
            from datasets import load_dataset
        except ImportError:
            logger.error("HuggingFace datasets library required: pip install datasets")
            return

        logger.info(f"Streaming dataset {DATASET_ID} (filtering for {SOURCE_FILTER})")

        ds = load_dataset(DATASET_ID, split="corpus", streaming=True)
        count = 0
        total_scanned = 0

        for row in ds:
            total_scanned += 1
            if total_scanned % 10000 == 0:
                logger.info(f"Scanned {total_scanned} records, found {count} FCA judgments...")

            if row.get("source") != SOURCE_FILTER:
                continue

            text = row.get("text", "")
            if not text or len(text.strip()) < 100:
                continue

            normalized = self.normalize(row)
            count += 1
            yield normalized

        logger.info(f"Completed: {count} Federal Court judgments from {total_scanned} total records")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent judgments from the RSS feed."""
        import xml.etree.ElementTree as ET
        try:
            from common.http_client import HttpClient
        except ImportError:
            logger.error("HttpClient not available")
            return

        logger.info(f"Fetching RSS feed: {RSS_URL}")
        client = HttpClient(
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=30,
        )

        try:
            resp = client.get(RSS_URL)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch RSS feed: {e}")
            return

        try:
            root = ET.fromstring(resp.text)
        except ET.ParseError as e:
            logger.error(f"Failed to parse RSS XML: {e}")
            return

        count = 0
        for item in root.iter("item"):
            title_el = item.find("title")
            link_el = item.find("link")
            desc_el = item.find("description")
            date_el = item.find("pubDate")

            title = title_el.text if title_el is not None else ""
            url = link_el.text if link_el is not None else ""
            description = desc_el.text if desc_el is not None else ""
            pub_date = date_el.text if date_el is not None else ""

            # Parse RSS date
            date_iso = None
            if pub_date:
                try:
                    from email.utils import parsedate_to_datetime
                    dt = parsedate_to_datetime(pub_date)
                    date_iso = dt.strftime("%Y-%m-%d")
                except Exception:
                    pass

            # RSS only provides description (summary), not full text
            # Mark these as needing full text fetch
            version_id = url.split("/judgments/Judgments/")[-1] if "/judgments/Judgments/" in url else url

            record = {
                "_id": f"federal_court_of_australia:{version_id}",
                "_source": "AU/FedCourt",
                "_type": "case_law",
                "_fetched_at": datetime.now(timezone.utc).isoformat(),
                "title": title,
                "text": description,  # RSS only has summary
                "date": date_iso,
                "url": url,
                "citation": title,
                "jurisdiction": "commonwealth",
                "court": "FCAFC" if "fcafc" in url.lower() else "FCA",
                "version_id": f"federal_court_of_australia:{version_id}",
                "_rss_only": True,  # Flag that this is summary only
            }
            count += 1
            yield record

        logger.info(f"RSS feed: {count} recent judgments")

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            from datasets import load_dataset
            ds = load_dataset(DATASET_ID, split="corpus", streaming=True)
            count = 0
            for row in ds:
                if row.get("source") == SOURCE_FILTER:
                    count += 1
                    if count >= 3:
                        logger.info(f"Test passed: found {count} FCA records")
                        return True
                if count == 0 and sum(1 for _ in []) > 1000:
                    break
            return count > 0
        except Exception as e:
            logger.error(f"Test failed: {e}")
            return False


# === CLI ===
def main():
    import argparse

    parser = argparse.ArgumentParser(description="AU/FedCourt data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    args = parser.parse_args()

    scraper = AustraliaFedCourtScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "update":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_updates():
            out_path = sample_dir / f"{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info(f"[{count}] {record.get('citation', 'unknown')}")
        logger.info(f"Saved {count} records from RSS feed")

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        max_records = 15 if args.sample else None
        count = 0

        for record in scraper.fetch_all():
            out_path = sample_dir / f"{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            text_len = len(record.get("text", ""))
            logger.info(
                f"[{count + 1}] {record.get('citation', 'unknown')} "
                f"({text_len:,} chars)"
            )

            count += 1
            if max_records and count >= max_records:
                break

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
