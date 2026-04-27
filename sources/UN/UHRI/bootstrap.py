#!/usr/bin/env python3
"""
UN/UHRI -- Universal Human Rights Index

Fetches observations and recommendations from UN human rights mechanisms
(Treaty Bodies, Special Procedures, UPR) with full text content.

Strategy:
  - Download bulk JSON export from dataex.ohchr.org (~360 MB)
  - Stream-parse the JSON array to avoid loading all into memory
  - Normalize each record: strip HTML from text, standardize fields
  - 260,000+ records with full text

Data: Open data from OHCHR. No authentication required.
Rate limit: Single bulk download, no API rate concerns.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import html as html_module
import time
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
logger = logging.getLogger("legal-data-hunter.UN.UHRI")

EXPORT_URL = "https://dataex.ohchr.org/uhri/export-results/export-full-en.json"


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities, preserving paragraph breaks."""
    if not text:
        return ""
    # Replace block-level tags with newlines
    text = re.sub(r'<(?:br|/p|/div|/h[1-6]|/li|/tr)[^>]*>', '\n', text, flags=re.IGNORECASE)
    # Remove all remaining HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Decode HTML entities
    text = html_module.unescape(text)
    # Normalize whitespace: collapse multiple spaces, keep paragraph breaks
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class UHRIScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
                "Accept": "application/json",
            },
            timeout=600,  # Large file download
        )

    def test_api(self):
        """Test connectivity to the OHCHR export endpoint."""
        logger.info("Testing UHRI export endpoint...")
        try:
            # Use stream=True and read only first chunk to verify connectivity
            resp = self.http.get(EXPORT_URL, stream=True)
            content_length = resp.headers.get("Content-Length", "unknown")
            content_type = resp.headers.get("Content-Type", "unknown")
            logger.info(f"  Status: {resp.status_code}")
            logger.info(f"  Content-Type: {content_type}")
            logger.info(f"  Content-Length: {content_length} bytes")
            # Read first chunk to verify data flows
            first_chunk = next(resp.iter_content(chunk_size=1024), b"")
            resp.close()
            logger.info(f"  First chunk: {len(first_chunk)} bytes, starts with: {first_chunk[:30]}")
            if resp.status_code == 200 and first_chunk:
                logger.info("API test PASSED")
                return True
            else:
                logger.error(f"API test FAILED: HTTP {resp.status_code}")
                return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw UHRI JSON record into standard schema."""
        annotation_id = raw.get("AnnotationId", "")
        if not annotation_id:
            return None

        raw_text = raw.get("Text", "")
        text = strip_html(raw_text)
        if not text:
            return None

        # Parse date
        pub_date = raw.get("PublicationDate", "")
        date_str = None
        if pub_date:
            try:
                dt = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
                date_str = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                date_str = None

        countries = raw.get("Countries", []) or []
        title_parts = []
        symbol = raw.get("Symbol", "")
        annotation_type = (raw.get("AnnotationType", "") or "").strip("- ")
        body = (raw.get("Body", "") or "").strip("- ")

        if symbol:
            title_parts.append(symbol)
        if annotation_type:
            title_parts.append(annotation_type)
        if countries:
            title_parts.append(", ".join(countries[:3]))
        title = " — ".join(title_parts) if title_parts else f"UHRI {annotation_id[:8]}"

        return {
            "_id": annotation_id,
            "_source": "UN/UHRI",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": f"https://uhri.ohchr.org/en/search-human-rights-recommendations",
            "symbol": symbol,
            "body": body,
            "annotation_type": annotation_type,
            "countries": countries,
            "themes": raw.get("Themes", []) or [],
            "sdgs": raw.get("Sdgs", []) or [],
            "affected_persons": raw.get("AffectedPersons", []) or [],
            "regions": raw.get("Regions", []) or [],
            "upr_cycle": raw.get("UprCycle"),
            "upr_recommending_country": raw.get("UprRecommendingCountry", []) or [],
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Download bulk JSON and yield normalized records."""
        sample_limit = 15 if sample else None

        logger.info(f"Downloading UHRI bulk export from {EXPORT_URL}...")
        if sample:
            logger.info(f"Sample mode: will yield first {sample_limit} valid records")

        try:
            # Stream the response to handle large file
            resp = self.http.get(EXPORT_URL, stream=True)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to download export: {e}")
            return

        # For sample mode, download only first chunk and parse
        # For full mode, download and parse entire file
        if sample:
            # Download enough data for sample records (~2MB should have 15+ records)
            chunks = []
            downloaded = 0
            for chunk in resp.iter_content(chunk_size=65536):
                chunks.append(chunk)
                downloaded += len(chunk)
                if downloaded > 2_000_000:
                    break
            raw_data = b"".join(chunks).decode("utf-8", errors="replace")
            resp.close()

            # Use JSON decoder to extract complete objects from partial download
            count = 0
            decoder = json.JSONDecoder()
            # Skip opening bracket
            pos = raw_data.find("[")
            if pos < 0:
                logger.error("JSON array not found in response")
                return
            pos += 1

            while count < sample_limit:
                # Skip whitespace and commas
                while pos < len(raw_data) and raw_data[pos] in " \t\n\r,":
                    pos += 1
                if pos >= len(raw_data) or raw_data[pos] == "]":
                    break
                try:
                    record, end_pos = decoder.raw_decode(raw_data, pos)
                    pos = end_pos
                except json.JSONDecodeError:
                    break

                normalized = self.normalize(record)
                if normalized:
                    yield normalized
                    count += 1
                    logger.info(f"  [{count}/{sample_limit}] {normalized['symbol']} — {normalized['countries']}")

            logger.info(f"Sample complete: yielded {count} records")
        else:
            # Full mode: download entire file and parse
            logger.info("Downloading full export (this may take a while)...")
            raw_data = resp.content.decode("utf-8", errors="replace")
            logger.info(f"Downloaded {len(raw_data):,} bytes, parsing JSON...")

            try:
                records = json.loads(raw_data)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON: {e}")
                return

            total = len(records)
            logger.info(f"Parsed {total:,} records, normalizing...")
            count = 0
            for i, record in enumerate(records):
                normalized = self.normalize(record)
                if normalized:
                    yield normalized
                    count += 1
                if (i + 1) % 10000 == 0:
                    logger.info(f"  Progress: {i+1:,}/{total:,} processed, {count:,} yielded")

            logger.info(f"Full export complete: {count:,}/{total:,} records yielded")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch records updated since a given date."""
        since_dt = datetime.fromisoformat(since)
        for record in self.fetch_all():
            if record.get("date"):
                try:
                    rec_dt = datetime.fromisoformat(record["date"])
                    if rec_dt >= since_dt:
                        yield record
                except (ValueError, TypeError):
                    yield record
            else:
                yield record


def main():
    import argparse

    parser = argparse.ArgumentParser(description="UN/UHRI data fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = UHRIScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            out_path = sample_dir / f"{record['_id'][:50]}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1

        logger.info(f"Saved {count} records to {sample_dir}")
        if count == 0:
            logger.error("No records fetched!")
            sys.exit(1)


if __name__ == "__main__":
    main()
