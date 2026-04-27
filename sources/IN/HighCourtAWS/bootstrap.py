#!/usr/bin/env python3
"""
IN/HighCourtAWS -- Indian High Court Judgments (AWS Open Data)

Fetches Indian High Court judgments from the AWS Open Data Registry bucket
(indian-high-court-judgments) in ap-south-1.

Strategy:
  - Lists JSON metadata files from S3 using the REST ListObjectsV2 API
  - For each JSON, downloads the corresponding PDF from the data/pdf/ prefix
  - Extracts full text from PDFs using pdfplumber
  - Normalizes records to standard schema with full text

Data:
  - 16.7M judgments from 25 High Courts across India (1950-present)
  - Full text extracted from PDF judgments
  - JSON metadata includes court, case parties, judges, dates
  - CC-BY-4.0 license, updated quarterly

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent year's judgments
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import logging
import xml.etree.ElementTree as ET
from html import unescape
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from urllib.parse import quote

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IN.HighCourtAWS")

S3_BASE = "https://s3.ap-south-1.amazonaws.com/indian-high-court-judgments"
METADATA_PREFIX = "metadata/json/"
PDF_PREFIX = "data/pdf/"

# Court code mapping (court_code in JSON uses ~ separator, S3 paths use _)
# This is derived from the high_courts.csv in the source repo


class IndianHighCourtAWSScraper(BaseScraper):
    """
    Scraper for IN/HighCourtAWS -- Indian High Court Judgments.
    Country: IN
    URL: https://registry.opendata.aws/indian-high-court-judgments/

    Data types: case_law
    Auth: none (public S3 bucket)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "LegalDataHunter/1.0"})

    def _s3_get(self, url: str, timeout: int = 120, retries: int = 3, stream: bool = False) -> requests.Response:
        """GET from S3 with retry logic for intermittent connectivity."""
        for attempt in range(retries):
            try:
                r = self.session.get(url, timeout=timeout, stream=stream)
                r.raise_for_status()
                return r
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                if attempt < retries - 1:
                    wait = 5 * (attempt + 1)
                    logger.warning(f"S3 request failed (attempt {attempt + 1}/{retries}), retrying in {wait}s: {e}")
                    time.sleep(wait)
                else:
                    raise

    def _list_s3_objects(self, prefix: str, max_keys: int = 1000, delimiter: str = "") -> list:
        """List objects in S3 bucket under a prefix."""
        results = []
        continuation_token = None

        while True:
            url = f"{S3_BASE}/?list-type=2&prefix={quote(prefix, safe='/')}&max-keys={max_keys}"
            if delimiter:
                url += f"&delimiter={delimiter}"
            if continuation_token:
                url += f"&continuation-token={quote(continuation_token, safe='')}"

            r = self._s3_get(url)
            root = ET.fromstring(r.text)
            ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

            if delimiter:
                # Return common prefixes (directories)
                for cp in root.findall("s3:CommonPrefixes/s3:Prefix", ns):
                    results.append(cp.text)
            else:
                # Return object keys
                for content in root.findall("s3:Contents", ns):
                    key = content.find("s3:Key", ns).text
                    size = int(content.find("s3:Size", ns).text)
                    results.append({"key": key, "size": size})

            is_truncated = root.find("s3:IsTruncated", ns)
            if is_truncated is not None and is_truncated.text == "true":
                token_el = root.find("s3:NextContinuationToken", ns)
                if token_el is not None:
                    continuation_token = token_el.text
                else:
                    break
            else:
                break

        return results

    def _list_years(self) -> list:
        """List available years in the metadata."""
        prefixes = self._list_s3_objects(METADATA_PREFIX, delimiter="/")
        years = []
        for p in prefixes:
            m = re.search(r'year=(\d{4})/', p)
            if m:
                years.append(int(m.group(1)))
        return sorted(years)

    def _list_courts(self, year: int) -> list:
        """List courts for a given year."""
        prefix = f"{METADATA_PREFIX}year={year}/"
        return self._list_s3_objects(prefix, delimiter="/")

    def _list_benches(self, year: int, court_path: str) -> list:
        """List benches under a court path."""
        return self._list_s3_objects(court_path, delimiter="/")

    def _extract_pdf_text(self, pdf_content: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="IN/HighCourtAWS",
            source_id="",
            pdf_bytes=pdf_content,
            table="case_law",
        ) or ""

    def _parse_raw_html(self, raw_html: str) -> dict:
        """Parse the raw_html field from JSON metadata to extract case details."""
        info = {
            "case_number": "",
            "parties": "",
            "judges": "",
            "cnr_number": "",
            "registration_date": "",
            "decision_date": "",
            "disposal_nature": "",
        }

        if not raw_html:
            return info

        # Clean HTML entities
        html = unescape(raw_html)

        # Extract case parties from the button text
        # Pattern: "CASE/NUM/YEAR of PETITIONER Vs RESPONDENT" or similar
        btn_match = re.search(r"aria-label=\"([^\"]+)\"", html)
        if btn_match:
            label = btn_match.group(1)
            # Remove " pdf" suffix
            label = re.sub(r'\s+pdf$', '', label)
            # Parse case number and parties
            # Formats: "CASE/NUM/YEAR of PARTY1 .Array[N]. PARTY2"
            #          "CASE/NUM/YEAR of PARTY1 Vs PARTY2"
            label = re.sub(r'\.Array\[\d+\]\.?\s*', ' Vs ', label)
            parts = re.split(r'\s+of\s+', label, maxsplit=1)
            if len(parts) == 2:
                info["case_number"] = parts[0].strip()
                info["parties"] = parts[1].strip()
            else:
                info["parties"] = label
        else:
            # Fallback: try to extract from button text content
            btn_text = re.search(r"'>([^<]+)</button>", html)
            if btn_text:
                text = btn_text.group(1).strip()
                text = re.sub(r'\.Array\[\d+\]\.?\s*', ' Vs ', text)
                parts = re.split(r'\s+of\s+', text, maxsplit=1)
                if len(parts) == 2:
                    info["case_number"] = parts[0].strip()
                    info["parties"] = parts[1].strip()
                else:
                    info["parties"] = text

        # Extract judges
        judge_match = re.search(r"Judge\s*:\s*([^<]+)", html)
        if judge_match:
            info["judges"] = judge_match.group(1).strip()

        # Extract CNR number
        cnr_match = re.search(r"CNR\s*:\s*</span><font[^>]*>\s*(\w+)", html)
        if cnr_match:
            info["cnr_number"] = cnr_match.group(1).strip()

        # Extract registration date
        reg_match = re.search(r"Date of registration\s*:\s*</span><font[^>]*>\s*([\d-]+)", html)
        if reg_match:
            info["registration_date"] = reg_match.group(1).strip()

        # Extract decision date
        dec_match = re.search(r"Decision Date\s*:\s*</span><font[^>]*>\s*([\d-]+)", html)
        if dec_match:
            info["decision_date"] = dec_match.group(1).strip()

        # Extract disposal nature
        disp_match = re.search(r"Disposal Nature\s*:\s*</span><font[^>]*>\s*([^<]+)", html)
        if disp_match:
            info["disposal_nature"] = disp_match.group(1).strip()

        return info

    def _json_key_to_pdf_key(self, json_key: str) -> str:
        """Convert a metadata JSON S3 key to the corresponding PDF S3 key."""
        # metadata/json/year=Y/court=X/bench=Z/FILE.json -> data/pdf/year=Y/court=X/bench=Z/FILE.pdf
        return json_key.replace("metadata/json/", "data/pdf/").replace(".json", ".pdf")

    def _fetch_judgment(self, json_key: str) -> Optional[dict]:
        """Fetch a single judgment: download JSON metadata + PDF, extract text."""
        try:
            # Download JSON metadata
            json_url = f"{S3_BASE}/{json_key}"
            r = self._s3_get(json_url)
            metadata = r.json()

            # Download PDF
            pdf_key = self._json_key_to_pdf_key(json_key)
            pdf_url = f"{S3_BASE}/{pdf_key}"
            try:
                pdf_r = self._s3_get(pdf_url, timeout=180)
                # Verify it's actually a PDF, not an HTML error page
                if pdf_r.content[:5] == b'%PDF-' or pdf_r.content[:5] == b'\x25PDF':
                    text = self._extract_pdf_text(pdf_r.content)
                else:
                    logger.warning(f"Not a PDF: {pdf_key}")
                    text = ""
            except Exception as e:
                logger.warning(f"PDF download failed for {pdf_key}: {e}")
                text = ""

            return {
                "json_key": json_key,
                "pdf_key": pdf_key,
                "metadata": metadata,
                "text": text,
            }
        except Exception as e:
            logger.error(f"Failed to fetch judgment {json_key}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all judgments from S3. Iterates over years/courts/benches.
        For full bootstrap, this would take a very long time (16.7M records).
        In practice, use sample mode or iterate by year.
        """
        years = self._list_years()
        logger.info(f"Found {len(years)} years of data: {years[0]}-{years[-1]}")

        for year in sorted(years, reverse=True):
            court_prefixes = self._list_courts(year)
            logger.info(f"Year {year}: {len(court_prefixes)} courts")

            for court_prefix in court_prefixes:
                bench_prefixes = self._list_benches(year, court_prefix)

                for bench_prefix in bench_prefixes:
                    # List JSON files in this bench
                    objects = self._list_s3_objects(bench_prefix, max_keys=100)

                    for obj in objects:
                        key = obj["key"] if isinstance(obj, dict) else obj
                        if not key.endswith(".json"):
                            continue

                        result = self._fetch_judgment(key)
                        if result:
                            yield result
                            time.sleep(0.5)  # Rate limit S3 requests

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch judgments from recent years (the dataset is updated quarterly)."""
        current_year = datetime.now().year
        for year in [current_year, current_year - 1]:
            court_prefixes = self._list_courts(year)
            for court_prefix in court_prefixes:
                bench_prefixes = self._list_benches(year, court_prefix)
                for bench_prefix in bench_prefixes:
                    objects = self._list_s3_objects(bench_prefix, max_keys=100)
                    for obj in objects:
                        key = obj["key"] if isinstance(obj, dict) else obj
                        if not key.endswith(".json"):
                            continue
                        result = self._fetch_judgment(key)
                        if result:
                            yield result
                            time.sleep(0.5)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw judgment data into standard schema."""
        metadata = raw.get("metadata", {})
        text = raw.get("text", "")
        json_key = raw.get("json_key", "")

        # Skip if no text extracted
        if not text or len(text.strip()) < 50:
            return None

        # Parse metadata from raw_html
        parsed = self._parse_raw_html(metadata.get("raw_html", ""))

        # Extract CNR from filename if not in HTML
        cnr = parsed["cnr_number"]
        if not cnr:
            # Filename: JKHC020000011979_1_2024-04-29.json
            filename = json_key.split("/")[-1].replace(".json", "")
            parts = filename.split("_")
            if parts:
                cnr = parts[0]

        # Extract date from filename or HTML
        decision_date = parsed["decision_date"]
        if not decision_date:
            # Try filename: ..._2024-04-29.json
            date_match = re.search(r'(\d{4}-\d{2}-\d{2})\.json$', json_key)
            if date_match:
                decision_date = date_match.group(1)

        # Convert date format DD-MM-YYYY to ISO
        if decision_date and re.match(r'\d{2}-\d{2}-\d{4}$', decision_date):
            parts = decision_date.split("-")
            decision_date = f"{parts[2]}-{parts[1]}-{parts[0]}"

        court_name = metadata.get("court_name", "")
        court_code = metadata.get("court_code", "")

        # Build title from parties or case number
        title = parsed["parties"] or parsed["case_number"] or cnr
        if parsed["case_number"] and parsed["parties"]:
            title = f"{parsed['case_number']} - {parsed['parties']}"

        # Build PDF URL
        pdf_key = raw.get("pdf_key", "")
        pdf_url = f"{S3_BASE}/{pdf_key}" if pdf_key else ""

        return {
            "_id": cnr,
            "_source": "IN/HighCourtAWS",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": decision_date or None,
            "url": pdf_url,
            "cnr_number": cnr,
            "court_name": court_name,
            "court_code": court_code,
            "case_number": parsed["case_number"],
            "parties": parsed["parties"],
            "judges": parsed["judges"],
            "registration_date": parsed["registration_date"] or None,
            "disposal_nature": parsed["disposal_nature"],
        }


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="IN/HighCourtAWS data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # bootstrap
    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    # update
    subparsers.add_parser("update", help="Incremental update")

    # test
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = IndianHighCourtAWSScraper()

    if args.command == "test":
        logger.info("Testing S3 connectivity...")
        try:
            years = scraper._list_years()
            logger.info(f"OK: Found {len(years)} years ({years[0]}-{years[-1]})")
            # Test one JSON download
            courts = scraper._list_courts(years[-1])
            logger.info(f"Latest year {years[-1]}: {len(courts)} courts")
            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
