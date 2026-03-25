#!/usr/bin/env python3
"""
IN/SCJudgments -- Indian Supreme Court Judgments (AWS Open Data)

Fetches Indian Supreme Court judgments from the AWS Open Data Registry bucket
(indian-supreme-court-judgments) in ap-south-1.

Strategy:
  - Downloads Parquet metadata files per year (small, ~1MB each)
  - Downloads tar archives containing PDF judgments per year
  - Extracts full text from PDFs using pdfplumber
  - Normalizes records to standard schema with full text

Data:
  - 35K+ judgments from the Supreme Court of India (1950-present)
  - Full text extracted from PDF judgments
  - Parquet metadata includes title, parties, judges, citation, dates
  - CC-BY-4.0 license, updated bi-monthly

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
import tarfile
import logging
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import quote

import requests
import pdfplumber
import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IN.SCJudgments")

# Use path-style S3 URL (virtual-hosted style times out from some networks)
S3_BASE = "https://s3.ap-south-1.amazonaws.com/indian-supreme-court-judgments"


class IndianSCJudgmentsScraper(BaseScraper):
    """
    Scraper for IN/SCJudgments -- Indian Supreme Court Judgments.
    Country: IN
    URL: https://registry.opendata.aws/indian-supreme-court-judgments/

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

    def _list_s3_prefixes(self, prefix: str, delimiter: str = "/") -> list:
        """List common prefixes (subdirectories) under an S3 prefix."""
        results = []
        continuation_token = None
        while True:
            url = f"{S3_BASE}?list-type=2&prefix={quote(prefix, safe='/')}&delimiter={delimiter}&max-keys=1000"
            if continuation_token:
                url += f"&continuation-token={quote(continuation_token, safe='')}"
            r = self._s3_get(url)
            root = ET.fromstring(r.text)
            ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
            for cp in root.findall("s3:CommonPrefixes/s3:Prefix", ns):
                results.append(cp.text)
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
        prefixes = self._list_s3_prefixes("metadata/parquet/")
        years = []
        for p in prefixes:
            m = re.search(r'year=(\d{4})/', p)
            if m:
                years.append(int(m.group(1)))
        return sorted(years)

    def _download_parquet(self, year: int) -> pd.DataFrame:
        """Download parquet metadata for a given year."""
        url = f"{S3_BASE}/metadata/parquet/year={year}/metadata.parquet"
        r = self._s3_get(url, timeout=180)
        return pd.read_parquet(io.BytesIO(r.content))

    def _download_tar_and_extract(self, year: int, language: str = "english") -> dict:
        """Download a year's tar archive and extract PDFs into memory.
        Returns dict mapping filename -> pdf_bytes.
        """
        url = f"{S3_BASE}/data/tar/year={year}/{language}/{language}.tar"
        logger.info(f"Downloading tar for year={year} language={language}...")
        r = self._s3_get(url, timeout=600)
        logger.info(f"Downloaded {len(r.content) / 1024 / 1024:.1f} MB tar")

        pdfs = {}
        with tarfile.open(fileobj=io.BytesIO(r.content), mode="r:") as tf:
            for member in tf.getmembers():
                if member.isfile() and member.name.endswith(".pdf"):
                    f = tf.extractfile(member)
                    if f:
                        filename = member.name.split("/")[-1]
                        pdfs[filename] = f.read()
        logger.info(f"Extracted {len(pdfs)} PDFs from tar")
        return pdfs

    def _extract_pdf_text(self, pdf_content: bytes) -> str:
        """Extract text from PDF using pdfplumber."""
        try:
            with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                pages_text = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
                return "\n\n".join(pages_text)
        except Exception as e:
            logger.warning(f"PDF text extraction failed: {e}")
            return ""

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Convert date string to ISO 8601 format."""
        if not date_str:
            return None
        # Try DD-MM-YYYY format
        if re.match(r'\d{2}-\d{2}-\d{4}$', date_str):
            parts = date_str.split("-")
            return f"{parts[2]}-{parts[1]}-{parts[0]}"
        # Already ISO
        if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
            return date_str[:10]
        return date_str

    def _process_year(self, year: int, sample_limit: int = 0) -> Generator[dict, None, None]:
        """Process all judgments for a given year."""
        # Download metadata
        logger.info(f"Processing year {year}...")
        try:
            df = self._download_parquet(year)
        except Exception as e:
            logger.error(f"Failed to download parquet for year {year}: {e}")
            return

        logger.info(f"Year {year}: {len(df)} records in metadata")

        # Check tar size before downloading
        try:
            index_url = f"{S3_BASE}/data/tar/year={year}/english/english.index.json"
            r = self._s3_get(index_url, timeout=60)
            index_data = r.json()
            total_size = index_data.get("total_size", 0)
            if total_size > 2 * 1024 * 1024 * 1024:  # 2GB limit
                logger.warning(f"Year {year} tar is {total_size / 1024 / 1024:.0f} MB, skipping (>2GB)")
                return
        except Exception as e:
            logger.warning(f"Could not check tar size for year {year}: {e}")

        # Download tar with PDFs
        try:
            pdfs = self._download_tar_and_extract(year)
        except Exception as e:
            logger.error(f"Failed to download tar for year {year}: {e}")
            return

        count = 0
        for _, row in df.iterrows():
            if sample_limit and count >= sample_limit:
                break

            path = row.get("path", "")
            if not path:
                continue

            # Map path to PDF filename: path + _EN.pdf
            pdf_filename = f"{path}_EN.pdf"
            pdf_bytes = pdfs.get(pdf_filename)
            if not pdf_bytes:
                # Try without language suffix
                pdf_filename = f"{path}.pdf"
                pdf_bytes = pdfs.get(pdf_filename)

            text = ""
            if pdf_bytes:
                text = self._extract_pdf_text(pdf_bytes)

            if not text or len(text.strip()) < 50:
                continue

            yield {
                "metadata": row.to_dict(),
                "text": text,
                "pdf_filename": pdf_filename,
                "year": year,
            }
            count += 1
            time.sleep(0.1)

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all judgments, iterating over years (newest first)."""
        years = self._list_years()
        logger.info(f"Found {len(years)} years of data: {years[0]}-{years[-1]}")

        for year in sorted(years, reverse=True):
            for record in self._process_year(year):
                yield record

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch judgments from recent years."""
        current_year = datetime.now().year
        for year in [current_year, current_year - 1]:
            for record in self._process_year(year):
                yield record

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw judgment data into standard schema."""
        metadata = raw.get("metadata", {})
        text = raw.get("text", "")

        if not text or len(text.strip()) < 50:
            return None

        # Extract fields from parquet metadata
        title = metadata.get("title", "")
        petitioner = metadata.get("petitioner", "")
        respondent = metadata.get("respondent", "")
        judge = metadata.get("judge", "")
        author_judge = metadata.get("author_judge", "")
        citation = metadata.get("citation", "")
        case_id = metadata.get("case_id", "")
        cnr = metadata.get("cnr", "")
        decision_date = self._parse_date(str(metadata.get("decision_date", "")))
        disposal_nature = metadata.get("disposal_nature", "")
        path = metadata.get("path", "")

        # Build unique ID from CNR or case_id
        doc_id = cnr or case_id or path

        # Build PDF URL
        year = raw.get("year", "")
        pdf_filename = raw.get("pdf_filename", "")
        pdf_url = f"{S3_BASE}/data/tar/year={year}/english/english.tar" if year else ""

        # Build title if empty
        if not title and petitioner and respondent:
            title = f"{petitioner} versus {respondent}"

        return {
            "_id": doc_id,
            "_source": "IN/SCJudgments",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": decision_date,
            "url": pdf_url,
            "cnr_number": cnr,
            "case_id": case_id,
            "citation": citation,
            "court_name": "Supreme Court of India",
            "petitioner": petitioner,
            "respondent": respondent,
            "judges": judge,
            "author_judge": str(author_judge) if author_judge and str(author_judge) != "nan" else "",
            "disposal_nature": disposal_nature,
        }


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="IN/SCJudgments data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = IndianSCJudgmentsScraper()

    if args.command == "test":
        logger.info("Testing S3 connectivity...")
        try:
            years = scraper._list_years()
            logger.info(f"OK: Found {len(years)} years ({years[0]}-{years[-1]})")
            df = scraper._download_parquet(years[-1])
            logger.info(f"Latest year {years[-1]}: {len(df)} records in parquet")
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
