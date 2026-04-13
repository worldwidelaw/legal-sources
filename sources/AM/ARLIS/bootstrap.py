#!/usr/bin/env python3
"""
AM/ARLIS -- Armenian Legal Information System Data Fetcher

Fetches Armenian legislation from ARLIS (arlis.am).

Strategy:
  - Download metadata index from OpenData Armenia (data.opendata.am/dataset/arlis-db)
  - For each document, fetch full text from HTML print endpoint
  - Fall back to PDF extraction if HTML fails

Endpoints:
  - Metadata: https://data.opendata.am/.../arlis_docs_metadata.jsonl.xz
  - Full text HTML: https://www.arlis.am/hy/acts/{uniqid}/print/act
  - Full text PDF: https://pdf.arlis.am/{uniqid}

Data:
  - Document types: Laws (Օdelays), Decisions (Որdelays), Orders, Treaties
  - Full text in Armenian (primary), some in Russian/English
  - 154,000+ documents (1998-present)
  - License: Public government data

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent documents)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import lzma
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from html.parser import HTMLParser

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
logger = logging.getLogger("legal-data-hunter.AM.arlis")

# URLs
METADATA_URL = "https://data.opendata.am/dataset/672b9642-ab33-4098-8acc-c6c9e79c039f/resource/f9217c10-8197-4ec1-b57d-439f4e943b1f/download/arlis_docs_metadata.jsonl.xz"
FULL_TEXT_HTML_URL = "https://www.arlis.am/hy/acts/{uniqid}/print/act"
PDF_URL = "https://pdf.arlis.am/{uniqid}"

# Request headers
HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (EU Legal Research; contact@example.com)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "hy,en;q=0.9",
}


class ARLISTextExtractor(HTMLParser):
    """Extract text content from ARLIS HTML print page."""

    def __init__(self):
        super().__init__()
        self.text_parts = []
        self.in_body = False
        self.in_script = False
        self.in_style = False

    def handle_starttag(self, tag, attrs):
        if tag == "body":
            self.in_body = True
        elif tag == "script":
            self.in_script = True
        elif tag == "style":
            self.in_style = True

    def handle_endtag(self, tag):
        if tag == "script":
            self.in_script = False
        elif tag == "style":
            self.in_style = False

    def handle_data(self, data):
        if self.in_body and not self.in_script and not self.in_style:
            text = data.strip()
            if text:
                self.text_parts.append(text)

    def get_text(self) -> str:
        return "\n".join(self.text_parts)


class ARLISScraper(BaseScraper):
    """
    Scraper for AM/ARLIS -- Armenian Legal Information System.
    Country: AM
    URL: https://www.arlis.am

    Data types: legislation
    Auth: none (public government data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._metadata_cache = None
        self._metadata_path = None

    def _download_metadata(self) -> Path:
        """Download and cache the metadata JSONL file."""
        if self._metadata_path and self._metadata_path.exists():
            return self._metadata_path

        logger.info("Downloading ARLIS metadata index from OpenData Armenia...")

        # Download to temp file
        self._metadata_path = Path(tempfile.gettempdir()) / "arlis_metadata.jsonl.xz"

        resp = self.session.get(METADATA_URL, stream=True, timeout=120)
        resp.raise_for_status()

        with open(self._metadata_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info(f"Downloaded metadata to {self._metadata_path}")
        return self._metadata_path

    def _iter_metadata(self) -> Generator[Dict[str, Any], None, None]:
        """Iterate over all documents in the metadata file."""
        metadata_path = self._download_metadata()

        with lzma.open(metadata_path, "rt", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content."""
        if not text:
            return ""

        # Decode HTML entities
        text = html.unescape(text)

        # Remove excessive whitespace while preserving paragraph structure
        text = re.sub(r"\r\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]+", " ", text)

        # Strip leading/trailing whitespace from each line
        lines = [line.strip() for line in text.split("\n")]
        text = "\n".join(lines)

        return text.strip()

    def _fetch_full_text_html(self, uniqid: str) -> Optional[str]:
        """Fetch full text from the HTML print endpoint."""
        url = FULL_TEXT_HTML_URL.format(uniqid=uniqid)

        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()

            # Parse HTML and extract text
            parser = ARLISTextExtractor()
            parser.feed(resp.text)
            text = parser.get_text()

            if len(text) > 100:  # Sanity check - should have meaningful content
                return self._clean_text(text)

            return None

        except Exception as e:
            logger.debug(f"HTML fetch failed for {uniqid}: {e}")
            return None

    def _fetch_full_text_pdf(self, uniqid: str) -> Optional[str]:
        """Fetch and extract text from PDF as fallback."""
        url = PDF_URL.format(uniqid=uniqid)

        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()

            # Extract text from PDF
            doc = fitz.open(stream=resp.content, filetype="pdf")
            text_parts = []
            for page in doc:
                text_parts.append(page.get_text())
            doc.close()

            text = "\n".join(text_parts)
            if len(text) > 100:
                return self._clean_text(text)

            return None

        except Exception as e:
            logger.debug(f"PDF fetch failed for {uniqid}: {e}")
            return None

    def _fetch_full_text(self, uniqid: str) -> Optional[str]:
        """Fetch full text, trying HTML first, then PDF."""
        text = self._fetch_full_text_html(uniqid)
        if text:
            return text

        # Fallback to PDF
        logger.debug(f"Falling back to PDF for {uniqid}")
        return self._fetch_full_text_pdf(uniqid)

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from ARLIS.

        Iterates through the metadata index and fetches full text for each.
        """
        total_count = 0

        for metadata in self._iter_metadata():
            total_count += 1

            uniqid = metadata.get("uniqid", "")
            if not uniqid:
                continue

            # Fetch full text
            full_text = self._fetch_full_text(uniqid)

            if not full_text:
                logger.debug(f"Could not fetch full text for {uniqid}, skipping")
                continue

            # Combine metadata with full text
            metadata["full_text"] = full_text

            if total_count % 100 == 0:
                logger.info(f"Processed {total_count} documents...")

            yield metadata

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents updated since the given date.

        Note: The metadata file is a snapshot, so we filter by enactment date.
        For true incremental updates, would need to re-download metadata and compare.
        """
        for metadata in self._iter_metadata():
            # Check enactment date
            enact_date_str = metadata.get("EnactmentDate", "")
            if enact_date_str:
                try:
                    # Parse DD.MM.YYYY format
                    parts = enact_date_str.split(".")
                    if len(parts) == 3:
                        day, month, year = parts
                        doc_date = datetime(int(year), int(month), int(day), tzinfo=timezone.utc)
                        if doc_date < since:
                            continue
                except (ValueError, IndexError):
                    pass

            uniqid = metadata.get("uniqid", "")
            if not uniqid:
                continue

            full_text = self._fetch_full_text(uniqid)
            if not full_text:
                continue

            metadata["full_text"] = full_text
            yield metadata

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        uniqid = raw.get("uniqid", "")
        title = raw.get("title", "")
        full_text = raw.get("full_text", "")

        # Parse Armenian date format (DD.MM.YYYY) to ISO 8601
        def parse_date(date_str: str) -> str:
            if not date_str:
                return ""
            try:
                parts = date_str.split(".")
                if len(parts) == 3:
                    day, month, year = parts
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            except (ValueError, IndexError):
                pass
            return ""

        enactment_date = parse_date(raw.get("EnactmentDate", ""))
        effective_date = parse_date(raw.get("EffectiveDate", ""))

        # Use effective date if available, otherwise enactment date
        date = effective_date or enactment_date

        # Build URL
        url = f"https://www.arlis.am/hy/acts/{uniqid}/latest" if uniqid else ""

        return {
            # Required base fields
            "_id": uniqid,
            "_source": "AM/ARLIS",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # Additional metadata
            "uniqid": uniqid,
            "act_number": raw.get("ActNumber", ""),
            "act_type": raw.get("ActType", ""),
            "doc_type": raw.get("DocType", ""),
            "act_status": raw.get("ActStatus", ""),
            "source": raw.get("Source", ""),
            "enactment_location": raw.get("EnactmentLocation", ""),
            "enactment_organ": raw.get("EnactmentOrgan", ""),
            "enactment_date": enactment_date,
            "signing_organ": raw.get("SigningOrgan", ""),
            "signing_date": parse_date(raw.get("SigningDate", "")),
            "ratification_organ": raw.get("RatificationOrgan", ""),
            "ratification_date": parse_date(raw.get("RatificationDate", "")),
            "effective_date": effective_date,
            "interrupt_date": parse_date(raw.get("InterruptDate", "") or ""),
            "pdf_link": raw.get("pdf_link", ""),
            "language": raw.get("language", "AM"),
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing AM/ARLIS endpoints...")

        # Test metadata download
        print("\n1. Testing metadata endpoint...")
        try:
            resp = self.session.head(METADATA_URL, timeout=30)
            print(f"   Status: {resp.status_code}")
            content_length = resp.headers.get("Content-Length", "unknown")
            print(f"   Content-Length: {content_length}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test HTML print endpoint
        print("\n2. Testing HTML full text endpoint...")
        test_id = "6611"  # Known good document
        try:
            text = self._fetch_full_text_html(test_id)
            if text:
                print(f"   Success! Got {len(text)} characters")
                print(f"   Preview: {text[:200]}...")
            else:
                print("   Failed to extract text from HTML")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test PDF endpoint
        print("\n3. Testing PDF endpoint...")
        try:
            url = PDF_URL.format(uniqid=test_id)
            resp = self.session.head(url, timeout=30)
            print(f"   Status: {resp.status_code}")
            content_type = resp.headers.get("Content-Type", "unknown")
            print(f"   Content-Type: {content_type}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test metadata parsing
        print("\n4. Testing metadata parsing...")
        try:
            count = 0
            for doc in self._iter_metadata():
                count += 1
                if count == 1:
                    print(f"   Sample document ID: {doc.get('uniqid')}")
                    print(f"   Title: {doc.get('title', 'N/A')[:60]}...")
                    print(f"   Type: {doc.get('ActType', 'N/A')}")
                    print(f"   Status: {doc.get('ActStatus', 'N/A')}")
                if count >= 5:
                    break
            print(f"   Successfully parsed {count} metadata records")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = ARLISScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

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
    main()
