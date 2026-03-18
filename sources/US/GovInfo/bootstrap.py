#!/usr/bin/env python3
"""
US/GovInfo -- GovInfo Bulk Data Source

Fetches US federal legislation from GovInfo bulk data repository.
Collections include: Congressional Bills, Code of Federal Regulations (CFR),
Public Laws (PLAW), and Federal Register (FR).

NO API KEY REQUIRED - uses public bulk data endpoints.

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py bootstrap --recent   # Fetch last 30 days
    python bootstrap.py updates --since YYYY-MM-DD  # Incremental updates
"""

import argparse
import html
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import requests

# Configuration
SOURCE_ID = "US/GovInfo"
BULK_DATA_BASE = "https://www.govinfo.gov/bulkdata"
USER_AGENT = "WorldWideLaw/1.0 (Open Data Research; contact@legaldatahunter.com)"
REQUEST_DELAY = 0.3  # seconds between requests

# Collections available via bulk data
COLLECTIONS = {
    "BILLS": {"name": "Congressional Bills", "type": "legislation"},
    "CFR": {"name": "Code of Federal Regulations", "type": "legislation"},
    "PLAW": {"name": "Public and Private Laws", "type": "legislation"},
    "FR": {"name": "Federal Register", "type": "legislation"},
}

# Paths
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
SAMPLE_DIR = SCRIPT_DIR / "sample"


class GovInfoBulkData:
    """Client for GovInfo bulk data repository (no API key required)."""

    def __init__(self):
        self.base_url = BULK_DATA_BASE
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a requests session."""
        session = requests.Session()
        session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })
        return session

    def _request_json(self, path: str, retries: int = 3) -> Dict:
        """Make a request to get JSON listing."""
        url = f"{self.base_url}/json/{path}"
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=60)
                if response.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    print(f"  Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                    continue
                if response.status_code == 404:
                    return {}
                response.raise_for_status()
                return response.json()
            except requests.exceptions.Timeout:
                if attempt < retries - 1:
                    print(f"  Timeout, retrying...")
                    time.sleep(2)
                    continue
                raise
            except json.JSONDecodeError:
                return {}
        return {}

    def _fetch_xml(self, url: str, retries: int = 3) -> str:
        """Fetch XML content from a URL."""
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=120)
                if response.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    print(f"  Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                    continue
                if response.status_code == 404:
                    return ""
                response.raise_for_status()
                return response.text
            except requests.exceptions.Timeout:
                if attempt < retries - 1:
                    print(f"  Timeout, retrying...")
                    time.sleep(2)
                    continue
                raise
            except requests.exceptions.RequestException:
                return ""
        return ""

    def list_collection(self, collection: str) -> List[Dict]:
        """List items in a collection."""
        result = self._request_json(collection)
        return result.get("files", [])

    def list_bills_congress(self, congress: int, session: int, bill_type: str) -> List[Dict]:
        """List bills for a specific congress/session/type."""
        path = f"BILLS/{congress}/{session}/{bill_type}"
        result = self._request_json(path)
        return result.get("files", [])

    def list_cfr_title(self, year: int, title: int) -> List[Dict]:
        """List CFR volumes for a specific year/title."""
        path = f"CFR/{year}/title-{title}"
        result = self._request_json(path)
        return result.get("files", [])

    def list_plaw_congress(self, congress: int) -> List[Dict]:
        """List public laws for a congress."""
        path = f"PLAW/{congress}"
        result = self._request_json(path)
        return result.get("files", [])

    def list_fr_year(self, year: int) -> List[Dict]:
        """List Federal Register issues for a year."""
        path = f"FR/{year}"
        result = self._request_json(path)
        return result.get("files", [])

    def fetch_xml_file(self, url: str) -> str:
        """Fetch and return XML content."""
        return self._fetch_xml(url)


def extract_text_from_bill_xml(xml_content: str) -> tuple[str, Dict]:
    """Extract text and metadata from a bill XML."""
    if not xml_content:
        return "", {}

    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError:
        return "", {}

    metadata = {}
    text_parts = []

    # Extract Dublin Core metadata
    ns = {"dc": "http://purl.org/dc/elements/1.1/"}
    dc_title = root.find(".//dc:title", ns)
    if dc_title is not None and dc_title.text:
        metadata["title"] = dc_title.text.strip()

    dc_date = root.find(".//dc:date", ns)
    if dc_date is not None and dc_date.text:
        metadata["date"] = dc_date.text.strip()

    dc_publisher = root.find(".//dc:publisher", ns)
    if dc_publisher is not None and dc_publisher.text:
        metadata["publisher"] = dc_publisher.text.strip()

    # Extract official title from form
    official_title = root.find(".//official-title")
    if official_title is not None:
        title_text = "".join(official_title.itertext()).strip()
        if title_text:
            text_parts.append(f"OFFICIAL TITLE: {title_text}\n\n")

    # Extract short title
    short_title = root.find(".//short-title")
    if short_title is not None:
        st_text = "".join(short_title.itertext()).strip()
        if st_text:
            metadata["short_title"] = st_text

    # Extract legis-body content
    legis_body = root.find(".//legis-body")
    if legis_body is not None:
        for elem in legis_body.iter():
            if elem.tag in ["section", "paragraph", "subparagraph", "clause", "subclause"]:
                # Get enum if present
                enum_elem = elem.find("enum")
                enum_text = enum_elem.text.strip() if enum_elem is not None and enum_elem.text else ""

                # Get header if present
                header_elem = elem.find("header")
                header_text = header_elem.text.strip() if header_elem is not None and header_elem.text else ""

                # Get text content
                text_elem = elem.find("text")
                if text_elem is not None:
                    content = "".join(text_elem.itertext()).strip()
                    if content:
                        prefix = f"{enum_text} {header_text}".strip()
                        if prefix:
                            text_parts.append(f"{prefix}: {content}\n\n")
                        else:
                            text_parts.append(f"{content}\n\n")

    full_text = "".join(text_parts).strip()

    # If structured extraction didn't get much, fall back to all text
    if len(full_text) < 500:
        all_text = "".join(root.itertext())
        # Clean up whitespace
        all_text = re.sub(r'\s+', ' ', all_text).strip()
        if len(all_text) > len(full_text):
            full_text = all_text

    return full_text, metadata


def extract_text_from_cfr_xml(xml_content: str) -> tuple[str, Dict]:
    """Extract text and metadata from a CFR XML."""
    if not xml_content:
        return "", {}

    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError:
        return "", {}

    metadata = {}
    text_parts = []

    # Get title info
    title_elem = root.find(".//TITLE")
    if title_elem is not None:
        metadata["cfr_title"] = title_elem.text.strip() if title_elem.text else ""

    # Get subtitle/chapter info
    subtitle = root.find(".//SUBTITLE")
    if subtitle is not None:
        metadata["subtitle"] = subtitle.text.strip() if subtitle.text.text else ""

    chapter = root.find(".//CHAPTER")
    if chapter is not None:
        metadata["chapter"] = chapter.text.strip() if chapter.text else ""

    # Extract all text from parts and sections
    for part in root.iter("PART"):
        part_num = part.find("HD")
        if part_num is not None and part_num.text:
            text_parts.append(f"\n\nPART: {part_num.text.strip()}\n")

        for section in part.iter("SECTION"):
            sectno = section.find("SECTNO")
            subject = section.find("SUBJECT")
            if sectno is not None and sectno.text:
                text_parts.append(f"\n{sectno.text.strip()}")
                if subject is not None and subject.text:
                    text_parts.append(f" - {subject.text.strip()}")
                text_parts.append("\n")

            for p in section.iter("P"):
                if p.text:
                    text_parts.append(p.text.strip() + "\n")

    full_text = "".join(text_parts).strip()

    # Fallback to all text if structured extraction failed
    if len(full_text) < 500:
        all_text = "".join(root.itertext())
        all_text = re.sub(r'\s+', ' ', all_text).strip()
        if len(all_text) > len(full_text):
            full_text = all_text

    return full_text, metadata


def extract_text_from_plaw_xml(xml_content: str) -> tuple[str, Dict]:
    """Extract text and metadata from a public law XML."""
    if not xml_content:
        return "", {}

    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError:
        return "", {}

    metadata = {}
    text_parts = []

    # Get Dublin Core metadata
    ns = {"dc": "http://purl.org/dc/elements/1.1/"}
    dc_title = root.find(".//dc:title", ns)
    if dc_title is not None and dc_title.text:
        metadata["title"] = dc_title.text.strip()

    dc_date = root.find(".//dc:date", ns)
    if dc_date is not None and dc_date.text:
        metadata["date"] = dc_date.text.strip()

    # Extract all text content
    all_text = "".join(root.itertext())
    all_text = re.sub(r'\s+', ' ', all_text).strip()

    return all_text, metadata


def extract_text_from_fr_xml(xml_content: str) -> tuple[str, Dict]:
    """Extract text and metadata from a Federal Register XML."""
    if not xml_content:
        return "", {}

    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError:
        return "", {}

    metadata = {}
    text_parts = []

    # Try to get title
    subject = root.find(".//SUBJECT")
    if subject is not None and subject.text:
        metadata["title"] = subject.text.strip()

    # Get agency
    agency = root.find(".//AGENCY")
    if agency is not None and agency.text:
        metadata["agency"] = agency.text.strip()

    # Get date
    date_elem = root.find(".//DATE")
    if date_elem is not None and date_elem.text:
        metadata["date"] = date_elem.text.strip()

    # Extract text from CONTENTS
    for p in root.iter("P"):
        if p.text:
            text_parts.append(p.text.strip() + "\n\n")

    full_text = "".join(text_parts).strip()

    # Fallback
    if len(full_text) < 500:
        all_text = "".join(root.itertext())
        all_text = re.sub(r'\s+', ' ', all_text).strip()
        if len(all_text) > len(full_text):
            full_text = all_text

    return full_text, metadata


def normalize(doc_id: str, full_text: str, metadata: Dict, collection: str, url: str) -> Dict:
    """Transform extracted data into normalized schema."""
    collection_info = COLLECTIONS.get(collection, {})
    doc_type = collection_info.get("type", "legislation")

    return {
        "_id": f"govinfo-{collection.lower()}-{doc_id}",
        "_source": SOURCE_ID,
        "_type": doc_type,
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": metadata.get("title", ""),
        "text": full_text,
        "date": metadata.get("date"),
        "url": url,
        "collection_code": collection,
        "collection_name": collection_info.get("name", collection),
        "publisher": metadata.get("publisher"),
        "short_title": metadata.get("short_title"),
        "agency": metadata.get("agency"),
    }


def fetch_sample_bills(client: GovInfoBulkData, count: int = 5) -> List[Dict]:
    """Fetch sample bills from recent congress."""
    print(f"  Fetching {count} sample bills...")
    records = []

    # Get current congress (119th as of 2025)
    congress = 119
    session = 1
    bill_types = ["hr", "s", "hjres", "sjres"]

    for bill_type in bill_types:
        if len(records) >= count:
            break

        files = client.list_bills_congress(congress, session, bill_type)
        time.sleep(REQUEST_DELAY)

        xml_files = [f for f in files if f.get("fileExtension") == "xml"]

        for f in xml_files[:3]:  # Try up to 3 per type
            if len(records) >= count:
                break

            url = f.get("link")
            name = f.get("name", "")

            if not url or not name.endswith(".xml"):
                continue

            print(f"    Fetching {name}...")
            xml_content = client.fetch_xml_file(url)
            time.sleep(REQUEST_DELAY)

            if not xml_content:
                continue

            full_text, metadata = extract_text_from_bill_xml(xml_content)

            if len(full_text) < 500:
                print(f"      Skipping {name}: only {len(full_text)} chars")
                continue

            # Extract doc_id from filename
            doc_id = name.replace(".xml", "")
            record = normalize(doc_id, full_text, metadata, "BILLS", url)
            records.append(record)

            print(f"    [{len(records)}] {record['_id']}: {len(full_text):,} chars")

    return records


def fetch_sample_cfr(client: GovInfoBulkData, count: int = 5) -> List[Dict]:
    """Fetch sample CFR sections."""
    print(f"  Fetching {count} sample CFR volumes...")
    records = []

    # Get recent year CFR
    year = 2025
    titles = [1, 5, 12, 21, 29]  # Various CFR titles

    for title in titles:
        if len(records) >= count:
            break

        files = client.list_cfr_title(year, title)
        time.sleep(REQUEST_DELAY)

        xml_files = [f for f in files if f.get("fileExtension") == "xml"]

        for f in xml_files[:1]:  # One volume per title
            if len(records) >= count:
                break

            url = f.get("link")
            name = f.get("name", "")
            size = f.get("size", 0)

            if not url or not name.endswith(".xml"):
                continue

            # Skip very large files (>10MB)
            if size > 10_000_000:
                print(f"    Skipping {name}: too large ({size:,} bytes)")
                continue

            print(f"    Fetching {name} ({size:,} bytes)...")
            xml_content = client.fetch_xml_file(url)
            time.sleep(REQUEST_DELAY)

            if not xml_content:
                continue

            full_text, metadata = extract_text_from_cfr_xml(xml_content)

            if len(full_text) < 500:
                print(f"      Skipping {name}: only {len(full_text)} chars")
                continue

            # Truncate very long texts for samples
            if len(full_text) > 100000:
                full_text = full_text[:100000] + "\n\n[TRUNCATED FOR SAMPLE]"

            doc_id = name.replace(".xml", "")
            if not metadata.get("title"):
                metadata["title"] = f"Code of Federal Regulations, Title {title}"
            record = normalize(doc_id, full_text, metadata, "CFR", url)
            records.append(record)

            print(f"    [{len(records)}] {record['_id']}: {len(full_text):,} chars")

    return records


def fetch_sample_plaw(client: GovInfoBulkData, count: int = 3) -> List[Dict]:
    """Fetch sample public laws."""
    print(f"  Fetching {count} sample public laws...")
    records = []

    # Get recent congress
    congress = 118  # Recent completed congress

    items = client.list_plaw_congress(congress)
    time.sleep(REQUEST_DELAY)

    # Look for public laws
    publ_items = [i for i in items if i.get("name", "").startswith("publ")]

    for item in publ_items[:5]:
        if len(records) >= count:
            break

        if item.get("folder"):
            # It's a folder, get files inside
            path = f"PLAW/{congress}/{item['name']}"
            files = client._request_json(path)
            time.sleep(REQUEST_DELAY)

            xml_files = [f for f in files.get("files", []) if f.get("fileExtension") == "xml"]
            for f in xml_files[:1]:
                url = f.get("link")
                name = f.get("name", "")

                if not url:
                    continue

                print(f"    Fetching {name}...")
                xml_content = client.fetch_xml_file(url)
                time.sleep(REQUEST_DELAY)

                if not xml_content:
                    continue

                full_text, metadata = extract_text_from_plaw_xml(xml_content)

                if len(full_text) < 500:
                    continue

                doc_id = name.replace(".xml", "")
                if not metadata.get("title"):
                    metadata["title"] = f"Public Law {congress}-{item['name'].replace('publ', '')}"
                record = normalize(doc_id, full_text, metadata, "PLAW", url)
                records.append(record)

                print(f"    [{len(records)}] {record['_id']}: {len(full_text):,} chars")
                break

    return records


def fetch_sample(client: GovInfoBulkData) -> List[Dict]:
    """Fetch sample documents from multiple collections."""
    print("Fetching samples from GovInfo Bulk Data (no API key required)...")
    all_records = []

    # Fetch from different collections
    all_records.extend(fetch_sample_bills(client, 6))
    all_records.extend(fetch_sample_cfr(client, 4))
    all_records.extend(fetch_sample_plaw(client, 3))

    return all_records


def fetch_all(sample: bool = False) -> Generator[Dict, None, None]:
    """
    Fetch all documents from GovInfo bulk data.

    This is the standard interface expected by the VPS bootstrap runner.

    Args:
        sample: If True, fetch only a small sample (10-15 records)

    Yields:
        Normalized document records
    """
    client = GovInfoBulkData()

    if sample:
        # Yield sample records
        records = fetch_sample(client)
        for record in records:
            yield record
    else:
        # Full fetch - iterate through all collections
        print("Starting full GovInfo bulk data fetch...")

        # Congressional Bills (recent congress)
        congress = 119
        for session in [1, 2]:
            for bill_type in ["hr", "s", "hjres", "sjres", "hconres", "sconres"]:
                print(f"Fetching BILLS/{congress}/{session}/{bill_type}...")
                try:
                    files = client.list_bills_congress(congress, session, bill_type)
                    time.sleep(REQUEST_DELAY)

                    xml_files = [f for f in files if f.get("fileExtension") == "xml"]
                    for f in xml_files:
                        url = f.get("link")
                        name = f.get("name", "")

                        if not url or not name.endswith(".xml"):
                            continue

                        xml_content = client.fetch_xml_file(url)
                        time.sleep(REQUEST_DELAY)

                        if not xml_content:
                            continue

                        full_text, metadata = extract_text_from_bill_xml(xml_content)
                        if len(full_text) < 200:
                            continue

                        doc_id = name.replace(".xml", "")
                        record = normalize(doc_id, full_text, metadata, "BILLS", url)
                        yield record
                except Exception as e:
                    print(f"  Error fetching {bill_type}: {e}")

        # Public Laws (recent congress)
        for congress in [118, 117]:
            print(f"Fetching PLAW/{congress}...")
            try:
                items = client.list_plaw_congress(congress)
                time.sleep(REQUEST_DELAY)

                for item in items:
                    if not item.get("folder"):
                        continue

                    if not item.get("name", "").startswith("publ"):
                        continue

                    path = f"PLAW/{congress}/{item['name']}"
                    files = client._request_json(path)
                    time.sleep(REQUEST_DELAY)

                    xml_files = [f for f in files.get("files", []) if f.get("fileExtension") == "xml"]
                    for f in xml_files[:1]:
                        url = f.get("link")
                        name = f.get("name", "")

                        if not url:
                            continue

                        xml_content = client.fetch_xml_file(url)
                        time.sleep(REQUEST_DELAY)

                        if not xml_content:
                            continue

                        full_text, metadata = extract_text_from_plaw_xml(xml_content)
                        if len(full_text) < 200:
                            continue

                        doc_id = name.replace(".xml", "")
                        if not metadata.get("title"):
                            metadata["title"] = f"Public Law {congress}-{item['name'].replace('publ', '')}"
                        record = normalize(doc_id, full_text, metadata, "PLAW", url)
                        yield record
            except Exception as e:
                print(f"  Error fetching PLAW/{congress}: {e}")


def save_samples(records: List[Dict]) -> None:
    """Save sample records to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    for i, record in enumerate(records):
        filename = f"record_{i:04d}.json"
        filepath = SAMPLE_DIR / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    # Also save all samples in one file
    all_samples = SAMPLE_DIR / "all_samples.json"
    with open(all_samples, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"\nSaved {len(records)} samples to {SAMPLE_DIR}")


def validate_samples(sample_dir: Path) -> bool:
    """Validate sample records meet requirements."""
    samples = list(sample_dir.glob("record_*.json"))

    if len(samples) < 10:
        print(f"FAIL: Only {len(samples)} samples, need at least 10")
        return False

    total_text_len = 0
    all_valid = True

    for sample_path in samples:
        with open(sample_path, "r", encoding="utf-8") as f:
            record = json.load(f)

        text = record.get("text", "")
        if not text:
            print(f"FAIL: {sample_path.name} has no text")
            all_valid = False
        elif len(text) < 500:
            print(f"WARN: {sample_path.name} has short text ({len(text)} chars)")

        total_text_len += len(text)

        # Check required fields
        for field in ["_id", "_source", "_type", "title"]:
            if not record.get(field):
                print(f"WARN: {sample_path.name} missing {field}")

        # Check for raw HTML tags
        if text and re.search(r"<[a-z]+[^>]*>", text, re.IGNORECASE):
            print(f"WARN: {sample_path.name} may contain HTML tags")

    avg_len = total_text_len // len(samples) if samples else 0
    print(f"\nValidation summary:")
    print(f"  Samples: {len(samples)}")
    print(f"  Average text length: {avg_len:,} chars")
    print(f"  All valid: {all_valid}")

    return all_valid and len(samples) >= 10


def main():
    parser = argparse.ArgumentParser(description="US/GovInfo bulk data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bootstrap_parser = subparsers.add_parser("bootstrap", help="Initial data fetch")
    bootstrap_parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    bootstrap_parser.add_argument("--recent", action="store_true", help="Last 30 days only")

    updates_parser = subparsers.add_parser("updates", help="Fetch updates")
    updates_parser.add_argument("--since", required=True, help="Date to fetch from (YYYY-MM-DD)")

    subparsers.add_parser("validate", help="Validate sample records")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "validate":
        valid = validate_samples(SAMPLE_DIR)
        sys.exit(0 if valid else 1)

    client = GovInfoBulkData()

    if args.command == "bootstrap":
        if args.sample:
            print("Fetching samples from GovInfo Bulk Data...")
            try:
                records = fetch_sample(client)
                if records:
                    save_samples(records)

                    # Validation summary
                    text_lengths = [len(r.get('text', '')) for r in records]
                    avg_len = sum(text_lengths) / len(text_lengths) if text_lengths else 0
                    print(f"\nSummary:")
                    print(f"  Records: {len(records)}")
                    print(f"  Avg text length: {avg_len:,.0f} chars")
                    print(f"  Min text length: {min(text_lengths):,} chars")
                    print(f"  Max text length: {max(text_lengths):,} chars")

                    # Run validation
                    print("\nValidating samples...")
                    valid = validate_samples(SAMPLE_DIR)
                    sys.exit(0 if len(records) >= 10 and valid else 1)
                else:
                    print("No records fetched!", file=sys.stderr)
                    sys.exit(1)

            except requests.HTTPError as e:
                print(f"HTTP error: {e}", file=sys.stderr)
                sys.exit(1)

        elif args.recent:
            print("Recent fetch not yet implemented for bulk data")
            sys.exit(1)

        else:
            print("Use --sample for sample mode or --recent for recent data")
            sys.exit(1)

    elif args.command == "updates":
        print("Updates not yet implemented for bulk data")
        sys.exit(1)


if __name__ == "__main__":
    main()
