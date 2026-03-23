#!/usr/bin/env python3
"""
SI/LegislativeDatabase Bootstrap
Slovenian Legislation Database (PISRS)

Fetches laws passed by the Slovenian National Assembly (Državni zbor).

Data sources:
- Metadata: Parliament open data XML (all adopted laws since 1991)
- Full text: Official Gazette (uradni-list.si)

Usage:
    python bootstrap.py bootstrap --sample   # Fetch 10-15 sample records
    python bootstrap.py bootstrap --full     # Fetch all records
"""

import argparse
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from html import unescape
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Configuration
METADATA_URL = "https://fotogalerija.dz-rs.si/datoteke/opendata/SZ.XML"
GAZETTE_BASE = "https://www.uradni-list.si/glasilo-uradni-list-rs/vsebina"
RATE_LIMIT = 2  # seconds between full text fetches

# Source directory
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

# HTTP session with user agent
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/legal-data-hunter)"
})


def fetch_metadata_xml():
    """Download and parse the Parliament XML file with all adopted laws."""
    print(f"Fetching metadata from {METADATA_URL}...")
    response = session.get(METADATA_URL, timeout=120)
    response.raise_for_status()
    return ET.fromstring(response.content)


def parse_predpis(predpis_el):
    """Parse a single PREDPIS element into a metadata dict."""
    kartica = predpis_el.find("KARTICA_PREDPISA")
    if kartica is None:
        return None

    # Extract fields
    def get_text(tag):
        el = kartica.find(tag)
        return el.text.strip() if el is not None and el.text else ""

    sop = get_text("KARTICA_SOP")
    if not sop:
        return None

    # Keywords can appear multiple times
    keywords = [kw.text.strip() for kw in kartica.findall("KARTICA_KLJUCNE_BESEDE") if kw.text]

    return {
        "sop_number": sop,
        "unid": get_text("UNID"),
        "epa": get_text("KARTICA_EPA"),
        "abbreviation": get_text("KARTICA_KRATICA"),
        "title": get_text("KARTICA_NAZIV"),
        "type": get_text("KARTICA_VRSTA"),
        "date": get_text("KARTICA_DATUM"),
        "mandate": get_text("KARTICA_MANDAT"),
        "proposer": get_text("KARTICA_PREDLAGATELJ"),
        "procedure": get_text("KARTICA_POSTOPEK"),
        "phase": get_text("KARTICA_FAZA_POSTOPKA"),
        "committees": get_text("KARTICA_DELOVNA_TELESA"),
        "publication": get_text("KARTICA_OBJAVA"),
        "keywords": keywords,
        "session": get_text("KARTICA_SEJA"),
        "classification": get_text("KARTICA_KLASIFIKACIJSKA_STEVILKA"),
        "pisrs_url": get_text("KARTICA_URL_REGISTER_PREDPISOV"),
        "gazette_url": get_text("KARTICA_URL_URADNI_LIST"),
        "dz_url": get_text("KARTICA_URL_DZ"),
    }


def fetch_full_text_from_gazette(sop_number):
    """
    Fetch full text from the Official Gazette (uradni-list.si).

    URL pattern: https://www.uradni-list.si/glasilo-uradni-list-rs/vsebina/{sop_number}
    """
    url = f"{GAZETTE_BASE}/{sop_number}"

    try:
        response = session.get(url, timeout=30)
        if response.status_code != 200:
            return None, url

        soup = BeautifulSoup(response.content, "html.parser")

        # Find all content segments - these contain the law text
        content_segments = soup.find("div", class_="content-segments")
        if not content_segments:
            # Try alternative container
            content_segments = soup.find("div", id="divSection")

        if not content_segments:
            return None, url

        # Extract text from each segment
        text_parts = []
        for div in content_segments.find_all("div", class_=lambda c: c and c.startswith("esegment")):
            # Get text content, preserving line breaks
            text = div.get_text(separator=" ", strip=True)
            if text:
                # Clean up HTML entities and normalize whitespace
                text = unescape(text)
                text = re.sub(r'\s+', ' ', text).strip()
                text_parts.append(text)

        if not text_parts:
            # Fallback: get all text from content segments
            text = content_segments.get_text(separator="\n", strip=True)
            text = unescape(text)
            text = re.sub(r'\n\s*\n', '\n\n', text)  # Normalize blank lines
            return text, url

        # Join parts with double newlines for readability
        full_text = "\n\n".join(text_parts)
        return full_text, url

    except Exception as e:
        print(f"  Error fetching {url}: {e}")
        return None, url


def normalize(raw_data, full_text=None, text_url=None):
    """Transform raw metadata + full text into standard schema."""
    return {
        "_id": f"SI/LegislativeDatabase/{raw_data['sop_number']}",
        "_source": "SI/LegislativeDatabase",
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),

        # Core fields
        "title": raw_data["title"],
        "text": full_text or "",
        "date": raw_data["date"] if raw_data["date"] else None,
        "url": text_url or raw_data.get("gazette_url") or raw_data.get("pisrs_url") or "",

        # Slovenian-specific metadata
        "sop_number": raw_data["sop_number"],
        "abbreviation": raw_data.get("abbreviation", ""),
        "document_type": raw_data.get("type", ""),
        "publication": raw_data.get("publication", ""),
        "proposer": raw_data.get("proposer", ""),
        "procedure": raw_data.get("procedure", ""),
        "phase": raw_data.get("phase", ""),
        "session": raw_data.get("session", ""),
        "keywords": raw_data.get("keywords", []),
        "mandate": raw_data.get("mandate", ""),

        # Source URLs
        "pisrs_url": raw_data.get("pisrs_url", ""),
        "gazette_url": raw_data.get("gazette_url", ""),
        "dz_url": raw_data.get("dz_url", ""),
    }


def fetch_all(sample_mode=False, sample_size=15):
    """
    Fetch all records or a sample.

    Args:
        sample_mode: If True, only fetch sample_size records
        sample_size: Number of records to fetch in sample mode
    """
    # Parse the XML metadata
    root = fetch_metadata_xml()
    predpisi = root.findall("PREDPIS")
    print(f"Found {len(predpisi)} laws in XML")

    if sample_mode:
        # Select a diverse sample: some old, some new, some from middle
        total = len(predpisi)
        indices = []
        # First few (oldest)
        indices.extend(range(0, min(3, total)))
        # Some from middle
        mid = total // 2
        indices.extend(range(mid, min(mid + 5, total)))
        # Recent ones
        indices.extend(range(max(0, total - 7), total))
        # Deduplicate and limit
        indices = sorted(set(indices))[:sample_size]
        selected = [predpisi[i] for i in indices]
        print(f"Selected {len(selected)} records for sample")
    else:
        selected = predpisi

    # Process each record
    for i, predpis in enumerate(selected):
        metadata = parse_predpis(predpis)
        if not metadata:
            continue

        sop = metadata["sop_number"]
        print(f"[{i+1}/{len(selected)}] Processing {sop}: {metadata['title'][:60]}...")

        # Fetch full text from gazette
        full_text, text_url = fetch_full_text_from_gazette(sop)

        if full_text:
            print(f"  Full text: {len(full_text)} chars")
        else:
            print(f"  No full text found at {text_url}")
            # Try with the gazette URL from metadata if available
            if metadata.get("gazette_url"):
                print(f"  Trying gazette_url from metadata...")
                try:
                    response = session.get(metadata["gazette_url"], timeout=30)
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.content, "html.parser")
                        content = soup.find("div", class_="content-segments")
                        if content:
                            full_text = content.get_text(separator="\n", strip=True)
                            text_url = metadata["gazette_url"]
                            print(f"  Found via gazette_url: {len(full_text)} chars")
                except Exception as e:
                    print(f"  Error: {e}")

        # Normalize and yield
        record = normalize(metadata, full_text, text_url)
        yield record

        # Rate limiting (only for full text fetches)
        if i < len(selected) - 1:
            time.sleep(RATE_LIMIT)


def fetch_updates(since):
    """Fetch records modified since a given date."""
    # The XML file is a complete dump, so we parse and filter by date
    root = fetch_metadata_xml()

    since_date = datetime.fromisoformat(since.replace("Z", "+00:00")).date()

    for predpis in root.findall("PREDPIS"):
        metadata = parse_predpis(predpis)
        if not metadata or not metadata["date"]:
            continue

        try:
            record_date = datetime.strptime(metadata["date"], "%Y-%m-%d").date()
            if record_date >= since_date:
                full_text, text_url = fetch_full_text_from_gazette(metadata["sop_number"])
                yield normalize(metadata, full_text, text_url)
                time.sleep(RATE_LIMIT)
        except ValueError:
            continue


def bootstrap(sample=False):
    """Main bootstrap function."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = []
    text_lengths = []

    for record in fetch_all(sample_mode=sample, sample_size=15):
        records.append(record)

        # Track text lengths
        if record.get("text"):
            text_lengths.append(len(record["text"]))

        # Save individual record
        filename = record["sop_number"].replace("/", "-").replace(" ", "_") + ".json"
        filepath = SAMPLE_DIR / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    # Print summary
    print("\n" + "=" * 60)
    print("BOOTSTRAP SUMMARY")
    print("=" * 60)
    print(f"Total records: {len(records)}")
    print(f"Records with full text: {len(text_lengths)}")
    if text_lengths:
        print(f"Average text length: {sum(text_lengths) // len(text_lengths):,} chars")
        print(f"Min text length: {min(text_lengths):,} chars")
        print(f"Max text length: {max(text_lengths):,} chars")

    # Validate minimum requirements
    if len(records) < 10:
        print("\nWARNING: Less than 10 records fetched!")
        return False

    if len(text_lengths) < 10:
        print("\nWARNING: Less than 10 records have full text!")
        return False

    print("\nValidation PASSED")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SI/LegislativeDatabase Bootstrap")
    parser.add_argument("command", choices=["bootstrap"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "bootstrap":
        success = bootstrap(sample=args.sample or not args.full)
        sys.exit(0 if success else 1)
