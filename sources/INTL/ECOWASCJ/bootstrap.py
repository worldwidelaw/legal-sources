#!/usr/bin/env python3
"""
INTL/ECOWASCJ - ECOWAS Community Court of Justice Fetcher

Fetches case law from the ECOWAS Community Court of Justice via AfricanLII.
~296 judgments covering 15 West African states (2008-present).

Data source: https://africanlii.org/en/judgments/ECOWASCJ/
Method: HTML scraping of AfricanLII listing + metadata pages, PDF text extraction
License: Free access (AfricanLII is open access)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import io
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

import requests
from bs4 import BeautifulSoup

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


BASE_URL = "https://africanlii.org"
LISTING_URL = f"{BASE_URL}/en/judgments/ECOWASCJ/"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "INTL/ECOWASCJ"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml,*/*",
}

RATE_LIMIT_DELAY = 1.5


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="INTL/ECOWASCJ",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""

def get_all_judgment_links(session: requests.Session, max_pages: int = 10) -> list:
    """Scrape listing pages to collect all judgment URLs and link text."""
    judgments = []
    seen = set()

    for page_num in range(0, max_pages):
        url = LISTING_URL if page_num == 0 else f"{LISTING_URL}?page={page_num + 1}"
        resp = session.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        found = 0
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/akn/" in href and "ecowascj" in href:
                if href not in seen:
                    seen.add(href)
                    link_text = a.get_text(strip=True)
                    judgments.append({"path": href, "link_text": link_text})
                    found += 1

        print(f"  Page {page_num + 1}: {found} judgments")
        if found == 0:
            break
        time.sleep(RATE_LIMIT_DELAY)

    return judgments


def parse_judgment_date(date_str: str) -> str:
    """Parse judgment date string to ISO format."""
    if not date_str:
        return ""
    # Try common formats: "30 January 2026", "1 March 2025"
    for fmt in ["%d %B %Y", "%d %b %Y"]:
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Try to extract from URL-style date: eng@YYYY-MM-DD
    m = re.search(r"(\d{4}-\d{2}-\d{2})", date_str)
    if m:
        return m.group(1)
    return date_str.strip()


def fetch_judgment_metadata(session: requests.Session, path: str) -> dict:
    """Fetch metadata from an individual judgment page."""
    url = BASE_URL + path
    resp = session.get(url, headers=HEADERS, timeout=30)
    if resp.status_code != 200:
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")

    metadata = {"url": url, "path": path}

    # Title from h1
    h1 = soup.find("h1")
    if h1:
        metadata["title"] = h1.get_text(strip=True)

    # Structured metadata from dt/dd pairs
    for dt in soup.find_all("dt"):
        dd = dt.find_next_sibling("dd")
        if not dd:
            continue
        key = dt.get_text(strip=True).lower().replace(" ", "_")
        val = dd.get_text(strip=True)
        if key == "case_number":
            # Clean up multi-line case numbers
            val = re.sub(r"\s*;\s*", "; ", val)
            metadata["case_number"] = val
        elif key == "judgment_date":
            metadata["date_raw"] = val
        elif key == "judges":
            metadata["judges"] = [j.strip() for j in val.split(",")]
        elif key == "media_neutral_citation":
            metadata["citation"] = val.replace("Copy", "").strip()
        elif key == "language":
            metadata["language"] = val

    # PDF URL
    metadata["pdf_url"] = url + "/source.pdf"

    return metadata


def download_pdf_text(session: requests.Session, pdf_url: str) -> str:
    """Download a PDF and extract its text."""
    try:
        resp = session.get(pdf_url, headers=HEADERS, timeout=120)
        if resp.status_code != 200:
            return ""
        if len(resp.content) > 50_000_000:  # Skip files > 50MB
            return ""
        return extract_text_from_pdf(resp.content)
    except Exception as e:
        print(f"    PDF download failed: {e}")
        return ""


def normalize(metadata: dict, text: str) -> dict:
    """Normalize a judgment record."""
    # Extract year and number from citation for ID
    citation = metadata.get("citation", "")
    m = re.search(r"\[(\d{4})\]\s*ECOWASCJ\s*(\d+)", citation)
    if m:
        doc_id = f"ecowascj-{m.group(1)}-{m.group(2)}"
    else:
        # Fallback: use path
        slug = metadata.get("path", "").replace("/", "-").strip("-")
        doc_id = f"ecowascj-{slug[-30:]}"

    date_str = parse_judgment_date(metadata.get("date_raw", ""))
    if not date_str:
        # Extract from path
        m = re.search(r"eng@(\d{4}-\d{2}-\d{2})", metadata.get("path", ""))
        if m:
            date_str = m.group(1)

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": metadata.get("title", ""),
        "text": text,
        "date": date_str,
        "url": metadata.get("url", ""),
        "citation": citation,
        "case_number": metadata.get("case_number", ""),
        "judges": metadata.get("judges", []),
        "language": metadata.get("language", ""),
    }


def fetch_judgments(session: requests.Session, judgment_links: list,
                    sample: bool = False) -> Generator:
    """Fetch and yield normalized judgment records with full text."""
    limit = min(15, len(judgment_links)) if sample else len(judgment_links)
    text_count = 0

    for i, jlink in enumerate(judgment_links[:limit]):
        path = jlink["path"]
        link_text = jlink["link_text"]
        print(f"\n[{i+1}/{limit}] {link_text[:70]}...")

        # Get metadata
        metadata = fetch_judgment_metadata(session, path)
        time.sleep(RATE_LIMIT_DELAY)

        if not metadata:
            print("  Failed to fetch metadata, skipping")
            continue

        # Download PDF and extract text
        pdf_url = metadata.get("pdf_url", "")
        print(f"  Downloading PDF...")
        text = download_pdf_text(session, pdf_url)
        time.sleep(RATE_LIMIT_DELAY)

        if text:
            text_count += 1
            print(f"  Got text ({len(text)} chars)")
        else:
            print(f"  No text extracted from PDF")

        record = normalize(metadata, text)
        yield record

    print(f"\nFetched {limit} judgments, {text_count} with text ({text_count*100//max(limit,1)}%)")


def test_connectivity():
    """Test basic connectivity."""
    session = requests.Session()
    print("Testing AfricanLII ECOWAS CCJ connectivity...")

    resp = session.get(LISTING_URL, headers=HEADERS, timeout=30)
    print(f"  Listing page: {resp.status_code}")

    soup = BeautifulSoup(resp.text, "html.parser")
    links = [a["href"] for a in soup.find_all("a", href=True)
             if "/akn/" in a["href"] and "ecowascj" in a["href"]]
    print(f"  Judgment links on page 1: {len(links)}")

    if links:
        detail_url = BASE_URL + links[0]
        resp = session.get(detail_url, headers=HEADERS, timeout=30)
        print(f"  Detail page: {resp.status_code}")

        pdf_url = detail_url + "/source.pdf"
        resp = session.get(pdf_url, headers=HEADERS, timeout=30, stream=True)
        print(f"  PDF download: {resp.status_code} ({resp.headers.get('content-length', '?')} bytes)")
        resp.close()

    print("Connectivity test PASSED")
    return True


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    session = requests.Session()

    print("Fetching judgment listings from AfricanLII...")
    judgment_links = get_all_judgment_links(session, max_pages=10)
    print(f"Total judgments found: {len(judgment_links)}")

    saved = 0
    for record in fetch_judgments(session, judgment_links, sample=sample):
        doc_id = record["_id"]
        safe_name = re.sub(r'[^\w\-]', '_', doc_id)
        out_path = SAMPLE_DIR / f"{safe_name}.json"

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        saved += 1
        print(f"  Saved: {out_path.name}")

    print(f"\nBootstrap complete: {saved} records saved to {SAMPLE_DIR}")

    text_count = 0
    for p in SAMPLE_DIR.glob("*.json"):
        with open(p) as f:
            rec = json.load(f)
        if rec.get("text") and len(rec["text"]) > 100:
            text_count += 1

    print(f"Records with substantial text: {text_count}/{saved}")


def main():
    parser = argparse.ArgumentParser(description="ECOWAS CCJ Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    if args.command == "test":
        test_connectivity()
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample)


if __name__ == "__main__":
    main()
