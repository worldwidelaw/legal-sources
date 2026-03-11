#!/usr/bin/env python3
"""
LI/LILEX - Liechtenstein Consolidated Legislation
Fetches legislation from gesetze.li with full text via HTML scraping.
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Generator, Optional
from html import unescape

import requests
from bs4 import BeautifulSoup

# Configuration
BASE_URL = "https://www.gesetze.li"
RATE_LIMIT_DELAY = 1.0  # seconds between requests

# Request headers
HEADERS = {
    "User-Agent": "WorldWideLaw/1.0 (legal research project)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de,en;q=0.5"
}


def fetch_page(url: str, timeout: int = 30) -> Optional[str]:
    """Fetch HTML page with rate limiting."""
    time.sleep(RATE_LIMIT_DELAY)
    try:
        response = requests.get(url, headers=HEADERS, timeout=timeout)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        return None


def get_latest_laws(limit: int = 50) -> list:
    """Get list of recently updated laws from neueste-konso page."""
    url = f"{BASE_URL}/konso/neueste-konso"
    html = fetch_page(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    laws = []

    # Find all law links in the table
    for link in soup.find_all("a", href=re.compile(r"^/konso/\d+")):
        href = link.get("href", "")
        lgbl_id = href.replace("/konso/", "")

        # Get parent row for metadata
        row = link.find_parent("tr")
        if row:
            cells = row.find_all("td")
            title_cell = row.find("a", href=re.compile(r"^/konso/\d+"))

            law_info = {
                "lgbl_id": lgbl_id,
                "title": title_cell.get_text(strip=True) if title_cell else "",
                "url": f"{BASE_URL}{href}"
            }

            # Avoid duplicates
            if lgbl_id and not any(l["lgbl_id"] == lgbl_id for l in laws):
                laws.append(law_info)

        if len(laws) >= limit:
            break

    return laws


def get_chronological_laws(limit: int = 50) -> list:
    """Get list of recent LGBl entries from chronological gazette."""
    url = f"{BASE_URL}/chrono/neueste-lgbl"
    html = fetch_page(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    laws = []

    # Find all gazette entry links
    for link in soup.find_all("a", href=re.compile(r"^/chrono/\d+")):
        href = link.get("href", "")
        lgbl_id = href.replace("/chrono/", "")

        # Get parent row for metadata
        row = link.find_parent("tr")
        title = ""
        date = ""

        if row:
            # Title is usually in a later cell
            cells = row.find_all("td")
            for cell in cells:
                cell_text = cell.get_text(strip=True)
                # Check if this looks like a date (DD.MM.YYYY)
                if re.match(r"\d{2}\.\d{2}\.\d{4}", cell_text):
                    date = cell_text
                elif len(cell_text) > 20 and not cell_text.startswith("LGBl"):
                    title = cell_text

        law_info = {
            "lgbl_id": lgbl_id,
            "title": title,
            "date": date,
            "url": f"{BASE_URL}/chrono/{lgbl_id}"
        }

        # Avoid duplicates
        if lgbl_id and not any(l["lgbl_id"] == lgbl_id for l in laws):
            laws.append(law_info)

        if len(laws) >= limit:
            break

    return laws


def get_law_metadata(lgbl_id: str) -> dict:
    """Get metadata for a specific law from its overview page."""
    # Try consolidated version first
    url = f"{BASE_URL}/konso/{lgbl_id}"
    html = fetch_page(url)

    if not html:
        # Try chronological if konso fails
        url = f"{BASE_URL}/chrono/{lgbl_id}"
        html = fetch_page(url)
        if not html:
            return {}

    soup = BeautifulSoup(html, "html.parser")

    metadata = {
        "lgbl_id": lgbl_id,
        "url": url
    }

    # Extract title from h2.law-title
    title_elem = soup.find("h2", class_="law-title")
    if title_elem:
        metadata["title"] = title_elem.get_text(strip=True)

    # Extract LGBl-Nr (formatted as YYYY.NNN)
    lgbl_link = soup.find("a", href=re.compile(r"^/chrono/\d+"))
    if lgbl_link:
        lgbl_text = lgbl_link.get_text(strip=True)
        metadata["lgbl_nr"] = lgbl_text

    # Extract LR-Nr (systematic number)
    lr_link = soup.find("a", href=re.compile(r"/konso/gebietssystematik\?lrstart="))
    if lr_link:
        metadata["lr_nr"] = lr_link.get_text(strip=True)

    # Extract current version from dropdown
    version_select = soup.find("select", {"name": "version"})
    if version_select:
        selected = version_select.find("option", selected=True)
        if selected:
            metadata["current_version"] = selected.get("value")
            metadata["version_date"] = selected.get_text(strip=True)

    # Get iframe URL for full text
    iframe = soup.find("iframe", class_="iframe")
    if iframe and iframe.get("src"):
        metadata["html_url"] = BASE_URL + iframe.get("src")

    return metadata


def fetch_full_text(lgbl_id: str, version: str = None) -> Optional[str]:
    """Fetch full text HTML content for a law."""
    # Build URL for HTML content
    if version:
        url = f"{BASE_URL}/konso/html/{lgbl_id}?version={version}"
    else:
        url = f"{BASE_URL}/konso/html/{lgbl_id}"

    html = fetch_page(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Remove script and style elements
    for element in soup(["script", "style", "meta", "link", "head"]):
        element.decompose()

    # Get body content
    body = soup.find("body")
    if body:
        # Extract text while preserving some structure
        text_parts = []
        for element in body.find_all(["div", "p", "h1", "h2", "h3", "h4", "table"]):
            element_text = element.get_text(separator=" ", strip=True)
            if element_text:
                text_parts.append(element_text)

        text = "\n\n".join(text_parts)
    else:
        text = soup.get_text(separator="\n", strip=True)

    # Clean up excessive whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)

    # Decode HTML entities
    text = unescape(text)

    return text.strip()


def fetch_document(lgbl_id: str) -> Optional[dict]:
    """Fetch a complete document with metadata and full text."""
    print(f"Fetching document: {lgbl_id}", file=sys.stderr)

    # Get metadata
    metadata = get_law_metadata(lgbl_id)
    if not metadata:
        print(f"  No metadata found", file=sys.stderr)
        return None

    # Get full text
    version = metadata.get("current_version")
    text = fetch_full_text(lgbl_id, version)

    if not text or len(text) < 50:
        print(f"  No full text found (len={len(text) if text else 0})", file=sys.stderr)
        return None

    metadata["text"] = text
    return metadata


def fetch_all() -> Generator[dict, None, None]:
    """Yield all available legislation documents."""
    # Get laws from both consolidated and chronological lists
    konso_laws = get_latest_laws(limit=100)
    chrono_laws = get_chronological_laws(limit=100)

    # Combine and deduplicate
    seen = set()
    all_laws = []

    for law in konso_laws + chrono_laws:
        lgbl_id = law["lgbl_id"]
        if lgbl_id not in seen:
            seen.add(lgbl_id)
            all_laws.append(law)

    print(f"Found {len(all_laws)} unique laws to fetch", file=sys.stderr)

    for law in all_laws:
        doc = fetch_document(law["lgbl_id"])
        if doc:
            yield doc


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Yield documents modified since a given date."""
    # Gesetze.li doesn't have a modification date API
    # So we fetch recent chronological entries and filter by date
    chrono_laws = get_chronological_laws(limit=50)

    since_str = since.strftime("%d.%m.%Y")

    for law in chrono_laws:
        date_str = law.get("date", "")
        if date_str:
            try:
                # Parse German date format
                law_date = datetime.strptime(date_str, "%d.%m.%Y")
                if law_date >= since:
                    doc = fetch_document(law["lgbl_id"])
                    if doc:
                        yield doc
            except ValueError:
                pass


def normalize(raw: dict) -> dict:
    """Transform raw data into standard schema."""
    lgbl_id = raw.get("lgbl_id", "")

    # Parse LGBl-Nr to extract year
    lgbl_nr = raw.get("lgbl_nr", "")
    date = ""
    if lgbl_nr:
        match = re.match(r"(\d{4})\.(\d+)", lgbl_nr)
        if match:
            date = f"{match.group(1)}-01-01"  # Use year from LGBl number

    # Use version date if available
    version_date = raw.get("version_date", "")
    if version_date and "-" not in version_date:
        # Parse DD.MM.YYYY format
        match = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", version_date)
        if match:
            date = f"{match.group(3)}-{match.group(2)}-{match.group(1)}"

    return {
        "_id": lgbl_id,
        "_source": "LI/LILEX",
        "_type": "legislation",
        "_fetched_at": datetime.utcnow().isoformat() + "Z",
        "lgbl_id": lgbl_id,
        "lgbl_nr": lgbl_nr,
        "lr_nr": raw.get("lr_nr", ""),
        "title": raw.get("title", ""),
        "text": raw.get("text", ""),
        "date": date,
        "version": raw.get("current_version", ""),
        "version_date": version_date,
        "url": raw.get("url", f"https://www.gesetze.li/konso/{lgbl_id}"),
        "language": "de"
    }


def bootstrap_sample(sample_dir: Path, sample_count: int = 12):
    """Fetch sample documents for testing."""
    print(f"Fetching {sample_count} sample documents...", file=sys.stderr)

    sample_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    total_chars = 0

    for raw_doc in fetch_all():
        if count >= sample_count:
            break

        normalized = normalize(raw_doc)

        # Validate full text
        text = normalized.get("text", "")
        if not text or len(text) < 100:
            print(f"Skipping {normalized['_id']}: insufficient text ({len(text)} chars)", file=sys.stderr)
            continue

        # Save to sample directory
        safe_id = normalized["_id"].replace("/", "_")
        sample_file = sample_dir / f"{safe_id}.json"

        with open(sample_file, "w", encoding="utf-8") as f:
            json.dump(normalized, f, ensure_ascii=False, indent=2)

        print(f"Saved: {normalized['_id']} ({len(text)} chars)", file=sys.stderr)
        total_chars += len(text)
        count += 1

    print(f"\nBootstrap complete: {count} documents, avg {total_chars // max(count, 1)} chars/doc", file=sys.stderr)
    return count


def main():
    parser = argparse.ArgumentParser(description="LI/LILEX Liechtenstein Legislation Fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Bootstrap command
    bootstrap_parser = subparsers.add_parser("bootstrap", help="Fetch sample documents")
    bootstrap_parser.add_argument("--sample", action="store_true", help="Fetch sample data")
    bootstrap_parser.add_argument("--count", type=int, default=12, help="Number of samples")

    # Update command
    update_parser = subparsers.add_parser("update", help="Fetch recent updates")
    update_parser.add_argument("--days", type=int, default=7, help="Days to look back")

    # List command
    list_parser = subparsers.add_parser("list", help="List available laws")
    list_parser.add_argument("--limit", type=int, default=20, help="Number of laws to list")

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(sample_dir, args.count)
        else:
            print("Use --sample flag to fetch sample data", file=sys.stderr)
    elif args.command == "update":
        since = datetime.utcnow() - timedelta(days=args.days)
        for doc in fetch_updates(since):
            print(json.dumps(normalize(doc), ensure_ascii=False))
    elif args.command == "list":
        laws = get_latest_laws(limit=args.limit)
        for law in laws:
            print(f"{law['lgbl_id']}: {law.get('title', 'N/A')}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
