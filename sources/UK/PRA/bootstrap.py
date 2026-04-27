#!/usr/bin/env python3
"""
UK/PRA - Prudential Regulation Authority Fetcher

Fetches PRA publications (policy statements, supervisory statements,
consultation papers, letters) from the Bank of England website.

Data source: https://www.bankofengland.co.uk/prudential-regulation
Method: BoE internal News API for discovery + HTML scraping for full text
License: Open Government Licence v3.0
Rate limit: 1.5 seconds between requests

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test API connectivity
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

NEWS_API_URL = "https://www.bankofengland.co.uk/_api/News/RefreshPagedNewsList"
BASE_URL = "https://www.bankofengland.co.uk"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "UK/PRA"

# PRA data source ID in the BoE CMS
PRA_DATASOURCE_ID = "CE377CC8-BFBC-418B-B4D9-DBC1C64774A8"

# Publication type taxonomy IDs
PUB_TYPES = {
    "708a477279ec403d9df9930c9504e164": "policy_statement",
    "65a33f20fd5241d58bd01d5fb54bded8": "supervisory_statement",
    "20b5fe84fcfe44ef89a95cbd39626fde": "consultation_paper",
    "21bc0c544c324596abd90643c9a2a205": "letter",
}

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research; github.com/ZachLaik/LegalDataHunter)",
    "Accept": "*/*",
    "X-Requested-With": "XMLHttpRequest",
}


def fetch_publication_list(page: int = 1, page_size: int = 30) -> Tuple[str, int]:
    """Fetch a page of PRA publications from the BoE News API.

    Returns:
        Tuple of (results_html, total_count)
    """
    data = {
        "Id": f"{{{PRA_DATASOURCE_ID}}}",
        "NewsTypes": "65d34b0d42784c6bb1dd302c1ed63653",
        "PageSize": str(page_size),
        "Page": str(page),
        "Direction": "1",
        "Taxonomies": "",
        "DateFrom": "",
        "DateTo": "",
        "SearchTerm": "",
    }
    resp = requests.post(NEWS_API_URL, data=data, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    obj = resp.json()
    results_html = obj.get("Results", "")

    # Extract total count from "1275 results" text
    total = 0
    m = re.search(r"(\d+)\s+results?", results_html)
    if m:
        total = int(m.group(1))

    return results_html, total


def parse_publication_list(html: str) -> List[dict]:
    """Parse publication entries from the News API Results HTML."""
    soup = BeautifulSoup(html, "html.parser")
    entries = []

    for col in soup.select("div.col3"):
        link = col.find("a", class_="release", href=True)
        if not link:
            continue

        href = link.get("href", "")
        if not href:
            continue

        url = href if href.startswith("http") else urljoin(BASE_URL, href)

        # Get full title from list view h3
        title_elem = link.select_one("h3.list")
        if not title_elem:
            title_elem = link.find("h3")
        title = title_elem.get_text(strip=True) if title_elem else ""

        # Extract date (prefer datetime attr for ISO format)
        time_elem = link.find("time")
        date_str = None
        if time_elem:
            date_str = time_elem.get("datetime") or time_elem.get_text(strip=True)

        # Extract publication type from tag
        tag_elem = link.select_one("div.release-tag")
        tag_text = tag_elem.get_text(strip=True).lower() if tag_elem else ""

        pub_type = "other"
        if "policy statement" in tag_text:
            pub_type = "policy_statement"
        elif "supervisory statement" in tag_text:
            pub_type = "supervisory_statement"
        elif "consultation paper" in tag_text:
            pub_type = "consultation_paper"
        elif "letter" in tag_text:
            pub_type = "letter"

        entries.append({
            "url": url,
            "title": title,
            "date_str": date_str,
            "publication_type": pub_type,
        })

    return entries


def parse_date(date_str: Optional[str]) -> Optional[str]:
    """Parse a date string into ISO format."""
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in ["%d %B %Y", "%d %b %Y", "%B %Y", "%d/%m/%Y", "%Y-%m-%d"]:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def fetch_full_text(url: str) -> Tuple[str, str, Optional[str]]:
    """
    Fetch the full text content of a PRA publication page.

    Returns:
        Tuple of (text, title, date_str)
    """
    headers = {k: v for k, v in HEADERS.items() if k != "X-Requested-With"}
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    # Extract title
    title = ""
    title_elem = soup.find("h1")
    if title_elem:
        title = title_elem.get_text(strip=True)

    # Extract date from page
    date_str = None
    date_elem = soup.find("span", class_=re.compile(r"published-date|date", re.I))
    if date_elem:
        date_str = date_elem.get_text(strip=True)
    if not date_str:
        # Try meta tags
        meta_date = soup.find("meta", attrs={"name": re.compile(r"date|DC\.date", re.I)})
        if meta_date:
            date_str = meta_date.get("content", "")

    # Extract main content from the page
    text_parts = []

    # Primary content areas
    content_selectors = [
        "div.page-content",
        "div.content-block",
        "article",
        "div[class*='body']",
        "main",
        "div.publication-content",
        "div#content",
    ]

    content_area = None
    for selector in content_selectors:
        content_area = soup.select_one(selector)
        if content_area:
            # Check it has substantial text
            test_text = content_area.get_text(strip=True)
            if len(test_text) > 200:
                break
            content_area = None

    if content_area:
        # Remove nav, header, footer, sidebar elements
        for unwanted in content_area.select("nav, header, footer, aside, script, style, .cookie-banner, .breadcrumb, .pagination, .related-links, .share-links"):
            unwanted.decompose()

        # Process sections
        for elem in content_area.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "li", "td", "blockquote", "pre"]):
            text = elem.get_text(separator=" ", strip=True)
            if text:
                if elem.name.startswith("h"):
                    text_parts.append(f"\n## {text}\n")
                elif elem.name == "li":
                    text_parts.append(f"- {text}")
                else:
                    text_parts.append(text)

    full_text = "\n".join(text_parts).strip()

    # Clean up excessive whitespace
    full_text = re.sub(r"\n{3,}", "\n\n", full_text)
    full_text = re.sub(r" {2,}", " ", full_text)

    return full_text, title, date_str


def discover_all_publications(max_pages: int = 50) -> List[dict]:
    """Discover all PRA publications by paginating the News API."""
    all_entries = []
    seen_urls = set()

    for page in range(1, max_pages + 1):
        print(f"  Fetching page {page}...")
        try:
            html, total = fetch_publication_list(page=page)
        except requests.RequestException as e:
            print(f"  Error on page {page}: {e}")
            break

        if page == 1:
            print(f"  Total publications available: {total}")

        entries = parse_publication_list(html)
        if not entries:
            print(f"  No more entries on page {page}, stopping.")
            break

        new_count = 0
        for entry in entries:
            if entry["url"] not in seen_urls:
                seen_urls.add(entry["url"])
                all_entries.append(entry)
                new_count += 1

        print(f"  Page {page}: {new_count} new entries (total: {len(all_entries)})")
        time.sleep(1.0)

    return all_entries


def generate_doc_id(url: str) -> str:
    """Generate a document ID from a URL."""
    # Extract path after domain
    path = url.replace(BASE_URL, "").strip("/")
    # Clean and shorten
    doc_id = re.sub(r"[^a-zA-Z0-9/_-]", "_", path)
    doc_id = re.sub(r"_+", "_", doc_id)
    return doc_id[:200]


def fetch_all(max_records: int = None) -> Generator[dict, None, None]:
    """
    Fetch all PRA publications with full text.

    Yields:
        Normalized publication records
    """
    print("Discovering PRA publications...")
    # For limited fetches, only discover enough pages
    max_pages = 50
    if max_records:
        max_pages = (max_records // 30) + 2  # A few extra pages for safety
    entries = discover_all_publications(max_pages=max_pages)
    print(f"Found {len(entries)} publications")

    total_yielded = 0
    errors = 0

    for entry in entries:
        if max_records and total_yielded >= max_records:
            return

        print(f"\n[{total_yielded + 1}] Fetching: {entry['url'].split('/')[-1]}")

        try:
            text, page_title, page_date = fetch_full_text(entry["url"])
        except requests.RequestException as e:
            print(f"  Error: {e}")
            errors += 1
            time.sleep(2.0)
            continue

        if not text or len(text) < 100:
            print(f"  Skipping: insufficient text ({len(text) if text else 0} chars)")
            continue

        title = page_title or entry["title"]
        date = parse_date(page_date) or parse_date(entry.get("date_str"))

        record = {
            "_id": generate_doc_id(entry["url"]),
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": entry["url"],
            "publication_type": entry.get("publication_type", "other"),
            "language": "en",
        }

        yield record
        total_yielded += 1

        if total_yielded % 10 == 0:
            print(f"  Progress: {total_yielded} records ({errors} errors)")

        time.sleep(1.5)

    print(f"\nCompleted: {total_yielded} records ({errors} errors)")


def normalize(raw: dict) -> dict:
    """Validate and normalize a record."""
    required = ["_id", "_source", "_type", "_fetched_at", "title", "text", "url"]
    for field in required:
        if field not in raw:
            raise ValueError(f"Missing required field: {field}")
    if not raw.get("text") or len(raw["text"]) < 100:
        raise ValueError(f"Insufficient text content ({len(raw.get('text', ''))} chars)")
    return raw


def bootstrap_sample(sample_count: int = 15):
    """Fetch sample records for validation."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample PRA publications...")
    print("=" * 60)

    count = 0
    for record in fetch_all(max_records=sample_count):
        try:
            record = normalize(record)
        except ValueError as e:
            print(f"  Validation error: {e}")
            continue

        filename = SAMPLE_DIR / f"{count:03d}_{record['_id'][:60].replace('/', '_')}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(record, f, indent=2, ensure_ascii=False)

        text_len = len(record.get("text", ""))
        print(f"  Saved: {filename.name} ({text_len} chars)")
        count += 1

    print(f"\n{'=' * 60}")
    print(f"Sample complete: {count} records saved to {SAMPLE_DIR}/")

    # Validate
    if count < 10:
        print(f"WARNING: Only {count} records - need at least 10!")
        return False

    # Check text field
    for f in SAMPLE_DIR.glob("*.json"):
        with open(f) as fh:
            rec = json.load(fh)
        if not rec.get("text") or len(rec["text"]) < 100:
            print(f"WARNING: {f.name} has insufficient text!")
            return False

    print("All validation checks passed!")
    return True


def test_connectivity():
    """Test API connectivity."""
    print("Testing BoE News API...")
    try:
        html, total = fetch_publication_list(page=1, page_size=5)
        entries = parse_publication_list(html)
        print(f"  News API: OK ({len(entries)} entries on page 1, {total} total)")
    except Exception as e:
        print(f"  News API: FAILED - {e}")
        return False

    if entries:
        print(f"\nTesting full text fetch: {entries[0]['url']}")
        try:
            text, title, date = fetch_full_text(entries[0]["url"])
            print(f"  Full text: OK ({len(text)} chars)")
            print(f"  Title: {title[:80]}")
            print(f"  Date: {date}")
            if len(text) > 200:
                print(f"  Preview: {text[:200]}...")
        except Exception as e:
            print(f"  Full text: FAILED - {e}")
            return False

    print("\nAll connectivity tests passed!")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UK/PRA data fetcher")
    parser.add_argument("command", choices=["bootstrap", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command == "test":
        success = test_connectivity()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        if args.sample:
            success = bootstrap_sample()
            sys.exit(0 if success else 1)
        else:
            for record in fetch_all():
                pass
