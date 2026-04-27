#!/usr/bin/env python3
"""
DE/Bayern - Bavaria State Law (BAYERN.RECHT)

Fetches court decisions from all Bavarian courts via paginated search
and RSS feed for updates.

Coverage:
- 24K+ court decisions across 7 judicial branches
- Verfassungsgerichtsbarkeit (154), Ordentliche Gerichtsbarkeit (7770),
  Verwaltungsgerichtsbarkeit (14009), Finanzgerichtsbarkeit (384),
  Arbeitsgerichtsbarkeit (577), Sozialgerichtsbarkeit (1082), etc.

Data source: https://www.gesetze-bayern.de
Search pagination: /Search/Filter/DOKTYP/rspr then /Search/Page/{n}
RSS Feed: https://www.gesetze-bayern.de/Api/Feed (for updates only)
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
from typing import Iterator, Optional, Dict, Any, List
from html import unescape

import requests
from bs4 import BeautifulSoup

# Configuration
BASE_URL = "https://www.gesetze-bayern.de"
RSS_FEED_URL = f"{BASE_URL}/Api/Feed"
SEARCH_FILTER_URL = f"{BASE_URL}/Search/Filter/DOKTYP/rspr"
SEARCH_PAGE_URL = f"{BASE_URL}/Search/Page"
RATE_LIMIT_DELAY = 1.0  # seconds between requests
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "DE/Bayern"
DOCS_PER_PAGE = 10  # Search results show 10 items per page


def clean_html(html_content: str) -> str:
    """Strip HTML tags and clean up text content."""
    if not html_content:
        return ""

    # Parse with BeautifulSoup
    soup = BeautifulSoup(html_content, "html.parser")

    # Get text content preserving some structure
    text = soup.get_text(separator="\n")

    # Clean up whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' +', ' ', text)
    text = text.strip()

    # Unescape HTML entities
    text = unescape(text)

    return text


def fetch_page(url: str, retries: int = 3) -> Optional[str]:
    """Fetch a webpage with rate limiting and retries."""
    for attempt in range(retries):
        try:
            time.sleep(RATE_LIMIT_DELAY)

            response = requests.get(
                url,
                headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
                    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
                    "User-Agent": "LegalDataHunter/1.0 (research; contact@example.com)"
                },
                timeout=30
            )

            if response.status_code == 200:
                return response.text
            elif response.status_code == 429:
                wait_time = 10 * (attempt + 1)
                print(f"Rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
            elif response.status_code >= 500:
                print(f"Server error {response.status_code}, retrying...")
                time.sleep(5 * (attempt + 1))
            else:
                print(f"Error: {response.status_code} for {url}")
                return None

        except requests.exceptions.Timeout:
            print(f"Timeout for {url}, attempt {attempt + 1}/{retries}")
            time.sleep(5)
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            time.sleep(5)

    return None


def fetch_rss_feed() -> List[Dict]:
    """Fetch the RSS feed to get list of recent decisions."""
    try:
        time.sleep(RATE_LIMIT_DELAY)
        response = requests.get(
            RSS_FEED_URL,
            headers={
                "Accept": "application/xml,application/rss+xml,text/xml",
                "User-Agent": "LegalDataHunter/1.0 (research; contact@example.com)"
            },
            timeout=30
        )

        if response.status_code != 200:
            print(f"RSS feed error: {response.status_code}")
            return []

        # Parse RSS XML
        root = ET.fromstring(response.content)
        items = []

        for item in root.findall(".//item"):
            guid = item.find("guid")
            link = item.find("link")
            title = item.find("title")
            description = item.find("description")
            pub_date = item.find("pubDate")

            if guid is not None and link is not None:
                items.append({
                    "guid": guid.text,
                    "link": link.text,
                    "title": title.text if title is not None else "",
                    "description": description.text if description is not None else "",
                    "pub_date": pub_date.text if pub_date is not None else ""
                })

        return items

    except Exception as e:
        print(f"RSS feed error: {e}")
        return []


def create_session() -> requests.Session:
    """Create a requests session with proper headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    })
    return session


def fetch_search_page(session: requests.Session, page_num: int) -> List[Dict]:
    """Fetch a single page of search results.

    Returns list of dicts with keys: guid, link, title, subtitle
    """
    time.sleep(RATE_LIMIT_DELAY)

    try:
        url = f"{SEARCH_PAGE_URL}/{page_num}"
        response = session.get(url, timeout=30)

        if response.status_code != 200:
            print(f"Search page {page_num} error: {response.status_code}")
            return []

        soup = BeautifulSoup(response.content, "html.parser")
        hitlist = soup.find(id="hitlist")

        if not hitlist:
            return []

        items = []
        for li in hitlist.find_all("li", class_="hitlistItem"):
            # Extract link and title
            title_link = li.find("a", href=lambda h: h and "/Content/Document/" in h)
            if not title_link:
                continue

            href = title_link.get("href", "")
            # Remove ?hl=true parameter
            href = href.split("?")[0]

            # Extract GUID from URL (e.g., Y-300-Z-BECKRS-B-2026-N-2964)
            guid_match = re.search(r"/Content/Document/([^/?]+)", href)
            guid = guid_match.group(1) if guid_match else ""

            # Get title text
            title_text = title_link.get_text(strip=True)

            # Get subtitle (date and file number)
            subtitle_el = li.find("p", class_="hlSubTitel")
            subtitle = subtitle_el.get_text(strip=True) if subtitle_el else ""

            if guid:
                items.append({
                    "guid": guid,
                    "link": f"{BASE_URL}{href}",
                    "title": title_text,
                    "subtitle": subtitle,
                    "pub_date": ""  # Not available in search results
                })

        return items

    except requests.exceptions.RequestException as e:
        print(f"Search page {page_num} error: {e}")
        return []


def init_search_session(session: requests.Session) -> Optional[BeautifulSoup]:
    """Initialize search session by loading the filter page.

    This establishes the session cookie and sets up the search context.
    Returns the parsed soup for extracting total count, or None on failure.
    """
    try:
        time.sleep(RATE_LIMIT_DELAY)
        response = session.get(SEARCH_FILTER_URL, timeout=30)

        if response.status_code != 200:
            print(f"Failed to initialize search session: {response.status_code}")
            return None

        # Verify we got search results
        soup = BeautifulSoup(response.content, "html.parser")
        hitlist = soup.find(id="hitlist")

        if not hitlist:
            print("Warning: No hitlist found on filter page")
            return None

        items = hitlist.find_all("li", class_="hitlistItem")
        print(f"Search session initialized: {len(items)} items on first page")
        return soup

    except requests.exceptions.RequestException as e:
        print(f"Failed to initialize search session: {e}")
        return None


def get_total_count_from_facets(soup: BeautifulSoup) -> int:
    """Extract total document count from facet panel.

    Looks for "Gerichtsentscheidungen(XXXXX)" in the filter panel.
    """
    for fc in soup.find_all(class_="facet-count"):
        parent = fc.parent
        if parent:
            parent_text = parent.get_text(strip=True)
            # Look for "Gerichtsentscheidungen(23992)" pattern
            if "Gerichtsentscheidungen" in parent_text:
                count_text = fc.get_text(strip=True)
                # Remove parentheses and dots: "(23.992)" -> "23992"
                count_text = count_text.strip("()").replace(".", "")
                if count_text.isdigit():
                    return int(count_text)
    return 0


def get_total_pages(session: requests.Session, initial_soup: BeautifulSoup = None) -> int:
    """Get the total number of pages available.

    Parses the total document count from the facet panel on the filter page,
    then calculates total pages (10 docs per page).
    """
    if initial_soup:
        total_count = get_total_count_from_facets(initial_soup)
        if total_count > 0:
            total_pages = (total_count + DOCS_PER_PAGE - 1) // DOCS_PER_PAGE
            print(f"Total documents: {total_count:,}, pages: {total_pages}")
            return total_pages

    # Fallback: fetch filter page if not provided
    try:
        time.sleep(RATE_LIMIT_DELAY)
        response = session.get(SEARCH_FILTER_URL, timeout=30)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            total_count = get_total_count_from_facets(soup)
            if total_count > 0:
                total_pages = (total_count + DOCS_PER_PAGE - 1) // DOCS_PER_PAGE
                print(f"Total documents: {total_count:,}, pages: {total_pages}")
                return total_pages
    except requests.exceptions.RequestException as e:
        print(f"Error getting total pages: {e}")

    # Final fallback: estimate based on known data
    print("Warning: Could not detect total pages, using fallback estimate")
    return 2500


def fetch_all_via_pagination(limit: Optional[int] = None) -> Iterator[Dict]:
    """Fetch all court decisions via search pagination.

    This method:
    1. Initializes a session with the search filter
    2. Iterates through all pages (about 2400 pages, 10 docs each)
    3. Fetches full content for each document
    """
    session = create_session()

    # Initialize search context
    initial_soup = init_search_session(session)
    if not initial_soup:
        print("Failed to initialize search session")
        return

    # Get total pages from the facet counts
    total_pages = get_total_pages(session, initial_soup)
    print(f"Total pages to fetch: ~{total_pages}")

    count = 0
    empty_pages = 0

    for page in range(1, total_pages + 1):
        items = fetch_search_page(session, page)

        if not items:
            empty_pages += 1
            if empty_pages >= 3:
                print(f"Multiple empty pages, stopping at page {page}")
                break
            continue
        else:
            empty_pages = 0

        if page % 50 == 0:
            print(f"Processing page {page}/{total_pages}...")

        for item in items:
            record = fetch_decision(item)
            if record and record.get("text"):
                yield record
                count += 1

                if limit and count >= limit:
                    print(f"Reached limit of {limit} records")
                    return

    print(f"Fetched {count} records from {page} pages")


def extract_decision_content(html: str) -> Dict:
    """Extract structured content from a court decision page."""
    soup = BeautifulSoup(html, "html.parser")

    # Extract metadata from rsprbox
    metadata = {}
    rsprbox = soup.find("div", class_="rsprbox")
    if rsprbox:
        for ueber in rsprbox.find_all("div", class_="rsprboxueber"):
            label = ueber.get_text(strip=True).rstrip(":")
            zeile = ueber.find_next_sibling("div", class_="rsprboxzeile")
            if zeile:
                metadata[label] = zeile.get_text(strip=True)
            # Check for h1 (title)
            h1 = ueber.find_next_sibling("h1", class_="titelzeile")
            if h1:
                metadata["Titel"] = h1.get_text(strip=True)

    # Extract main content
    content_parts = []

    # Get tenor
    tenor_header = soup.find("h2", class_="entsueber", string=re.compile("Tenor", re.I))
    if tenor_header:
        content_parts.append("TENOR:")
        tenor_blocks = []
        for sibling in tenor_header.find_next_siblings():
            if sibling.name == "h2":
                break
            if sibling.get("class") and "rdblock" in sibling.get("class", []):
                absatz = sibling.find("div", class_="absatz")
                if absatz:
                    tenor_blocks.append(absatz.get_text(strip=True))
        content_parts.append("\n".join(tenor_blocks))

    # Get reasons (Gründe)
    gruende_header = soup.find("h2", class_="entsueber", string=re.compile("Gründe", re.I))
    if gruende_header:
        content_parts.append("\nGRÜNDE:")
        gruende_blocks = []
        for sibling in gruende_header.find_next_siblings():
            if sibling.name == "h2":
                break
            if sibling.get("class") and "rdblock" in sibling.get("class", []):
                rd = sibling.find("div", class_="rd")
                absatz = sibling.find("div", class_="absatz")
                if absatz:
                    rd_num = rd.get_text(strip=True) if rd else ""
                    text = absatz.get_text(strip=True)
                    if rd_num:
                        gruende_blocks.append(f"[{rd_num}] {text}")
                    else:
                        gruende_blocks.append(text)
        content_parts.append("\n\n".join(gruende_blocks))

    # Combine
    full_text = "\n\n".join(content_parts)

    # If we couldn't extract structured content, try getting all text from cont div
    if not full_text.strip():
        cont_div = soup.find("div", class_="cont")
        if cont_div:
            full_text = clean_html(str(cont_div))

    return {
        "metadata": metadata,
        "text": full_text
    }


def parse_decision_title(title: str) -> Dict:
    """Parse court decision title to extract court, date, and file number."""
    # Format: "VGH München – 19.01.2026, 10 ZB 25.1305 – Ausweisung, ..."
    result = {
        "court": "",
        "date": None,
        "file_number": "",
        "keywords": ""
    }

    parts = title.split(" – ")
    if len(parts) >= 1:
        result["court"] = parts[0].strip()

    if len(parts) >= 2:
        # Second part: "19.01.2026, 10 ZB 25.1305"
        date_file = parts[1].strip()
        date_match = re.match(r"(\d{2}\.\d{2}\.\d{4})", date_file)
        if date_match:
            date_str = date_match.group(1)
            try:
                # Convert DD.MM.YYYY to ISO
                dt = datetime.strptime(date_str, "%d.%m.%Y")
                result["date"] = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass
            # File number comes after the date
            file_part = date_file[len(date_str):].strip(" ,")
            result["file_number"] = file_part

    if len(parts) >= 3:
        result["keywords"] = parts[2].strip()

    return result


def normalize_decision(item: Dict, content: Dict) -> Dict:
    """Normalize court decision to standard schema."""
    parsed = parse_decision_title(item.get("title", ""))
    metadata = content.get("metadata", {})

    # Build full text
    text = content.get("text", "")

    normalized = {
        "_id": f"BY-{item.get('guid', '')}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),

        # Required fields
        "title": metadata.get("Titel", item.get("title", "")),
        "text": text,
        "date": parsed.get("date"),
        "url": item.get("link", ""),

        # Court information
        "court_name": parsed.get("court", ""),
        "file_number": parsed.get("file_number") or metadata.get("Aktenzeichen", ""),
        "case_type": None,  # Would need to extract from metadata
        "jurisdiction": "Bayern",

        # Additional metadata
        "norm_chain": metadata.get("Normenkette", ""),
        "keywords": parsed.get("keywords") or metadata.get("Schlagworte", ""),
        "prior_instance": metadata.get("Vorinstanz", ""),
        "citation": metadata.get("Fundstelle", ""),

        # Publication date
        "pub_date": item.get("pub_date", ""),

        # Original ID
        "guid": item.get("guid", ""),
    }

    return normalized


def fetch_decision(item: Dict) -> Optional[Dict]:
    """Fetch and normalize a single court decision."""
    url = item.get("link")
    if not url:
        return None

    print(f"Fetching: {item.get('guid', 'unknown')}")
    html = fetch_page(url)
    if not html:
        return None

    content = extract_decision_content(html)
    return normalize_decision(item, content)


def fetch_all(limit: Optional[int] = None) -> Iterator[Dict]:
    """Fetch all court decisions via search pagination.

    For full bootstrap, uses paginated search (~24K documents).
    """
    yield from fetch_all_via_pagination(limit=limit)


def fetch_updates(limit: int = 100) -> Iterator[Dict]:
    """Fetch recent updates via RSS feed.

    RSS feed contains the most recent ~35 decisions,
    suitable for incremental updates.
    """
    items = fetch_rss_feed()
    print(f"Found {len(items)} items in RSS feed")

    count = 0
    for item in items:
        record = fetch_decision(item)
        if record and record.get("text"):
            yield record
            count += 1
            if count >= limit:
                return


def fetch_sample(count: int = 15) -> List[Dict]:
    """Fetch sample records for validation.

    Uses paginated search to get diverse samples from multiple pages.
    """
    samples = []
    session = create_session()

    # Initialize search context
    initial_soup = init_search_session(session)
    if not initial_soup:
        print("Failed to initialize search session, falling back to RSS")
        items = fetch_rss_feed()
        for item in items[:count + 5]:
            record = fetch_decision(item)
            if record and record.get("text") and len(record.get("text", "")) > 200:
                samples.append(record)
                print(f"  Sample {len(samples)}: {len(record.get('text', '')):,} chars")
                if len(samples) >= count:
                    break
        return samples

    # Get samples from different pages to ensure diversity
    pages_to_sample = [1, 100, 500, 1000, 1500, 2000]
    items_per_page = (count // len(pages_to_sample)) + 1

    for page in pages_to_sample:
        if len(samples) >= count:
            break

        print(f"Sampling page {page}...")
        items = fetch_search_page(session, page)

        for item in items[:items_per_page]:
            if len(samples) >= count:
                break

            record = fetch_decision(item)
            if record and record.get("text") and len(record.get("text", "")) > 200:
                samples.append(record)
                print(f"  Sample {len(samples)}: {len(record.get('text', '')):,} chars - {record.get('court_name', 'Unknown')}")

    return samples


def save_samples(samples: List[Dict]) -> None:
    """Save sample records to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    # Save individual records
    for i, record in enumerate(samples):
        filepath = SAMPLE_DIR / f"record_{i:04d}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, indent=2, ensure_ascii=False)

    # Save all samples in one file
    all_samples_path = SAMPLE_DIR / "all_samples.json"
    with open(all_samples_path, "w", encoding="utf-8") as f:
        json.dump(samples, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(samples)} samples to {SAMPLE_DIR}")


def validate_samples(samples: List[Dict]) -> bool:
    """Validate sample records meet requirements."""
    print("\n=== Sample Validation ===")

    issues = []

    # Check count
    if len(samples) < 10:
        issues.append(f"Only {len(samples)} samples, need at least 10")

    # Check required fields
    text_lengths = []
    for i, record in enumerate(samples):
        text = record.get("text", "")
        if not text:
            issues.append(f"Record {i}: missing 'text' field")
        elif len(text) < 200:
            issues.append(f"Record {i}: text too short ({len(text)} chars)")
        else:
            text_lengths.append(len(text))

        if not record.get("_id"):
            issues.append(f"Record {i}: missing '_id'")
        if not record.get("title"):
            issues.append(f"Record {i}: missing 'title'")

    # Report
    if text_lengths:
        avg_len = sum(text_lengths) / len(text_lengths)
        print(f"Records with text: {len(text_lengths)}/{len(samples)}")
        print(f"Average text length: {avg_len:,.0f} chars")
        print(f"Min text length: {min(text_lengths):,} chars")
        print(f"Max text length: {max(text_lengths):,} chars")

    # Check courts covered
    courts = set(r.get("court_name") for r in samples if r.get("court_name"))
    print(f"Courts covered: {len(courts)}")
    for court in sorted(courts)[:5]:
        print(f"  - {court}")

    if issues:
        print(f"\nIssues found ({len(issues)}):")
        for issue in issues[:10]:
            print(f"  - {issue}")
        return False

    print("\n✓ All validation checks passed")
    return True


def main():
    parser = argparse.ArgumentParser(description="DE/Bayern data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "status"],
        help="Command to run"
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Fetch sample records only (for bootstrap)"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=15,
        help="Number of sample records to fetch"
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample:
            print("Fetching sample records...")
            samples = fetch_sample(args.count)
            save_samples(samples)

            if validate_samples(samples):
                print("\n✓ Bootstrap sample complete")
                return 0
            else:
                print("\n✗ Validation failed")
                return 1
        else:
            print("Full bootstrap - fetching all decisions...")
            count = 0
            for record in fetch_all():
                count += 1
                if count % 10 == 0:
                    print(f"Fetched {count} records...")
            print(f"Total: {count} records")

    elif args.command == "update":
        print("Fetching recent updates from RSS feed...")
        count = 0
        for record in fetch_updates(limit=100):
            count += 1
        print(f"Fetched {count} recent decisions")

    elif args.command == "status":
        print("Checking sources...")

        # Check RSS feed
        items = fetch_rss_feed()
        print(f"\nDE/Bayern Status:")
        print(f"  RSS feed items: {len(items)}")

        # Check pagination
        session = create_session()
        initial_soup = init_search_session(session)
        if initial_soup:
            total_pages = get_total_pages(session, initial_soup)
            total_count = get_total_count_from_facets(initial_soup)
            print(f"  Total pages: ~{total_pages}")
            print(f"  Total documents (from facets): {total_count:,}")

        if SAMPLE_DIR.exists():
            sample_files = list(SAMPLE_DIR.glob("record_*.json"))
            print(f"  Sample files: {len(sample_files)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
