#!/usr/bin/env python3
"""
NL/AP - Dutch Data Protection Authority (Autoriteit Persoonsgegevens)

Fetches GDPR enforcement decisions and policy documents from the Dutch DPA.
The AP is the supervisory authority for data protection in the Netherlands
and publishes fines, enforcement decisions, and guidelines.

Data sources:
- Document listing: https://www.autoriteitpersoonsgegevens.nl/documenten
- Sanctions page: https://www.autoriteitpersoonsgegevens.nl/boetes-en-andere-sancties
- Documents include PDFs with full decision text

License: Public domain (government decisions)

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py bootstrap            # Full fetch (all documents)
    python bootstrap.py test-api             # Test API connectivity
"""

import argparse
import io
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

from bs4 import BeautifulSoup

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


# Constants
SOURCE_ID = "NL/AP"
BASE_URL = "https://www.autoriteitpersoonsgegevens.nl"
DOCUMENTS_URL = f"{BASE_URL}/documenten"
SANCTIONS_URL = f"{BASE_URL}/boetes-en-andere-sancties"
RSS_FEED = f"{BASE_URL}/feed/publication/rss.xml"

RATE_LIMIT_DELAY = 2.0
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"

# Paths
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
DATA_DIR = SCRIPT_DIR / "data"


def curl_fetch(url: str, binary: bool = False):
    """Fetch URL using curl subprocess (workaround for SSL issues)."""
    try:
        cmd = [
            "curl", "-s", "-L",
            "-H", f"User-Agent: {USER_AGENT}",
            "-H", "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "-H", "Accept-Language: nl-NL,nl;q=0.9,en;q=0.8",
            "--max-time", "60",
            url
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=90)
        if result.returncode != 0:
            print(f"curl error for {url}: {result.stderr.decode()}", file=sys.stderr)
            return None
        if binary:
            return result.stdout
        return result.stdout.decode("utf-8", errors="replace")
    except subprocess.TimeoutExpired:
        print(f"Timeout fetching {url}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        return None


def extract_text_from_pdf(pdf_content: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="NL/AP",
        source_id="",
        pdf_bytes=pdf_content,
        table="case_law",
    ) or ""

def parse_dutch_date(date_str: str) -> str:
    """Parse Dutch date format (e.g., '26 augustus 2024') to ISO format."""
    dutch_months = {
        'januari': '01', 'februari': '02', 'maart': '03', 'april': '04',
        'mei': '05', 'juni': '06', 'juli': '07', 'augustus': '08',
        'september': '09', 'oktober': '10', 'november': '11', 'december': '12'
    }

    date_str = date_str.strip().lower()

    # Try to parse Dutch date format: "26 augustus 2024"
    for dutch_month, month_num in dutch_months.items():
        if dutch_month in date_str:
            parts = date_str.replace(dutch_month, month_num).split()
            if len(parts) >= 3:
                try:
                    day = parts[0].zfill(2)
                    month = parts[1].zfill(2)
                    year = parts[2]
                    return f"{year}-{month}-{day}"
                except (ValueError, IndexError):
                    pass

    return date_str


def fetch_document_list(page: int = 0) -> list[dict]:
    """Fetch document list from a single page."""
    documents = []

    url = f"{DOCUMENTS_URL}?page={page}"
    print(f"Fetching page {page}...", file=sys.stderr)

    html = curl_fetch(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")

    # Find all document links in the listing
    # Documents are in links with /documenten/ path
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        if href.startswith("/documenten/") and href != "/documenten":
            # Avoid duplicates
            doc_url = BASE_URL + href

            # Get title from link text
            title = link.get_text(strip=True)
            if not title:
                continue

            documents.append({
                "url": doc_url,
                "title": title,
            })

    # Remove duplicates (links often appear twice)
    seen = set()
    unique_docs = []
    for doc in documents:
        if doc["url"] not in seen:
            seen.add(doc["url"])
            unique_docs.append(doc)

    return unique_docs


def fetch_document_details(doc_url: str) -> Optional[dict]:
    """Fetch full details and PDF from a document page."""

    html = curl_fetch(doc_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    result = {
        "url": doc_url,
        "title": "",
        "date": "",
        "description": "",
        "topics": [],
        "pdf_url": "",
        "text": "",
    }

    # Extract title
    title_elem = soup.find("h1", class_="node-publication-full__title")
    if title_elem:
        result["title"] = title_elem.get_text(strip=True)
    else:
        # Fallback to page title
        title_tag = soup.find("title")
        if title_tag:
            result["title"] = title_tag.get_text(strip=True).replace(" | Autoriteit Persoonsgegevens", "")

    # Extract date (in meta-submitted div)
    date_elem = soup.find("div", class_="node-publication-full__meta-submitted")
    if date_elem:
        result["date"] = parse_dutch_date(date_elem.get_text(strip=True))

    # Extract description from meta tag
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        result["description"] = meta_desc["content"]

    # Extract topics
    topics_container = soup.find("div", class_="node-publication-full__topics")
    if topics_container:
        for topic_item in topics_container.find_all("div", class_="node-publication-full__topics-item"):
            topic_text = topic_item.get_text(strip=True)
            if topic_text:
                result["topics"].append(topic_text)

    # Extract intro text
    intro_elem = soup.find("div", class_="node-publication-full__intro")
    if intro_elem:
        intro_text = intro_elem.get_text(strip=True)
        if intro_text:
            result["description"] = intro_text

    # Extract body text from primary content
    body_parts = []
    primary_content = soup.find("div", class_="node-publication-full__primary-content")
    if primary_content:
        for text_div in primary_content.find_all("div", class_="text-basic-html"):
            text = text_div.get_text(strip=True)
            if text:
                body_parts.append(text)

    # Find PDF download link
    downloads_div = soup.find("div", class_="node-publication-full__downloads")
    if downloads_div:
        pdf_link = downloads_div.find("a", href=True)
        if pdf_link:
            pdf_href = pdf_link.get("href", "")
            if pdf_href:
                if pdf_href.startswith("/"):
                    result["pdf_url"] = BASE_URL + pdf_href
                else:
                    result["pdf_url"] = pdf_href

    # If we have a PDF, download and extract text
    if result["pdf_url"]:
        print(f"  Downloading PDF: {result['pdf_url']}", file=sys.stderr)
        time.sleep(RATE_LIMIT_DELAY)

        pdf_content = curl_fetch(result["pdf_url"], binary=True)
        if pdf_content:
            pdf_text = extract_text_from_pdf(pdf_content)
            if pdf_text:
                result["text"] = pdf_text
                print(f"  Extracted {len(pdf_text)} chars from PDF", file=sys.stderr)

    # If no PDF text, use the body text
    if not result["text"] and body_parts:
        result["text"] = "\n\n".join(body_parts)

    return result


def normalize(raw: dict) -> dict:
    """Transform raw AP data into standard schema."""

    # Generate a unique ID from the URL
    url = raw.get("url", "")
    doc_id = url.split("/")[-1] if url else ""

    # Determine document type from URL or title
    doc_type = "decision"  # default
    title_lower = raw.get("title", "").lower()
    if "boete" in title_lower:
        doc_type = "fine"
    elif "woo-besluit" in title_lower:
        doc_type = "foi_decision"
    elif "toets" in title_lower:
        doc_type = "advice"
    elif "infographic" in title_lower:
        doc_type = "guidance"

    return {
        # Required base fields
        "_id": f"NL-AP-{doc_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        # Standard fields
        "title": raw.get("title", ""),
        "text": raw.get("text", ""),
        "date": raw.get("date", ""),
        "url": url,
        # Source-specific
        "document_type": doc_type,
        "topics": raw.get("topics", []),
        "summary": raw.get("description", ""),
        "pdf_url": raw.get("pdf_url", ""),
    }


def fetch_all() -> Generator[dict, None, None]:
    """Fetch all documents from AP website."""

    seen_urls = set()
    page = 0
    max_pages = 200
    consecutive_empty = 0

    while page < max_pages:
        docs = fetch_document_list(page)

        if not docs:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                print(f"No more documents found after page {page}", file=sys.stderr)
                break
            page += 1
            time.sleep(RATE_LIMIT_DELAY)
            continue

        consecutive_empty = 0

        for doc in docs:
            if doc["url"] in seen_urls:
                continue
            seen_urls.add(doc["url"])

            time.sleep(RATE_LIMIT_DELAY)

            details = fetch_document_details(doc["url"])
            if details and details.get("text"):
                yield details

        page += 1
        time.sleep(RATE_LIMIT_DELAY)


def run_sample(n: int = 15) -> dict:
    """Fetch sample records and save to sample directory."""
    SAMPLE_DIR.mkdir(exist_ok=True)

    count = 0
    total_chars = 0

    for raw in fetch_all():
        if count >= n:
            break

        normalized = normalize(raw)
        text_len = len(normalized.get("text", ""))

        # Only save records with substantial text
        if text_len < 100:
            continue

        # Save to file
        doc_id = normalized["_id"]
        safe_id = re.sub(r'[^\w\-]', '_', doc_id)
        filename = f"{safe_id}.json"
        filepath = SAMPLE_DIR / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(normalized, f, indent=2, ensure_ascii=False)

        print(f"[{count+1}/{n}] Saved: {normalized['title'][:50]}... ({text_len} chars)")
        count += 1
        total_chars += text_len

    return {
        "sample_records_saved": count,
        "total_chars": total_chars,
        "avg_chars_per_doc": total_chars // max(count, 1),
        "sample_dir": str(SAMPLE_DIR),
    }


def test_api():
    """Test API connectivity."""
    print("Testing NL/AP connectivity...")

    # Test document listing
    html = curl_fetch(DOCUMENTS_URL)
    if html:
        print(f"  Document listing: OK ({len(html)} bytes)")
    else:
        print(f"  Document listing: FAILED")
        return

    # Test fetching first page documents
    docs = fetch_document_list(0)
    print(f"  Found {len(docs)} documents on first page")

    if docs:
        # Test fetching a single document
        test_doc = docs[0]
        print(f"  Testing document fetch: {test_doc['title'][:50]}...")

        time.sleep(RATE_LIMIT_DELAY)
        details = fetch_document_details(test_doc["url"])

        if details:
            print(f"  Document fetch: OK")
            print(f"    Title: {details.get('title', 'N/A')}")
            print(f"    Date: {details.get('date', 'N/A')}")
            print(f"    PDF URL: {details.get('pdf_url', 'N/A')}")
            text_len = len(details.get("text", ""))
            print(f"    Text length: {text_len} chars")
            if text_len > 0:
                print(f"    Text preview: {details['text'][:200]}...")
        else:
            print(f"  Document fetch: FAILED")

    print("\nAPI test complete!")


def main():
    parser = argparse.ArgumentParser(description="NL/AP Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"],
                       help="Command to run")
    parser.add_argument("--sample", action="store_true",
                       help="Fetch only sample records")
    parser.add_argument("--sample-size", type=int, default=15,
                       help="Number of sample records to fetch")

    args = parser.parse_args()

    if args.command == "test-api":
        test_api()

    elif args.command == "bootstrap":
        if args.sample:
            stats = run_sample(n=args.sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
            print(json.dumps(stats, indent=2))
        else:
            # Full bootstrap
            DATA_DIR.mkdir(exist_ok=True)

            count = 0
            for raw in fetch_all():
                normalized = normalize(raw)
                # In full mode, would save to database
                count += 1
                if count % 50 == 0:
                    print(f"Processed {count} records...")

            print(f"\nBootstrap complete: {count} records processed")


if __name__ == "__main__":
    main()
