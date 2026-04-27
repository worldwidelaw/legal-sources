#!/usr/bin/env python3
"""
VG/FSC-Enforcement - BVI Financial Services Commission Enforcement Actions

Fetches enforcement actions from the BVI FSC Drupal 7 site. Content includes
administrative penalties, licence revocations, directives, and public alerts.

Data source: https://www.bvifsc.vg/library/alerts/enforcement-actions
Format: Drupal 7 Views (HTML listing) + individual node pages + PDF attachments
License: Public government data
Rate limit: 1 req/sec (courtesy)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap --full      # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

# Add project root to path for common imports
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

BASE_URL = "https://www.bvifsc.vg"
LISTING_URL = f"{BASE_URL}/library/alerts/enforcement-actions"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "VG/FSC-Enforcement"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research; github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml",
}


def fetch_listing_page(page: int) -> Optional[str]:
    """Fetch a listing page of enforcement actions."""
    url = f"{LISTING_URL}?page={page}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as e:
        print(f"  Error fetching listing page {page}: {e}")
        return None


def parse_listing_page(html: str) -> list:
    """
    Parse enforcement actions from a listing page HTML table.

    Returns list of dicts with: date, action_type, entity, node_url.
    """
    soup = BeautifulSoup(html, "html.parser")
    entries = []

    table = soup.find("table", class_="views-table")
    if not table:
        return entries

    tbody = table.find("tbody")
    if not tbody:
        return entries

    for row in tbody.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) < 3:
            continue

        # Date column - may have datetime in content attr or span
        date_cell = cols[0]
        date_str = None
        date_span = date_cell.find("span", class_="date-display-single")
        if date_span:
            content = date_span.get("content", "")
            if content:
                date_str = content[:10]  # ISO date part
            else:
                date_str = date_span.get_text(strip=True)

        # Action type column
        action_type = cols[1].get_text(strip=True)

        # Entity name column - contains link
        entity_cell = cols[2]
        link = entity_cell.find("a")
        entity = entity_cell.get_text(strip=True)
        node_url = None
        if link and link.get("href"):
            href = link["href"]
            if href.startswith("/"):
                node_url = BASE_URL + href
            elif href.startswith("http"):
                node_url = href

        if node_url:
            entries.append({
                "date": date_str,
                "action_type": action_type,
                "entity": entity,
                "node_url": node_url,
            })

    return entries


def fetch_node_page(url: str) -> Optional[str]:
    """Fetch an individual enforcement action node page."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as e:
        print(f"    Error fetching {url}: {e}")
        return None


def parse_node_page(html: str, url: str) -> dict:
    """
    Parse an enforcement action node page.

    Extracts: summary text, matter ID, PDF URL.
    """
    soup = BeautifulSoup(html, "html.parser")
    result = {"summary": "", "matter_id": None, "pdf_url": None}

    # Title from page
    title_elem = soup.find("h1", class_="page-title") or soup.find("h1")
    if title_elem:
        result["title"] = title_elem.get_text(strip=True)

    # Enforcement summary field
    summary_div = soup.find("div", class_="field-name-field-enforcement-summary")
    if summary_div:
        field_item = summary_div.find("div", class_="field-item")
        if field_item:
            # Get text content, clean HTML
            for br in field_item.find_all("br"):
                br.replace_with("\n")
            result["summary"] = field_item.get_text(separator="\n", strip=True)

    # If no summary field, try body field
    if not result["summary"]:
        body_div = soup.find("div", class_="field-name-body")
        if body_div:
            field_item = body_div.find("div", class_="field-item")
            if field_item:
                for br in field_item.find_all("br"):
                    br.replace_with("\n")
                result["summary"] = field_item.get_text(separator="\n", strip=True)

    # Also try the main content area
    if not result["summary"]:
        content = soup.find("div", class_="node-content") or soup.find("article")
        if content:
            for br in content.find_all("br"):
                br.replace_with("\n")
            text = content.get_text(separator="\n", strip=True)
            # Remove redundant header text
            text = re.sub(r'^.*?(?=\w)', '', text, count=1)
            result["summary"] = text

    # Matter ID
    matter_div = soup.find("div", class_="field-name-field-matter-id")
    if matter_div:
        field_item = matter_div.find("div", class_="field-item")
        if field_item:
            result["matter_id"] = field_item.get_text(strip=True)

    # PDF attachment
    pdf_div = soup.find("div", class_="field-name-field-pdf")
    if pdf_div:
        pdf_link = pdf_div.find("a", href=True)
        if pdf_link:
            href = pdf_link["href"]
            if href.startswith("/"):
                result["pdf_url"] = BASE_URL + href
            elif href.startswith("http"):
                result["pdf_url"] = href

    # Also check for PDF links in general content
    if not result["pdf_url"]:
        for link in soup.find_all("a", href=True):
            if link["href"].lower().endswith(".pdf"):
                href = link["href"]
                if href.startswith("/"):
                    result["pdf_url"] = BASE_URL + href
                elif href.startswith("http"):
                    result["pdf_url"] = href
                break

    return result


def extract_pdf_text(pdf_url: str, doc_id: str) -> Optional[str]:
    """Download and extract text from a PDF."""
    try:
        from common.pdf_extract import extract_pdf_markdown
        text = extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=doc_id,
            pdf_url=pdf_url,
            table="doctrine",
            force=True,
        )
        return text
    except ImportError:
        pass

    # Fallback: try pdfplumber directly
    try:
        import pdfplumber
        resp = requests.get(pdf_url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        import io
        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            pages = [p.extract_text() or "" for p in pdf.pages]
            return "\n\n".join(pages).strip()
    except Exception as e:
        print(f"    PDF extraction failed: {e}")
        return None


def normalize(entry: dict, node_data: dict, pdf_text: Optional[str]) -> Optional[dict]:
    """Build normalized record from listing + node data."""
    # Combine summary and PDF text for full content
    text_parts = []
    if node_data.get("summary"):
        text_parts.append(node_data["summary"])
    if pdf_text:
        text_parts.append(pdf_text)

    text = "\n\n".join(text_parts).strip()

    if not text or len(text) < 20:
        return None

    # Build title
    title = node_data.get("title", entry.get("entity", "Unknown"))
    action = entry.get("action_type", "")
    if action and action not in title:
        title = f"{title} — {action}"

    # Parse date
    date = entry.get("date")
    if date and not re.match(r'\d{4}-\d{2}-\d{2}', date):
        # Try parsing text date
        for fmt in ["%B %d, %Y", "%d %B %Y", "%d/%m/%Y", "%m/%d/%Y"]:
            try:
                date = datetime.strptime(date, fmt).strftime("%Y-%m-%d")
                break
            except ValueError:
                continue

    # Document ID from node URL
    node_url = entry.get("node_url", "")
    doc_id = node_url.rstrip("/").split("/")[-1]

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date,
        "url": node_url,
        "action_type": entry.get("action_type"),
        "matter_id": node_data.get("matter_id"),
        "language": "en",
    }


def get_total_pages() -> int:
    """Determine total listing pages by checking the pager."""
    html = fetch_listing_page(0)
    if not html:
        return 0
    soup = BeautifulSoup(html, "html.parser")
    # Find the "Last" link in the pager - Drupal 7 uses plain <ul> with <a> links
    for link in soup.find_all("a", href=True):
        if link.get_text(strip=True) == "Last":
            match = re.search(r'page=(\d+)', link["href"])
            if match:
                return int(match.group(1))
    return 0


def fetch_all(max_records: int = None, max_pages: int = None,
              fetch_pdfs: bool = True) -> Generator[dict, None, None]:
    """Fetch all enforcement actions."""
    total_pages = get_total_pages()
    if max_pages:
        total_pages = min(total_pages, max_pages)

    print(f"Total listing pages: {total_pages + 1}")
    total_yielded = 0

    for page in range(0, total_pages + 1):
        if max_records and total_yielded >= max_records:
            return

        print(f"\n--- Page {page + 1}/{total_pages + 1} ---")
        html = fetch_listing_page(page)
        if not html:
            continue

        entries = parse_listing_page(html)
        print(f"  Found {len(entries)} entries")

        for entry in entries:
            if max_records and total_yielded >= max_records:
                return

            time.sleep(1)

            node_html = fetch_node_page(entry["node_url"])
            if not node_html:
                continue

            node_data = parse_node_page(node_html, entry["node_url"])

            # Try PDF extraction if available
            pdf_text = None
            if fetch_pdfs and node_data.get("pdf_url"):
                pdf_text = extract_pdf_text(node_data["pdf_url"], entry["node_url"].split("/")[-1])

            record = normalize(entry, node_data, pdf_text)
            if record:
                yield record
                total_yielded += 1
                if total_yielded % 25 == 0:
                    print(f"  Total: {total_yielded} records...")

        time.sleep(1)

    print(f"\nCompleted: {total_yielded} total records")


def bootstrap_sample(sample_count: int = 15):
    """Fetch sample enforcement action records."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample BVI FSC enforcement actions...")
    print("=" * 60)

    records = []
    for record in fetch_all(max_records=sample_count, max_pages=2, fetch_pdfs=True):
        records.append(record)
        idx = len(records)
        filename = SAMPLE_DIR / f"record_{idx:03d}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        text_len = len(record.get("text", ""))
        date = record.get("date", "??")
        print(f"  [{idx:02d}] {date} {record['title'][:55]}... ({text_len:,} chars)")

    print("\n" + "=" * 60)
    print(f"Saved {len(records)} records to {SAMPLE_DIR}")

    if records:
        avg_len = sum(len(r.get("text", "")) for r in records) / len(records)
        print(f"Average text length: {avg_len:,.0f} chars/doc")
        has_pdf = sum(1 for r in records if len(r.get("text", "")) > 500)
        print(f"Records with substantial text (>500 chars): {has_pdf}")

    if len(records) < 10:
        print("WARNING: Fewer than 10 records fetched!")
        return False

    empty = sum(1 for r in records if not r.get("text"))
    if empty > 0:
        print(f"WARNING: {empty} records have empty text!")
        return False

    print(f"\nVALIDATION PASSED: {len(records)} records with text.")
    return True


def bootstrap_full():
    """Full bootstrap."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    print("Starting full bootstrap of BVI FSC enforcement actions...")
    count = 0
    for record in fetch_all():
        count += 1
        filename = SAMPLE_DIR / f"record_{count:04d}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
    print(f"\nFull bootstrap complete: {count} records saved.")
    return count > 0


def test_api():
    """Test site connectivity."""
    print("Testing BVI FSC enforcement page...")

    print("\n1. Testing listing page...")
    html = fetch_listing_page(0)
    if not html:
        print("   FAILED")
        return False
    entries = parse_listing_page(html)
    print(f"   OK: {len(entries)} entries on page 1")

    total_pages = get_total_pages()
    print(f"   Total pages: {total_pages + 1}")

    print("\n2. Testing node page...")
    if entries:
        node_html = fetch_node_page(entries[0]["node_url"])
        if node_html:
            node_data = parse_node_page(node_html, entries[0]["node_url"])
            print(f"   OK: title='{node_data.get('title', 'N/A')[:50]}'")
            print(f"   Summary: {len(node_data.get('summary', ''))} chars")
            print(f"   PDF: {node_data.get('pdf_url', 'None')}")
            print(f"   Matter ID: {node_data.get('matter_id', 'None')}")
        else:
            print("   FAILED")
            return False

    print("\nAll tests passed!")
    return True


def main():
    parser = argparse.ArgumentParser(description="VG/FSC-Enforcement fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--count", type=int, default=15)
    parser.add_argument("--full", action="store_true")

    args = parser.parse_args()

    if args.command == "test":
        sys.exit(0 if test_api() else 1)
    elif args.command == "bootstrap":
        if args.sample:
            sys.exit(0 if bootstrap_sample(args.count) else 1)
        elif args.full:
            sys.exit(0 if bootstrap_full() else 1)
        else:
            print("Use --sample or --full flag.")
            sys.exit(1)
    elif args.command == "update":
        print("Update not implemented.")
        sys.exit(1)


if __name__ == "__main__":
    main()
