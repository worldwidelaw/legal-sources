#!/usr/bin/env python3
"""
FR/ANJ -- French Gaming Authority (Autorité nationale des jeux)

Fetches regulatory decisions from the ANJ website via Drupal Views AJAX.
Full text is extracted from PDF documents using pdfplumber.

Coverage: ~3200 decisions from 2010 to present.
Categories: sanctions, approvals (agréments), deliberations, homologations.

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py bootstrap            # Full fetch
"""

import io
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional
from urllib.parse import urljoin

import pdfplumber
import requests
from bs4 import BeautifulSoup

# Configuration
BASE_URL = "https://www.anj.fr"
DECISIONS_URL = f"{BASE_URL}/decisions"
REQUEST_DELAY = 2.0
SOURCE_ID = "FR/ANJ"

SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"


def get_session() -> requests.Session:
    """Create a requests session."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "WorldWideLaw/1.0 (legal research project)",
    })
    return session


def fetch_decisions_page(session: requests.Session, page: int = 0) -> List[Dict]:
    """
    Fetch a page of decisions via HTML pagination.

    Returns list of dicts with title, date, description, pdf_url, node_id.
    """
    try:
        resp = session.get(
            f"{DECISIONS_URL}?page={page}",
            timeout=30,
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"  Page {page} failed: {e}", file=sys.stderr)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    decisions = []

    for article in soup.find_all("article"):
        decision = {}

        # Title and URL
        title_el = article.find("h2") or article.find("h3")
        if title_el:
            link = title_el.find("a")
            if link:
                decision["title"] = link.get_text(strip=True)
                href = link.get("href", "")
                if href:
                    decision["page_url"] = urljoin(BASE_URL, href)
                    # PDF URLs are directly in the title link
                    if href.lower().endswith(".pdf"):
                        decision["pdf_url"] = urljoin(BASE_URL, href)

        # Date - try sub-title first (DD/MM/YYYY), then title (French month)
        date_el = article.find("div", class_="sub-title")
        if date_el:
            date_text = date_el.get_text(strip=True)
            decision["date_text"] = date_text
            dm = re.search(r"(\d{2})/(\d{2})/(\d{4})", date_text)
            if dm:
                decision["date"] = f"{dm.group(3)}-{dm.group(2)}-{dm.group(1)}"
        if not decision.get("date") and decision.get("title"):
            decision["date"] = parse_french_date(decision["title"])

        # Node ID from article about attribute or data-history-node-id
        about = article.get("about", "")
        node_match = re.search(r"/node/(\d+)", about)
        if node_match:
            decision["node_id"] = node_match.group(1)
        else:
            node_div = article.find(attrs={"data-history-node-id": True})
            if node_div:
                decision["node_id"] = node_div.get("data-history-node-id")

        # Description
        content_el = article.find("div", class_="content")
        if content_el:
            decision["description"] = content_el.get_text(strip=True)[:500]

        if decision.get("title"):
            decisions.append(decision)

    return decisions


def parse_french_date(text: str) -> Optional[str]:
    """Parse French date string to ISO format."""
    months = {
        "janvier": "01", "février": "02", "mars": "03", "avril": "04",
        "mai": "05", "juin": "06", "juillet": "07", "août": "08",
        "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12",
    }
    match = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})", text)
    if match:
        day = match.group(1).zfill(2)
        month_name = match.group(2).lower()
        year = match.group(3)
        month = months.get(month_name, "01")
        return f"{year}-{month}-{day}"
    return None


def extract_pdf_text(session: requests.Session, pdf_url: str) -> str:
    """Download PDF and extract text using pdfplumber."""
    try:
        resp = session.get(pdf_url, timeout=60)
        resp.raise_for_status()

        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            pages_text = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)
            return "\n\n".join(pages_text)
    except Exception as e:
        print(f"  PDF extraction failed for {pdf_url}: {e}", file=sys.stderr)
        return ""


def normalize(raw: Dict, full_text: str) -> Dict:
    """Normalize a decision to standard schema."""
    title = raw.get("title", "")
    node_id = raw.get("node_id", "")

    # Generate unique ID
    if node_id:
        _id = f"FR-ANJ-{node_id}"
    else:
        slug = re.sub(r"[^\w]", "-", title[:50]).strip("-")
        _id = f"FR-ANJ-{slug}"

    return {
        "_id": _id,
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": raw.get("date"),
        "url": raw.get("page_url") or raw.get("pdf_url", ""),
        "pdf_url": raw.get("pdf_url", ""),
        "node_id": node_id,
        "description": raw.get("description", ""),
        "language": "fr",
    }


def fetch_all(session: requests.Session, max_pages: int = 500) -> Iterator[Dict]:
    """Fetch all decisions with full text from PDFs."""
    page = 0
    total = 0

    while page < max_pages:
        print(f"  Fetching page {page}...", file=sys.stderr)
        decisions = fetch_decisions_page(session, page)

        if not decisions:
            print(f"  No more decisions at page {page}", file=sys.stderr)
            break

        for dec in decisions:
            pdf_url = dec.get("pdf_url")
            if not pdf_url:
                # Try fetching the decision page to find PDF link
                continue

            print(f"  [{total+1}] {dec.get('title', '')[:50]}...", file=sys.stderr)
            full_text = extract_pdf_text(session, pdf_url)
            time.sleep(REQUEST_DELAY)

            if len(full_text) < 100:
                print(f"    Skipping: text too short ({len(full_text)} chars)", file=sys.stderr)
                continue

            yield normalize(dec, full_text)
            total += 1

        page += 1
        time.sleep(REQUEST_DELAY)

    print(f"  Fetched {total} decisions total", file=sys.stderr)


def bootstrap_sample(sample_size: int = 12):
    """Fetch sample documents."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    session = get_session()

    print(f"Fetching {sample_size} sample decisions from ANJ...", file=sys.stderr)

    count = 0
    total_chars = 0

    for record in fetch_all(session, max_pages=3):
        if count >= sample_size:
            break

        text_len = len(record.get("text", ""))
        if text_len < 200:
            continue

        filename = f"{record['_id']}.json"
        filepath = SAMPLE_DIR / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"    Saved: {filename} ({text_len:,} chars)", file=sys.stderr)
        total_chars += text_len
        count += 1

    print(f"\n=== SUMMARY ===", file=sys.stderr)
    print(f"  Documents: {count}", file=sys.stderr)
    print(f"  Total chars: {total_chars:,}", file=sys.stderr)
    print(f"  Avg chars/doc: {total_chars // count if count else 0:,}", file=sys.stderr)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="FR/ANJ Data Fetcher")
    subparsers = parser.add_subparsers(dest="command")

    bootstrap_parser = subparsers.add_parser("bootstrap")
    bootstrap_parser.add_argument("--sample", action="store_true")
    bootstrap_parser.add_argument("--count", type=int, default=12)

    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(args.count)
        else:
            session = get_session()
            data_dir = SCRIPT_DIR / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            count = 0
            with open(data_dir / "records.jsonl", "w", encoding="utf-8") as f:
                for record in fetch_all(session):
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    count += 1
            print(f"Bootstrap complete: {count} records")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
