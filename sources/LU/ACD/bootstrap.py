#!/usr/bin/env python3
"""
LU/ACD -- Luxembourg Tax Administration Circulars

Fetches tax circulars (circulaires fiscales) from the Administration des
Contributions Directes (ACD) website.

Strategy:
  - Parse the main circulaires index page for PDF links organized by category
  - Download individual PDFs and extract text with pdfplumber
  - Categories: L.I.R., Eval., I.Fort., I.C.C., L.G.-A, L.G.-P, Conv. D.I., etc.

Endpoints:
  - Circulaires index: https://impotsdirects.public.lu/fr/legislation/circulaires.html
  - PDFs: https://impotsdirects.public.lu/dam-assets/fr/legislation/...

Data:
  - ~200+ tax circulars currently in force
  - Text-based PDFs (not scanned), good text extraction quality
  - French language

License: Open Data (Luxembourg public sector)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test-api           # Quick connectivity test
"""

import argparse
import io
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urljoin

import pdfplumber
import requests

# Configuration
SOURCE_ID = "LU/ACD"
BASE_URL = "https://impotsdirects.public.lu"
CIRCULAIRES_URL = f"{BASE_URL}/fr/legislation/circulaires.html"
REQUEST_DELAY = 1.5  # seconds between PDF downloads
REQUEST_TIMEOUT = 60

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (academic research; contact: github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
}


class CirculaireLinkParser(HTMLParser):
    """Parse the circulaires HTML page to extract PDF links with categories."""

    def __init__(self):
        super().__init__()
        self.links = []
        self.current_category = ""
        self.in_heading = False
        self.heading_text = ""
        self.in_link = False
        self.link_href = ""
        self.link_text = ""
        self.heading_tags = {"h2", "h3", "h4"}

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag in self.heading_tags:
            self.in_heading = True
            self.heading_text = ""
        elif tag == "a":
            href = attrs_dict.get("href", "")
            if href and ".pdf" in href.lower():
                self.in_link = True
                self.link_href = href
                self.link_text = ""

    def handle_endtag(self, tag):
        if tag in self.heading_tags and self.in_heading:
            self.in_heading = False
            text = self.heading_text.strip()
            if text:
                self.current_category = text
        elif tag == "a" and self.in_link:
            self.in_link = False
            if self.link_href:
                # Normalize URL
                href = self.link_href
                if href.startswith("//"):
                    href = "https:" + href
                elif href.startswith("/"):
                    href = BASE_URL + href

                self.links.append({
                    "url": href,
                    "title": self.link_text.strip(),
                    "category": self.current_category,
                })

    def handle_data(self, data):
        if self.in_heading:
            self.heading_text += data
        if self.in_link:
            self.link_text += data


def get_circular_links() -> list[dict]:
    """Fetch and parse the circulaires index page for all PDF links."""
    print(f"Fetching circulaires index: {CIRCULAIRES_URL}")
    resp = requests.get(CIRCULAIRES_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    parser = CirculaireLinkParser()
    parser.feed(resp.text)

    # Deduplicate by URL
    seen = set()
    unique = []
    for link in parser.links:
        if link["url"] not in seen:
            seen.add(link["url"])
            unique.append(link)

    print(f"Found {len(unique)} unique PDF links")
    return unique


def extract_text_from_pdf(pdf_content: bytes, max_pages: int = 200) -> str:
    """Extract text from PDF using pdfplumber with memory bounds."""
    text_parts = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
            for i, page in enumerate(pdf.pages):
                if i >= max_pages:
                    break
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
    except Exception as e:
        print(f"    Warning: PDF extraction failed: {e}", file=sys.stderr)
        return ""

    return "\n\n".join(text_parts)


def extract_circular_number(title: str, text: str) -> str:
    """Extract circular number from title or text."""
    # Try from title: "L.I.R. n° 115/7" or "Eval. n° 168/2"
    m = re.search(r"n°\s*([\d/\w.-]+)", title, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    # Try from PDF text
    m = re.search(r"[Cc]irculaire\s+(?:du\s+\w+\s+)?n°?\s*([\d/\w.-]+)", text[:1000])
    if m:
        return m.group(1).strip()

    return ""


def extract_date_from_title(title: str) -> Optional[str]:
    """Extract date from circular title text."""
    # Pattern: "du DD/MM/YYYY" or "du DD.MM.YYYY" or "du DDMMYYYY"
    m = re.search(r"du\s+(\d{1,2})[./](\d{1,2})[./](\d{4})", title)
    if m:
        d, mo, y = m.groups()
        return f"{y}-{mo.zfill(2)}-{d.zfill(2)}"

    # Pattern: "du DDMMYYYY" (no separator)
    m = re.search(r"du\s+(\d{1,2})\s*(janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s*(\d{4})", title, re.IGNORECASE)
    if m:
        d, month_name, y = m.groups()
        months = {
            "janvier": "01", "février": "02", "mars": "03", "avril": "04",
            "mai": "05", "juin": "06", "juillet": "07", "août": "08",
            "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12",
        }
        mo = months.get(month_name.lower(), "01")
        return f"{y}-{mo}-{d.zfill(2)}"

    return None


def extract_date_from_filename(url: str) -> Optional[str]:
    """Extract date from PDF filename in URL."""
    filename = url.rsplit("/", 1)[-1]

    # Pattern: YYYY-MM-DD at start of filename
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", filename)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # Pattern: du-DDMMYYYY or du-DD-MM-YYYY in filename
    m = re.search(r"du[_-](\d{1,2})(\d{1,2})(\d{4})", filename)
    if m:
        d, mo, y = m.groups()
        if 1 <= int(mo) <= 12:
            return f"{y}-{mo.zfill(2)}-{d.zfill(2)}"

    # Pattern: -du-DD-MOIS-YYYY
    m = re.search(r"du-(\d{1,2})-(janvier|fevrier|mars|avril|mai|juin|juillet|aout|septembre|octobre|novembre|decembre)-(\d{4})", filename, re.IGNORECASE)
    if m:
        d, month_name, y = m.groups()
        months = {
            "janvier": "01", "fevrier": "02", "mars": "03", "avril": "04",
            "mai": "05", "juin": "06", "juillet": "07", "aout": "08",
            "septembre": "09", "octobre": "10", "novembre": "11", "decembre": "12",
        }
        mo = months.get(month_name.lower(), "01")
        return f"{y}-{mo}-{d.zfill(2)}"

    return None


def make_id(url: str, title: str) -> str:
    """Generate a unique ID from the URL/title."""
    # Use filename without extension as base
    filename = url.rsplit("/", 1)[-1]
    filename = re.sub(r"\.pdf$", "", filename, flags=re.IGNORECASE)
    # Sanitize
    clean = re.sub(r"[^a-zA-Z0-9_-]", "_", filename)
    clean = re.sub(r"_+", "_", clean).strip("_")
    return f"LU_ACD_{clean}"[:200]


def normalize(link: dict, text: str) -> dict:
    """Transform a circular link + extracted text into normalized schema."""
    title = link["title"] or link["url"].rsplit("/", 1)[-1].replace(".pdf", "")
    url = link["url"]
    category = link.get("category", "")

    circular_num = extract_circular_number(title, text)
    date = extract_date_from_title(title) or extract_date_from_filename(url)

    return {
        "_id": make_id(url, title),
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": f"Circulaire {title}" if not title.lower().startswith("circ") else title,
        "text": text,
        "date": date,
        "url": url,
        "circular_number": circular_num,
        "category": category,
        "language": "fr",
        "jurisdiction": "Luxembourg",
    }


def fetch_all(limit: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch all circulars from the ACD website."""
    links = get_circular_links()
    count = 0

    for link in links:
        if limit and count >= limit:
            break

        url = link["url"]
        title = link.get("title", "")
        print(f"  [{count+1}] Downloading: {title or url.rsplit('/', 1)[-1]}")

        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()

            if len(resp.content) > 50 * 1024 * 1024:  # Skip >50MB PDFs
                print(f"    Skipping: file too large ({len(resp.content)} bytes)")
                continue

            text = extract_text_from_pdf(resp.content)

            if not text or len(text) < 50:
                print(f"    Skipping: no text extracted (len={len(text)})")
                continue

            record = normalize(link, text)
            print(f"    Text: {len(text)} chars, category: {link.get('category', '?')}")
            yield record
            count += 1

            time.sleep(REQUEST_DELAY)

        except requests.RequestException as e:
            print(f"    Error downloading: {e}", file=sys.stderr)
            continue
        except Exception as e:
            print(f"    Error processing: {e}", file=sys.stderr)
            continue

    print(f"\nTotal records fetched: {count}")


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch circulars updated since a given date (re-fetches all, filters by date)."""
    for record in fetch_all():
        if record.get("date"):
            try:
                rec_date = datetime.fromisoformat(record["date"])
                if rec_date.date() >= since.date():
                    yield record
            except (ValueError, TypeError):
                yield record
        else:
            yield record


def bootstrap_sample(sample_size: int = 15) -> None:
    """Fetch a sample of records for validation."""
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    print(f"Fetching {sample_size} sample records...")
    count = 0

    for record in fetch_all(limit=sample_size):
        filepath = sample_dir / f"{record['_id']}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        count += 1
        print(f"  Saved: {filepath.name} ({len(record.get('text', ''))} chars)")

    print(f"\nSample complete: {count} records saved to {sample_dir}")


def test_api() -> None:
    """Test connectivity and PDF extraction."""
    print("Testing ACD circulaires access...")

    # Test index page
    resp = requests.get(CIRCULAIRES_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    print(f"Index page: {resp.status_code} ({len(resp.text)} bytes)")

    # Parse links
    links = get_circular_links()
    print(f"Found {len(links)} PDF links")

    # Count by category
    categories = {}
    for link in links:
        cat = link.get("category", "Unknown")
        categories[cat] = categories.get(cat, 0) + 1
    print(f"Categories: {json.dumps(categories, ensure_ascii=False, indent=2)}")

    if links:
        # Test one PDF download
        test_link = links[0]
        print(f"\nTest PDF: {test_link['title'] or test_link['url'].rsplit('/', 1)[-1]}")
        resp = requests.get(test_link["url"], headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        text = extract_text_from_pdf(resp.content)
        print(f"PDF test PASSED: {len(text)} chars extracted")
    else:
        print("WARNING: No PDF links found!")


def main():
    parser = argparse.ArgumentParser(description="LU/ACD Tax Circulars Fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--count", type=int, default=15, help="Sample size")
    args = parser.parse_args()

    if args.command == "test-api":
        test_api()
    elif args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(args.count)
        else:
            data_dir = Path(__file__).parent / "data"
            data_dir.mkdir(exist_ok=True)
            for record in fetch_all():
                filepath = data_dir / f"{record['_id']}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
    elif args.command == "update":
        # Default: last 90 days
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=90)
        data_dir = Path(__file__).parent / "data"
        data_dir.mkdir(exist_ok=True)
        for record in fetch_updates(since):
            filepath = data_dir / f"{record['_id']}.json"
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
