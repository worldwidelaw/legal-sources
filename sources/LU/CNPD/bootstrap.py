#!/usr/bin/env python3
"""
LU/CNPD -- Luxembourg Data Protection Authority (CNPD)

Fetches decisions and opinions from the Commission Nationale pour la
Protection des Données website.

Strategy:
  - Crawl paginated listing pages for decisions and opinions
  - Extract PDF links from individual detail pages
  - Download PDFs and extract text with pdfplumber

Endpoints:
  - Decisions: https://cnpd.public.lu/fr/decisions-sanctions.html?b={offset}
  - Opinions:  https://cnpd.public.lu/fr/decisions-avis.html?b={offset}
  - PDFs:      https://cnpd.public.lu/content/dam/cnpd/fr/...

Data:
  - ~88 decisions (2021-2025)
  - ~357 opinions (2003-2026)
  - French language, pseudonymized PDFs

License: Open Data (Luxembourg public sector)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
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

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


# Configuration
SOURCE_ID = "LU/CNPD"
BASE_URL = "https://cnpd.public.lu"
DECISIONS_URL = f"{BASE_URL}/fr/decisions-sanctions.html"
OPINIONS_URL = f"{BASE_URL}/fr/decisions-avis.html"
REQUEST_DELAY = 1.5
REQUEST_TIMEOUT = 60
PAGE_SIZE = 20

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (academic research; contact: github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
}


class ListingParser(HTMLParser):
    """Parse a CNPD listing page to extract links to detail pages."""

    def __init__(self, section: str):
        super().__init__()
        self.links = []
        self.section = section  # "decisions-sanctions" or "decisions-avis"
        self.in_link = False
        self.link_href = ""
        self.link_text = ""

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            attrs_dict = dict(attrs)
            href = attrs_dict.get("href", "")
            if f"/fr/{self.section}/" in href and href.endswith(".html"):
                # Skip the listing page itself
                if href.rstrip("/").endswith(f"{self.section}.html"):
                    return
                self.in_link = True
                self.link_href = href
                self.link_text = ""

    def handle_endtag(self, tag):
        if tag == "a" and self.in_link:
            self.in_link = False
            if self.link_href:
                href = self.link_href
                if href.startswith("/"):
                    href = BASE_URL + href
                self.links.append({
                    "url": href,
                    "title": self.link_text.strip(),
                })

    def handle_data(self, data):
        if self.in_link:
            self.link_text += data


class DetailParser(HTMLParser):
    """Parse a CNPD detail page to extract PDF link and metadata."""

    def __init__(self):
        super().__init__()
        self.pdf_urls = []
        self.in_title = False
        self.title = ""
        self.in_date = False
        self.date_text = ""
        self.in_content = False
        self.summary_parts = []
        self._tag_stack = []

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        cls = attrs_dict.get("class", "")

        if tag == "a":
            href = attrs_dict.get("href", "")
            if href and ".pdf" in href.lower():
                if href.startswith("/"):
                    href = BASE_URL + href
                self.pdf_urls.append(href)

        if tag == "h1" and not self.title:
            self.in_title = True

        # Look for date in various elements
        if "date" in cls.lower() or "time" in tag:
            self.in_date = True
            self.date_text = ""
            # Check datetime attribute
            dt = attrs_dict.get("datetime", "")
            if dt:
                self.date_text = dt

        if tag == "div" and ("content" in cls or "text" in cls or "body" in cls):
            self.in_content = True

    def handle_endtag(self, tag):
        if tag == "h1" and self.in_title:
            self.in_title = False
        if self.in_date and tag in ("span", "div", "time", "p"):
            self.in_date = False
        if tag == "div" and self.in_content:
            self.in_content = False

    def handle_data(self, data):
        if self.in_title:
            self.title += data
        if self.in_date and not self.date_text:
            self.date_text += data
        if self.in_content:
            self.summary_parts.append(data)


def get_listing_pages(base_url: str, section: str, max_pages: int = 25) -> list[dict]:
    """Fetch all items from a paginated listing."""
    all_links = []
    seen_urls = set()

    for page_num in range(max_pages):
        offset = page_num * PAGE_SIZE
        url = f"{base_url}?b={offset}" if offset > 0 else base_url
        print(f"  Fetching listing page {page_num + 1}: {url}")

        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"    Error: {e}", file=sys.stderr)
            break

        parser = ListingParser(section)
        parser.feed(resp.text)

        new_links = [l for l in parser.links if l["url"] not in seen_urls]
        if not new_links:
            print(f"    No new links on page {page_num + 1}, stopping pagination")
            break

        for link in new_links:
            seen_urls.add(link["url"])
            all_links.append(link)

        time.sleep(REQUEST_DELAY)

    return all_links


def extract_pdf_url_from_detail(detail_url: str) -> dict:
    """Fetch a detail page and extract PDF URL + metadata."""
    try:
        resp = requests.get(detail_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"    Error fetching detail page: {e}", file=sys.stderr)
        return {}

    parser = DetailParser()
    parser.feed(resp.text)

    result = {
        "title": parser.title.strip(),
        "summary": " ".join(parser.summary_parts).strip()[:500],
        "date_text": parser.date_text.strip(),
    }

    # Pick the most relevant PDF
    if parser.pdf_urls:
        result["pdf_url"] = parser.pdf_urls[0]

    return result


def extract_text_from_pdf(pdf_content: bytes, max_pages: int = 200) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="LU/CNPD",
        source_id="",
        pdf_bytes=pdf_content,
        table="doctrine",
    ) or ""

def parse_date(text: str) -> Optional[str]:
    """Parse a date string into ISO format."""
    if not text:
        return None

    # ISO format already
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", text)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # DD/MM/YYYY or DD.MM.YYYY
    m = re.search(r"(\d{1,2})[./](\d{1,2})[./](\d{4})", text)
    if m:
        d, mo, y = m.groups()
        return f"{y}-{mo.zfill(2)}-{d.zfill(2)}"

    # French month names
    months = {
        "janvier": "01", "février": "02", "mars": "03", "avril": "04",
        "mai": "05", "juin": "06", "juillet": "07", "août": "08",
        "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12",
    }
    m = re.search(r"(\d{1,2})\s+(janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+(\d{4})", text, re.IGNORECASE)
    if m:
        d, month_name, y = m.groups()
        mo = months.get(month_name.lower(), "01")
        return f"{y}-{mo}-{d.zfill(2)}"

    return None


def extract_date_from_url(url: str) -> Optional[str]:
    """Try to extract year from URL path."""
    m = re.search(r"/(\d{4})/", url)
    if m:
        return f"{m.group(1)}-01-01"
    return None


def extract_reference(title: str, url: str) -> str:
    """Extract reference number from title or URL."""
    # Pattern: "Décision ... n° NNN/YYYY" or "Nr. NNN"
    m = re.search(r"n[°ro.]+\s*([\d/FR-]+\d{4})", title, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    m = re.search(r"(d[ée]cision[^/]*\d{4})", title, re.IGNORECASE)
    if m:
        return m.group(1).strip()[:80]

    # From URL filename
    filename = url.rsplit("/", 1)[-1].replace(".html", "").replace(".pdf", "")
    return filename[:80]


def make_id(url: str, doc_type: str) -> str:
    """Generate unique ID from URL."""
    filename = url.rsplit("/", 1)[-1]
    filename = re.sub(r"\.(html|pdf)$", "", filename, flags=re.IGNORECASE)
    clean = re.sub(r"[^a-zA-Z0-9_-]", "_", filename)
    clean = re.sub(r"_+", "_", clean).strip("_")
    prefix = "dec" if doc_type == "decision" else "avis"
    return f"LU_CNPD_{prefix}_{clean}"[:200]


def normalize(detail: dict, text: str, page_url: str, doc_type: str) -> dict:
    """Transform extracted data into normalized schema."""
    title = detail.get("title", "") or page_url.rsplit("/", 1)[-1].replace(".html", "")
    date = parse_date(detail.get("date_text", "")) or parse_date(title) or extract_date_from_url(page_url)
    reference = extract_reference(title, page_url)

    return {
        "_id": make_id(page_url, doc_type),
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date,
        "url": page_url,
        "reference": reference,
        "doc_type": doc_type,
        "language": "fr",
        "jurisdiction": "Luxembourg",
    }


def fetch_section(section_url: str, section_name: str, doc_type: str,
                  limit: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch all documents from one section (decisions or opinions)."""
    print(f"\n=== Fetching {section_name} ===")
    links = get_listing_pages(section_url, section_name)
    print(f"Found {len(links)} {doc_type} pages")
    count = 0

    for link in links:
        if limit is not None and count >= limit:
            break

        page_url = link["url"]
        print(f"  [{count+1}] {link.get('title', '')[:60] or page_url.rsplit('/', 1)[-1]}")

        detail = extract_pdf_url_from_detail(page_url)
        time.sleep(REQUEST_DELAY)

        pdf_url = detail.get("pdf_url")
        if not pdf_url:
            print(f"    No PDF found on detail page, skipping")
            continue

        try:
            resp = requests.get(pdf_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()

            if len(resp.content) > 50 * 1024 * 1024:
                print(f"    Skipping: too large ({len(resp.content)} bytes)")
                continue

            text = extract_text_from_pdf(resp.content)
            if not text or len(text) < 50:
                print(f"    Skipping: no text extracted (len={len(text)})")
                continue

            record = normalize(detail, text, page_url, doc_type)
            print(f"    Text: {len(text)} chars")
            yield record
            count += 1

        except requests.RequestException as e:
            print(f"    Error downloading PDF: {e}", file=sys.stderr)
            continue
        except Exception as e:
            print(f"    Error processing: {e}", file=sys.stderr)
            continue

        time.sleep(REQUEST_DELAY)

    print(f"  Total {doc_type}s fetched: {count}")


def fetch_all(limit: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch all decisions and opinions."""
    dec_limit = limit // 2 if limit else None
    avis_limit = (limit - (limit // 2)) if limit else None

    yield from fetch_section(DECISIONS_URL, "decisions-sanctions", "decision", dec_limit)
    yield from fetch_section(OPINIONS_URL, "decisions-avis", "opinion", avis_limit)


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch documents updated since a given date."""
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
    print("Testing CNPD access...")

    # Test decisions listing
    resp = requests.get(DECISIONS_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    print(f"Decisions page: {resp.status_code} ({len(resp.text)} bytes)")

    dec_parser = ListingParser("decisions-sanctions")
    dec_parser.feed(resp.text)
    print(f"Decision links on page 1: {len(dec_parser.links)}")

    # Test opinions listing
    resp = requests.get(OPINIONS_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    print(f"Opinions page: {resp.status_code} ({len(resp.text)} bytes)")

    avis_parser = ListingParser("decisions-avis")
    avis_parser.feed(resp.text)
    print(f"Opinion links on page 1: {len(avis_parser.links)}")

    # Test one detail page + PDF
    test_links = dec_parser.links or avis_parser.links
    if test_links:
        test_url = test_links[0]["url"]
        print(f"\nTest detail page: {test_url}")
        detail = extract_pdf_url_from_detail(test_url)
        print(f"  Title: {detail.get('title', '')[:80]}")
        print(f"  PDF URL: {detail.get('pdf_url', 'NOT FOUND')}")

        if detail.get("pdf_url"):
            resp = requests.get(detail["pdf_url"], headers=HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            text = extract_text_from_pdf(resp.content)
            print(f"  PDF test PASSED: {len(text)} chars extracted")
    else:
        print("WARNING: No links found on listing pages!")


def main():
    parser = argparse.ArgumentParser(description="LU/CNPD Data Protection Authority Fetcher")
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
