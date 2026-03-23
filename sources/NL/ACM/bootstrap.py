#!/usr/bin/env python3
"""
NL/ACM - Dutch Competition Authority (Autoriteit Consument & Markt)

Fetches decisions, regulations, and other official publications from ACM.
Uses the sitemap to discover publication URLs, then extracts metadata from
dcterms meta tags and full text from article body + PDF attachments.

Data source:
- https://www.acm.nl/nl/publicaties

License: CC-0 (Public Domain)

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py bootstrap --full     # Full fetch all decisions
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
from urllib.parse import urljoin

from bs4 import BeautifulSoup

try:
    import pypdf
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    print("Warning: pypdf not available, PDF text extraction disabled", file=sys.stderr)

# Constants
SOURCE_ID = "NL/ACM"
BASE_URL = "https://www.acm.nl"
SITEMAP_INDEX_URL = f"{BASE_URL}/sitemap.xml"

RATE_LIMIT_DELAY = 2.0
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"

# Publication types we want (doctrine: decisions, regulations, opinions, etc.)
WANTED_TYPES = {
    "Besluit",
    "Beslissing op bezwaar",
    "Concentratiemelding",
    "Regelgeving",
    "Zienswijze en consultatie",
    "Visie en opinie",
    "Waarschuwing",
    "Onderzoek",
}

# Paths
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"


def curl_fetch(url: str, binary: bool = False):
    """Fetch URL using curl subprocess (workaround for SSL issues)."""
    try:
        cmd = [
            "curl", "-s", "-L",
            "-H", f"User-Agent: {USER_AGENT}",
            "-H", "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "-H", "Accept-Language: nl-NL,nl;q=0.9,en;q=0.5",
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
    """Extract text from PDF content using pypdf."""
    if not PDF_AVAILABLE:
        return ""
    try:
        reader = pypdf.PdfReader(io.BytesIO(pdf_content))
        text_parts = []
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)
        full_text = "\n\n".join(text_parts)
        full_text = re.sub(r'[ \t]+', ' ', full_text)
        full_text = re.sub(r'\n\s*\n\s*\n+', '\n\n', full_text)
        return full_text.strip()
    except Exception as e:
        print(f"Error extracting text from PDF: {e}", file=sys.stderr)
        return ""


def get_sitemap_publication_urls(max_pages: Optional[int] = None) -> list[str]:
    """Get all publication URLs from the sitemap."""
    print("Fetching sitemap index...", file=sys.stderr)
    content = curl_fetch(SITEMAP_INDEX_URL)
    if not content:
        print("Error fetching sitemap index", file=sys.stderr)
        return []

    # Parse sitemap index to find sub-sitemap URLs
    sitemap_urls = re.findall(r'<loc>(https://www\.acm\.nl/sitemap\.xml\?page=\d+)</loc>', content)
    if not sitemap_urls:
        print("No sitemap pages found", file=sys.stderr)
        return []

    print(f"Found {len(sitemap_urls)} sitemap pages", file=sys.stderr)

    if max_pages:
        sitemap_urls = sitemap_urls[:max_pages]

    publication_urls = []
    for i, sitemap_url in enumerate(sitemap_urls):
        print(f"  Parsing sitemap page {i+1}/{len(sitemap_urls)}...", file=sys.stderr)
        page_content = curl_fetch(sitemap_url)
        if page_content:
            urls = re.findall(r'<loc>(https://www\.acm\.nl/nl/publicaties/[^<]+)</loc>', page_content)
            publication_urls.extend(urls)
        time.sleep(0.5)

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for url in publication_urls:
        if url not in seen:
            seen.add(url)
            unique.append(url)

    print(f"Found {len(unique)} unique publication URLs", file=sys.stderr)
    return unique


def parse_publication_page(url: str) -> Optional[dict]:
    """Parse a single publication page and extract metadata + full text."""
    html = curl_fetch(url)
    if not html:
        return None

    soup = BeautifulSoup(html, 'html.parser')

    # Check publication type from dcterms.type meta tag
    dctype_meta = soup.find('meta', attrs={'name': 'dcterms.type'})
    pub_type = dctype_meta['content'].strip() if dctype_meta and dctype_meta.get('content') else None

    if not pub_type or pub_type not in WANTED_TYPES:
        return None

    # Title
    h1 = soup.find('h1')
    title = h1.get_text().strip() if h1 else None
    if not title:
        title_meta = soup.find('meta', attrs={'name': 'dcterms.title'})
        title = title_meta['content'].strip() if title_meta and title_meta.get('content') else url.split('/')[-1]

    # Date from dcterms
    date = None
    date_meta = soup.find('meta', attrs={'name': 'dcterms.issued'})
    if date_meta and date_meta.get('content'):
        date = date_meta['content'].strip()
    if not date:
        time_el = soup.find('time', attrs={'datetime': True})
        if time_el:
            dt_str = time_el['datetime']
            date = dt_str[:10] if len(dt_str) >= 10 else dt_str

    # Description
    desc_meta = soup.find('meta', attrs={'name': 'dcterms.description'})
    description = desc_meta['content'].strip() if desc_meta and desc_meta.get('content') else None

    # Keywords / subject
    kw_meta = soup.find('meta', attrs={'name': 'keywords'})
    keywords = [k.strip() for k in kw_meta['content'].split(',') if k.strip()] if kw_meta and kw_meta.get('content') else []

    subject_meta = soup.find('meta', attrs={'name': 'dcterms.subject'})
    subject = subject_meta['content'].strip() if subject_meta and subject_meta.get('content') else None

    # Extract body text from article
    article = soup.find('article')
    body_text = ""
    if article:
        for tag in article.find_all(['nav', 'script', 'style', 'noscript']):
            tag.decompose()
        body_text = article.get_text(separator='\n', strip=True)
        body_text = re.sub(r'\n\s*\n\s*\n+', '\n\n', body_text)
        body_text = body_text.strip()

    # Find PDF attachment URLs
    pdf_urls = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if '.pdf' in href.lower():
            full_url = urljoin(BASE_URL, href) if href.startswith('/') else href
            if full_url not in pdf_urls:
                pdf_urls.append(full_url)

    # Download PDFs and extract text (max 2)
    pdf_texts = []
    for pdf_url in pdf_urls[:2]:
        time.sleep(1)
        print(f"  Downloading PDF: {pdf_url.split('/')[-1][:50]}", file=sys.stderr)
        pdf_content = curl_fetch(pdf_url, binary=True)
        if pdf_content:
            # Skip very large PDFs (>20MB)
            if len(pdf_content) > 20 * 1024 * 1024:
                print(f"  Skipping large PDF ({len(pdf_content)//1024//1024}MB)", file=sys.stderr)
                continue
            pdf_text = extract_text_from_pdf(pdf_content)
            if pdf_text:
                pdf_texts.append(pdf_text)

    # Combine body text and PDF text
    all_text_parts = []
    if body_text:
        all_text_parts.append(body_text)
    if pdf_texts:
        all_text_parts.append("\n\n--- PDF Content ---\n\n")
        all_text_parts.extend(pdf_texts)

    full_text = "\n\n".join(all_text_parts).strip()

    if not full_text:
        return None

    # Build normalized record
    record = {
        "_id": url.split('/')[-1],
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": date,
        "url": url,
        "publication_type": pub_type,
        "subject": subject,
        "keywords": keywords,
        "description": description,
        "pdf_urls": pdf_urls if pdf_urls else None,
    }

    return record


def fetch_all(sample: bool = False, count: int = 15) -> Generator[dict, None, None]:
    """Fetch all ACM decisions."""
    if sample:
        urls = get_sitemap_publication_urls(max_pages=2)
    else:
        urls = get_sitemap_publication_urls()

    print(f"Processing {len(urls)} publication URLs (sample={sample})...", file=sys.stderr)

    yielded = 0
    for i, url in enumerate(urls):
        if sample and yielded >= count:
            break

        print(f"  [{i+1}/{len(urls)}] {url.split('/')[-1][:60]}...", file=sys.stderr)

        record = parse_publication_page(url)
        if record:
            yielded += 1
            print(f"    -> {record['publication_type']}: {record['title'][:50]}... ({len(record['text'])} chars)", file=sys.stderr)
            yield record

        time.sleep(RATE_LIMIT_DELAY)

    print(f"\nTotal records fetched: {yielded}", file=sys.stderr)


def save_samples(records: list[dict], sample_dir: Path):
    """Save sample records to JSON files."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    for i, record in enumerate(records):
        filepath = sample_dir / f"record_{i:04d}.json"
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    all_path = sample_dir / "all_samples.json"
    with open(all_path, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(records)} samples to {sample_dir}", file=sys.stderr)


def validate_samples(records: list[dict]) -> bool:
    """Validate that sample records meet quality criteria."""
    if len(records) < 10:
        print(f"FAIL: Only {len(records)} records, need at least 10", file=sys.stderr)
        return False

    issues = []
    for i, rec in enumerate(records):
        if not rec.get('text'):
            issues.append(f"Record {i}: missing text")
        elif len(rec['text']) < 50:
            issues.append(f"Record {i}: text too short ({len(rec['text'])} chars)")
        if not rec.get('title'):
            issues.append(f"Record {i}: missing title")
        if not rec.get('date'):
            issues.append(f"Record {i}: missing date")
        if '<' in (rec.get('text', '') or '')[:500] and '>' in (rec.get('text', '') or '')[:500]:
            issues.append(f"Record {i}: possible HTML in text field")

    if issues:
        print("Validation issues:", file=sys.stderr)
        for issue in issues:
            print(f"  - {issue}", file=sys.stderr)
        text_issues = [i for i in issues if 'missing text' in i]
        if len(text_issues) > len(records) // 2:
            return False

    print(f"Validation passed: {len(records)} records", file=sys.stderr)
    return True


def main():
    parser = argparse.ArgumentParser(description="NL/ACM - Dutch Competition Authority data fetcher")
    parser.add_argument('command', choices=['bootstrap', 'test-api'],
                        help='Command to run')
    parser.add_argument('--sample', action='store_true',
                        help='Fetch only sample records')
    parser.add_argument('--full', action='store_true',
                        help='Full fetch all records')
    parser.add_argument('--count', type=int, default=15,
                        help='Number of sample records to fetch')
    args = parser.parse_args()

    if args.command == 'test-api':
        print("Testing ACM sitemap access...", file=sys.stderr)
        content = curl_fetch(SITEMAP_INDEX_URL)
        if content:
            pages = re.findall(r'<loc>[^<]+</loc>', content)
            print(f"OK: Sitemap has {len(pages)} entries", file=sys.stderr)
        else:
            print("FAIL: Could not fetch sitemap", file=sys.stderr)
            sys.exit(1)

        test_url = "https://www.acm.nl/nl/publicaties/acm-stelt-handhaving-van-besluit-tegen-apple-voorlopig-uit"
        html = curl_fetch(test_url)
        if html:
            dctype = BeautifulSoup(html, 'html.parser').find('meta', attrs={'name': 'dcterms.type'})
            print(f"OK: Test page type = {dctype['content'] if dctype else 'N/A'}", file=sys.stderr)
        else:
            print("FAIL: Could not fetch test page", file=sys.stderr)
            sys.exit(1)

    elif args.command == 'bootstrap':
        records = list(fetch_all(sample=args.sample, count=args.count))

        if args.sample or not args.full:
            save_samples(records, SAMPLE_DIR)
            if validate_samples(records):
                print("Sample validation PASSED", file=sys.stderr)
            else:
                print("Sample validation FAILED", file=sys.stderr)
                sys.exit(1)
        else:
            save_samples(records, SCRIPT_DIR / "data")


if __name__ == '__main__':
    main()
