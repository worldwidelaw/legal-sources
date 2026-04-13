#!/usr/bin/env python3
"""
BE/APD - Belgian Data Protection Authority (Autorité de protection des données / Gegevensbeschermingsautoriteit)

Fetches GDPR enforcement decisions from the Belgian DPA official website.
Decisions are published as PDFs in both French and Dutch.

Data source:
- French: https://www.autoriteprotectiondonnees.be/citoyen/publications/decisions
- Dutch: https://www.gegevensbeschermingsautoriteit.be/burger/publicaties/beslissingen

License: Belgian Federal Government Open Data

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py bootstrap --full     # Full fetch all decisions
"""

import argparse
import io
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional
from urllib.parse import urljoin, urlparse, parse_qs, urlencode

import requests
from bs4 import BeautifulSoup

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


# Constants
SOURCE_ID = "BE/APD"
BASE_URL_FR = "https://www.autoriteprotectiondonnees.be"
BASE_URL_NL = "https://www.gegevensbeschermingsautoriteit.be"

# Search URLs for decisions (50 results per page)
SEARCH_URL_FR = f"{BASE_URL_FR}/citoyen/chercher?q=&search_category[]=taxonomy%3Apublications&search_type[]=decision&s=recent&l=50"
SEARCH_URL_NL = f"{BASE_URL_NL}/burger/zoeken?q=&search_category[]=taxonomy%3Apublications&search_type[]=decision&s=recent&l=50"

RATE_LIMIT_DELAY = 2.0
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"

# Paths
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"


def get_session() -> requests.Session:
    """Create requests session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-BE,fr;q=0.9,nl-BE;q=0.8,nl;q=0.7,en;q=0.6",
    })
    return session


def extract_text_from_pdf(pdf_content: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="BE/APD",
        source_id="",
        pdf_bytes=pdf_content,
        table="case_law",
    ) or ""

def parse_decision_filename(filename: str) -> dict:
    """Parse decision metadata from PDF filename."""
    metadata = {
        'decision_number': None,
        'decision_type': None,
        'year': None,
        'language': None,
    }

    # Detect language from URL patterns
    if 'quant-au-fond' in filename or 'classement-sans-suite' in filename or 'avertissement' in filename:
        metadata['language'] = 'fr'
    elif 'ten-gronde' in filename or 'zonder-gevolg' in filename or 'waarschuwing' in filename:
        metadata['language'] = 'nl'

    # French patterns
    fr_patterns = {
        r'decision-quant-au-fond-n0-(\d+)-(\d{4})': ('substantive_decision', 'fr'),
        r'classement-sans-suite-n0-(\d+)-(\d{4})': ('dismissal', 'fr'),
        r'avertissement-n0-(\d+)-(\d{4})': ('warning', 'fr'),
        r'ordonnance-n0-(\d+)-(\d{4})': ('order', 'fr'),
        r'arret-.*-ar-(\d+)': ('court_judgment', 'fr'),
        r'avertissement-et-reprimande-n0-(\d+)-(\d{4})': ('warning_reprimand', 'fr'),
    }

    # Dutch patterns
    nl_patterns = {
        r'beslissing-ten-gronde-nr-(\d+)-(\d{4})': ('substantive_decision', 'nl'),
        r'zonder-gevolg-nr\.-(\d+)-(\d{4})': ('dismissal', 'nl'),
        r'waarschuwing-nr\.-(\d+)-(\d{4})': ('warning', 'nl'),
        r'bevel-nr\.-(\d+)-(\d{4})': ('order', 'nl'),
        r'arrest-.*-ar-(\d+)': ('court_judgment', 'nl'),
    }

    all_patterns = {**fr_patterns, **nl_patterns}

    for pattern, (dtype, lang) in all_patterns.items():
        match = re.search(pattern, filename.lower())
        if match:
            groups = match.groups()
            metadata['decision_type'] = dtype
            metadata['language'] = lang
            if len(groups) >= 1:
                metadata['decision_number'] = groups[0]
            if len(groups) >= 2:
                metadata['year'] = groups[1]
            break

    return metadata


def fetch_pdf_links_from_page(session: requests.Session, url: str, base_url: str) -> list[dict]:
    """Fetch all PDF links from a search results page."""
    pdfs = []

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching page {url}: {e}", file=sys.stderr)
        return pdfs

    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all PDF links
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.lower().endswith('.pdf') and '/publications/' in href.lower():
            # Get full URL
            if href.startswith('http'):
                pdf_url = href
            else:
                pdf_url = urljoin(base_url, href)

            # Get title from link text or surrounding context
            title = link.get_text().strip()
            if not title or len(title) < 5:
                # Try to get title from parent elements
                parent = link.find_parent(['li', 'div', 'td', 'article'])
                if parent:
                    h_tag = parent.find(['h3', 'h4', 'h5', 'strong'])
                    if h_tag:
                        title = h_tag.get_text().strip()

            # Parse filename for metadata
            filename = href.split('/')[-1]
            file_metadata = parse_decision_filename(filename)

            if not title:
                title = filename.replace('.pdf', '').replace('-', ' ').title()

            pdfs.append({
                'pdf_url': pdf_url,
                'title': title,
                'filename': filename,
                **file_metadata
            })

    return pdfs


def get_total_pages(session: requests.Session, url: str) -> int:
    """Get total number of pages from search results."""
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Look for total results count in title or page
        title = soup.find('title')
        if title:
            match = re.search(r'(\d+)\s+(?:résultats|resultaten)', title.get_text())
            if match:
                total = int(match.group(1))
                return (total // 50) + 1

        # Look for pagination links
        pagination = soup.find_all('a', href=re.compile(r'[?&]p=\d+'))
        if pagination:
            max_page = 0
            for link in pagination:
                href = link.get('href', '')
                match = re.search(r'[?&]p=(\d+)', href)
                if match:
                    page_num = int(match.group(1))
                    max_page = max(max_page, page_num)
            return max_page + 1  # p is 0-indexed
    except Exception as e:
        print(f"Error getting total pages: {e}", file=sys.stderr)

    return 10  # Default fallback


def fetch_all_pdf_links(session: requests.Session, language: str = 'fr', max_pages: int = None) -> list[dict]:
    """Fetch all PDF links from all pages of search results."""
    if language == 'fr':
        base_search_url = SEARCH_URL_FR
        base_url = BASE_URL_FR
    else:
        base_search_url = SEARCH_URL_NL
        base_url = BASE_URL_NL

    # Get total pages
    total_pages = get_total_pages(session, base_search_url)
    if max_pages:
        total_pages = min(total_pages, max_pages)

    print(f"Fetching {language.upper()} decisions from {total_pages} pages...", file=sys.stderr)

    all_pdfs = []
    seen_urls = set()

    for page in range(total_pages):
        # Add pagination parameter
        page_url = f"{base_search_url}&p={page}"

        print(f"  Page {page + 1}/{total_pages}...", file=sys.stderr)
        pdfs = fetch_pdf_links_from_page(session, page_url, base_url)

        for pdf in pdfs:
            if pdf['pdf_url'] not in seen_urls:
                seen_urls.add(pdf['pdf_url'])
                all_pdfs.append(pdf)

        time.sleep(RATE_LIMIT_DELAY)

    return all_pdfs


def fetch_decision_with_text(session: requests.Session, decision: dict, max_pdf_size_mb: float = 10.0) -> Optional[dict]:
    """Download PDF and extract text for a decision."""
    pdf_url = decision.get('pdf_url')
    if not pdf_url:
        return None

    try:
        # First check PDF size with HEAD request
        head_resp = session.head(pdf_url, timeout=20, allow_redirects=True)
        content_length = head_resp.headers.get('Content-Length')
        if content_length:
            size_mb = int(content_length) / (1024 * 1024)
            if size_mb > max_pdf_size_mb:
                print(f"Skipping large PDF ({size_mb:.1f} MB > {max_pdf_size_mb} MB): {pdf_url}", file=sys.stderr)
                return None

        response = session.get(pdf_url, timeout=180)
        response.raise_for_status()
        pdf_content = response.content
    except requests.RequestException as e:
        print(f"Error downloading PDF {pdf_url}: {e}", file=sys.stderr)
        return None

    # Extract text
    text = extract_text_from_pdf(pdf_content)
    if not text or len(text) < 100:
        print(f"Warning: Could not extract meaningful text from {pdf_url}", file=sys.stderr)
        return None

    decision['text'] = text
    decision['pdf_size'] = len(pdf_content)

    # Try to extract date from text if not already set
    if not decision.get('year'):
        date_patterns = [
            r'(\d{1,2})\s+(janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+(\d{4})',
            r'(\d{1,2})\s+(januari|februari|maart|april|mei|juni|juli|augustus|september|oktober|november|december)\s+(\d{4})',
            r'(\d{4})-(\d{2})-(\d{2})',
        ]
        for pattern in date_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                groups = match.groups()
                if len(groups) == 3 and groups[2].isdigit() and len(groups[2]) == 4:
                    decision['year'] = groups[2]
                    break

    # Try to extract fine amount from text
    fine_patterns = [
        r'(?:amende|boete|fine)\s+(?:administrative?\s+)?(?:de\s+)?(?:van\s+)?€?\s*([\d\s.,]+)\s*(?:euros?|EUR)?',
        r'€\s*([\d\s.,]+)',
    ]
    for pattern in fine_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            amount_str = match.group(1).replace(' ', '').replace('.', '').replace(',', '.')
            try:
                amount = float(amount_str)
                if amount > 100:  # Likely a real fine
                    decision['fine_amount'] = amount
                    break
            except ValueError:
                pass

    # Try to extract GDPR articles mentioned
    gdpr_articles = []
    article_matches = re.findall(r'[Aa]rticle[s]?\s+(\d+(?:\.\d+)?(?:\s*,?\s*\d+)*)', text)
    for match in article_matches:
        articles = re.findall(r'\d+', match)
        gdpr_articles.extend(articles)
    decision['gdpr_articles'] = list(set(gdpr_articles))[:15]

    return decision


def normalize(raw: dict) -> dict:
    """Transform raw APD decision data into normalized schema."""
    # Create unique ID from filename
    filename = raw.get('filename', 'unknown')
    slug = filename.replace('.pdf', '')
    doc_id = f"{SOURCE_ID}/{slug}"

    # Build date from year
    date = None
    if raw.get('year'):
        date = f"{raw['year']}-01-01"  # Approximate to year start

    return {
        '_id': doc_id,
        '_source': SOURCE_ID,
        '_type': 'case_law',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': raw.get('title', ''),
        'text': raw.get('text', ''),
        'date': date,
        'url': raw.get('pdf_url', ''),
        'decision_number': raw.get('decision_number'),
        'decision_type': raw.get('decision_type'),
        'language': raw.get('language'),
        'fine_amount': raw.get('fine_amount'),
        'gdpr_articles': raw.get('gdpr_articles', []),
        'pdf_size': raw.get('pdf_size'),
    }


def fetch_all(session: requests.Session) -> Generator[dict, None, None]:
    """Fetch all APD decisions from both French and Dutch sites."""
    seen_ids = set()

    # Fetch from both language sites
    for lang in ['fr', 'nl']:
        print(f"\nFetching {lang.upper()} decisions...", file=sys.stderr)
        pdf_links = fetch_all_pdf_links(session, language=lang)

        for pdf_info in pdf_links:
            time.sleep(RATE_LIMIT_DELAY)

            full_decision = fetch_decision_with_text(session, pdf_info)
            if full_decision and full_decision.get('text'):
                record = normalize(full_decision)

                # Skip duplicates (same decision in both languages)
                decision_key = f"{record.get('decision_number', '')}-{record.get('decision_type', '')}"
                if decision_key and decision_key != '-' and decision_key in seen_ids:
                    continue
                seen_ids.add(decision_key)

                yield record


def fetch_sample(session: requests.Session, count: int = 15, save_dir: Path = None) -> list[dict]:
    """Fetch a sample of decisions. Saves incrementally to avoid data loss."""
    records = []
    save_dir = save_dir or SAMPLE_DIR
    save_dir.mkdir(parents=True, exist_ok=True)

    # Fetch from French site only for sample (more consistent naming)
    print(f"Fetching sample of {count} decisions...", file=sys.stderr)
    pdf_links = fetch_all_pdf_links(session, language='fr', max_pages=2)

    for pdf_info in pdf_links[:count + 5]:
        if len(records) >= count:
            break

        print(f"Fetching {pdf_info['filename']}...", file=sys.stderr)
        time.sleep(RATE_LIMIT_DELAY)

        full_decision = fetch_decision_with_text(session, pdf_info)
        if full_decision and full_decision.get('text'):
            record = normalize(full_decision)
            records.append(record)

            # Save incrementally
            filepath = save_dir / f"record_{len(records)-1:04d}.json"
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"  Extracted {len(full_decision['text'])} chars, saved to {filepath.name}", file=sys.stderr)

    return records


def save_samples(records: list[dict]) -> None:
    """Save sample records to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    # Also save all samples in one file
    all_samples = SAMPLE_DIR / "all_samples.json"
    with open(all_samples, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(records)} samples to {SAMPLE_DIR}")


def main():
    parser = argparse.ArgumentParser(description="BE/APD Data Protection Authority Fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Bootstrap command
    bootstrap_parser = subparsers.add_parser("bootstrap", help="Initial data fetch")
    bootstrap_parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    bootstrap_parser.add_argument("--full", action="store_true", help="Full fetch")
    bootstrap_parser.add_argument("--count", type=int, default=15, help="Number of samples")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if not PDF_AVAILABLE:
        print("ERROR: pypdf library required for PDF text extraction", file=sys.stderr)
        sys.exit(1)

    session = get_session()

    if args.command == "bootstrap":
        if args.sample:
            print(f"Fetching {args.count} sample records...", file=sys.stderr)
            records = fetch_sample(session, args.count)

            if records:
                save_samples(records)

                # Print summary
                text_lengths = [len(r.get('text', '')) for r in records]
                fines = [r.get('fine_amount') for r in records if r.get('fine_amount')]
                types = {}
                for r in records:
                    dt = r.get('decision_type', 'unknown')
                    types[dt] = types.get(dt, 0) + 1

                print(f"\n=== Sample Statistics ===", file=sys.stderr)
                print(f"  Records: {len(records)}", file=sys.stderr)
                print(f"  Avg text length: {sum(text_lengths) / len(text_lengths):.0f} chars", file=sys.stderr)
                print(f"  Min text length: {min(text_lengths)} chars", file=sys.stderr)
                print(f"  Max text length: {max(text_lengths)} chars", file=sys.stderr)
                if fines:
                    print(f"  Fines found: {len(fines)} (total €{sum(fines):,.0f})", file=sys.stderr)
                print(f"\nBy decision type:", file=sys.stderr)
                for dtype, cnt in sorted(types.items(), key=lambda x: -x[1]):
                    print(f"  {dtype}: {cnt}", file=sys.stderr)
            else:
                print("No records fetched!", file=sys.stderr)
                sys.exit(1)

        elif args.full:
            print("Starting full fetch...", file=sys.stderr)
            count = 0
            for record in fetch_all(session):
                count += 1
                print(f"Fetched: {record.get('title', 'unknown')[:60]}", file=sys.stderr)
            print(f"Total: {count} decisions", file=sys.stderr)


if __name__ == "__main__":
    main()
