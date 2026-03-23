#!/usr/bin/env python3
"""
FR/ANC - Autorité des Normes Comptables (French Accounting Standards Authority)

Fetches French accounting regulations and opinions from the ANC website.
Documents are published as PDFs and text is extracted for full-text indexing.

Data sources:
- Regulations 2022+ (new site structure)
- Regulations 2018-2021
- Avis (opinions) from the ANC
- Other publications (saisines, responses)

License: Open Licence Etalab

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py bootstrap --full     # Full fetch all documents
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
from urllib.parse import urljoin, unquote

import requests
from bs4 import BeautifulSoup

try:
    import pypdf
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    print("Warning: pypdf not available, PDF text extraction disabled", file=sys.stderr)

# Constants
SOURCE_ID = "FR/ANC"
BASE_URL = "https://www.anc.gouv.fr"
RATE_LIMIT_DELAY = 1.5  # seconds between requests
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"

# All listing pages to scrape
LISTING_PAGES = [
    # Regulations 2022 and later
    {
        "url": f"{BASE_URL}/normes-comptables-francaises/reglements-de-lanc",
        "doc_type": "regulation",
        "category": "regulations_2022_plus",
    },
    # Regulations 2018-2021
    {
        "url": f"{BASE_URL}/reglements-de-lanc-2018-2021",
        "doc_type": "regulation",
        "category": "regulations_2018_2021",
    },
    # Avis (opinions)
    {
        "url": f"{BASE_URL}/normes-comptables-francaises/avis-de-lanc",
        "doc_type": "avis",
        "category": "avis",
    },
    # Other publications
    {
        "url": f"{BASE_URL}/normes-comptables-francaises/autres-publications",
        "doc_type": "publication",
        "category": "autres_publications",
    },
    # Recueils main page
    {
        "url": f"{BASE_URL}/normes-comptables-francaises/recueils-des-normes-comptables",
        "doc_type": "recueil",
        "category": "recueils_main",
    },
]

# Detail pages with recueils (consolidated standards by sector and year)
RECUEIL_DETAIL_PAGES = [
    # Plan Comptable Général (PCG) - different years
    f"{BASE_URL}/pcg-reglement-ndeg-2014-03-du-5-juin-2014-relatif-au-plan-comptable-general",
    f"{BASE_URL}/pcg-2016",
    f"{BASE_URL}/pcg-2017",
    f"{BASE_URL}/pcg-2019",
    f"{BASE_URL}/pcg-2023",
    # Industrial and commercial enterprises - different years
    f"{BASE_URL}/entreprises-industrielles-et-commerciales-juillet-2014",
    f"{BASE_URL}/entreprises-industrielles-et-commerciales-novembre-2014",
    f"{BASE_URL}/entreprises-industrielles-et-commerciales-2016",
    f"{BASE_URL}/entreprises-industrielles-et-commerciales-2017",
    f"{BASE_URL}/entreprises-industrielles-et-commerciales-2018",
    f"{BASE_URL}/entreprises-industrielles-et-commerciales-2020",
    f"{BASE_URL}/entreprises-industrielles-et-commerciales-2021",
    f"{BASE_URL}/entreprises-industrielles-et-commerciales-2022",
    f"{BASE_URL}/entreprises-industrielles-et-commerciales-2023",
    f"{BASE_URL}/entreprises-industrielles-et-commerciales-2024",
    # Banking sector
    f"{BASE_URL}/entreprises-du-secteur-bancaire-version-decembre-2015",
    f"{BASE_URL}/entreprises-du-secteur-bancaire-version-au-30-decembre-2020",
    # Insurance sector
    f"{BASE_URL}/entreprises-dassurance-version-consolidee-au-31-decembre-2016",
    f"{BASE_URL}/entreprises-dassurance-version-consolidee-au-31-decembre-2018",
    f"{BASE_URL}/entreprises-dassurance-version-consolidee-au-30-decembre-2020",
    f"{BASE_URL}/entreprises-dassurance-version-consolidee-au-31-decembre-2023",
    # Non-profit sector
    f"{BASE_URL}/entites-du-secteur-non-lucratif-en-vigueur-au-1er-janvier-2023",
    f"{BASE_URL}/entites-du-secteur-non-lucratif-en-vigueur-au-1er-janvier-2024",
    # Asset management
    f"{BASE_URL}/entreprises-du-secteur-de-la-gestion-dactifs-en-vigueur-au-1er-octobre-2023",
    # Consolidated accounts
    f"{BASE_URL}/comptes-consolides-des-societes-commerciales-et-des-entreprises-publiques-version-consolidee-du",
    f"{BASE_URL}/comptes-consolides-des-entreprises-du-secteur-bancaire-version-consolidee-du-reglement-crc-ndeg99",
    f"{BASE_URL}/comptes-consolides-et-combines-des-entreprises-regies-par-le-code-des-assurances-et-des",
    f"{BASE_URL}/reglement-ndeg-2020-01-du-6-mars-2020-relatif-aux-comptes-consolides-version-recueil-au-1er-janvier",
    f"{BASE_URL}/reglement-ndeg-2020-01-du-6-mars-2020-relatif-aux-comptes-consolides-version-recueil-au-1er-0",
    f"{BASE_URL}/reglement-ndeg-2020-01-du-6-mars-2020-relatif-aux-comptes-consolides-version-recueil-au-1er-1",
]

# Paths
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
DATA_DIR = SCRIPT_DIR / "data"


def get_session() -> requests.Session:
    """Create requests session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    })
    return session


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

        # Clean up the text
        full_text = re.sub(r'[ \t]+', ' ', full_text)
        full_text = re.sub(r'\n\s*\n\s*\n+', '\n\n', full_text)

        return full_text.strip()
    except Exception as e:
        print(f"Error extracting text from PDF: {e}", file=sys.stderr)
        return ""


def fetch_detail_page_urls(session: requests.Session, listing_url: str, doc_type: str) -> list[dict]:
    """Fetch all detail page URLs from a listing page."""
    results = []

    print(f"Fetching listing page: {listing_url}", file=sys.stderr)
    try:
        response = session.get(listing_url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching listing page {listing_url}: {e}", file=sys.stderr)
        return results

    soup = BeautifulSoup(response.text, 'html.parser')

    # Pattern matching for different document types
    patterns = {
        "regulation": [r'/reglement[s]?[-_]', r'ndeg\d{4}[-_]\d{2}'],
        "avis": [r'/avis[-_]ndeg', r'/avis[-_]\d{4}'],
        "publication": [r'/autres[-_]publications/', r'/saisine', r'/reponse'],
    }

    seen_urls = set()
    for link in soup.find_all('a', href=True):
        href = link['href']
        href_lower = href.lower()

        # Check if this link matches our document type patterns
        is_match = False
        for pattern in patterns.get(doc_type, []):
            if re.search(pattern, href_lower):
                is_match = True
                break

        # Also match generic detail page patterns
        if not is_match:
            if doc_type == "regulation" and ('reglement' in href_lower or 'reglt' in href_lower):
                is_match = True
            elif doc_type == "avis" and 'avis' in href_lower and 'ndeg' in href_lower:
                is_match = True

        if is_match:
            full_url = urljoin(BASE_URL, href)
            # Filter out social sharing links and anchors
            if 'facebook.com' in full_url or 'twitter.com' in full_url or 'linkedin.com' in full_url:
                continue
            if full_url.endswith('#'):
                continue
            if '#' in full_url and not full_url.split('#')[0] in seen_urls:
                # Remove anchor for deduplication
                full_url = full_url.split('#')[0]

            if full_url not in seen_urls and full_url != listing_url:
                seen_urls.add(full_url)
                results.append({
                    "detail_url": full_url,
                    "doc_type": doc_type,
                    "link_text": link.get_text().strip(),
                })

    print(f"Found {len(results)} detail pages for {doc_type}", file=sys.stderr)
    return results


def fetch_pdfs_from_detail_page(session: requests.Session, detail_info: dict) -> list[dict]:
    """Fetch PDF URLs from a detail page."""
    pdfs = []
    detail_url = detail_info["detail_url"]

    try:
        response = session.get(detail_url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching detail page {detail_url}: {e}", file=sys.stderr)
        return pdfs

    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract page title
    title_tag = soup.find('h1')
    page_title = title_tag.get_text().strip() if title_tag else detail_info.get("link_text", "")

    # Find PDF links
    seen_pdfs = set()
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.lower().endswith('.pdf'):
            full_url = urljoin(BASE_URL, href)
            if full_url in seen_pdfs:
                continue
            seen_pdfs.add(full_url)

            # Determine version type from URL or link text
            link_text = link.get_text().lower()
            url_lower = full_url.lower()

            if 'recueil' in url_lower or 'comment' in link_text or 'comment' in url_lower:
                version = 'recueil'
            elif '_jo' in url_lower or 'journal' in link_text:
                version = 'JO'
            elif 'web' in url_lower:
                version = 'web'
            else:
                version = 'standard'

            pdfs.append({
                'url': full_url,
                'version': version,
                'link_text': link.get_text().strip(),
                'doc_type': detail_info["doc_type"],
                'page_title': page_title,
                'detail_url': detail_url,
            })

    return pdfs


def fetch_direct_pdfs_from_listing(session: requests.Session, listing_url: str, doc_type: str) -> list[dict]:
    """Fetch PDFs directly linked from a listing page (for pages without detail pages)."""
    pdfs = []

    try:
        response = session.get(listing_url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching listing page {listing_url}: {e}", file=sys.stderr)
        return pdfs

    soup = BeautifulSoup(response.text, 'html.parser')

    seen_pdfs = set()
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.lower().endswith('.pdf'):
            full_url = urljoin(BASE_URL, href)
            if full_url in seen_pdfs:
                continue
            seen_pdfs.add(full_url)

            pdfs.append({
                'url': full_url,
                'version': 'standard',
                'link_text': link.get_text().strip(),
                'doc_type': doc_type,
                'page_title': '',
                'detail_url': listing_url,
            })

    return pdfs


def parse_document_info_from_url(url: str, doc_type: str) -> dict:
    """Extract document number and date from URL."""
    info = {
        'document_number': None,
        'year': None,
        'doc_type': doc_type,
    }

    url_lower = url.lower()

    # Regulation patterns: REGLT_2024_07, ndeg2024-07
    if doc_type == "regulation":
        match = re.search(r'REGLT[_-]?(\d{4})[_-](\d{2})', url, re.IGNORECASE)
        if match:
            year = match.group(1)
            number = match.group(2)
            info['document_number'] = f"ANC N°{year}-{number}"
            info['year'] = year
        else:
            match = re.search(r'ndeg[_-]?(\d{4})[_-](\d{2})', url, re.IGNORECASE)
            if match:
                year = match.group(1)
                number = match.group(2)
                info['document_number'] = f"ANC N°{year}-{number}"
                info['year'] = year

    # Avis patterns: Avis-2025_01, avis-ndeg-2025-01
    elif doc_type == "avis":
        match = re.search(r'[Aa]vis[_-]?(\d{4})[_-](\d{2})', url)
        if match:
            year = match.group(1)
            number = match.group(2)
            info['document_number'] = f"Avis ANC N°{year}-{number}"
            info['year'] = year
        else:
            match = re.search(r'avis[_-]ndeg[_-]?(\d{4})[_-](\d{2})', url, re.IGNORECASE)
            if match:
                year = match.group(1)
                number = match.group(2)
                info['document_number'] = f"Avis ANC N°{year}-{number}"
                info['year'] = year

    # Generic year extraction fallback
    if not info['year']:
        year_match = re.search(r'/(\d{4})/', url)
        if year_match:
            info['year'] = year_match.group(1)

    return info


def fetch_document_with_text(session: requests.Session, pdf_info: dict) -> Optional[dict]:
    """Download PDF and extract text for a document."""
    url = pdf_info['url']

    try:
        response = session.get(url, timeout=120)
        response.raise_for_status()
        pdf_content = response.content
    except requests.RequestException as e:
        print(f"Error downloading PDF {url}: {e}", file=sys.stderr)
        return None

    # Check PDF size (skip very large files)
    if len(pdf_content) > 50_000_000:  # 50 MB
        print(f"Skipping very large PDF ({len(pdf_content)} bytes): {url}", file=sys.stderr)
        return None

    # Extract text
    text = extract_text_from_pdf(pdf_content)
    if not text or len(text) < 100:
        print(f"Warning: Could not extract meaningful text from {url}", file=sys.stderr)
        return None

    # Parse document info from URL
    doc_info = parse_document_info_from_url(url, pdf_info.get('doc_type', 'regulation'))

    # Extract title from first lines of text or page title
    title = pdf_info.get('page_title', '')
    if not title or len(title) < 10:
        lines = text.split('\n')
        title_lines = []
        for line in lines[:10]:
            line = line.strip()
            if line and len(line) > 5:
                title_lines.append(line)
                if len(' '.join(title_lines)) > 100:
                    break
        title = ' '.join(title_lines)[:200] if title_lines else doc_info.get('document_number', 'Document ANC')

    # Try to extract date from text
    adoption_date = None
    date_match = re.search(
        r'(\d{1,2})\s+(janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+(\d{4})',
        text, re.IGNORECASE
    )
    if date_match:
        day = date_match.group(1).zfill(2)
        month_name = date_match.group(2).lower()
        year = date_match.group(3)
        months = {
            'janvier': '01', 'février': '02', 'mars': '03', 'avril': '04',
            'mai': '05', 'juin': '06', 'juillet': '07', 'août': '08',
            'septembre': '09', 'octobre': '10', 'novembre': '11', 'décembre': '12'
        }
        month = months.get(month_name, '01')
        adoption_date = f"{year}-{month}-{day}"

    return {
        'document_number': doc_info.get('document_number'),
        'year': doc_info.get('year'),
        'doc_type': doc_info.get('doc_type', 'regulation'),
        'title': title,
        'text': text,
        'url': url,
        'detail_url': pdf_info.get('detail_url', ''),
        'version': pdf_info.get('version', 'unknown'),
        'adoption_date': adoption_date,
        'pdf_size': len(pdf_content),
    }


def normalize(raw: dict) -> dict:
    """Transform raw ANC document data into normalized schema."""
    doc_num = raw.get('document_number', '')
    doc_type = raw.get('doc_type', 'regulation')

    # Create unique ID
    version = raw.get('version', 'unknown')
    if doc_num:
        doc_id = f"{SOURCE_ID}/{doc_num}_{version}".replace(' ', '_').replace('°', '')
    else:
        # Fallback: use URL hash
        import hashlib
        url_hash = hashlib.md5(raw.get('url', '').encode()).hexdigest()[:8]
        doc_id = f"{SOURCE_ID}/doc_{url_hash}_{version}"

    # Determine _type based on document type
    if doc_type == 'avis':
        _type = 'doctrine'
    elif doc_type == 'publication':
        _type = 'doctrine'
    elif doc_type == 'recueil':
        _type = 'legislation'  # recueils are consolidated legislation
    else:
        _type = 'legislation'

    return {
        '_id': doc_id,
        '_source': SOURCE_ID,
        '_type': _type,
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': raw.get('title', ''),
        'text': raw.get('text', ''),
        'date': raw.get('adoption_date'),
        'url': raw.get('url', ''),
        'detail_url': raw.get('detail_url', ''),
        'document_number': doc_num,
        'year': raw.get('year'),
        'version': version,
        'doc_type': doc_type,
        'pdf_size': raw.get('pdf_size'),
    }


def fetch_recueil_detail_page(session: requests.Session, page_url: str) -> list[dict]:
    """Fetch PDF URLs from a recueil detail page."""
    pdfs = []

    print(f"Fetching recueil page: {page_url}", file=sys.stderr)
    try:
        response = session.get(page_url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching recueil page {page_url}: {e}", file=sys.stderr)
        return pdfs

    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract page title
    title_tag = soup.find('h1')
    page_title = title_tag.get_text().strip() if title_tag else ""

    # Find PDF links
    seen_pdfs = set()
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.lower().endswith('.pdf'):
            full_url = urljoin(BASE_URL, href)
            if full_url in seen_pdfs:
                continue
            seen_pdfs.add(full_url)

            pdfs.append({
                'url': full_url,
                'version': 'recueil',
                'link_text': link.get_text().strip(),
                'doc_type': 'recueil',
                'page_title': page_title,
                'detail_url': page_url,
            })

    print(f"Found {len(pdfs)} PDFs on recueil page", file=sys.stderr)
    return pdfs


def fetch_all(session: requests.Session) -> Generator[dict, None, None]:
    """Fetch all ANC documents from all listing pages."""
    fetched_urls = set()

    for listing in LISTING_PAGES:
        listing_url = listing["url"]
        doc_type = listing["doc_type"]
        category = listing["category"]

        print(f"\n=== Processing {category} ===", file=sys.stderr)

        # First, get detail page URLs from the listing
        detail_pages = fetch_detail_page_urls(session, listing_url, doc_type)
        time.sleep(RATE_LIMIT_DELAY)

        # Process each detail page
        for detail_info in detail_pages:
            time.sleep(RATE_LIMIT_DELAY)
            pdf_infos = fetch_pdfs_from_detail_page(session, detail_info)

            for pdf_info in pdf_infos:
                if pdf_info['url'] in fetched_urls:
                    continue
                fetched_urls.add(pdf_info['url'])

                time.sleep(RATE_LIMIT_DELAY)
                raw = fetch_document_with_text(session, pdf_info)
                if raw and raw.get('text'):
                    yield normalize(raw)

        # Also fetch any PDFs directly linked from the listing page
        # (e.g., "autres publications" often has direct links)
        time.sleep(RATE_LIMIT_DELAY)
        direct_pdfs = fetch_direct_pdfs_from_listing(session, listing_url, doc_type)

        for pdf_info in direct_pdfs:
            if pdf_info['url'] in fetched_urls:
                continue
            fetched_urls.add(pdf_info['url'])

            time.sleep(RATE_LIMIT_DELAY)
            raw = fetch_document_with_text(session, pdf_info)
            if raw and raw.get('text'):
                yield normalize(raw)

    # Process recueil detail pages (consolidated standards by sector/year)
    print(f"\n=== Processing recueil detail pages ({len(RECUEIL_DETAIL_PAGES)} pages) ===", file=sys.stderr)

    for page_url in RECUEIL_DETAIL_PAGES:
        time.sleep(RATE_LIMIT_DELAY)
        pdf_infos = fetch_recueil_detail_page(session, page_url)

        for pdf_info in pdf_infos:
            if pdf_info['url'] in fetched_urls:
                continue
            fetched_urls.add(pdf_info['url'])

            time.sleep(RATE_LIMIT_DELAY)
            raw = fetch_document_with_text(session, pdf_info)
            if raw and raw.get('text'):
                yield normalize(raw)


def fetch_sample(session: requests.Session, count: int = 15) -> list[dict]:
    """Fetch a sample of documents from various categories."""
    records = []
    fetched_urls = set()

    # Allocate samples: 2/3 from listing pages, 1/3 from recueil pages
    listing_samples = max(count * 2 // 3, 10)
    recueil_samples = count - listing_samples

    # Sample from each listing category
    samples_per_category = max(2, listing_samples // len(LISTING_PAGES))

    for listing in LISTING_PAGES:
        if len(records) >= listing_samples:
            break

        listing_url = listing["url"]
        doc_type = listing["doc_type"]
        category = listing["category"]

        print(f"\n=== Sampling from {category} ===", file=sys.stderr)

        # Get detail pages
        detail_pages = fetch_detail_page_urls(session, listing_url, doc_type)
        time.sleep(RATE_LIMIT_DELAY)

        category_count = 0
        for detail_info in detail_pages[:samples_per_category + 2]:  # extra buffer
            if category_count >= samples_per_category or len(records) >= listing_samples:
                break

            time.sleep(RATE_LIMIT_DELAY)
            pdf_infos = fetch_pdfs_from_detail_page(session, detail_info)

            # Take first PDF from each detail page
            for pdf_info in pdf_infos[:1]:
                if pdf_info['url'] in fetched_urls:
                    continue
                fetched_urls.add(pdf_info['url'])

                print(f"Fetching {pdf_info['url']}...", file=sys.stderr)
                time.sleep(RATE_LIMIT_DELAY)

                raw = fetch_document_with_text(session, pdf_info)
                if raw and raw.get('text'):
                    records.append(normalize(raw))
                    category_count += 1
                    print(f"  Extracted {len(raw['text'])} chars", file=sys.stderr)

                    if len(records) >= listing_samples:
                        break

    # Sample from recueil detail pages
    print(f"\n=== Sampling from recueil pages ===", file=sys.stderr)
    recueil_count = 0
    for page_url in RECUEIL_DETAIL_PAGES[:recueil_samples + 3]:  # extra buffer
        if recueil_count >= recueil_samples or len(records) >= count:
            break

        time.sleep(RATE_LIMIT_DELAY)
        pdf_infos = fetch_recueil_detail_page(session, page_url)

        # Take first PDF from each recueil page
        for pdf_info in pdf_infos[:1]:
            if pdf_info['url'] in fetched_urls:
                continue
            fetched_urls.add(pdf_info['url'])

            print(f"Fetching {pdf_info['url']}...", file=sys.stderr)
            time.sleep(RATE_LIMIT_DELAY)

            raw = fetch_document_with_text(session, pdf_info)
            if raw and raw.get('text'):
                records.append(normalize(raw))
                recueil_count += 1
                print(f"  Extracted {len(raw['text'])} chars", file=sys.stderr)

                if len(records) >= count:
                    break

    return records


def save_samples(records: list[dict]) -> None:
    """Save sample records to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    for i, record in enumerate(records):
        filepath = SAMPLE_DIR / f"record_{i:04d}.json"
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    # Also save all samples in one file
    all_samples = SAMPLE_DIR / "all_samples.json"
    with open(all_samples, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(records)} samples to {SAMPLE_DIR}")


def main():
    parser = argparse.ArgumentParser(description="FR/ANC Accounting Standards Fetcher")
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
                doc_types = {}
                for r in records:
                    dt = r.get('doc_type', 'unknown')
                    doc_types[dt] = doc_types.get(dt, 0) + 1

                print(f"\nSummary:")
                print(f"  Records: {len(records)}")
                print(f"  Avg text length: {sum(text_lengths) / len(text_lengths):.0f} chars")
                print(f"  Min text length: {min(text_lengths)} chars")
                print(f"  Max text length: {max(text_lengths)} chars")
                print(f"  Document types: {doc_types}")
            else:
                print("No records fetched!", file=sys.stderr)
                sys.exit(1)

        elif args.full:
            print("Starting full fetch...", file=sys.stderr)
            count = 0
            for record in fetch_all(session):
                count += 1
                print(f"Fetched: {record.get('document_number', 'unknown')}", file=sys.stderr)
            print(f"Total: {count} documents", file=sys.stderr)


if __name__ == "__main__":
    main()
