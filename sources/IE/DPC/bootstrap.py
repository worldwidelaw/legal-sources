#!/usr/bin/env python3
"""
IE/DPC - Irish Data Protection Commission

Fetches DPC enforcement decisions, court judgments, and case studies.
Ireland hosts EU headquarters for Meta, Google, Apple, Microsoft, TikTok etc.,
making DPC decisions highly significant for EU-wide GDPR enforcement.

Data sources:
- Decisions: https://www.dataprotection.ie/en/dpc-guidance/law/decisions-made-under-data-protection-act-2018
- Judgments: https://www.dataprotection.ie/en/dpc-guidance/law/judgments
- Case Studies: https://www.dataprotection.ie/en/dpc-guidance/case-studies/*

License: Irish Public Sector Open Licence

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
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


# Constants
SOURCE_ID = "IE/DPC"
BASE_URL = "https://www.dataprotection.ie"
DECISIONS_URL = f"{BASE_URL}/en/dpc-guidance/law/decisions-made-under-data-protection-act-2018"
JUDGMENTS_URL = f"{BASE_URL}/en/dpc-guidance/law/judgments"
CASE_STUDIES_URL = f"{BASE_URL}/en/dpc-guidance/case-studies"
RATE_LIMIT_DELAY = 2.0
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"

# Case study categories
CASE_STUDY_CATEGORIES = [
    'access-request-complaints',
    'accuracy',
    'cctv',
    'cross-border-complaints',
    'data-breach-notification',
    'disclosure-unauthorised-disclosure',
    'electronic-direct-marketing',
    'erasure',
    'general-accountability',
    'law-enforcement-directive',
    'misc',
    'objection-to-processing',
    'purpose-limitation',
    'transparency',
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
        "Accept-Language": "en-IE,en;q=0.9",
    })
    return session


def extract_text_from_pdf(pdf_content: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="IE/DPC",
        source_id="",
        pdf_bytes=pdf_content,
        table="case_law",
    ) or ""

def fetch_decision_page_urls(session: requests.Session) -> list[dict]:
    """Fetch all decision page URLs from the listing page."""
    decisions = []

    print("Fetching decisions listing page...", file=sys.stderr)
    try:
        response = session.get(DECISIONS_URL, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching listing page: {e}", file=sys.stderr)
        return decisions

    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all links to decision pages - multiple URL patterns exist
    # The page uses various paths including:
    # - /en/dpc-guidance/law/decisions/...
    # - /en/dpc-guidance/law/decisions-made-under-data-protection-act-2018/...
    # - /en/resources/law/decisions/...
    # - /en/dpc-guidance/resources/law/decisions/...
    # - /en/treoir-ccs/law/decisions/... (Irish language)
    decision_patterns = [
        '/decisions/',
        '/law/decisions/',
        '/decisions-made-under-data-protection-act-2018/'
    ]

    for link in soup.find_all('a', href=True):
        href = link['href']

        # Check if it's a decision link (not a PDF directly)
        # Look for decision-related URL patterns, but exclude the main listing page itself
        is_decision = any(pattern in href for pattern in decision_patterns)
        is_listing_page = href.rstrip('/').endswith('decisions-made-under-data-protection-act-2018')
        if is_decision and not href.endswith('.pdf') and not is_listing_page:
            # Normalize URL
            if href.startswith('http'):
                full_url = href
            else:
                full_url = urljoin(BASE_URL, href)

            # Extract title from link text
            title = link.get_text().strip()
            if title and full_url not in [d['url'] for d in decisions]:
                decisions.append({
                    'url': full_url,
                    'title': title,
                })

    print(f"Found {len(decisions)} decision pages", file=sys.stderr)
    return decisions


def fetch_decision_details(session: requests.Session, decision_info: dict) -> Optional[dict]:
    """Fetch decision page and extract PDF link and metadata."""
    url = decision_info['url']

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching decision page {url}: {e}", file=sys.stderr)
        return None

    soup = BeautifulSoup(response.text, 'html.parser')

    # Find PDF links
    pdf_urls = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.lower().endswith('.pdf'):
            pdf_url = urljoin(BASE_URL, href) if not href.startswith('http') else href
            pdf_urls.append(pdf_url)

    if not pdf_urls:
        print(f"No PDF found for {url}", file=sys.stderr)
        return None

    # Use the first PDF (usually the main decision)
    pdf_url = pdf_urls[0]

    # Extract metadata from the page
    title = decision_info.get('title', '')

    # Try to get better title from page
    h1 = soup.find('h1')
    if h1:
        title = h1.get_text().strip()

    # Extract date from title or page content
    date = None
    date_patterns = [
        r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
        r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
        r'(\d{4})-(\d{2})-(\d{2})',
    ]

    months = {
        'January': '01', 'February': '02', 'March': '03', 'April': '04',
        'May': '05', 'June': '06', 'July': '07', 'August': '08',
        'September': '09', 'October': '10', 'November': '11', 'December': '12'
    }

    page_text = soup.get_text()
    for pattern in date_patterns:
        match = re.search(pattern, page_text, re.IGNORECASE)
        if match:
            groups = match.groups()
            if len(groups) == 3 and groups[0].isdigit():
                # Pattern: DD Month YYYY
                day = groups[0].zfill(2)
                month = months.get(groups[1].title(), '01')
                year = groups[2]
                date = f"{year}-{month}-{day}"
            elif len(groups) == 2:
                # Pattern: Month YYYY
                month = months.get(groups[0].title(), '01')
                year = groups[1]
                date = f"{year}-{month}-01"
            break

    # Try to extract organization name from title
    organization = None
    org_patterns = [
        r'(?:Inquiry\s+(?:into|concerning)\s+)([\w\s,]+?)(?:\s*[-–—]\s*|\s+\d)',
        r'^([\w\s,]+?)(?:\s*[-–—]\s*|\s+(?:January|February|March|April|May|June|July|August|September|October|November|December))',
    ]
    for pattern in org_patterns:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            organization = match.group(1).strip()
            break

    return {
        'url': url,
        'pdf_url': pdf_url,
        'title': title,
        'date': date,
        'organization': organization,
    }


def fetch_decision_with_text(session: requests.Session, decision: dict, max_pdf_size_mb: float = 15.0) -> Optional[dict]:
    """Download PDF and extract text for a decision."""
    pdf_url = decision.get('pdf_url')
    if not pdf_url:
        return None

    try:
        # First check PDF size with HEAD request
        head_resp = session.head(pdf_url, timeout=20)
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

    # Try to extract fine amount from text
    fine_amount = None
    fine_patterns = [
        r'€\s*([\d,]+(?:\.\d{2})?)\s*(?:million|m)',
        r'([\d,]+(?:\.\d{2})?)\s*(?:million|m)\s*(?:euro|EUR|€)',
        r'fine\s+of\s+€?\s*([\d,]+(?:\.\d{2})?)',
        r'administrative\s+fine[s]?\s+(?:of\s+)?(?:totaling\s+)?€?\s*([\d,]+(?:\.\d{2})?)',
    ]
    for pattern in fine_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            amount_str = match.group(1).replace(',', '')
            try:
                amount = float(amount_str)
                if 'million' in match.group(0).lower() or 'm' in match.group(0).lower():
                    amount *= 1_000_000
                fine_amount = amount
                break
            except ValueError:
                pass

    # Try to extract GDPR articles mentioned
    gdpr_articles = []
    article_matches = re.findall(r'Article\s+(\d+(?:\(\d+\))?(?:\([a-z]\))?)', text, re.IGNORECASE)
    gdpr_articles = list(set(article_matches))[:10]  # Limit to 10 unique articles

    decision['text'] = text
    decision['pdf_size'] = len(pdf_content)
    decision['fine_amount'] = fine_amount
    decision['gdpr_articles'] = gdpr_articles

    return decision


def fetch_judgment_urls(session: requests.Session) -> list[dict]:
    """Fetch all judgment URLs from the judgments page."""
    judgments = []

    print("Fetching judgments listing page...", file=sys.stderr)
    try:
        response = session.get(JUDGMENTS_URL, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching judgments page: {e}", file=sys.stderr)
        return judgments

    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all links that look like judgment links (contain "v" and DPC-related)
    for link in soup.find_all('a', href=True):
        href = link['href']
        text = link.get_text().strip()

        # Skip empty links or non-judgment links
        if not text or len(text) < 10:
            continue

        # Look for case-style names (X v Y format)
        if ' v ' not in text.lower() and ' v. ' not in text.lower():
            continue

        # Must reference DPC or Data Protection
        if 'dpc' not in text.lower() and 'data protection' not in text.lower():
            continue

        # Normalize URL
        if href.startswith('http'):
            full_url = href
        else:
            full_url = urljoin(BASE_URL, href)

        # Skip if already seen
        if full_url in [j['url'] for j in judgments]:
            continue

        # Extract date from title if present
        date = None
        date_patterns = [
            r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
        ]
        months = {
            'January': '01', 'February': '02', 'March': '03', 'April': '04',
            'May': '05', 'June': '06', 'July': '07', 'August': '08',
            'September': '09', 'October': '10', 'November': '11', 'December': '12'
        }
        for pattern in date_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                groups = match.groups()
                if len(groups) == 3:
                    day = groups[0].zfill(2)
                    month = months.get(groups[1].title(), '01')
                    year = groups[2]
                    date = f"{year}-{month}-{day}"
                elif len(groups) == 2:
                    month = months.get(groups[0].title(), '01')
                    year = groups[1]
                    date = f"{year}-{month}-01"
                break

        judgments.append({
            'url': full_url,
            'title': text,
            'date': date,
        })

    print(f"Found {len(judgments)} judgment links", file=sys.stderr)
    return judgments


def fetch_judgment_text(session: requests.Session, judgment_info: dict) -> Optional[dict]:
    """Fetch judgment PDF and extract text."""
    url = judgment_info['url']

    # Remove fragment (e.g., #view=fitH)
    if '#' in url:
        url = url.split('#')[0]

    # Check if URL is a PDF (external court sites link directly to PDFs)
    is_pdf = (url.lower().endswith('.pdf') or
              '/pdf/' in url.lower() or
              '/alfresco/' in url.lower() or  # courts.ie alfresco links are PDFs
              'viewer/pdf' in url.lower())

    if is_pdf:
        # Direct PDF link
        try:
            head_resp = session.head(url, timeout=20)
            content_length = head_resp.headers.get('Content-Length')
            if content_length:
                size_mb = int(content_length) / (1024 * 1024)
                if size_mb > 15.0:
                    print(f"Skipping large PDF ({size_mb:.1f} MB): {url}", file=sys.stderr)
                    return None

            response = session.get(url, timeout=180)
            response.raise_for_status()
            text = extract_text_from_pdf(response.content)

            if not text or len(text) < 100:
                print(f"Warning: Could not extract text from {url}", file=sys.stderr)
                return None

            judgment_info['text'] = text
            judgment_info['pdf_url'] = url
            judgment_info['pdf_size'] = len(response.content)
            return judgment_info

        except requests.RequestException as e:
            print(f"Error fetching PDF {url}: {e}", file=sys.stderr)
            return None
    else:
        # Internal DPC page with PDF links
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching judgment page {url}: {e}", file=sys.stderr)
            return None

        soup = BeautifulSoup(response.text, 'html.parser')

        # Find PDF links
        pdf_urls = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.lower().endswith('.pdf'):
                pdf_url = urljoin(BASE_URL, href) if not href.startswith('http') else href
                pdf_urls.append(pdf_url)

        if not pdf_urls:
            print(f"No PDF found for judgment {url}", file=sys.stderr)
            return None

        pdf_url = pdf_urls[0]
        try:
            response = session.get(pdf_url, timeout=180)
            response.raise_for_status()
            text = extract_text_from_pdf(response.content)

            if not text or len(text) < 100:
                print(f"Warning: Could not extract text from {pdf_url}", file=sys.stderr)
                return None

            judgment_info['text'] = text
            judgment_info['pdf_url'] = pdf_url
            judgment_info['pdf_size'] = len(response.content)
            return judgment_info

        except requests.RequestException as e:
            print(f"Error downloading PDF {pdf_url}: {e}", file=sys.stderr)
            return None


def fetch_case_study_urls(session: requests.Session, category: str) -> list[dict]:
    """Fetch all case study URLs from a category listing page."""
    case_studies = []
    url = f"{CASE_STUDIES_URL}/{category}"

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching category {category}: {e}", file=sys.stderr)
        return case_studies

    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all case study boxes
    boxes = soup.find_all('div', class_='faq-section-results-box')

    for box in boxes:
        # Extract title and URL from the h3 link
        h3 = box.find('h3')
        if not h3:
            continue

        link = h3.find('a', href=True)
        if not link:
            continue

        href = link['href']
        title = link.get_text(strip=True)

        # Get the full URL
        if href.startswith('http'):
            detail_url = href
        else:
            detail_url = urljoin(BASE_URL, href)

        # Extract year from the datetime element
        year = None
        time_elem = box.find('time', class_='datetime')
        if time_elem:
            year = time_elem.get_text(strip=True)

        # Extract summary text
        summary = ''
        p = box.find('p')
        if p:
            summary = p.get_text(strip=True)

        case_studies.append({
            'url': detail_url,
            'title': title,
            'year': year,
            'category': category,
            'summary': summary,
        })

    return case_studies


def fetch_case_study_detail(session: requests.Session, case_info: dict) -> Optional[dict]:
    """Fetch full text from a case study detail page."""
    url = case_info['url']

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching case study {url}: {e}", file=sys.stderr)
        return None

    soup = BeautifulSoup(response.text, 'html.parser')

    # Find the main content area
    main = soup.find('main') or soup.find('article')
    if not main:
        main = soup

    # Get all text, then clean it up
    text = main.get_text(separator='\n', strip=True)

    # Remove navigation/boilerplate at the start
    # Look for the title and start from there
    title = case_info.get('title', '')
    if title and title in text:
        # Find where the case study content starts
        idx = text.find(title)
        if idx > 0:
            # Find the second occurrence (first is nav, second is content)
            idx2 = text.find(title, idx + len(title))
            if idx2 > 0:
                text = text[idx2:]

    # Clean up: remove footer content
    footer_markers = [
        'Your Data',
        'Data Protection: The Basics',
        'Contact us',
        'Data Protection Commission',
        '6 Pembroke Row',
    ]
    for marker in footer_markers:
        if marker in text:
            idx = text.find(marker)
            if idx > 0 and idx < len(text) - 100:
                text = text[:idx]
            break

    # Clean up whitespace
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
    text = text.strip()

    if len(text) < 100:
        print(f"Warning: case study too short ({len(text)} chars): {url}", file=sys.stderr)
        return None

    # Extract date from year
    date = None
    year = case_info.get('year')
    if year and re.match(r'^20\d{2}$', year):
        date = f"{year}-01-01"

    return {
        'title': case_info.get('title', ''),
        'text': text,
        'date': date,
        'category': case_info.get('category'),
        'url': url,
        '_record_type': 'case_study',
    }


def fetch_case_studies(session: requests.Session) -> Generator[dict, None, None]:
    """Fetch all case studies from all categories."""
    for category in CASE_STUDY_CATEGORIES:
        print(f"Fetching case studies from category: {category}", file=sys.stderr)

        # Get list of case study URLs in this category
        case_study_urls = fetch_case_study_urls(session, category)
        print(f"  Found {len(case_study_urls)} case studies", file=sys.stderr)

        for case_info in case_study_urls:
            time.sleep(RATE_LIMIT_DELAY)

            detail = fetch_case_study_detail(session, case_info)
            if detail and detail.get('text'):
                yield detail


def normalize(raw: dict) -> dict:
    """Transform raw DPC data into normalized schema."""
    record_type = raw.get('_record_type', 'decision')

    # Create unique ID
    if record_type == 'case_study':
        # For case studies, use category + hash of title
        category = raw.get('category', 'misc')
        title_slug = re.sub(r'[^a-z0-9]+', '-', raw.get('title', 'unknown').lower())[:50]
        doc_id = f"{SOURCE_ID}/case-study/{category}/{title_slug}"
        doc_type = 'doctrine'
    elif record_type == 'judgment':
        # For judgments, create a slug from the title since URLs are external PDFs
        title = raw.get('title', 'unknown')
        title_slug = re.sub(r'[^a-z0-9]+', '-', title.lower())[:60].strip('-')
        doc_id = f"{SOURCE_ID}/judgment/{title_slug}"
        doc_type = 'case_law'
    else:
        # Decision
        url_path = urlparse(raw.get('url', '')).path
        slug = url_path.rstrip('/').split('/')[-1] or 'unknown'
        doc_id = f"{SOURCE_ID}/{slug}"
        doc_type = 'case_law'

    return {
        '_id': doc_id,
        '_source': SOURCE_ID,
        '_type': doc_type,
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': raw.get('title', ''),
        'text': raw.get('text', ''),
        'date': raw.get('date'),
        'url': raw.get('url', ''),
        'pdf_url': raw.get('pdf_url'),
        'organization': raw.get('organization'),
        'fine_amount': raw.get('fine_amount'),
        'gdpr_articles': raw.get('gdpr_articles', []),
        'pdf_size': raw.get('pdf_size'),
        'category': raw.get('category'),  # For case studies
        'record_type': record_type,
    }


def fetch_all(session: requests.Session) -> Generator[dict, None, None]:
    """Fetch all DPC decisions, judgments, and case studies."""
    # 1. Fetch decisions
    print("=== Fetching DPC Decisions ===", file=sys.stderr)
    decision_pages = fetch_decision_page_urls(session)

    for decision_info in decision_pages:
        time.sleep(RATE_LIMIT_DELAY)

        details = fetch_decision_details(session, decision_info)
        if not details:
            continue

        time.sleep(RATE_LIMIT_DELAY)

        full_decision = fetch_decision_with_text(session, details)
        if full_decision and full_decision.get('text'):
            full_decision['_record_type'] = 'decision'
            yield normalize(full_decision)

    # 2. Fetch judgments
    print("\n=== Fetching Court Judgments ===", file=sys.stderr)
    judgment_urls = fetch_judgment_urls(session)

    for judgment_info in judgment_urls:
        time.sleep(RATE_LIMIT_DELAY)

        full_judgment = fetch_judgment_text(session, judgment_info)
        if full_judgment and full_judgment.get('text'):
            full_judgment['_record_type'] = 'judgment'
            yield normalize(full_judgment)

    # 3. Fetch case studies
    print("\n=== Fetching Case Studies ===", file=sys.stderr)
    for case_study in fetch_case_studies(session):
        yield normalize(case_study)


def fetch_sample(session: requests.Session, count: int = 15, save_dir: Path = None) -> list[dict]:
    """Fetch a sample of all content types. Saves incrementally to avoid data loss."""
    records = []
    save_dir = save_dir or SAMPLE_DIR
    save_dir.mkdir(parents=True, exist_ok=True)

    # Allocate samples across content types: 5 decisions, 5 judgments, 5 case studies
    decisions_target = min(5, count // 3 + 1)
    judgments_target = min(5, count // 3 + 1)
    case_studies_target = count - decisions_target - judgments_target

    # 1. Fetch decisions
    print("=== Fetching DPC Decisions ===", file=sys.stderr)
    decision_pages = fetch_decision_page_urls(session)
    decision_count = 0

    for decision_info in decision_pages[:decisions_target + 5]:
        if decision_count >= decisions_target:
            break

        print(f"Fetching decision: {decision_info['url'][:60]}...", file=sys.stderr)
        time.sleep(RATE_LIMIT_DELAY)

        details = fetch_decision_details(session, decision_info)
        if not details:
            continue

        time.sleep(RATE_LIMIT_DELAY)

        full_decision = fetch_decision_with_text(session, details)
        if full_decision and full_decision.get('text'):
            full_decision['_record_type'] = 'decision'
            record = normalize(full_decision)
            records.append(record)
            decision_count += 1

            # Save incrementally
            filepath = save_dir / f"record_{len(records)-1:04d}.json"
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"  [Decision] {len(full_decision['text'])} chars -> {filepath.name}", file=sys.stderr)

    # 2. Fetch judgments
    print("\n=== Fetching Court Judgments ===", file=sys.stderr)
    judgment_urls = fetch_judgment_urls(session)
    judgment_count = 0

    for judgment_info in judgment_urls[:judgments_target + 5]:
        if judgment_count >= judgments_target:
            break

        print(f"Fetching judgment: {judgment_info['title'][:50]}...", file=sys.stderr)
        time.sleep(RATE_LIMIT_DELAY)

        full_judgment = fetch_judgment_text(session, judgment_info)
        if full_judgment and full_judgment.get('text'):
            full_judgment['_record_type'] = 'judgment'
            record = normalize(full_judgment)
            records.append(record)
            judgment_count += 1

            filepath = save_dir / f"record_{len(records)-1:04d}.json"
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"  [Judgment] {len(full_judgment['text'])} chars -> {filepath.name}", file=sys.stderr)

    # 3. Fetch case studies
    print("\n=== Fetching Case Studies ===", file=sys.stderr)
    case_study_count = 0

    for case_study in fetch_case_studies(session):
        if case_study_count >= case_studies_target:
            break

        record = normalize(case_study)
        records.append(record)
        case_study_count += 1

        filepath = save_dir / f"record_{len(records)-1:04d}.json"
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"  [CaseStudy] {len(case_study.get('text', ''))} chars -> {filepath.name}", file=sys.stderr)

    print(f"\nTotal: {decision_count} decisions, {judgment_count} judgments, {case_study_count} case studies", file=sys.stderr)
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
    parser = argparse.ArgumentParser(description="IE/DPC Data Protection Commission Fetcher")
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
                print(f"\nSummary:")
                print(f"  Records: {len(records)}")
                print(f"  Avg text length: {sum(text_lengths) / len(text_lengths):.0f} chars")
                print(f"  Min text length: {min(text_lengths)} chars")
                print(f"  Max text length: {max(text_lengths)} chars")
                if fines:
                    print(f"  Fines found: {len(fines)} (total €{sum(fines):,.0f})")
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
