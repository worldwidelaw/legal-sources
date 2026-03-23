#!/usr/bin/env python3
"""
BE/BIPT - Belgian Institute for Postal Services and Telecommunications

Fetches regulatory decisions from the BIPT official website via XML RSS feed.
Decisions are published as PDFs in French and Dutch.

Data source:
- XML feed: https://www.bipt.be/operators/search.xml?s=publication_date&tgGroup=operators&type[0]=publication_type:decision
- Individual decisions: https://www.bipt.be/operators/publication/{slug}

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
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

try:
    import pdfplumber
    PDF_AVAILABLE = True
except ImportError:
    try:
        import pypdf
        PDF_AVAILABLE = True
        USE_PYPDF = True
    except ImportError:
        PDF_AVAILABLE = False
        print("Warning: pdfplumber/pypdf not available, PDF text extraction disabled", file=sys.stderr)
else:
    USE_PYPDF = False

# Constants
SOURCE_ID = "BE/BIPT"
BASE_URL = "https://www.bipt.be"

# RSS feed for all decisions
RSS_URL = "https://www.bipt.be/operators/search.xml?s=publication_date&tgGroup=operators&type%5B0%5D=publication_type:decision"

RATE_LIMIT_DELAY = 2.0
USER_AGENT = "WorldWideLaw/1.0 (Open Data Research)"

# Paths
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"


def get_session() -> requests.Session:
    """Create requests session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,fr-BE;q=0.8,nl-BE;q=0.7",
    })
    return session


def extract_text_from_pdf(pdf_content: bytes) -> str:
    """Extract text from PDF content."""
    if not PDF_AVAILABLE:
        return ""

    try:
        if USE_PYPDF:
            import pypdf
            reader = pypdf.PdfReader(io.BytesIO(pdf_content))
            text_parts = []
            for page in reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
            full_text = "\n\n".join(text_parts)
        else:
            with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                text_parts = []
                for page in pdf.pages:
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


def parse_rss_feed(xml_content: str) -> list[dict]:
    """Parse RSS XML feed and extract decision metadata."""
    decisions = []

    try:
        root = ET.fromstring(xml_content)
        channel = root.find('channel')
        if channel is None:
            return decisions

        for item in channel.findall('item'):
            title_elem = item.find('title')
            link_elem = item.find('link')
            desc_elem = item.find('description')
            pubdate_elem = item.find('pubDate')

            if title_elem is None or link_elem is None:
                continue

            title = title_elem.text.strip() if title_elem.text else ""
            link = link_elem.text.strip() if link_elem.text else ""
            description = desc_elem.text.strip() if desc_elem is not None and desc_elem.text else ""
            pub_date = pubdate_elem.text.strip() if pubdate_elem is not None and pubdate_elem.text else ""

            # Build full URL
            if link and not link.startswith('http'):
                link = urljoin(BASE_URL, link)

            # Parse date (format: YYYY-MM-DD HH:MM)
            date = None
            if pub_date:
                try:
                    dt = datetime.strptime(pub_date.split()[0], "%Y-%m-%d")
                    date = dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

            decisions.append({
                'title': title,
                'url': link,
                'description': description,
                'date': date,
                'pub_date_raw': pub_date,
            })

    except ET.ParseError as e:
        print(f"Error parsing RSS XML: {e}", file=sys.stderr)

    return decisions


def fetch_rss_page(session: requests.Session, page: int = 0) -> str:
    """Fetch a single page of the RSS feed."""
    url = f"{RSS_URL}&p={page}"
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching RSS page {page}: {e}", file=sys.stderr)
        return ""


def get_total_results(xml_content: str) -> int:
    """Extract total result count from RSS feed title."""
    try:
        root = ET.fromstring(xml_content)
        channel = root.find('channel')
        if channel is not None:
            title = channel.find('title')
            if title is not None and title.text:
                match = re.search(r'(\d+)\s+results', title.text)
                if match:
                    return int(match.group(1))
    except Exception:
        pass
    return 0


def fetch_all_decision_metadata(session: requests.Session, max_pages: int = None) -> list[dict]:
    """Fetch all decision metadata from the RSS feed."""
    # Get first page to determine total results
    xml_content = fetch_rss_page(session, 0)
    if not xml_content:
        return []

    total = get_total_results(xml_content)
    results_per_page = 10  # RSS appears to return 10 per page
    total_pages = (total // results_per_page) + 1 if total > 0 else 1

    if max_pages:
        total_pages = min(total_pages, max_pages)

    print(f"Found {total} decisions across ~{total_pages} pages", file=sys.stderr)

    all_decisions = []
    seen_urls = set()

    # Parse first page
    decisions = parse_rss_feed(xml_content)
    for d in decisions:
        if d['url'] not in seen_urls:
            seen_urls.add(d['url'])
            all_decisions.append(d)

    # Fetch remaining pages
    for page in range(1, total_pages):
        time.sleep(RATE_LIMIT_DELAY)
        print(f"  Fetching page {page + 1}/{total_pages}...", file=sys.stderr)

        xml_content = fetch_rss_page(session, page)
        if not xml_content:
            continue

        decisions = parse_rss_feed(xml_content)
        if not decisions:
            break  # No more results

        for d in decisions:
            if d['url'] not in seen_urls:
                seen_urls.add(d['url'])
                all_decisions.append(d)

    return all_decisions


def fetch_decision_page(session: requests.Session, url: str) -> dict:
    """Fetch a decision page and extract PDF links and additional metadata."""
    result = {
        'pdf_urls': [],
        'themes': [],
        'language': None,
    }

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching decision page {url}: {e}", file=sys.stderr)
        return result

    soup = BeautifulSoup(response.text, 'html.parser')

    # Find PDF links
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.lower().endswith('.pdf'):
            full_url = urljoin(BASE_URL, href)
            link_text = link.get_text().strip()

            # Determine language from link text or filename
            lang = None
            if 'français' in link_text.lower() or '/fr/' in href or '-fr-' in href:
                lang = 'fr'
            elif 'néerlandais' in link_text.lower() or 'dutch' in link_text.lower() or '/nl/' in href or '-nl-' in href:
                lang = 'nl'
            # Check filename for language hints
            elif 'besluit' in href.lower() or 'beslissing' in href.lower():
                lang = 'nl'
            elif 'decision' in href.lower() or 'décision' in href.lower():
                lang = 'fr'

            result['pdf_urls'].append({
                'url': full_url,
                'language': lang,
                'filename': href.split('/')[-1],
            })

    # Try to extract themes/categories
    for tag in soup.find_all(['span', 'a'], class_=re.compile(r'tag|theme|category', re.I)):
        theme = tag.get_text().strip()
        if theme and len(theme) < 100 and theme not in result['themes']:
            result['themes'].append(theme)

    # Look for theme in meta tags or structured data
    for meta in soup.find_all('meta', {'name': re.compile(r'keywords|theme', re.I)}):
        content = meta.get('content', '')
        for kw in content.split(','):
            kw = kw.strip()
            if kw and kw not in result['themes']:
                result['themes'].append(kw)

    return result


def fetch_pdf_text(session: requests.Session, pdf_url: str, max_size_mb: float = 15.0) -> Optional[str]:
    """Download and extract text from a PDF."""
    try:
        # Check size first
        head_resp = session.head(pdf_url, timeout=20, allow_redirects=True)
        content_length = head_resp.headers.get('Content-Length')
        if content_length:
            size_mb = int(content_length) / (1024 * 1024)
            if size_mb > max_size_mb:
                print(f"Skipping large PDF ({size_mb:.1f} MB): {pdf_url}", file=sys.stderr)
                return None

        response = session.get(pdf_url, timeout=180)
        response.raise_for_status()

        text = extract_text_from_pdf(response.content)
        return text if text else None

    except requests.RequestException as e:
        print(f"Error downloading PDF {pdf_url}: {e}", file=sys.stderr)
        return None


def fetch_decision_with_text(session: requests.Session, decision: dict) -> Optional[dict]:
    """Fetch full decision with text from PDF."""
    page_url = decision.get('url')
    if not page_url:
        return None

    # Fetch decision page to get PDF links
    page_data = fetch_decision_page(session, page_url)

    if not page_data['pdf_urls']:
        print(f"No PDFs found for: {decision.get('title', 'unknown')[:60]}", file=sys.stderr)
        return None

    # Update decision with page data
    decision['themes'] = page_data['themes']

    # Try to get text from the first available PDF
    text = None
    pdf_info = None
    for pdf in page_data['pdf_urls']:
        time.sleep(RATE_LIMIT_DELAY)
        text = fetch_pdf_text(session, pdf['url'])
        if text and len(text) > 100:
            pdf_info = pdf
            break

    if not text or len(text) < 100:
        print(f"Could not extract text from: {decision.get('title', 'unknown')[:60]}", file=sys.stderr)
        return None

    decision['text'] = text
    decision['pdf_url'] = pdf_info['url'] if pdf_info else None
    decision['language'] = pdf_info['language'] if pdf_info else None
    decision['pdf_filename'] = pdf_info['filename'] if pdf_info else None

    return decision


def normalize(raw: dict) -> dict:
    """Transform raw BIPT decision data into normalized schema."""
    # Create unique ID from URL slug
    url = raw.get('url', '')
    slug = url.split('/')[-1] if url else 'unknown'
    doc_id = f"{SOURCE_ID}/{slug}"

    return {
        '_id': doc_id,
        '_source': SOURCE_ID,
        '_type': 'doctrine',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': raw.get('title', ''),
        'text': raw.get('text', ''),
        'date': raw.get('date'),
        'url': raw.get('url', ''),
        'description': raw.get('description', ''),
        'themes': raw.get('themes', []),
        'language': raw.get('language'),
        'pdf_url': raw.get('pdf_url'),
        'pdf_filename': raw.get('pdf_filename'),
    }


def fetch_all(session: requests.Session) -> Generator[dict, None, None]:
    """Fetch all BIPT decisions."""
    print("Fetching all decision metadata...", file=sys.stderr)
    decisions = fetch_all_decision_metadata(session)
    print(f"Found {len(decisions)} decisions", file=sys.stderr)

    for i, decision in enumerate(decisions):
        time.sleep(RATE_LIMIT_DELAY)
        print(f"[{i+1}/{len(decisions)}] {decision.get('title', 'unknown')[:60]}...", file=sys.stderr)

        full_decision = fetch_decision_with_text(session, decision)
        if full_decision and full_decision.get('text'):
            record = normalize(full_decision)
            yield record


def fetch_sample(session: requests.Session, count: int = 15, save_dir: Path = None) -> list[dict]:
    """Fetch a sample of decisions."""
    records = []
    save_dir = save_dir or SAMPLE_DIR
    save_dir.mkdir(parents=True, exist_ok=True)

    print(f"Fetching sample of {count} decisions...", file=sys.stderr)

    # Fetch first few pages of metadata
    decisions = fetch_all_decision_metadata(session, max_pages=3)
    print(f"Got {len(decisions)} decision metadata entries", file=sys.stderr)

    for i, decision in enumerate(decisions[:count + 5]):
        if len(records) >= count:
            break

        print(f"\n[{i+1}] Fetching: {decision.get('title', 'unknown')[:60]}...", file=sys.stderr)
        time.sleep(RATE_LIMIT_DELAY)

        full_decision = fetch_decision_with_text(session, decision)
        if full_decision and full_decision.get('text'):
            record = normalize(full_decision)
            records.append(record)

            # Save incrementally
            filepath = save_dir / f"record_{len(records)-1:04d}.json"
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"  -> Extracted {len(full_decision['text'])} chars", file=sys.stderr)
        else:
            print(f"  -> Skipped (no text)", file=sys.stderr)

    return records


def save_samples(records: list[dict]) -> None:
    """Save sample records to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    # Save all samples in one file
    all_samples = SAMPLE_DIR / "all_samples.json"
    with open(all_samples, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"\nSaved {len(records)} samples to {SAMPLE_DIR}")


def main():
    parser = argparse.ArgumentParser(description="BE/BIPT Telecom Regulator Fetcher")
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
        print("ERROR: pdfplumber or pypdf library required for PDF text extraction", file=sys.stderr)
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
                themes_all = []
                for r in records:
                    themes_all.extend(r.get('themes', []))
                unique_themes = set(themes_all)

                print(f"\n=== Sample Statistics ===", file=sys.stderr)
                print(f"  Records: {len(records)}", file=sys.stderr)
                print(f"  Avg text length: {sum(text_lengths) / len(text_lengths):.0f} chars", file=sys.stderr)
                print(f"  Min text length: {min(text_lengths)} chars", file=sys.stderr)
                print(f"  Max text length: {max(text_lengths)} chars", file=sys.stderr)
                print(f"  Unique themes: {len(unique_themes)}", file=sys.stderr)

                # Show sample themes
                if unique_themes:
                    print(f"\nSample themes: {', '.join(list(unique_themes)[:10])}", file=sys.stderr)
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
