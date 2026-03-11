#!/usr/bin/env python3
"""
NO/Høyesterett - Norwegian Supreme Court Case Law Fetcher

Fetches Norwegian Supreme Court decisions from Lovdata.
Free public access since 2008 with ~350-450 decisions per year.

Data source: https://lovdata.no/register/avgjørelser
Full archive access via pagination: ?verdict=HRA&year=YYYY&offset=N
License: NLOD 2.0 (Norwegian License for Open Government Data)
"""

import argparse
import json
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urljoin, unquote

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://lovdata.no"
REGISTRY_URL = "https://lovdata.no/register/avgjørelser"
RSS_URL = "https://lovdata.no/feed?data=newJudgements&type=RSS"
SAMPLE_DIR = Path(__file__).parent / "sample"
CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"
SOURCE_ID = "NO/Høyesterett"

# Free access starts from 2008
START_YEAR = 2008

# Court base codes in URLs
COURT_BASES = {
    'HRSIV': 'Høyesterett (civil)',
    'HRSTR': 'Høyesterett (criminal)',
    'LBSIV': 'Borgarting lagmannsrett (civil)',
    'LBSTR': 'Borgarting lagmannsrett (criminal)',
    'LGSIV': 'Gulating lagmannsrett (civil)',
    'LGSTR': 'Gulating lagmannsrett (criminal)',
    'LHSIV': 'Hålogaland lagmannsrett (civil)',
    'LHSTR': 'Hålogaland lagmannsrett (criminal)',
    'LASIV': 'Agder lagmannsrett (civil)',
    'LASTR': 'Agder lagmannsrett (criminal)',
    'LESIV': 'Eidsivating lagmannsrett (civil)',
    'LESTR': 'Eidsivating lagmannsrett (criminal)',
    'LFSIV': 'Frostating lagmannsrett (civil)',
    'LFSTR': 'Frostating lagmannsrett (criminal)',
    'TRSIV': 'Tingrett (civil)',
    'TRSTR': 'Tingrett (criminal)',
}

# Case type suffixes
CASE_TYPES = {
    'A': 'Avdeling (panel)',
    'S': 'Storkammer (grand chamber)',
    'P': 'Plenum',
    'U': 'Ankeutvalget (appeals committee)',
    'F': 'Full bench',
}


def get_court_name(base_code: str) -> str:
    """Get human-readable court name from base code."""
    return COURT_BASES.get(base_code, base_code)


def parse_case_id(case_id: str) -> dict:
    """Parse case ID like HR-2026-346-U into components."""
    match = re.match(r'([A-Z]+)-(\d{4})-(\d+)(-([A-Z]))?', case_id)
    if match:
        return {
            'court_prefix': match.group(1),
            'year': int(match.group(2)),
            'number': int(match.group(3)),
            'type_suffix': match.group(5) if match.group(5) else None,
        }
    return {}


def load_checkpoint() -> dict:
    """Load checkpoint file if it exists."""
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {'completed_years': [], 'last_year': None, 'last_offset': 0, 'fetched_ids': []}


def save_checkpoint(checkpoint: dict):
    """Save checkpoint to file."""
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f, indent=2)


def fetch_rss_feed() -> list:
    """Fetch and parse RSS feed of recent court decisions."""
    print(f"Fetching RSS feed: {RSS_URL}")
    resp = requests.get(RSS_URL, timeout=30)
    resp.raise_for_status()

    items = []
    root = ET.fromstring(resp.content)

    for item in root.findall('.//item'):
        title = item.find('title')
        link = item.find('link')
        description = item.find('description')
        pub_date = item.find('pubDate')
        guid = item.find('guid')

        if title is not None and link is not None:
            items.append({
                'case_id': title.text.strip() if title.text else '',
                'url': link.text.strip() if link.text else '',
                'description': description.text.strip() if description is not None and description.text else '',
                'pub_date': pub_date.text.strip() if pub_date is not None and pub_date.text else '',
                'guid': guid.text.strip() if guid is not None and guid.text else '',
            })

    print(f"Found {len(items)} items in RSS feed")
    return items


def fetch_registry_page(year: int, offset: int = 0) -> tuple[list, int]:
    """
    Fetch a page of Supreme Court decisions from the registry.

    Args:
        year: Year to fetch decisions from
        offset: Pagination offset (0, 20, 40, ...)

    Returns:
        Tuple of (list of decision URLs, total count)
    """
    url = f"{REGISTRY_URL}?verdict=HRA&year={year}&offset={offset}"
    print(f"  Fetching registry page: year={year}, offset={offset}")

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, 'html.parser')

    # Extract total count
    total = 0
    count_text = soup.find(string=re.compile(r'Viser \d+ - \d+ av [\d,]+ treff'))
    if count_text:
        match = re.search(r'av ([\d,]+) treff', count_text)
        if match:
            total = int(match.group(1).replace(',', ''))

    # Extract decision URLs
    items = []
    for article in soup.find_all('article'):
        link = article.find('a', href=re.compile(r'/dokument/.*/avgjorelse/'))
        if link and link.get('href'):
            doc_url = urljoin(BASE_URL, link['href'])
            case_id_match = re.search(r'(HR-\d+-\d+-[A-Z])', link.get_text())
            case_id = case_id_match.group(1) if case_id_match else ''

            items.append({
                'case_id': case_id,
                'url': doc_url,
            })

    return items, total


def fetch_year_decisions(year: int, checkpoint: dict = None) -> Generator[dict, None, None]:
    """
    Fetch all Supreme Court decisions for a given year.

    Args:
        year: Year to fetch
        checkpoint: Optional checkpoint dict for resuming

    Yields:
        Decision items with case_id and url
    """
    offset = 0
    if checkpoint and checkpoint.get('last_year') == year:
        offset = checkpoint.get('last_offset', 0)
        print(f"  Resuming from offset {offset}")

    fetched_ids = set(checkpoint.get('fetched_ids', [])) if checkpoint else set()

    # Get first page to determine total
    items, total = fetch_registry_page(year, offset)
    print(f"  Year {year}: {total} total decisions")

    while offset < total:
        if offset > 0:  # Already fetched first page
            items, _ = fetch_registry_page(year, offset)
            time.sleep(1.0)

        for item in items:
            if item['case_id'] and item['case_id'] not in fetched_ids:
                yield item

        offset += 20

        # Update checkpoint
        if checkpoint is not None:
            checkpoint['last_year'] = year
            checkpoint['last_offset'] = offset
            save_checkpoint(checkpoint)

    # Mark year as complete
    if checkpoint is not None:
        if year not in checkpoint['completed_years']:
            checkpoint['completed_years'].append(year)
        checkpoint['last_year'] = None
        checkpoint['last_offset'] = 0
        save_checkpoint(checkpoint)


def extract_text_from_html(html_content: str) -> str:
    """Extract clean text from HTML document body."""
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find the document body
    doc_body = soup.find(id='documentBody')
    if not doc_body:
        # Try alternative selectors
        doc_body = soup.find('div', class_='documentContent')
        if not doc_body:
            doc_body = soup.find(id='lovdataDocument')

    if not doc_body:
        return ''

    # Remove script and style elements
    for element in doc_body(['script', 'style', 'nav', 'header', 'footer']):
        element.decompose()

    # Get text with proper spacing
    text = doc_body.get_text(separator='\n', strip=True)

    # Clean up excessive whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)

    return text.strip()


def extract_metadata(html_content: str) -> dict:
    """Extract metadata from the document page."""
    soup = BeautifulSoup(html_content, 'html.parser')
    metadata = {}

    # Extract from meta table
    meta_table = soup.find('table', class_='meta')
    if meta_table:
        for row in meta_table.find_all('tr'):
            th = row.find('th', class_='metafieldLabel')
            td = row.find('td', class_='metavalue')
            if th and td:
                field_name = th.get_text(strip=True).lower()
                field_value = td.get_text(strip=True)

                if 'instans' in field_name:
                    metadata['court'] = field_value
                elif 'dato' in field_name:
                    metadata['date'] = field_value
                elif 'publisert' in field_name:
                    metadata['published_id'] = field_value
                elif 'stikkord' in field_name:
                    metadata['keywords'] = field_value
                elif 'sammendrag' in field_name:
                    metadata['summary'] = field_value
                elif 'saksgang' in field_name:
                    metadata['case_history'] = field_value
                elif 'parter' in field_name:
                    metadata['parties'] = field_value
                elif 'forfatter' in field_name:
                    metadata['judges'] = field_value

    # Get title from h1
    title_h1 = soup.find('h1')
    if title_h1:
        metadata['title'] = title_h1.get_text(strip=True)

    return metadata


def fetch_decision(url: str) -> Optional[dict]:
    """Fetch a single court decision page and extract content."""
    try:
        print(f"  Fetching: {url}")
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()

        html_content = resp.text

        # Check for "full text not available" message
        if 'Full tekst til avgjørelsen er ikke tilgjengelig' in html_content:
            print(f"    -> Full text not available")
            return None

        # Extract text and metadata
        text = extract_text_from_html(html_content)
        metadata = extract_metadata(html_content)

        if not text or len(text) < 100:
            print(f"    -> Text too short ({len(text)} chars)")
            return None

        # Extract case ID from URL
        case_id_match = re.search(r'/avgjorelse/([a-z]+-\d+-\d+(?:-[a-z])?)', url, re.I)
        case_id = case_id_match.group(1).upper() if case_id_match else ''

        # Parse case ID for additional info
        case_info = parse_case_id(case_id)

        return {
            'case_id': case_id,
            'url': url,
            'text': text,
            'metadata': metadata,
            'case_info': case_info,
        }

    except requests.RequestException as e:
        print(f"    -> Error: {e}")
        return None


def normalize(raw: dict) -> dict:
    """Transform raw data into standard schema."""
    case_id = raw.get('case_id', '')
    metadata = raw.get('metadata', {})
    case_info = raw.get('case_info', {})

    # Determine court name
    court = metadata.get('court', '')
    if not court and case_info.get('court_prefix'):
        prefix = case_info['court_prefix']
        if prefix.startswith('HR'):
            court = 'Norges Høyesterett'
        elif prefix.startswith('L'):
            court = f"{get_court_name(prefix)} lagmannsrett"
        elif prefix.startswith('T'):
            court = 'Tingrett'

    # Parse date
    date = metadata.get('date', '')
    if date:
        # Convert YYYY-MM-DD format
        try:
            parsed = datetime.strptime(date, '%Y-%m-%d')
            date = parsed.strftime('%Y-%m-%d')
        except ValueError:
            pass

    # Build title
    title = metadata.get('title', case_id)
    if metadata.get('keywords'):
        title = f"{case_id} - {metadata['keywords'][:100]}"

    return {
        '_id': case_id,
        '_source': SOURCE_ID,
        '_type': 'case_law',
        '_fetched_at': datetime.utcnow().isoformat() + 'Z',
        'title': title,
        'text': raw.get('text', ''),
        'date': date,
        'url': raw.get('url', ''),
        'court': court,
        'case_id': case_id,
        'year': case_info.get('year'),
        'keywords': metadata.get('keywords'),
        'summary': metadata.get('summary'),
        'judges': metadata.get('judges'),
        'parties': metadata.get('parties'),
        'case_history': metadata.get('case_history'),
        'language': 'nob',  # Norwegian Bokmål
    }


def fetch_all(max_records: int = None, supreme_court_only: bool = True,
               use_checkpoint: bool = True, start_year: int = None,
               end_year: int = None) -> Generator[dict, None, None]:
    """
    Fetch Supreme Court decisions from the full registry archive.

    Args:
        max_records: Maximum number of records to yield
        supreme_court_only: Only fetch Supreme Court decisions (always True for this source)
        use_checkpoint: Whether to use checkpoint file for resuming
        start_year: Start year (default: 2008)
        end_year: End year (default: current year)

    Yields:
        Normalized document records
    """
    if start_year is None:
        start_year = START_YEAR
    if end_year is None:
        end_year = datetime.now().year

    checkpoint = load_checkpoint() if use_checkpoint else None
    completed_years = set(checkpoint.get('completed_years', [])) if checkpoint else set()
    fetched_ids = set(checkpoint.get('fetched_ids', [])) if checkpoint else set()

    count = 0
    years = list(range(end_year, start_year - 1, -1))  # Most recent first

    print(f"Fetching Supreme Court decisions from {start_year} to {end_year}")
    print(f"Already completed years: {sorted(completed_years)}")

    for year in years:
        if max_records and count >= max_records:
            break

        if year in completed_years:
            print(f"Year {year}: Already complete, skipping")
            continue

        print(f"\nProcessing year {year}...")

        for item in fetch_year_decisions(year, checkpoint):
            if max_records and count >= max_records:
                break

            case_id = item.get('case_id', '')
            if case_id in fetched_ids:
                continue

            url = item.get('url', '')
            if not url:
                continue

            # Fetch the full decision
            raw = fetch_decision(url)
            if raw and raw.get('text') and len(raw['text']) >= 500:
                try:
                    normalized = normalize(raw)
                    yield normalized
                    count += 1
                    fetched_ids.add(case_id)
                    print(f"    -> {len(raw['text']):,} chars")

                    # Update checkpoint with fetched IDs periodically
                    if checkpoint and count % 50 == 0:
                        checkpoint['fetched_ids'] = list(fetched_ids)
                        save_checkpoint(checkpoint)

                except Exception as e:
                    print(f"  Error normalizing {case_id}: {e}")

            # Rate limiting
            time.sleep(1.5)

    # Final checkpoint update
    if checkpoint:
        checkpoint['fetched_ids'] = list(fetched_ids)
        save_checkpoint(checkpoint)

    print(f"\nTotal records yielded: {count}")


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch documents updated since a given date."""
    for record in fetch_all():
        if record.get('date'):
            try:
                doc_date = datetime.strptime(record['date'], '%Y-%m-%d')
                if doc_date >= since:
                    yield record
            except (ValueError, TypeError):
                yield record


def bootstrap_sample(sample_count: int = 12, use_rss: bool = False):
    """Fetch sample records and save to sample directory.

    Args:
        sample_count: Number of sample records to fetch
        use_rss: Use RSS feed (for recent items) instead of full registry
    """
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample records from NO/Høyesterett...")
    print("=" * 60)

    records = []

    if use_rss:
        # Use RSS for quick samples of recent decisions
        rss_items = fetch_rss_feed()
        for item in rss_items:
            if len(records) >= sample_count:
                break

            case_id = item.get('case_id', '')
            if not case_id.startswith('HR-'):
                continue

            url = item.get('url', '')
            if not url:
                continue

            raw = fetch_decision(url)
            if raw and raw.get('text') and len(raw['text']) >= 500:
                try:
                    record = normalize(raw)
                    records.append(record)

                    filename = SAMPLE_DIR / f"record_{len(records):03d}.json"
                    with open(filename, 'w', encoding='utf-8') as f:
                        json.dump(record, f, ensure_ascii=False, indent=2)

                    text_len = len(record.get('text', ''))
                    print(f"  [{len(records):02d}] {record['_id']}: {text_len:,} chars")
                except Exception as e:
                    print(f"  Skipping: {e}")

            time.sleep(1.5)
    else:
        # Use registry pagination for sample from multiple years
        current_year = datetime.now().year
        years_to_sample = [current_year, current_year - 1, current_year - 5, current_year - 10]

        for year in years_to_sample:
            if len(records) >= sample_count:
                break

            if year < START_YEAR:
                continue

            print(f"\nSampling from year {year}...")
            items, total = fetch_registry_page(year, 0)

            for item in items[:5]:  # Take up to 5 from each year
                if len(records) >= sample_count:
                    break

                case_id = item.get('case_id', '')
                url = item.get('url', '')
                if not url:
                    continue

                raw = fetch_decision(url)
                if raw and raw.get('text') and len(raw['text']) >= 500:
                    try:
                        record = normalize(raw)
                        records.append(record)

                        filename = SAMPLE_DIR / f"record_{len(records):03d}.json"
                        with open(filename, 'w', encoding='utf-8') as f:
                            json.dump(record, f, ensure_ascii=False, indent=2)

                        text_len = len(record.get('text', ''))
                        print(f"  [{len(records):02d}] {record['_id']}: {text_len:,} chars (year {year})")
                    except Exception as e:
                        print(f"  Skipping: {e}")

                time.sleep(1.5)

    print("=" * 60)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")

    if records:
        avg_text_len = sum(len(r.get('text', '')) for r in records) / len(records)
        print(f"Average text length: {avg_text_len:,.0f} chars/doc")

        # Count by year
        year_counts = {}
        for r in records:
            year = r.get('year', 'Unknown')
            year_counts[year] = year_counts.get(year, 0) + 1

        print("Year distribution:")
        for year, count in sorted(year_counts.items(), reverse=True):
            print(f"  {year}: {count}")

    # Validation
    if len(records) < 10:
        print("WARNING: Fewer than 10 records fetched!")
        return False

    empty_text = sum(1 for r in records if not r.get('text') or len(r['text']) < 500)
    if empty_text > 0:
        print(f"WARNING: {empty_text} records have insufficient text!")
        return False

    print("VALIDATION PASSED: All records have full text content.")
    return True


def main():
    parser = argparse.ArgumentParser(description="NO/Høyesterett court decision fetcher")
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'rss', 'registry', 'status'],
                       help="Command to run")
    parser.add_argument('--sample', action='store_true',
                       help="Fetch sample records only")
    parser.add_argument('--count', type=int, default=12,
                       help="Number of sample records to fetch")
    parser.add_argument('--use-rss', action='store_true',
                       help="Use RSS feed for sampling (recent items only)")
    parser.add_argument('--start-year', type=int, default=START_YEAR,
                       help=f"Start year for full fetch (default: {START_YEAR})")
    parser.add_argument('--end-year', type=int, default=None,
                       help="End year for full fetch (default: current year)")
    parser.add_argument('--no-checkpoint', action='store_true',
                       help="Disable checkpoint/resume functionality")
    parser.add_argument('--clear-checkpoint', action='store_true',
                       help="Clear existing checkpoint before fetching")

    args = parser.parse_args()

    if args.command == 'status':
        # Show checkpoint status
        checkpoint = load_checkpoint()
        print("Checkpoint status:")
        print(f"  Completed years: {sorted(checkpoint.get('completed_years', []))}")
        print(f"  Last year in progress: {checkpoint.get('last_year')}")
        print(f"  Last offset: {checkpoint.get('last_offset', 0)}")
        print(f"  Total fetched IDs: {len(checkpoint.get('fetched_ids', []))}")

        # Estimate total
        current_year = datetime.now().year
        completed = len(checkpoint.get('completed_years', []))
        total_years = current_year - START_YEAR + 1
        print(f"  Progress: {completed}/{total_years} years complete")

    elif args.command == 'rss':
        items = fetch_rss_feed()
        for item in items[:20]:
            print(f"{item['case_id']}: {item['url']}")
            if item['description']:
                print(f"  {item['description'][:100]}...")

    elif args.command == 'registry':
        # Test registry pagination
        year = args.end_year or datetime.now().year
        items, total = fetch_registry_page(year, 0)
        print(f"Year {year}: {total} total decisions")
        for item in items[:10]:
            print(f"  {item['case_id']}: {item['url']}")

    elif args.command == 'bootstrap':
        if args.clear_checkpoint and CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()
            print("Cleared checkpoint file")

        success = bootstrap_sample(args.count, use_rss=args.use_rss)
        sys.exit(0 if success else 1)

    elif args.command == 'fetch':
        if args.clear_checkpoint and CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()
            print("Cleared checkpoint file")

        for record in fetch_all(
            use_checkpoint=not args.no_checkpoint,
            start_year=args.start_year,
            end_year=args.end_year
        ):
            print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
