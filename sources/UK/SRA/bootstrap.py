#!/usr/bin/env python3
"""
UK/SRA - Solicitors Regulation Authority & Solicitors Disciplinary Tribunal

Fetches regulatory decisions from SRA and tribunal decisions from SDT.
- SRA: RSS feed -> individual decision pages (full text HTML)
- SDT: Paginated case archive -> individual case pages (executive summaries)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from xml.etree import ElementTree as ET
from html.parser import HTMLParser

import requests

SOURCE_ID = "UK/SRA"
SAMPLE_DIR = Path(__file__).parent / "sample"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research; github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

SRA_RSS_URL = "https://www.sra.org.uk/contact-centre/rss/recent-decisions/"
SDT_BASE_URL = "https://solicitorstribunal.org.uk"
SDT_CASE_LIST_URL = f"{SDT_BASE_URL}/case/page/{{page}}/"
SDT_CASE_URL = f"{SDT_BASE_URL}/case/{{case_id}}/"

# Total SDT pages (approx 219 as of 2026-03)
SDT_MAX_PAGES = 250


class HTMLTextExtractor(HTMLParser):
    """Extract clean text from HTML, preserving paragraph breaks."""

    def __init__(self):
        super().__init__()
        self._text = []
        self._skip = False
        self._skip_tags = {'script', 'style', 'nav', 'header', 'footer', 'noscript'}

    def handle_starttag(self, tag, attrs):
        if tag in self._skip_tags:
            self._skip = True
        if tag in ('p', 'br', 'div', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'tr'):
            self._text.append('\n')

    def handle_endtag(self, tag):
        if tag in self._skip_tags:
            self._skip = False
        if tag in ('p', 'div', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'tr'):
            self._text.append('\n')

    def handle_data(self, data):
        if not self._skip:
            self._text.append(data)

    def get_text(self):
        text = ''.join(self._text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)
        return text.strip()


def extract_text_from_html(html: str) -> str:
    """Extract clean text from HTML content."""
    extractor = HTMLTextExtractor()
    extractor.feed(html)
    return extractor.get_text()


def fetch_with_retry(url: str, max_retries: int = 3, delay: float = 2.0) -> Optional[requests.Response]:
    """Fetch URL with retries and rate limiting."""
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=60)
            if resp.status_code == 429:
                wait = delay * (2 ** attempt)
                print(f"  Rate limited. Waiting {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                print(f"  Retry {attempt + 1}/{max_retries}: {e}")
                time.sleep(delay)
            else:
                print(f"  Failed after {max_retries} attempts: {e}")
    return None


# --- SRA Regulatory Decisions ---

def fetch_sra_rss() -> list:
    """Fetch SRA recent decisions RSS feed and return list of items."""
    resp = fetch_with_retry(SRA_RSS_URL)
    if not resp:
        return []

    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as e:
        print(f"  RSS parse error: {e}")
        return []

    items = []
    for item in root.findall('.//item'):
        title_el = item.find('title')
        link_el = item.find('link')
        desc_el = item.find('description')
        date_el = item.find('pubDate')
        guid_el = item.find('guid')

        items.append({
            'title': title_el.text if title_el is not None else None,
            'link': link_el.text if link_el is not None else None,
            'description': desc_el.text if desc_el is not None else None,
            'pub_date': date_el.text if date_el is not None else None,
            'guid': guid_el.text if guid_el is not None else None,
        })

    return items


def parse_sra_date(date_str: str) -> Optional[str]:
    """Parse RSS date to ISO format."""
    if not date_str:
        return None
    try:
        # RFC 2822 format: Fri, 20 Mar 2026 09:14:25 Z
        dt = datetime.strptime(date_str.strip().replace(' Z', ' +0000'),
                               '%a, %d %b %Y %H:%M:%S %z')
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        return None


def fetch_sra_decision_text(url: str) -> Optional[str]:
    """Fetch full text of an SRA regulatory decision page."""
    resp = fetch_with_retry(url)
    if not resp:
        return None

    html = resp.text

    # Extract the main decision content area
    # SRA pages use specific content sections
    text_parts = []

    # Try to find the main content area
    # Look for the decision content between markers
    patterns = [
        # Main content area
        r'<div[^>]*class="[^"]*main-content[^"]*"[^>]*>(.*?)</div>\s*</div>\s*</div>',
        r'<article[^>]*>(.*?)</article>',
        r'<div[^>]*class="[^"]*content-area[^"]*"[^>]*>(.*?)</div>',
        # Generic body content
        r'<main[^>]*>(.*?)</main>',
    ]

    content_html = None
    for pattern in patterns:
        match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
        if match:
            content_html = match.group(1)
            break

    if not content_html:
        # Fallback: extract between body tags, strip nav/header/footer
        body_match = re.search(r'<body[^>]*>(.*?)</body>', html, re.DOTALL | re.IGNORECASE)
        if body_match:
            content_html = body_match.group(1)
            # Remove nav, header, footer
            content_html = re.sub(r'<nav[^>]*>.*?</nav>', '', content_html, flags=re.DOTALL | re.IGNORECASE)
            content_html = re.sub(r'<header[^>]*>.*?</header>', '', content_html, flags=re.DOTALL | re.IGNORECASE)
            content_html = re.sub(r'<footer[^>]*>.*?</footer>', '', content_html, flags=re.DOTALL | re.IGNORECASE)

    if content_html:
        return extract_text_from_html(content_html)

    return None


def fetch_sra_decisions(sample: bool = False) -> Generator[dict, None, None]:
    """Yield normalized SRA regulatory decision records."""
    print("Fetching SRA regulatory decisions from RSS feed...")
    items = fetch_sra_rss()
    print(f"  Found {len(items)} items in RSS feed")

    if sample:
        items = items[:15]

    for i, item in enumerate(items):
        url = item.get('link')
        if not url:
            continue

        print(f"  [{i+1}/{len(items)}] Fetching {item.get('title', 'unknown')}...")

        # Extract SRA ID from URL
        sra_id = url.rstrip('/').split('/')[-1]

        # Fetch full decision text
        text = fetch_sra_decision_text(url)
        if not text or len(text) < 100:
            print(f"    Skipping: insufficient text ({len(text) if text else 0} chars)")
            continue

        date = parse_sra_date(item.get('pub_date'))

        record = {
            '_id': f"SRA-{sra_id}",
            '_source': SOURCE_ID,
            '_type': 'doctrine',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': item.get('title', ''),
            'text': text,
            'date': date,
            'url': url,
            'decision_type': item.get('description', ''),
            'outcome': item.get('description', ''),
            'sra_id': sra_id,
            'language': 'en',
        }

        yield record
        time.sleep(1.5)


# --- SDT Tribunal Decisions ---

def fetch_sdt_case_list(page: int) -> list:
    """Fetch a page of SDT case listings and return case IDs and metadata."""
    url = SDT_CASE_LIST_URL.format(page=page)
    resp = fetch_with_retry(url)
    if not resp:
        return []

    html = resp.text
    cases = []

    # Extract case links: /case/12754/
    case_links = re.findall(r'<a[^>]*href="' + re.escape(SDT_BASE_URL) + r'/case/(\d+)/?"[^>]*>', html)
    # Deduplicate while preserving order
    seen = set()
    unique_ids = []
    for cid in case_links:
        if cid not in seen:
            seen.add(cid)
            unique_ids.append(cid)

    return unique_ids


def fetch_sdt_case(case_id: str) -> Optional[dict]:
    """Fetch an individual SDT case page and extract structured data."""
    url = SDT_CASE_URL.format(case_id=case_id)
    resp = fetch_with_retry(url)
    if not resp:
        return None

    html = resp.text

    # Extract title from <title> tag (h1 is often empty on SDT)
    title_match = re.search(r'<title>(.*?)</title>', html, re.DOTALL)
    title = extract_text_from_html(title_match.group(1)).strip() if title_match else ""
    # Clean WordPress suffix
    title = re.sub(r'\s*[-–|]\s*Solicitors Disciplinary Tribunal\s*$', '', title).strip()
    if not title:
        title = f"SDT Case {case_id}"

    # Extract metadata from table cells: <th>Label</th> <td>Value</td>
    def extract_table_field(label: str) -> Optional[str]:
        pattern = rf'<th>\s*{re.escape(label)}\s*</th>\s*<td>(.*?)</td>'
        m = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
        if m:
            return extract_text_from_html(m.group(1)).strip()
        return None

    # Extract from sidebar spans: <span class="d-block fw-semibold">Label</span><span>Value</span>
    def extract_sidebar_field(label: str) -> Optional[str]:
        pattern = rf'<span[^>]*class="d-block fw-semibold[^"]*"[^>]*>\s*{re.escape(label)}\s*</span>\s*<span[^>]*>(.*?)</span>'
        m = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
        if m:
            return extract_text_from_html(m.group(1)).strip()
        return None

    case_number = extract_sidebar_field('Case ID') or case_id
    sra_id = extract_sidebar_field('SRA ID')
    year = extract_sidebar_field('Year')
    pub_date_str = extract_sidebar_field('Publication date')
    applicant = extract_table_field('Applicant')
    respondent = extract_table_field('Respondent')
    outcome = extract_table_field('Outcome')
    allegations = extract_table_field('Allegation')

    # Executive summary is in: <th>Executive summary</th><td>...</td>
    exec_match = re.search(
        r'<th>\s*Executive\s+summary\s*</th>\s*<td>(.*?)</td>',
        html, re.DOTALL | re.IGNORECASE
    )

    # Build text from metadata + executive summary
    text_parts = []
    if applicant:
        text_parts.append(f"Applicant: {applicant}")
    if respondent:
        text_parts.append(f"Respondent: {respondent}")
    if allegations:
        text_parts.append(f"Allegations: {allegations}")
    if outcome:
        text_parts.append(f"Outcome: {outcome}")

    if exec_match:
        summary_text = extract_text_from_html(exec_match.group(1))
        if summary_text:
            text_parts.append(f"\nExecutive Summary:\n{summary_text}")

    text = '\n'.join(text_parts)

    # Parse publication date
    date = None
    if pub_date_str:
        # Try DD/MM/YYYY format
        for fmt in ('%d/%m/%Y', '%d %B %Y', '%Y-%m-%d', '%d %b %Y'):
            try:
                date = datetime.strptime(pub_date_str.strip(), fmt).strftime('%Y-%m-%d')
                break
            except ValueError:
                continue

    return {
        'case_id': case_id,
        'title': title,
        'text': text,
        'date': date,
        'url': url,
        'case_number': case_number,
        'sra_id': sra_id,
        'year': year,
        'applicant': applicant,
        'respondent': respondent,
        'outcome': outcome,
        'allegations': allegations,
    }


def fetch_sdt_decisions(sample: bool = False) -> Generator[dict, None, None]:
    """Yield normalized SDT tribunal decision records."""
    print("Fetching SDT tribunal decisions...")

    max_pages = 3 if sample else SDT_MAX_PAGES
    total_yielded = 0

    for page in range(1, max_pages + 1):
        print(f"  Page {page}/{max_pages}...")
        case_ids = fetch_sdt_case_list(page)

        if not case_ids:
            print(f"  No more cases on page {page}, stopping.")
            break

        for case_id in case_ids:
            print(f"    Fetching case {case_id}...")
            case_data = fetch_sdt_case(case_id)

            if not case_data or not case_data.get('text') or len(case_data['text']) < 100:
                print(f"    Skipping case {case_id}: insufficient text")
                continue

            record = {
                '_id': f"SDT-{case_id}",
                '_source': SOURCE_ID,
                '_type': 'doctrine',
                '_fetched_at': datetime.now(timezone.utc).isoformat(),
                'title': case_data['title'],
                'text': case_data['text'],
                'date': case_data['date'],
                'url': case_data['url'],
                'decision_type': 'Tribunal Decision',
                'outcome': case_data.get('outcome'),
                'case_number': case_data.get('case_number'),
                'sra_id': case_data.get('sra_id'),
                'respondent': case_data.get('respondent'),
                'applicant': case_data.get('applicant'),
                'allegations': case_data.get('allegations'),
                'language': 'en',
            }

            yield record
            total_yielded += 1
            time.sleep(1.5)

            if sample and total_yielded >= 10:
                return

    print(f"  Total SDT records: {total_yielded}")


# --- Combined fetch ---

def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield all records from both SRA and SDT sources."""
    # SRA regulatory decisions first
    yield from fetch_sra_decisions(sample=sample)
    # SDT tribunal decisions
    yield from fetch_sdt_decisions(sample=sample)


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch documents updated since a given date."""
    since_date = datetime.fromisoformat(since).date()

    for record in fetch_sra_decisions(sample=False):
        if record.get('date'):
            try:
                rec_date = datetime.strptime(record['date'], '%Y-%m-%d').date()
                if rec_date >= since_date:
                    yield record
            except ValueError:
                yield record

    # SDT: check first few pages for recent cases
    for page in range(1, 10):
        case_ids = fetch_sdt_case_list(page)
        if not case_ids:
            break
        found_older = False
        for case_id in case_ids:
            case_data = fetch_sdt_case(case_id)
            if not case_data or not case_data.get('text') or len(case_data['text']) < 100:
                continue
            if case_data.get('date'):
                try:
                    rec_date = datetime.strptime(case_data['date'], '%Y-%m-%d').date()
                    if rec_date < since_date:
                        found_older = True
                        break
                except ValueError:
                    pass
            record = {
                '_id': f"SDT-{case_id}",
                '_source': SOURCE_ID,
                '_type': 'doctrine',
                '_fetched_at': datetime.now(timezone.utc).isoformat(),
                'title': case_data['title'],
                'text': case_data['text'],
                'date': case_data['date'],
                'url': case_data['url'],
                'decision_type': 'Tribunal Decision',
                'outcome': case_data.get('outcome'),
                'language': 'en',
            }
            yield record
            time.sleep(1.5)
        if found_older:
            break


# --- Bootstrap ---

def bootstrap_sample():
    """Fetch sample records for validation."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    count = 0
    for record in fetch_all(sample=True):
        filename = SAMPLE_DIR / f"{record['_id'].replace('/', '_')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(record, f, indent=2, ensure_ascii=False)
        count += 1
        text_len = len(record.get('text', ''))
        print(f"  Saved {record['_id']} ({text_len} chars)")

    print(f"\nSample complete: {count} records saved to {SAMPLE_DIR}")
    return count


def bootstrap_full():
    """Full bootstrap - fetch all records."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    count = 0
    for record in fetch_all(sample=False):
        filename = SAMPLE_DIR / f"{record['_id'].replace('/', '_')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(record, f, indent=2, ensure_ascii=False)
        count += 1
        if count % 50 == 0:
            print(f"  Progress: {count} records...")

    print(f"\nBootstrap complete: {count} records saved to {SAMPLE_DIR}")
    return count


def test_api():
    """Test connectivity to both SRA and SDT."""
    print("Testing SRA RSS feed...")
    resp = fetch_with_retry(SRA_RSS_URL)
    if resp:
        try:
            root = ET.fromstring(resp.content)
            items = root.findall('.//item')
            print(f"  OK: {len(items)} items in RSS feed")
        except ET.ParseError:
            print("  FAIL: RSS parse error")
            return False
    else:
        print("  FAIL: Cannot reach SRA RSS feed")
        return False

    print("\nTesting SDT case archive...")
    resp = fetch_with_retry(f"{SDT_BASE_URL}/case/page/1/")
    if resp:
        case_ids = re.findall(r'/case/(\d+)/', resp.text)
        unique = len(set(case_ids))
        print(f"  OK: {unique} cases on page 1")
    else:
        print("  FAIL: Cannot reach SDT case archive")
        return False

    # Test individual case fetch
    print("\nTesting individual case fetch...")
    if case_ids:
        test_id = list(set(case_ids))[0]
        case = fetch_sdt_case(test_id)
        if case and case.get('text') and len(case['text']) > 100:
            print(f"  OK: Case {test_id} has {len(case['text'])} chars")
        else:
            print(f"  WARNING: Case {test_id} has insufficient text")

    # Test SRA decision page
    print("\nTesting SRA decision page...")
    items = fetch_sra_rss()
    if items and items[0].get('link'):
        text = fetch_sra_decision_text(items[0]['link'])
        if text and len(text) > 100:
            print(f"  OK: SRA decision has {len(text)} chars")
        else:
            print(f"  WARNING: SRA decision has insufficient text ({len(text) if text else 0} chars)")

    print("\nAll tests passed!")
    return True


def main():
    parser = argparse.ArgumentParser(description="UK/SRA data fetcher")
    parser.add_argument('command', choices=['bootstrap', 'test'],
                        help='Command to run')
    parser.add_argument('--sample', action='store_true',
                        help='Fetch only sample records')
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command == 'test':
        success = test_api()
        sys.exit(0 if success else 1)
    elif args.command == 'bootstrap':
        if args.sample:
            count = bootstrap_sample()
        else:
            count = bootstrap_full()
        sys.exit(0 if count > 0 else 1)


if __name__ == '__main__':
    main()
