#!/usr/bin/env python3
"""
Uzbekistan Legislation (Lex.uz) Data Fetcher

Official national database of legislative information of the Republic of Uzbekistan.
https://lex.uz/

Content is server-rendered inside <div id="divCont"> with semantic CSS classes:
ACT_FORM, ACT_TITLE, ACT_TEXT, SIGNATURE, etc.
Text lives in <a id="NNNN">...</a> tags after the lx_elem2 UI chrome.

50K+ legislative acts. No authentication required. Russian language used.
"""

import json
import logging
import os
import re
import subprocess
import sys
import time
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://lex.uz"
SEARCH_URL = f"{BASE_URL}/ru/search/nat"
DOC_URL = f"{BASE_URL}/ru/docs/{{}}"
PAGE_SIZE = 20

# Content classes to extract (in order of document structure)
CONTENT_CLASSES = {
    'ACT_FORM', 'ACT_TITLE', 'ACT_TEXT', 'ACCEPTING_BODY', 'SIGNATURE',
    'DEPARTMENTAL', 'ACT_ESSENTIAL_ELEMENTS', 'ACT_ESSENTIAL_ELEMENTS_NUM',
    'GRIF_PARLAMENT', 'BY_DEFAULT', 'ACT_TITLE_APPL', 'UNOFFIAL',
    'COMMENT_FOR_WARNING',
}

# Classes that carry body text
BODY_CLASSES = {'ACT_TEXT', 'GRIF_PARLAMENT', 'BY_DEFAULT', 'COMMENT_FOR_WARNING'}


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = text.replace('&nbsp;', ' ').replace('&amp;', '&')
    text = text.replace('&lt;', '<').replace('&gt;', '>')
    text = text.replace('&quot;', '"').replace('&#39;', "'")
    return text


def extract_divCont(html: str) -> str:
    """Extract raw HTML inside <div id="divCont">."""
    m = re.search(r'<div\s+id="divCont"[^>]*>(.*)', html, re.DOTALL)
    if not m:
        return ''
    content = m.group(1)
    # Find matching closing — rough heuristic: take until the enclosing container ends
    # divCont is a single very long div; just take everything after it
    return content


def parse_document_content(html: str) -> Dict[str, Any]:
    """Parse the full document page and return structured fields."""
    divcont = extract_divCont(html)
    if not divcont:
        return {}

    # Build regex for elements with lx_elem class
    # Pattern: <div class="CLASS lx_elem" ...><div class="lx_elem2"><div class="lx_elem3">...buttons...</div></div><a id="NNN">CONTENT</a></div>
    cls_pattern = '|'.join(CONTENT_CLASSES)
    pattern = re.compile(
        r'<div\s+class="(' + cls_pattern + r')\s+lx_elem"[^>]*>'
        r'<div\s+class="lx_elem2">.*?</div></div>'
        r'<a\s+id="\d+">(.*?)</a></div>',
        re.DOTALL
    )

    elements = []
    for m in pattern.finditer(divcont):
        cls = m.group(1)
        raw_text = strip_html(m.group(2)).strip()
        if raw_text:
            elements.append((cls, raw_text))

    if not elements:
        return {}

    # Build structured output
    doc_type = ''
    title = ''
    body_parts = []
    signature = ''
    meta_parts = []

    for cls, text in elements:
        if cls == 'ACT_FORM':
            doc_type = text
        elif cls == 'ACT_TITLE':
            if not title:
                title = text
        elif cls == 'ACT_TITLE_APPL':
            body_parts.append(f"\n{text}\n")
        elif cls in BODY_CLASSES:
            body_parts.append(text)
        elif cls == 'ACCEPTING_BODY':
            meta_parts.append(text)
        elif cls == 'SIGNATURE':
            signature = text
        elif cls == 'DEPARTMENTAL':
            meta_parts.append(text)
        elif cls in ('ACT_ESSENTIAL_ELEMENTS', 'ACT_ESSENTIAL_ELEMENTS_NUM'):
            meta_parts.append(text)
        elif cls == 'UNOFFIAL':
            meta_parts.append(f"[{text}]")

    # Assemble full text
    full_text = '\n\n'.join(body_parts)
    if signature:
        full_text += f"\n\n{signature}"

    # Clean up whitespace
    full_text = re.sub(r'[ \t]+', ' ', full_text)
    full_text = re.sub(r'\n{3,}', '\n\n', full_text)
    full_text = full_text.strip()

    return {
        'doc_type': doc_type,
        'title': title,
        'text': full_text,
        'signature': signature,
        'meta': meta_parts,
    }


def parse_title_meta(html: str) -> Dict[str, str]:
    """Extract doc number and date from the <title> tag."""
    m = re.search(r'<title>\s*(?:&nbsp;)?\s*(.+?)\s*</title>', html, re.DOTALL)
    if not m:
        return {}
    raw = m.group(1).replace('&nbsp;', ' ').strip()
    # Format: "3604-сон 27.01.2025. Об утверждении..."
    dm = re.match(r'(\S+)\s+(\d{2}\.\d{2}\.\d{4})\.\s*(.*)', raw)
    if dm:
        date_str = dm.group(2)
        try:
            dt = datetime.strptime(date_str, '%d.%m.%Y')
            iso_date = dt.strftime('%Y-%m-%d')
        except ValueError:
            iso_date = date_str
        return {
            'doc_number': dm.group(1),
            'date': iso_date,
            'title_from_meta': dm.group(3).strip(),
        }
    return {'title_from_meta': raw}


class LexUzFetcher:
    """Fetcher for Uzbekistan legislation from lex.uz"""

    def __init__(self, slow_mode: bool = False):
        self.slow_mode = slow_mode
        self.doc_delay = 3.0 if slow_mode else 1.5
        self.page_delay = 5.0 if slow_mode else 2.0

    def _curl_get(self, url: str, max_attempts: int = 3) -> Optional[str]:
        """GET HTML content via curl."""
        for attempt in range(max_attempts):
            try:
                result = subprocess.run(
                    ['curl', '-s', '-L', '--max-time', '30',
                     '-H', 'User-Agent: Mozilla/5.0 (compatible; LegalDataHunter/1.0)',
                     '-H', 'Accept: text/html,application/xhtml+xml',
                     '-H', 'Accept-Language: ru,en;q=0.5',
                     url],
                    capture_output=True, text=True, timeout=40
                )
                if result.returncode == 0 and result.stdout:
                    return result.stdout
                delay = min(5 * (2 ** attempt), 30)
                logger.warning(f"GET failed attempt {attempt+1} for {url}, waiting {delay}s...")
                time.sleep(delay)
            except subprocess.TimeoutExpired:
                delay = min(5 * (2 ** attempt), 30)
                logger.warning(f"GET timeout attempt {attempt+1}, waiting {delay}s...")
                time.sleep(delay)
        return None

    def _curl_post(self, url: str, form_data: Dict[str, str], cookies: str = '',
                   max_attempts: int = 3) -> Optional[str]:
        """POST form data via curl."""
        encoded = urllib.parse.urlencode(form_data)
        for attempt in range(max_attempts):
            try:
                cmd = ['curl', '-s', '-L', '--max-time', '30',
                       '-X', 'POST',
                       '-H', 'User-Agent: Mozilla/5.0 (compatible; LegalDataHunter/1.0)',
                       '-H', 'Content-Type: application/x-www-form-urlencoded',
                       '-H', 'Accept: text/html,application/xhtml+xml',
                       '-d', encoded,
                       url]
                if cookies:
                    cmd.insert(-1, '-H')
                    cmd.insert(-1, f'Cookie: {cookies}')
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=40)
                if result.returncode == 0 and result.stdout:
                    return result.stdout
                delay = min(5 * (2 ** attempt), 30)
                logger.warning(f"POST failed attempt {attempt+1}, waiting {delay}s...")
                time.sleep(delay)
            except subprocess.TimeoutExpired:
                delay = min(5 * (2 ** attempt), 30)
                logger.warning(f"POST timeout attempt {attempt+1}, waiting {delay}s...")
                time.sleep(delay)
        return None

    def _extract_asp_fields(self, html: str) -> Dict[str, str]:
        """Extract ASP.NET hidden form fields from HTML."""
        fields = {}
        for m in re.finditer(r'<input[^>]+name="(__[^"]+)"[^>]+value="([^"]*)"', html):
            fields[m.group(1)] = m.group(2)
        # Also check value before name
        for m in re.finditer(r'<input[^>]+value="([^"]*)"[^>]+name="(__[^"]+)"', html):
            fields[m.group(2)] = m.group(1)
        return fields

    def _parse_search_doc_ids(self, html: str) -> List[Dict[str, str]]:
        """Extract document IDs and basic info from search results HTML."""
        results = []
        # Links like: <a class="lx_link" href="/ru/docs/7965945?query=...">TITLE</a>
        for m in re.finditer(
            r'<a[^>]+class="lx_link"[^>]+href="(/ru/docs/(\d+)[^"]*)"[^>]*>(.*?)</a>',
            html, re.DOTALL
        ):
            title = strip_html(m.group(3)).strip()
            results.append({
                'doc_id': m.group(2),
                'title': title,
                'href': m.group(1),
            })

        # Extract dates from result rows
        dates = re.findall(r'dd-table__main-item_date[^>]*>([^<]+)<', html)
        for i, r in enumerate(results):
            if i < len(dates):
                raw_date = dates[i].strip()
                try:
                    dt = datetime.strptime(raw_date, '%d.%m.%Y')
                    r['date'] = dt.strftime('%Y-%m-%d')
                except ValueError:
                    r['date'] = raw_date

        return results

    def search_legislation(self, date_from: str = "", date_to: str = "",
                           lang: str = "1", form_id: str = "",
                           max_pages: int = 0) -> Iterator[Dict[str, str]]:
        """Search for legislation and yield results across pages."""
        params = {}
        if date_from:
            params['from'] = date_from
        if date_to:
            params['to'] = date_to
        if lang:
            params['lang'] = lang
        if form_id:
            params['form_id'] = form_id

        url = SEARCH_URL + '?' + urllib.parse.urlencode(params) if params else SEARCH_URL

        logger.info(f"Searching: {url}")
        html = self._curl_get(url)
        if not html:
            logger.error("Failed to load search page")
            return

        page = 1
        while True:
            results = self._parse_search_doc_ids(html)
            if not results:
                logger.info(f"No results on page {page}, stopping")
                break

            logger.info(f"Page {page}: found {len(results)} results")
            for r in results:
                yield r

            if max_pages and page >= max_pages:
                logger.info(f"Reached max pages ({max_pages})")
                break

            # Check for next page link
            next_target = f'rptPaging$ctl{page:02d}$lbPaging'
            if next_target not in html:
                logger.info(f"No more pages after page {page}")
                break

            # ASP.NET PostBack pagination
            page += 1
            asp_fields = self._extract_asp_fields(html)
            form_data = {
                '__VIEWSTATE': asp_fields.get('__VIEWSTATE', ''),
                '__VIEWSTATEGENERATOR': asp_fields.get('__VIEWSTATEGENERATOR', ''),
                '__EVENTVALIDATION': asp_fields.get('__EVENTVALIDATION', ''),
                '__EVENTTARGET': f'ucFoundActsControl$rptPaging$ctl{(page-1):02d}$lbPaging',
                '__EVENTARGUMENT': '',
            }

            logger.info(f"Navigating to page {page}...")
            time.sleep(self.page_delay)
            html = self._curl_post(url, form_data)
            if not html:
                logger.warning(f"Failed to load page {page}")
                break

    def fetch_document(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single document with full text."""
        url = DOC_URL.format(doc_id)
        html = self._curl_get(url)
        if not html:
            return None

        # Parse content from divCont
        content = parse_document_content(html)
        if not content or not content.get('text'):
            logger.warning(f"Doc {doc_id}: no text extracted")
            return None

        # Parse title metadata
        title_meta = parse_title_meta(html)

        title = content.get('title') or title_meta.get('title_from_meta', '')
        date = title_meta.get('date', '')
        doc_number = title_meta.get('doc_number', '')
        doc_type = content.get('doc_type', '')

        return {
            'doc_id': doc_id,
            'title': title,
            'text': content['text'],
            'date': date,
            'doc_number': doc_number,
            'doc_type': doc_type,
            'url': f"{BASE_URL}/ru/docs/{doc_id}",
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Fetch all legislation documents."""
        count = 0
        for result in self.search_legislation():
            doc_id = result.get('doc_id')
            if not doc_id:
                continue
            doc = self.fetch_document(doc_id)
            if doc:
                if not doc.get('date') and result.get('date'):
                    doc['date'] = result['date']
                yield doc
                count += 1
            time.sleep(self.doc_delay)
        logger.info(f"Fetched {count} documents total")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch documents published since a given date."""
        since_str = since.strftime('%d.%m.%Y')
        today_str = datetime.now().strftime('%d.%m.%Y')
        count = 0
        for result in self.search_legislation(date_from=since_str, date_to=today_str):
            doc_id = result.get('doc_id')
            if not doc_id:
                continue
            doc = self.fetch_document(doc_id)
            if doc:
                if not doc.get('date') and result.get('date'):
                    doc['date'] = result['date']
                yield doc
                count += 1
            time.sleep(self.doc_delay)
        logger.info(f"Fetched {count} updated documents since {since_str}")

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema."""
        return {
            '_id': str(raw_doc.get('doc_id', '')),
            '_source': 'UZ/LexUz',
            '_type': 'legislation',
            '_fetched_at': datetime.now().isoformat(),
            'title': raw_doc.get('title', ''),
            'text': raw_doc.get('text', ''),
            'date': raw_doc.get('date', ''),
            'doc_number': raw_doc.get('doc_number', ''),
            'doc_type': raw_doc.get('doc_type', ''),
            'url': raw_doc.get('url', ''),
        }


def bootstrap_sample(slow_mode: bool = False):
    """Fetch a sample of documents for testing."""
    sample_dir = Path(__file__).parent / 'sample'
    sample_dir.mkdir(exist_ok=True)

    # Clear old samples
    for f in sample_dir.glob('*.json'):
        f.unlink()

    fetcher = LexUzFetcher(slow_mode=slow_mode)

    count = 0
    target = 15

    # Search recent legislation (laws specifically for richer text)
    for result in fetcher.search_legislation(
        date_from='01.01.2024',
        date_to='31.12.2025',
        lang='1',
        max_pages=10,
    ):
        if count >= target:
            break

        doc_id = result.get('doc_id')
        if not doc_id:
            continue

        logger.info(f"[{count+1}/{target}] Fetching doc {doc_id}: {result.get('title', '')[:60]}...")
        doc = fetcher.fetch_document(doc_id)
        if not doc:
            logger.warning(f"Skipping doc {doc_id} - no content")
            continue

        normalized = fetcher.normalize(doc)

        if not normalized.get('text') or len(normalized['text']) < 100:
            logger.warning(f"Skipping doc {doc_id} - text too short ({len(normalized.get('text', ''))} chars)")
            continue

        out_path = sample_dir / f"{doc_id}.json"
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(normalized, f, ensure_ascii=False, indent=2)

        count += 1
        logger.info(f"  Saved {out_path.name} ({len(normalized['text'])} chars)")
        time.sleep(fetcher.doc_delay)

    logger.info(f"\nSample complete: {count} documents saved to {sample_dir}/")
    validate_sample(sample_dir)


def validate_sample(sample_dir: Path):
    """Validate sample data quality."""
    files = list(sample_dir.glob('*.json'))
    if not files:
        logger.error("No sample files found!")
        return

    total = len(files)
    has_text = 0
    has_title = 0
    has_date = 0
    text_lengths = []

    for f in files:
        with open(f, 'r', encoding='utf-8') as fh:
            doc = json.load(fh)
        if doc.get('text') and len(doc['text']) > 50:
            has_text += 1
            text_lengths.append(len(doc['text']))
        if doc.get('title'):
            has_title += 1
        if doc.get('date'):
            has_date += 1

    logger.info(f"\n=== VALIDATION SUMMARY ===")
    logger.info(f"Total samples: {total}")
    logger.info(f"With full text: {has_text}/{total}")
    logger.info(f"With title: {has_title}/{total}")
    logger.info(f"With date: {has_date}/{total}")
    if text_lengths:
        avg_len = sum(text_lengths) // len(text_lengths)
        logger.info(f"Text length: min={min(text_lengths)}, avg={avg_len}, max={max(text_lengths)}")

    if has_text < total:
        logger.warning(f"WARNING: {total - has_text} documents missing full text!")
    if total >= 10 and has_text >= 10:
        logger.info("PASS: 10+ documents with full text")
    else:
        logger.warning(f"FAIL: Need 10+ docs with text, got {has_text}")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Uzbekistan LexUz Legislation Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'validate'],
                        help='Command to run')
    parser.add_argument('--sample', action='store_true',
                        help='Fetch sample data only')
    parser.add_argument('--slow', action='store_true',
                        help='Use slower rate limiting')
    args = parser.parse_args()

    if args.command == 'bootstrap':
        if args.sample:
            bootstrap_sample(slow_mode=args.slow)
        else:
            logger.info("Full fetch not implemented in bootstrap mode. Use --sample.")
    elif args.command == 'validate':
        sample_dir = Path(__file__).parent / 'sample'
        validate_sample(sample_dir)
