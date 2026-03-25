#!/usr/bin/env python3
"""
Andorra Portal Juridic Data Fetcher

Consolidated legislation of the Principality of Andorra.
https://www.portaljuridicandorra.ad/

Tiki Wiki CMS. Year-based enumeration via POST tracker filter.
Full text extracted from <article class="wikitext"> elements.
~1,658 laws and regulations since 1985. Catalan language.
"""

import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://www.portaljuridicandorra.ad"
INDEX_URL = f"{BASE_URL}/tiki-index.php?page=IndexPerAnys"
CRAWL_DELAY = 10.0  # Respecting robots.txt


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = text.replace('&nbsp;', ' ').replace('&amp;', '&')
    text = text.replace('&lt;', '<').replace('&gt;', '>')
    text = text.replace('&quot;', '"').replace('&#39;', "'")
    text = text.replace('&agrave;', 'à').replace('&eacute;', 'é')
    text = text.replace('&iacute;', 'í').replace('&ograve;', 'ò')
    text = text.replace('&uacute;', 'ú').replace('&uuml;', 'ü')
    text = text.replace('&ccedil;', 'ç').replace('&middot;', '·')
    # Decode numeric entities
    text = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), text)
    text = re.sub(r'&#x([0-9a-fA-F]+);', lambda m: chr(int(m.group(1), 16)), text)
    return text


class PortalJuridicFetcher:
    """Fetcher for Andorran consolidated legislation."""

    def __init__(self, fast_mode: bool = False):
        self.delay = 3.0 if fast_mode else CRAWL_DELAY

    def _curl_get(self, url: str, max_attempts: int = 3) -> Optional[str]:
        """GET HTML via curl."""
        for attempt in range(max_attempts):
            try:
                result = subprocess.run(
                    ['curl', '-s', '-L', '--max-time', '30',
                     '-H', 'User-Agent: Mozilla/5.0 (compatible; LegalDataHunter/1.0)',
                     '-H', 'Accept: text/html',
                     '-H', 'Accept-Language: ca,en;q=0.5',
                     url],
                    capture_output=True, text=True, timeout=40
                )
                if result.returncode == 0 and result.stdout:
                    return result.stdout
                delay = min(5 * (2 ** attempt), 30)
                logger.warning(f"GET failed attempt {attempt+1}, waiting {delay}s...")
                time.sleep(delay)
            except subprocess.TimeoutExpired:
                delay = min(5 * (2 ** attempt), 30)
                logger.warning(f"GET timeout attempt {attempt+1}, waiting {delay}s...")
                time.sleep(delay)
        return None

    def _curl_post(self, url: str, data: str, max_attempts: int = 3) -> Optional[str]:
        """POST form data via curl."""
        for attempt in range(max_attempts):
            try:
                result = subprocess.run(
                    ['curl', '-s', '-L', '--max-time', '30',
                     '-X', 'POST',
                     '-H', 'User-Agent: Mozilla/5.0 (compatible; LegalDataHunter/1.0)',
                     '-H', 'Content-Type: application/x-www-form-urlencoded',
                     '-d', data,
                     url],
                    capture_output=True, text=True, timeout=40
                )
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

    def list_year(self, year: int) -> List[Dict[str, str]]:
        """List all legislation for a given year."""
        data = f'trackerId=1&iTrackerFilter=1&f_6={year}&filter=Cerca'
        html = self._curl_post(INDEX_URL, data)
        if not html:
            return []

        results = []
        # Pattern: href="ID" title="FULL TITLE" class="wiki wiki_page">DISPLAY TITLE
        for m in re.finditer(
            r'href="([A-Z]\d{8}[A-Z])"[^>]*title="([^"]*)"[^>]*class="wiki wiki_page"',
            html
        ):
            doc_id = m.group(1)
            title = strip_html(m.group(2))
            results.append({
                'doc_id': doc_id,
                'title': title,
            })

        return results

    def fetch_document(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single document with full text."""
        url = f"{BASE_URL}/{doc_id}"
        html = self._curl_get(url)
        if not html:
            return None

        # Check for error page
        if "No s'ha trobat la" in html or 'rboxcontent' in html and 'Error' in html:
            logger.warning(f"Doc {doc_id}: page not found")
            return None

        # Extract title from <h1>
        title = ''
        h1_match = re.search(r'<h1[^>]*>(.+?)</h1>', html, re.DOTALL)
        if h1_match:
            title = strip_html(h1_match.group(1)).strip()

        # Extract full text from <article class="wikitext ...">
        article_match = re.search(
            r'<article[^>]*class="wikitext[^"]*"[^>]*>(.*?)</article>',
            html, re.DOTALL
        )
        if not article_match:
            logger.warning(f"Doc {doc_id}: no article element found")
            return None

        raw_content = article_match.group(1)

        # Remove script tags and their content
        raw_content = re.sub(r'<script[^>]*>.*?</script>', '', raw_content, flags=re.DOTALL)

        # Remove table of contents
        raw_content = re.sub(r'<div id=["\']toc["\'][^>]*>.*?</div>\s*</div>', '', raw_content, flags=re.DOTALL)

        # Convert to text
        text = strip_html(raw_content)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = text.strip()

        # Remove JS artifacts like "var UrlFitxa = ..."
        text = re.sub(r'var \w+ = "[^"]*";\s*', '', text)

        if not text or len(text) < 50:
            logger.warning(f"Doc {doc_id}: text too short ({len(text)} chars)")
            return None

        # Parse date from doc_id: format is X{YYYYMMDD}Y
        date = ''
        date_match = re.match(r'[A-Z](\d{4})(\d{2})(\d{2})[A-Z]', doc_id)
        if date_match:
            date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"

        # Determine doc type from prefix
        prefix = doc_id[0]
        doc_type_map = {'L': 'Llei', 'R': 'Reglament', 'A': 'Altres'}
        doc_type = doc_type_map.get(prefix, 'Unknown')

        return {
            'doc_id': doc_id,
            'title': title,
            'text': text,
            'date': date,
            'doc_type': doc_type,
            'url': f"{BASE_URL}/{doc_id}",
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Fetch all legislation across all years."""
        years = list(range(1985, datetime.now().year + 1))
        count = 0
        for year in years:
            logger.info(f"Listing year {year}...")
            docs = self.list_year(year)
            logger.info(f"  {len(docs)} documents in {year}")
            time.sleep(self.delay)

            for doc_info in docs:
                doc = self.fetch_document(doc_info['doc_id'])
                if doc:
                    yield doc
                    count += 1
                time.sleep(self.delay)

        logger.info(f"Fetched {count} documents total")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch documents from years since the given date."""
        start_year = since.year
        for year in range(start_year, datetime.now().year + 1):
            logger.info(f"Listing year {year}...")
            docs = self.list_year(year)
            time.sleep(self.delay)

            for doc_info in docs:
                doc = self.fetch_document(doc_info['doc_id'])
                if doc and doc.get('date', '') >= since.strftime('%Y-%m-%d'):
                    yield doc
                time.sleep(self.delay)

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema."""
        return {
            '_id': raw_doc.get('doc_id', ''),
            '_source': 'AD/PortalJuridic',
            '_type': 'legislation',
            '_fetched_at': datetime.now().isoformat(),
            'title': raw_doc.get('title', ''),
            'text': raw_doc.get('text', ''),
            'date': raw_doc.get('date', ''),
            'doc_type': raw_doc.get('doc_type', ''),
            'url': raw_doc.get('url', ''),
        }


def bootstrap_sample(fast_mode: bool = False):
    """Fetch a sample of documents for testing."""
    sample_dir = Path(__file__).parent / 'sample'
    sample_dir.mkdir(exist_ok=True)

    # Clear old samples
    for f in sample_dir.glob('*.json'):
        f.unlink()

    fetcher = PortalJuridicFetcher(fast_mode=fast_mode)

    # Get docs from 2024 (recent, good mix)
    logger.info("Listing 2024 legislation...")
    docs_2024 = fetcher.list_year(2024)
    logger.info(f"Found {len(docs_2024)} documents in 2024")

    # Also get some laws (L prefix) from 2023
    time.sleep(fetcher.delay)
    logger.info("Listing 2023 legislation...")
    docs_2023 = fetcher.list_year(2023)
    logger.info(f"Found {len(docs_2023)} documents in 2023")

    # Mix regulations and laws
    candidates = docs_2024[:10] + docs_2023[:10]

    count = 0
    target = 15
    for doc_info in candidates:
        if count >= target:
            break

        doc_id = doc_info['doc_id']
        logger.info(f"[{count+1}/{target}] Fetching {doc_id}: {doc_info['title'][:60]}...")

        time.sleep(fetcher.delay)
        doc = fetcher.fetch_document(doc_id)
        if not doc:
            continue

        normalized = fetcher.normalize(doc)
        if not normalized.get('text') or len(normalized['text']) < 100:
            logger.warning(f"Skipping {doc_id} - text too short")
            continue

        out_path = sample_dir / f"{doc_id}.json"
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(normalized, f, ensure_ascii=False, indent=2)

        count += 1
        logger.info(f"  Saved {out_path.name} ({len(normalized['text'])} chars)")

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
    parser = argparse.ArgumentParser(description='Andorra Portal Juridic Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'validate'],
                        help='Command to run')
    parser.add_argument('--sample', action='store_true',
                        help='Fetch sample data only')
    parser.add_argument('--fast', action='store_true',
                        help='Reduce crawl delay (for testing)')
    args = parser.parse_args()

    if args.command == 'bootstrap':
        if args.sample:
            bootstrap_sample(fast_mode=args.fast)
        else:
            logger.info("Full fetch not implemented in bootstrap mode. Use --sample.")
    elif args.command == 'validate':
        sample_dir = Path(__file__).parent / 'sample'
        validate_sample(sample_dir)
