#!/usr/bin/env python3
"""
NO/SKD - Norwegian Tax Administration (Skatteetaten) Doctrine Fetcher

Fetches tax doctrine documents from Skatteetaten's rettskilder (legal sources):
  - Prinsipputtalelser (principle statements)
  - BFU (binding advance rulings)
  - Uttalelser (statements)
  - Domskommentarer (case law commentaries)
  - Skatteklagenemnda decisions (Tax Appeals Board)
  - Klagenemnda for MVA decisions (VAT Appeals Board)
  - Klagevedtak arveavgift (inheritance tax appeal decisions)
  - Avgiftsrundskriv (excise tax circulars)
  - Rundskriv (general circulars)
  - Retningslinjer (guidelines)
  - Horinger (consultations)

Data source: https://www.skatteetaten.no/rettskilder/
Index method: Embedded 'allthedata' JavaScript variable on listing pages.
Full text: Fetched from individual document HTML pages.
License: NLOD 2.0 (Norwegian License for Open Government Data)
"""

import argparse
import hashlib
import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://www.skatteetaten.no"
SOURCE_ID = "NO/SKD"
SAMPLE_DIR = Path(__file__).parent / "sample"

# Category listing pages containing allthedata JSON indexes
CATEGORIES = {
    "prinsipputtalelser": "/rettskilder/type/uttalelser/prinsipputtalelser/",
    "bfu": "/rettskilder/type/uttalelser/bfu/",
    "uttalelser": "/rettskilder/type/uttalelser/uttalelser/",
    "domskommentarer": "/rettskilder/type/uttalelser/domskommentarer/",
    "skatteklagenemnda": "/rettskilder/type/vedtak/skatteklagenemnda/",
    "klagenemnda_mva": "/rettskilder/type/vedtak/klagenemnda-for-merverdiavgift/",
    "klagevedtak_arv": "/rettskilder/type/vedtak/klagevedtak-arveavgift/",
    "avgiftsrundskriv": "/rettskilder/type/rundskriv-retningslinjer-og-andre-rettskilder/avgiftsrundskriv/",
    "rundskriv": "/rettskilder/type/rundskriv-retningslinjer-og-andre-rettskilder/rundskriv/",
    "retningslinjer": "/rettskilder/type/retningslinjer/",
    "horinger": "/rettskilder/type/horinger/",
}


class SkatteetatenFetcher:
    """Fetcher for Norwegian Tax Administration doctrine documents."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'nb-NO,nb;q=0.9,no;q=0.8,en;q=0.5',
        })

    def _extract_allthedata(self, html: str) -> List[Dict[str, Any]]:
        """Extract the allthedata JSON array from a listing page's JavaScript."""
        match = re.search(r'var\s+allthedata\s*=\s*(\[.*?\])\s*;', html, re.DOTALL)
        if not match:
            return []
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse allthedata JSON: {e}")
            return []

    def fetch_index(self, category: str, path: str) -> List[Dict[str, Any]]:
        """Fetch the document index for a category."""
        url = BASE_URL + path
        logger.info(f"Fetching index for {category}: {url}")
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch index {url}: {e}")
            return []

        entries = self._extract_allthedata(resp.text)
        logger.info(f"  {category}: {len(entries)} entries found")
        for entry in entries:
            entry['_category'] = category
        return entries

    def fetch_all_indexes(self) -> List[Dict[str, Any]]:
        """Fetch indexes from all categories."""
        all_entries = []
        for cat_name, cat_path in CATEGORIES.items():
            entries = self.fetch_index(cat_name, cat_path)
            all_entries.extend(entries)
            time.sleep(1.0)
        logger.info(f"Total index entries: {len(all_entries)}")
        return all_entries

    def _extract_content(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract main article text from a document page."""
        # Try specific content containers used by Skatteetaten
        content = None
        for selector in [
            ('div', {'class': 'article-body'}),
            ('div', {'class': 'article-content'}),
            'article',
            'main',
            ('div', {'class': re.compile(r'content|body|text', re.I)}),
            ('div', {'role': 'main'}),
        ]:
            if isinstance(selector, str):
                content = soup.find(selector)
            else:
                content = soup.find(*selector)
            if content:
                break

        if not content:
            return None

        # Remove navigation, footer, sidebar, scripts
        for tag in content.find_all(['nav', 'footer', 'aside', 'script', 'style', 'noscript']):
            tag.decompose()
        for tag in content.find_all(class_=re.compile(
            r'nav|menu|sidebar|footer|breadcrumb|cookie|banner|share|social|print-button',
            re.I
        )):
            tag.decompose()

        text = content.get_text(separator="\n", strip=True)
        lines = [line.strip() for line in text.splitlines()]
        lines = [line for line in lines if line]
        text = "\n".join(lines)

        return text if len(text) > 50 else None

    def _extract_date(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract publication date from a document page."""
        # Check meta tags
        for tag in soup.find_all('meta'):
            name = (tag.get('name') or tag.get('property') or '').lower()
            content_val = tag.get('content', '')
            if content_val and ('published' in name or 'date' in name):
                parsed = self._parse_date(content_val)
                if parsed:
                    return parsed

        # Check page text for Norwegian date patterns
        page_text = soup.get_text()
        pub_match = re.search(r'Publisert:\s*(\d{1,2})\.(\d{1,2})\.(\d{4})', page_text)
        if pub_match:
            d, m, y = pub_match.groups()
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"

        return None

    def _parse_date(self, date_str: Optional[str]) -> Optional[str]:
        """Parse various date formats to ISO 8601."""
        if not date_str:
            return None
        if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
            return date_str[:10]
        match = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str)
        if match:
            d, m, y = match.groups()
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
        try:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            return None

    def fetch_document(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch full text and metadata from a single document page."""
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"HTTP {resp.status_code} for {url}")
                return None
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

        soup = BeautifulSoup(resp.text, 'html.parser')

        # Extract title
        title = None
        h1 = soup.find('h1')
        if h1:
            title = h1.get_text(strip=True)
        if not title:
            title_tag = soup.find('title')
            if title_tag:
                title = title_tag.get_text(strip=True).split('|')[0].strip()

        # Extract content from a fresh parse (decompose modifies soup)
        soup2 = BeautifulSoup(resp.text, 'html.parser')
        text = self._extract_content(soup2)

        # Extract date
        date = self._extract_date(soup)

        if not text:
            logger.warning(f"No content extracted from {url}")
            return None

        return {
            'title': title or '',
            'text': text,
            'date': date,
            'url': url,
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw document into standard schema."""
        url = raw['url']
        doc_id = hashlib.md5(url.encode()).hexdigest()[:12]

        date = self._parse_date(raw.get('date'))
        if not date and raw.get('_index_date'):
            date = self._parse_date(raw['_index_date'])

        return {
            '_id': f"NO-SKD-{doc_id}",
            '_source': SOURCE_ID,
            '_type': 'doctrine',
            '_fetched_at': datetime.utcnow().isoformat() + 'Z',
            'title': raw.get('title', ''),
            'text': raw.get('text', ''),
            'date': date,
            'url': url,
            'category': raw.get('_category', ''),
            'language': 'nob',
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Yield all normalized documents."""
        entries = self.fetch_all_indexes()
        for i, entry in enumerate(entries):
            url = entry.get('url', '')
            if not url:
                continue
            if not url.startswith('http'):
                url = BASE_URL + (url if url.startswith('/') else '/' + url)

            logger.info(f"Fetching document {i+1}/{len(entries)}: {url}")
            doc = self.fetch_document(url)
            if doc:
                doc['_category'] = entry.get('_category', '')
                # Use index date as fallback
                props = entry.get('properties', {})
                doc['_index_date'] = props.get('startPublish') or props.get('metadataDate')
                yield self.normalize(doc)

            time.sleep(1.5)

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Yield documents modified since a given date."""
        since_dt = datetime.fromisoformat(since)
        entries = self.fetch_all_indexes()
        for entry in entries:
            props = entry.get('properties', {})
            meta_date = props.get('metadataDate') or props.get('startPublish')
            if meta_date:
                try:
                    entry_dt = datetime.fromisoformat(meta_date.replace('Z', '+00:00'))
                    if entry_dt < since_dt:
                        continue
                except (ValueError, TypeError):
                    pass

            url = entry.get('url', '')
            if not url:
                continue
            if not url.startswith('http'):
                url = BASE_URL + (url if url.startswith('/') else '/' + url)

            doc = self.fetch_document(url)
            if doc:
                doc['_category'] = entry.get('_category', '')
                doc['_index_date'] = meta_date
                yield self.normalize(doc)
            time.sleep(1.5)

    def bootstrap_sample(self, n: int = 15) -> List[Dict[str, Any]]:
        """Fetch a sample of documents from different categories for testing."""
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

        # Fetch indexes and pick a few from each category
        all_entries = self.fetch_all_indexes()

        # Group by category
        by_cat: Dict[str, list] = {}
        for entry in all_entries:
            cat = entry.get('_category', 'unknown')
            by_cat.setdefault(cat, []).append(entry)

        # Pick samples from each category
        selected = []
        cats = list(by_cat.keys())
        per_cat = max(1, n // len(cats)) if cats else n
        for cat in cats:
            entries = by_cat[cat][:per_cat]
            for entry in entries:
                entry['_category'] = cat
                selected.append(entry)
            if len(selected) >= n:
                break

        # If we don't have enough, add more from largest categories
        if len(selected) < n:
            for cat in sorted(cats, key=lambda c: len(by_cat[c]), reverse=True):
                for entry in by_cat[cat][per_cat:]:
                    if len(selected) >= n:
                        break
                    entry['_category'] = cat
                    selected.append(entry)
                if len(selected) >= n:
                    break

        selected = selected[:n]
        results = []

        for i, entry in enumerate(selected):
            url = entry.get('url', '')
            if not url:
                continue
            if not url.startswith('http'):
                url = BASE_URL + (url if url.startswith('/') else '/' + url)

            logger.info(f"Sample {i+1}/{len(selected)}: {url}")
            doc = self.fetch_document(url)
            if not doc:
                logger.warning(f"  Skipped (no content)")
                continue

            doc['_category'] = entry.get('_category', '')
            props = entry.get('properties', {})
            doc['_index_date'] = props.get('startPublish') or props.get('metadataDate')
            normalized = self.normalize(doc)
            results.append(normalized)

            # Save sample
            fname = f"{normalized['_id']}.json"
            with open(SAMPLE_DIR / fname, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            logger.info(f"  Saved: {fname} ({len(normalized['text'])} chars)")

            time.sleep(1.5)

        logger.info(f"Sample complete: {len(results)} documents saved to {SAMPLE_DIR}")
        return results


def main():
    parser = argparse.ArgumentParser(description='NO/SKD - Skatteetaten Doctrine Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'updates'],
                        help='Command to run')
    parser.add_argument('--sample', action='store_true',
                        help='Fetch sample data only (for bootstrap)')
    parser.add_argument('--since', type=str,
                        help='Fetch updates since date (ISO format)')
    parser.add_argument('--limit', type=int, default=15,
                        help='Max documents for sample')
    args = parser.parse_args()

    fetcher = SkatteetatenFetcher()

    if args.command == 'bootstrap':
        if args.sample:
            results = fetcher.bootstrap_sample(n=args.limit)
            print(f"\nSample: {len(results)} documents fetched")
            for r in results:
                text_len = len(r.get('text', ''))
                print(f"  {r['_id']} | {r['category']:20s} | {text_len:>6} chars | {r['title'][:60]}")
        else:
            count = 0
            for doc in fetcher.fetch_all():
                count += 1
                if count % 100 == 0:
                    logger.info(f"Fetched {count} documents")
            print(f"Total: {count} documents fetched")

    elif args.command == 'updates':
        if not args.since:
            print("Error: --since required for updates command", file=sys.stderr)
            sys.exit(1)
        count = 0
        for doc in fetcher.fetch_updates(args.since):
            count += 1
        print(f"Updates: {count} documents fetched since {args.since}")

    elif args.command == 'fetch':
        count = 0
        for doc in fetcher.fetch_all():
            count += 1
        print(f"Total: {count} documents fetched")


if __name__ == '__main__':
    main()
