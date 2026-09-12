#!/usr/bin/env python3
"""
IR/IranDataPortal - Iran Data Portal (Syracuse University)

Fetches English translations of Iranian laws, regulations, court decisions,
and constitutional documents from irandataportal.syr.edu.

Data source: https://irandataportal.syr.edu/
License: Academic open access (Syracuse University)
"""

import argparse
import hashlib
import json
import logging
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urljoin

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://irandataportal.syr.edu/"
SITEMAP_URL = "https://irandataportal.syr.edu/wp-sitemap-posts-page-1.xml"
SOURCE_ID = "IR/IranDataPortal"
SAMPLE_DIR = Path(__file__).parent / "sample"

# Keywords indicating a page contains legal text
LEGAL_KEYWORDS = [
    'law', 'penal', 'code', 'constitution', 'regulation', 'electoral',
    'decree', 'act', 'statute', 'bylaw', 'bylaws', 'amendment',
    'judiciary', 'court', 'verdict', 'criminal', 'civil', 'labor',
    'tax', 'banking', 'press', 'ngo', 'inheritance', 'nationality',
    'budget', 'subsid', 'proclamation', 'guardian-council',
    'policy', 'policies', 'ultimatum', 'letter-to-the-guardian',
    'assembly-of-experts', 'expediency-council',
]

# Pages to exclude (not actual legal texts)
EXCLUDE_PATTERNS = [
    '/category/', '/tag/', '/author/', '/page/', '/wp-content/',
    '/feed/', '/comments/', '/wp-json/', '/wp-login',
    '/data-by-individual-researchers', '/organizational-directory',
    '/socio-economic-data', '/population', '/health/', '/housing/',
    '/energy/', '/agriculture/', '/education/', '/about',
    '/government-finance', '/economic-affairs', '/political-parties/',
    '/elections/', '/election-results', '/municipal-', '/presidential-election-results',
    '/parliamentary-election-results', '/assembly-of-experts-election-results',
    '/law-schools/', '/maps/', '/contact',
]


class IranDataPortalFetcher:
    """Fetcher for Iran Data Portal legal documents."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
        })

    def get_sitemap_urls(self) -> List[str]:
        """Get all page URLs from WordPress sitemap."""
        try:
            resp = self.session.get(SITEMAP_URL, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch sitemap: {e}")
            return []

        urls = []
        root = ET.fromstring(resp.content)
        ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        for url_elem in root.findall('.//sm:url/sm:loc', ns):
            urls.append(url_elem.text)

        logger.info(f"Found {len(urls)} URLs in sitemap")
        return urls

    def is_legal_page(self, url: str) -> bool:
        """Determine if a URL is likely a legal document page."""
        url_lower = url.lower()

        # Exclude non-content pages
        for pattern in EXCLUDE_PATTERNS:
            if pattern in url_lower:
                return False

        # Must match at least one legal keyword
        slug = url_lower.rstrip('/').split('/')[-1]
        for keyword in LEGAL_KEYWORDS:
            if keyword in slug:
                return True

        return False

    def extract_text(self, url: str) -> Optional[Dict[str, str]]:
        """Extract title and full text from a legal document page."""
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

        html = resp.text

        # Extract title
        title_match = re.search(r'<h1[^>]*class="title"[^>]*>([^<]+)</h1>', html)
        if not title_match:
            title_match = re.search(r'<title>([^<]+)</title>', html)
        title = title_match.group(1).strip() if title_match else url.split('/')[-2].replace('-', ' ').title()

        # Clean title
        title = re.sub(r'\s*[|–-]\s*Iran Data Portal$', '', title).strip()

        # Extract main content
        content_match = re.search(
            r'<div class="entry-content"[^>]*>(.*?)(?:<footer|<div class="post-nav|<div id="comments|<nav class)',
            html, re.DOTALL
        )
        if not content_match:
            return None

        content_html = content_match.group(1)

        # Clean HTML to text
        text = re.sub(r'<script[^>]*>.*?</script>', '', content_html, flags=re.DOTALL)
        text = re.sub(r'<style[^>]*>.*?</style>', '', content_html, flags=re.DOTALL)
        text = re.sub(r'<br\s*/?>', '\n', text)
        text = re.sub(r'</p>', '\n\n', text)
        text = re.sub(r'</(?:h[1-6]|li|tr|div)>', '\n', text)
        text = re.sub(r'<[^>]+>', '', text)
        # Decode HTML entities
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&lt;', '<', text)
        text = re.sub(r'&gt;', '>', text)
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&#8211;', '–', text)
        text = re.sub(r'&#8212;', '—', text)
        text = re.sub(r'&#8216;', "'", text)
        text = re.sub(r'&#8217;', "'", text)
        text = re.sub(r'&#8220;', '"', text)
        text = re.sub(r'&#8221;', '"', text)
        text = re.sub(r'&#\d+;', '', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = text.strip()

        if len(text) < 200:
            return None

        return {"title": title, "text": text, "url": url}

    def classify_type(self, url: str, title: str) -> str:
        """Classify as legislation or case_law."""
        lower = (url + ' ' + title).lower()
        if any(w in lower for w in ['verdict', 'court', 'judgment', 'decision', 'ruling']):
            return 'case_law'
        return 'legislation'

    def normalize(self, entry: Dict[str, str]) -> Dict[str, Any]:
        """Normalize into standard schema."""
        doc_id = hashlib.sha256(entry["url"].encode()).hexdigest()[:16]
        doc_type = self.classify_type(entry["url"], entry["title"])

        return {
            "_id": f"IR-IDP-{doc_id}",
            "_source": SOURCE_ID,
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": entry["title"],
            "text": entry["text"],
            "date": None,
            "url": entry["url"],
            "country": "IR",
            "language": "en",
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Yield all legal documents."""
        urls = self.get_sitemap_urls()
        legal_urls = [u for u in urls if self.is_legal_page(u)]
        logger.info(f"Filtered to {len(legal_urls)} legal pages")

        for i, url in enumerate(legal_urls):
            logger.info(f"[{i+1}/{len(legal_urls)}] {url.split('/')[-2][:50]}")
            time.sleep(1.5)
            entry = self.extract_text(url)
            if entry:
                yield self.normalize(entry)
            else:
                logger.warning(f"  No text extracted")

    def fetch_sample(self, max_records: int = 15) -> List[Dict[str, Any]]:
        """Fetch a diverse sample."""
        urls = self.get_sitemap_urls()
        legal_urls = [u for u in urls if self.is_legal_page(u)]
        logger.info(f"Filtered to {len(legal_urls)} legal pages for sampling")

        records = []
        for url in legal_urls:
            if len(records) >= max_records:
                break
            time.sleep(1.5)
            entry = self.extract_text(url)
            if entry:
                record = self.normalize(entry)
                records.append(record)
                logger.info(f"  Sample {len(records)}/{max_records}: {entry['title'][:50]} ({len(entry['text'])} chars)")
            else:
                logger.warning(f"  No text: {url.split('/')[-2][:50]}")

        return records


def bootstrap_sample():
    """Run sample mode."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = IranDataPortalFetcher()
    records = fetcher.fetch_sample(max_records=15)

    for i, record in enumerate(records):
        outfile = SAMPLE_DIR / f"record_{i+1:03d}.json"
        with open(outfile, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(records)} sample records to {SAMPLE_DIR}")
    texts = [r.get("text", "") for r in records]
    non_empty = sum(1 for t in texts if len(t) > 100)
    avg_len = sum(len(t) for t in texts) / max(len(texts), 1)
    logger.info(f"Validation: {non_empty}/{len(records)} records have text (avg {avg_len:.0f} chars)")
    return records


def bootstrap_full():
    """Run full mode."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = IranDataPortalFetcher()
    count = 0
    for record in fetcher.fetch_all():
        outfile = SAMPLE_DIR / f"record_{count+1:04d}.json"
        with open(outfile, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
    logger.info(f"Complete: {count} records saved to {SAMPLE_DIR}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    parser = argparse.ArgumentParser(description="IR/IranDataPortal Fetcher")
    parser.add_argument("command", choices=["bootstrap"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap")
    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample or not args.full:
            bootstrap_sample()
        else:
            bootstrap_full()
