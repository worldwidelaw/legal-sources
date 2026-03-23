#!/usr/bin/env python3
"""
RO/ANSPDCP -- Romanian Data Protection Authority Fetcher

Fetches GDPR enforcement decisions from the Romanian National Supervisory
Authority for Personal Data Processing (ANSPDCP).

Strategy:
  - Scrape sanctions list page at ?page=Sanctiuni_RGPD (single page, ~280 entries)
  - Extract individual decision page links (Comunicat_Presa_* pattern)
  - Fetch each decision page and extract full text from HTML
  - No PDFs needed — full text is inline HTML

License: Public (Romanian government open data)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
"""

import html as html_mod
import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urljoin, unquote

import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://www.dataprotection.ro"
SANCTIONS_LIST_URL = f"{BASE_URL}/?page=Sanctiuni_RGPD"


class ANSPDCPFetcher:
    """Fetcher for Romanian ANSPDCP GDPR decisions"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept-Language': 'ro,en;q=0.5',
        })

    def _fetch_with_retry(self, url: str, retries: int = 2, timeout: int = 30) -> Optional[requests.Response]:
        """Fetch URL with retries"""
        for attempt in range(retries + 1):
            try:
                response = self.session.get(url, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if attempt < retries:
                    wait = 3 * (attempt + 1)
                    logger.warning(f"Attempt {attempt+1} failed: {e}. Retry in {wait}s...")
                    time.sleep(wait)
                else:
                    logger.error(f"Failed to fetch {url}: {e}")
                    return None

    def _clean_html(self, text: str) -> str:
        """Strip HTML tags and clean text"""
        # Remove script/style blocks
        text = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', text, flags=re.DOTALL | re.IGNORECASE)
        # Replace <br>, <p>, <div> with newlines
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</(p|div|h[1-6]|li|tr)>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<(p|div|h[1-6]|li|tr)[^>]*>', '\n', text, flags=re.IGNORECASE)
        # Remove all remaining tags
        text = re.sub(r'<[^>]+>', '', text)
        # Decode entities
        text = html_mod.unescape(text)
        # Normalize whitespace
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n\s*\n', '\n\n', text)
        return text.strip()

    def _extract_date_from_page_param(self, page_param: str) -> str:
        """Try to extract a date from the page parameter"""
        # Normalize separators
        clean = page_param.replace('/', '').replace('..', '')

        # Pattern: DD_MM_YYYY or DD.MM.YYYY
        m = re.search(r'(\d{1,2})[._](\d{2})[._](\d{4})', clean)
        if m:
            day, month, year = m.group(1), m.group(2), m.group(3)
            return f"{year}-{month}-{int(day):02d}"

        # Pattern with underscores: DD_MM_YYYY
        m = re.search(r'(\d{1,2})_(\d{2})_(\d{4})', clean)
        if m:
            day, month, year = m.group(1), m.group(2), m.group(3)
            return f"{year}-{month}-{int(day):02d}"

        return ""

    def discover_sanctions(self) -> List[Dict[str, str]]:
        """Discover all sanction page links from the main sanctions page"""
        response = self._fetch_with_retry(SANCTIONS_LIST_URL)
        if not response:
            return []

        html_text = response.text
        entries = []
        seen = set()

        # Find all links to individual sanction pages
        # Patterns: ?page=Comunicat_Presa_*, ?page=*amenda*, ?page=*Amenda*, ?page=*sanctiune*, etc
        link_pattern = re.compile(
            r'<a[^>]+href=["\'](?:\.\./)?(?:https?://www\.dataprotection\.ro/)?\?page=([^"\'&]+)(?:&[^"\']*)?["\'][^>]*>(.*?)</a>',
            re.DOTALL | re.IGNORECASE
        )

        for match in link_pattern.finditer(html_text):
            page_param = match.group(1).strip()
            link_text = self._clean_html(match.group(2)).strip()

            # Filter to sanction-related pages only
            page_lower = page_param.lower()
            is_sanction = any(kw in page_lower for kw in [
                'comunicat_presa', 'comunicat_de_presa', 'comuncat_presa',
                'comunicat_', 'amenda', 'sanctiune', 'a_patra_amenda',
                'a_treia_amenda', 'noua_amenda', 'alta_amenda', 'alta_sanctiune',
                'noi_amenzi',
            ])

            if not is_sanction:
                continue

            # Skip non-content pages
            skip_keywords = ['regimul', 'plata', 'informare', 'formular']
            if any(kw in page_lower for kw in skip_keywords):
                continue

            if page_param in seen:
                continue
            seen.add(page_param)

            date = self._extract_date_from_page_param(page_param)

            entries.append({
                'page_param': page_param,
                'title': link_text if link_text else page_param,
                'date': date,
            })

        logger.info(f"Discovered {len(entries)} sanction pages")
        return entries

    def fetch_sanction_text(self, page_param: str) -> str:
        """Fetch full text of a single sanction page"""
        url = f"{BASE_URL}/?page={page_param}&lang=ro"
        response = self._fetch_with_retry(url)
        if not response:
            return ""

        html_text = response.text

        # Extract the main content area
        # The content is typically in a div with class containing "content" or "article"
        # Try to find the main body content between common markers
        content = ""

        # Try common content containers
        for pattern in [
            r'<div[^>]*class="[^"]*content[^"]*"[^>]*>(.*?)</div>\s*(?:<div[^>]*class="[^"]*footer|<div[^>]*id="footer)',
            r'<td[^>]*class="[^"]*content[^"]*"[^>]*>(.*?)</td>',
            r'<div[^>]*id="[^"]*content[^"]*"[^>]*>(.*?)</div>',
        ]:
            m = re.search(pattern, html_text, re.DOTALL | re.IGNORECASE)
            if m:
                content = m.group(1)
                break

        if not content:
            # Fallback: extract everything between <body> and </body>
            m = re.search(r'<body[^>]*>(.*?)</body>', html_text, re.DOTALL | re.IGNORECASE)
            if m:
                content = m.group(1)

        if not content:
            content = html_text

        # Remove navigation, header, footer elements
        for tag in ['nav', 'header', 'footer', 'menu']:
            content = re.sub(
                rf'<{tag}[^>]*>.*?</{tag}>',
                '', content, flags=re.DOTALL | re.IGNORECASE
            )

        text = self._clean_html(content)

        # Remove common boilerplate
        boilerplate = [
            r'(?:Pagina principală|Prezentare generală|Conducerea autorit|Organigrama|'
            r'Declaraţii de avere|Documente de interes|Rapoarte anuale|'
            r'Legislaţie internă|Legislaţie U\.E\.|Proiecte|Control|'
            r'Plângeri|Comitetul European|Consiliul Europei|Contact|'
            r'Întrebări frecvente|Anunturi posturi|Rezultate concurs)[^\n]*\n?',
            r'(?:Ştiri|Materiale Informative|Schengen|Europol|'
            r'Legaturi Utile|Termeni de Utilizare|©)[^\n]*\n?',
        ]
        for bp in boilerplate:
            text = re.sub(bp, '', text, flags=re.IGNORECASE)

        # Trim to just the substantive content
        text = re.sub(r'\n{3,}', '\n\n', text).strip()

        return text

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """Fetch all sanction documents with full text"""
        entries = self.discover_sanctions()

        if limit:
            entries = entries[:limit]

        count = 0
        for entry in entries:
            if limit and count >= limit:
                return

            logger.info(f"Fetching [{count+1}/{len(entries)}]: {entry['title'][:60]}...")
            text = self.fetch_sanction_text(entry['page_param'])

            if not text or len(text) < 100:
                logger.warning(f"Insufficient text for {entry['page_param']} ({len(text)} chars)")
                continue

            entry['text'] = text
            entry['url'] = f"{BASE_URL}/?page={entry['page_param']}&lang=ro"
            yield entry
            count += 1
            time.sleep(1.5)

        logger.info(f"Fetched {count} documents with full text")

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        page_param = raw.get('page_param', '')
        doc_id = re.sub(r'[^\w-]', '_', page_param)

        return {
            '_id': f"RO-ANSPDCP-{doc_id}",
            '_source': 'RO/ANSPDCP',
            '_type': 'case_law',
            '_fetched_at': datetime.now().isoformat(),
            'title': raw.get('title', ''),
            'text': raw.get('text', ''),
            'date': raw.get('date', ''),
            'url': raw.get('url', ''),
            'language': 'ro',
        }


def main():
    """Main entry point"""
    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = ANSPDCPFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        is_sample = '--sample' in sys.argv
        target_count = 12 if is_sample else None
        limit = target_count + 5 if target_count else None

        logger.info(f"Starting bootstrap (sample={is_sample})...")

        saved = 0
        for raw in fetcher.fetch_all(limit=limit):
            if target_count and saved >= target_count:
                break

            normalized = fetcher.normalize(raw)
            text_len = len(normalized.get('text', ''))

            if text_len < 100:
                continue

            filename = f"{normalized['_id']}.json"
            filename = re.sub(r'[^\w\-.]', '_', filename)
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{saved+1}]: {normalized['title'][:50]}... ({text_len:,} chars)")
            saved += 1

        logger.info(f"Bootstrap complete. Saved {saved} documents to {sample_dir}")

        files = list(sample_dir.glob('*.json'))
        total_chars = sum(
            len(json.load(open(fp)).get('text', ''))
            for fp in files
        )

        print(f"\n=== SUMMARY ===")
        print(f"Sample files: {len(files)}")
        print(f"Total text chars: {total_chars:,}")
        print(f"Average chars/doc: {total_chars // max(len(files), 1):,}")

    else:
        fetcher = ANSPDCPFetcher()
        print("Testing RO/ANSPDCP fetcher...")
        for i, raw in enumerate(fetcher.fetch_all(limit=3)):
            normalized = fetcher.normalize(raw)
            print(f"\n--- Document {i+1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Title: {normalized['title'][:80]}")
            print(f"Date: {normalized['date']}")
            print(f"Text length: {len(normalized['text'])} chars")
            print(f"Text preview: {normalized['text'][:300]}...")


if __name__ == '__main__':
    main()
