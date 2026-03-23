#!/usr/bin/env python3
"""
Czech Supreme Administrative Court (Nejvyšší správní soud) Data Fetcher

Access to the Collection of Decisions (Sbírka rozhodnutí NSS)
https://sbirka.nssoud.cz

This fetcher uses:
1. Archive pages to discover all issues (from 2003 onwards)
2. Issue pages to discover all decision URLs
3. Individual decision pages for full text extraction

Decision URL pattern: https://sbirka.nssoud.cz/cz/{slug}.p{id}.html
Archive URL pattern: https://sbirka.nssoud.cz/cz/{year}-{issue}
"""

import json
import logging
import re
import sys
import time
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List, Set
from urllib.parse import urljoin

import requests

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://sbirka.nssoud.cz"
ARCHIVE_URL = f"{BASE_URL}/cz/archiv"


class NSSFetcher:
    """Fetcher for Czech Supreme Administrative Court decisions from sbirka.nssoud.cz"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'cs,en;q=0.5',
        })
        self._request_count = 0
        self._last_request = 0

    def _rate_limit(self):
        """Ensure we don't make more than 1 request per 1.5 seconds"""
        now = time.time()
        elapsed = now - self._last_request
        if elapsed < 1.5:
            time.sleep(1.5 - elapsed)
        self._last_request = time.time()
        self._request_count += 1

    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch a page with rate limiting"""
        self._rate_limit()
        try:
            response = self.session.get(url, timeout=30)
            if response.status_code == 200:
                return response.text
            else:
                logger.warning(f"Page {url} returned status {response.status_code}")
                return None
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    def _get_archive_issues(self) -> List[str]:
        """Get all issue URLs from the archive page"""
        html = self._fetch_page(ARCHIVE_URL)
        if not html:
            return []

        # Pattern: href="/cz/2026-1"
        pattern = r'href="/cz/(\d{4}-\d+)"'
        matches = re.findall(pattern, html)

        # Deduplicate and create full URLs
        issue_codes = list(set(matches))
        issue_urls = [f"{BASE_URL}/cz/{code}" for code in sorted(issue_codes)]

        logger.info(f"Found {len(issue_urls)} archive issues")
        return issue_urls

    def _get_decision_urls_from_issue(self, issue_url: str) -> List[str]:
        """Get all decision URLs from an issue page"""
        html = self._fetch_page(issue_url)
        if not html:
            return []

        # Pattern: href="/cz/something.p1234.html"
        pattern = r'href="(/cz/[^"]+\.p\d+\.html)'
        matches = re.findall(pattern, html)

        # Deduplicate and create full URLs
        decision_paths = list(set(matches))
        decision_urls = [f"{BASE_URL}{path.split('?')[0]}" for path in decision_paths]

        return decision_urls

    def _clean_html_text(self, html_text: str) -> str:
        """Clean HTML tags and decode entities from text"""
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '\n', html_text)
        # Decode HTML entities
        text = unescape(text)
        # Normalize whitespace
        text = re.sub(r'\n\s*\n', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)
        # Clean up lines
        lines = [line.strip() for line in text.split('\n')]
        lines = [line for line in lines if line]
        return '\n'.join(lines)

    def _extract_id_from_url(self, url: str) -> str:
        """Extract decision ID from URL"""
        match = re.search(r'\.p(\d+)\.html', url)
        if match:
            return match.group(1)
        return ""

    def _parse_decision_html(self, html: str, url: str) -> Dict[str, Any]:
        """Parse decision HTML and extract structured data"""
        result = {
            'title': '',
            'legal_principle': '',
            'text': '',
            'metadata': {},
            'keywords': [],
            'regulations': [],
        }

        # Extract title from <title> tag
        title_match = re.search(r'<title>([^<]+)</title>', html)
        if title_match:
            result['title'] = unescape(title_match.group(1)).replace(' | Sbírka rozhodnutí Nejvyššího správního soudu', '').strip()

        # Extract headnote title (právní věta nadpis)
        headnote_title_match = re.search(r'<div CLASS="pravni-veta-nadpis">([^<]+)</div>', html)
        if headnote_title_match:
            result['metadata']['headnote_title'] = self._clean_html_text(headnote_title_match.group(1))
            if not result['title']:
                result['title'] = result['metadata']['headnote_title']

        # Extract legal principles (právní věta) - there can be multiple
        legal_principles = []
        for match in re.finditer(r'<div CLASS="pravni-veta">(.+?)</div>', html, re.DOTALL):
            principle = self._clean_html_text(match.group(1))
            if principle and len(principle) > 20:
                legal_principles.append(principle)
        if legal_principles:
            result['legal_principle'] = '\n\n'.join(legal_principles)

        # Extract regulations/legislation references (předpisy)
        regulations_match = re.search(r'<div CLASS="predpisy">(.+?)</div>\s*<div CLASS="pravni-veta">', html, re.DOTALL)
        if regulations_match:
            reg_text = self._clean_html_text(regulations_match.group(1))
            result['regulations'] = [r.strip() for r in reg_text.split('\n') if r.strip() and len(r.strip()) > 5]

        # Extract decision info line (Podle rozsudku...)
        info_match = re.search(r'\(Podle (?:rozsudku|usnesení|stanoviska) (.+?) ze dne (\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4}), čj\. ([^)]+)\)', html)
        if info_match:
            court_name, day, month, year, case_ref = info_match.groups()
            result['metadata']['court'] = court_name.strip()
            result['metadata']['decision_date'] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            result['metadata']['case_reference'] = case_ref.strip()
        else:
            # Try alternate patterns
            date_match = re.search(r'ze dne (\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})', html)
            if date_match:
                day, month, year = date_match.groups()
                result['metadata']['decision_date'] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"

            case_match = re.search(r'čj\.\s*([^,<\)]+)', html)
            if case_match:
                result['metadata']['case_reference'] = case_match.group(1).strip()

        # Extract Sbírka NSS number from meta keywords
        keywords_meta = re.search(r'<meta name="keywords" content="([^"]+)"', html)
        if keywords_meta:
            kw_text = keywords_meta.group(1)
            # Look for patterns like "30 Af 3/2023 - 129, Sb.NSS"
            if 'Sb.NSS' in kw_text or 'Sb. NSS' in kw_text:
                result['metadata']['sb_nss_confirmed'] = True

        # Extract full decision text from aspi-content div
        # The main text is in <div class="aspi-content"> and wrapped in <div CLASS="jud">
        text_parts = []

        # Find the start of aspi-content
        aspi_start = html.find('<div class="aspi-content">')
        if aspi_start != -1:
            # Get content from aspi-content to the end of the main area
            # Find a reasonable end point (footer or end of article)
            content_area = html[aspi_start:]

            # Extract the main body: vec (case description)
            vec_match = re.search(r'<div CLASS="vec">(.+?)</div>', content_area, re.DOTALL)
            if vec_match:
                vec_text = self._clean_html_text(vec_match.group(1))
                # Remove duplicate "Věc:" prefix if present in the extracted text
                vec_text = re.sub(r'^Věc:\s*', '', vec_text)
                text_parts.append("Věc:\n" + vec_text)

            # Get all paragraphs (the main body text)
            # These are typically <div CLASS="p">...</div>
            for p_match in re.finditer(r'<div CLASS="p">(.+?)</div>', content_area, re.DOTALL):
                p_text = self._clean_html_text(p_match.group(1))
                if p_text and len(p_text) > 20:
                    text_parts.append(p_text)

            # Get reasoning title if present
            reasoning_title = re.search(r'<div CLASS="oduvodneni-nadpis">([^<]+)</div>', content_area)
            if reasoning_title:
                # Insert reasoning title before the reasoning paragraphs
                title_text = reasoning_title.group(1).strip()
                # Find where to insert it in text_parts (after vec, before reasoning paragraphs)
                if title_text and title_text not in ''.join(text_parts):
                    # Find first paragraph that looks like reasoning (starts with [number])
                    for i, part in enumerate(text_parts):
                        if re.match(r'\[\d+\]', part):
                            text_parts.insert(i, "\n" + title_text)
                            break

        result['text'] = '\n\n'.join(text_parts)

        # Extract prejudikatura (case law citations)
        prej_match = re.search(r'<div CLASS="prejudikatura">(.+?)</div>\s*<div CLASS="vec">', html, re.DOTALL)
        if prej_match:
            prej_text = self._clean_html_text(prej_match.group(1))
            result['metadata']['prejudikatura'] = prej_text

        # Determine decision type
        if 'Rozsudek' in result['title'] or 'rozsudku' in html[:2000].lower():
            result['metadata']['decision_type'] = 'Rozsudek'
        elif 'Usnesení' in result['title'] or 'usnesení' in html[:2000].lower():
            result['metadata']['decision_type'] = 'Usnesení'
        elif 'Stanovisko' in result['title'] or 'stanoviska' in html[:2000].lower():
            result['metadata']['decision_type'] = 'Stanovisko'

        return result

    def _fetch_decision(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch a single decision by URL"""
        html = self._fetch_page(url)
        if not html:
            return None

        # Check if we got a real decision page
        if 'aspi-content' not in html:
            logger.debug(f"No decision content found at {url}")
            return None

        # Parse the HTML
        parsed = self._parse_decision_html(html, url)

        # Extract ID from URL
        decision_id = self._extract_id_from_url(url)

        return {
            'id': decision_id,
            'url': url,
            'title': parsed['title'],
            'text': parsed['text'],
            'legal_principle': parsed['legal_principle'],
            'metadata': parsed['metadata'],
            'regulations': parsed['regulations'],
        }

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch all decisions from archive.

        Args:
            limit: Maximum number of documents to fetch

        Yields:
            Raw document dictionaries with full text
        """
        count = 0
        seen_urls: Set[str] = set()

        # Get all issue URLs
        issue_urls = self._get_archive_issues()

        # Sort by year descending (most recent first)
        issue_urls.sort(key=lambda x: x.split('/')[-1], reverse=True)

        for issue_url in issue_urls:
            if limit and count >= limit:
                return

            logger.info(f"Processing issue: {issue_url}")
            decision_urls = self._get_decision_urls_from_issue(issue_url)
            logger.info(f"Found {len(decision_urls)} decisions in this issue")

            for decision_url in decision_urls:
                if limit and count >= limit:
                    return

                if decision_url in seen_urls:
                    continue
                seen_urls.add(decision_url)

                result = self._fetch_decision(decision_url)

                if result and result.get('text'):
                    text_len = len(result['text'])
                    if text_len > 500:  # Only count meaningful content
                        count += 1
                        logger.info(f"Fetched [{count}]: {result.get('title', '')[:60]}... ({text_len:,} chars)")
                        yield result

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """
        Fetch decisions from recent issues.

        Since there's no modification date API, we fetch from recent issues
        and let the caller decide based on decision dates.
        """
        # Get current year and fetch recent issues
        current_year = datetime.now().year
        recent_issues = []

        issue_urls = self._get_archive_issues()
        for url in issue_urls:
            # Check if issue is from current year or previous year
            match = re.search(r'/(\d{4})-\d+$', url)
            if match:
                year = int(match.group(1))
                if year >= current_year - 1:
                    recent_issues.append(url)

        seen_urls: Set[str] = set()

        for issue_url in recent_issues:
            decision_urls = self._get_decision_urls_from_issue(issue_url)

            for decision_url in decision_urls:
                if decision_url in seen_urls:
                    continue
                seen_urls.add(decision_url)

                result = self._fetch_decision(decision_url)

                if result and result.get('text'):
                    # Check if decision date is after 'since'
                    decision_date_str = result.get('metadata', {}).get('decision_date', '')
                    if decision_date_str:
                        try:
                            decision_date = datetime.fromisoformat(decision_date_str)
                            if decision_date >= since:
                                yield result
                        except ValueError:
                            # If we can't parse date, yield anyway
                            yield result

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema."""
        metadata = raw_doc.get('metadata', {})

        # Build ID
        case_ref = metadata.get('case_reference', '')
        doc_id = f"CZ-NSS-{raw_doc.get('id', '')}"

        # Get date
        decision_date = metadata.get('decision_date', '')

        # Build ECLI if we have enough info
        # ECLI:CZ:NSS:{year}:{case_ref}
        ecli = ''
        if decision_date and case_ref:
            year = decision_date[:4]
            # Clean case reference for ECLI
            clean_ref = re.sub(r'[^A-Za-z0-9]', '.', case_ref)
            ecli = f"ECLI:CZ:NSS:{year}:{clean_ref}"

        # Combine text with legal principle for complete content
        full_text = raw_doc.get('text', '')
        legal_principle = raw_doc.get('legal_principle', '')

        if legal_principle and legal_principle not in full_text:
            full_text = f"PRÁVNÍ VĚTA:\n{legal_principle}\n\n{full_text}"

        return {
            '_id': doc_id,
            '_source': 'CZ/NSS',
            '_type': 'case_law',
            '_fetched_at': datetime.now().isoformat(),
            'title': raw_doc.get('title', ''),
            'case_reference': case_ref,
            'ecli': ecli,
            'text': full_text,
            'legal_principle': legal_principle,
            'decision_type': metadata.get('decision_type', ''),
            'court': metadata.get('court', 'Nejvyšší správní soud'),
            'date': decision_date,
            'regulations': raw_doc.get('regulations', []),
            'prejudikatura': metadata.get('prejudikatura', ''),
            'url': raw_doc.get('url', ''),
            'language': 'cs',
        }


def main():
    """Main entry point for testing and bootstrap"""

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = NSSFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap...")
        logger.info("Fetching decisions from sbirka.nssoud.cz...")

        sample_count = 0
        target_count = 12 if '--sample' in sys.argv else 50

        for raw_doc in fetcher.fetch_all(limit=target_count * 2):
            if sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get('text', ''))

            if text_len < 500:  # Skip very short decisions
                continue

            # Save to sample directory
            doc_id = raw_doc.get('id', str(sample_count))
            filename = f"{doc_id}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{sample_count+1}/{target_count}]: {normalized.get('case_reference', doc_id)} ({text_len:,} chars)")
            sample_count += 1

        logger.info(f"Bootstrap complete. Saved {sample_count} documents to {sample_dir}")

        # Print summary
        files = list(sample_dir.glob('*.json'))
        total_chars = 0
        for f in files:
            with open(f, 'r', encoding='utf-8') as fp:
                data = json.load(fp)
                total_chars += len(data.get('text', ''))

        print(f"\n=== SUMMARY ===")
        print(f"Sample files: {len(files)}")
        print(f"Total text chars: {total_chars:,}")
        print(f"Average chars/doc: {total_chars // max(len(files), 1):,}")

    else:
        # Test mode - fetch a few decisions
        fetcher = NSSFetcher()
        print("Testing NSS fetcher...")

        count = 0
        for raw_doc in fetcher.fetch_all(limit=3):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Case Ref: {normalized['case_reference']}")
            print(f"ECLI: {normalized['ecli']}")
            print(f"Date: {normalized['date']}")
            print(f"Type: {normalized.get('decision_type', 'unknown')}")
            print(f"Text length: {len(normalized.get('text', ''))}")
            print(f"Text preview: {normalized.get('text', '')[:300]}...")
            count += 1


if __name__ == '__main__':
    main()
