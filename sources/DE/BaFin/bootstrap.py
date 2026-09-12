#!/usr/bin/env python3
"""
German Federal Financial Supervisory Authority (BaFin) Regulatory Documents Fetcher

Official open data from bafin.de
https://www.bafin.de

Fetches BaFin's "Verwaltungspraxis" corpus — the binding administrative guidance
the authority issues to banks, insurers and financial service providers:

  - Rundschreiben (circulars, e.g. MaRisk / MaComp)
  - Auslegungsentscheidungen (interpretative decisions)
  - Merkblätter (guidance notices)
  - Aufsichtsmitteilungen (supervisory notices)

Documents come in two shapes and both are followed to full text:
  - /SharedDocs/Veroeffentlichungen/... — HTML pages, body in `div.l-article`
  - /SharedDocs/Downloads/...          — landing pages linking a PDF

Data is public domain official government work under German law (§ 5 UrhG).

NOTE ON THE URL SCHEME (issue #1571): BaFin migrated to a new site in 2026. The
old `/SiteGlobals/Forms/Suche/Expertensuche_Formular.html` endpoint now 404s for
everyone; the live one is `.../Suche/Expertensuche/Servicesuche_Formular.html`,
result items are `div.c-teaser-search-result` (was `div.search-result`), and the
`pageNo` parameter is ACCEPTED BUT SILENTLY IGNORED — paging is driven by an
opaque `gtp=<node>_list%3D<n>` token, so we follow the rendered "next" link
instead of incrementing a page counter.
"""

import html
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

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

try:
    from common.pdf_extract import extract_pdf_markdown
except Exception:  # pragma: no cover - PDF support is optional for HTML-only runs
    extract_pdf_markdown = None

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://www.bafin.de"
SEARCH_URL = "https://www.bafin.de/SiteGlobals/Forms/Suche/Expertensuche/Servicesuche_Formular.html"

# `cl2Categories_Format` facet values, taken from BaFin's own Verwaltungspraxis
# navigation. The casing is the site's, not ours — the facet is case-sensitive.
FORMATS = {
    "rundschreiben": "Rundschreiben",
    "Auslegungsentscheidung": "Auslegungsentscheidung",
    "merkblatt": "Merkblatt",
    "Aufsichtsmitteilung": "Aufsichtsmitteilung",
}

RESULTS_PER_PAGE = 50
MAX_PAGES_PER_FORMAT = 200  # runaway guard; 50/page covers far more than the corpus
REQUEST_DELAY = 1.5
MIN_TEXT_CHARS = 200


class SourceBlockedError(RuntimeError):
    """Raised when discovery cannot reach the search index at all."""


class BaFinFetcher:
    """Fetcher for BaFin administrative-practice documents."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
        })

    # ------------------------------------------------------------------ HTTP

    def _request_with_backoff(self, url: str, **kwargs) -> requests.Response:
        """GET with exponential backoff on 429/5xx."""
        max_retries = 5
        response = None
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, **kwargs)
            except requests.RequestException as exc:
                if attempt == max_retries - 1:
                    raise
                wait = min(2 ** attempt * 5, 120)
                logger.warning(f"{type(exc).__name__} on {url}, waiting {wait}s "
                               f"(attempt {attempt+1}/{max_retries})")
                time.sleep(wait)
                continue
            if response.status_code == 429 or response.status_code >= 500:
                wait = min(2 ** attempt * 5, 120)
                retry_after = response.headers.get('Retry-After')
                if retry_after and retry_after.isdigit():
                    wait = max(wait, int(retry_after))
                logger.warning(f"HTTP {response.status_code} on {url}, waiting {wait}s "
                               f"(attempt {attempt+1}/{max_retries})")
                time.sleep(wait)
                continue
            response.raise_for_status()
            return response
        response.raise_for_status()
        return response

    # ------------------------------------------------------------- discovery

    def _search_url(self, doc_format: str) -> str:
        return (
            f"{SEARCH_URL}?pageLocale=de&templateQueryString=&submit="
            f"&cl2Categories_Format={doc_format}"
            f"&sortOrder=searchDate_dt%20desc"
            f"&resultsPerPage={RESULTS_PER_PAGE}"
        )

    def _parse_search_page(self, soup: BeautifulSoup, doc_format: str) -> List[Dict[str, Any]]:
        entries = []
        for teaser in soup.find_all(class_=re.compile(r'c-teaser-search-result$')):
            link = teaser.find('a', href=True)
            if not link:
                continue
            href = re.sub(r';jsessionid=[^?]*', '', link['href'])
            url = urljoin(BASE_URL, href)
            if '/SharedDocs/' not in url:
                continue

            title = re.sub(r'\s+', ' ', link.get_text(strip=True))

            date = None
            date_elem = teaser.find(class_=re.compile(r'c-teaser-search-result__date'))
            if date_elem:
                match = re.search(r'(\d{2}\.\d{2}\.\d{4})', date_elem.get_text())
                if match:
                    date = match.group(1)

            description = None
            text_elem = teaser.find(class_=re.compile(r'c-teaser-search-result__text$'))
            if text_elem:
                description = re.sub(r'\s+', ' ', text_elem.get_text(strip=True)) or None

            entries.append({
                'doc_id': self._doc_id(url),
                'url': url,
                'title': title,
                'date': date,
                'description': description,
                'doc_format': FORMATS.get(doc_format, doc_format),
            })
        return entries

    @staticmethod
    def _doc_id(url: str) -> str:
        slug = url.split('?')[0].rstrip('/').split('/')[-1]
        slug = re.sub(r'\.html?$', '', slug)
        return re.sub(r'[^a-zA-Z0-9_-]', '_', slug)

    def _discover(self, doc_format: str) -> Iterator[Dict[str, Any]]:
        """Walk the search result pages for one format facet.

        Paging follows the rendered "next" link: BaFin accepts `pageNo` with a
        200 but serves page 0 regardless, so a counter would silently cap the
        corpus at the first 50 hits.
        """
        url = self._search_url(doc_format)
        seen_pages = set()
        seen_urls = set()
        pages = 0

        while url and pages < MAX_PAGES_PER_FORMAT:
            if url in seen_pages:
                logger.warning(f"[{doc_format}] pagination looped back to a seen page — stopping")
                break
            seen_pages.add(url)

            response = self._request_with_backoff(url, timeout=60)
            soup = BeautifulSoup(response.content, 'html.parser')
            entries = self._parse_search_page(soup, doc_format)

            if pages == 0 and not entries:
                raise SourceBlockedError(
                    f"BaFin search returned no results for cl2Categories_Format={doc_format} "
                    f"on the first page ({url}). The facet value or the search endpoint has "
                    f"changed, or the request was refused — refusing to report an empty corpus."
                )
            if not entries:
                break

            new = 0
            for entry in entries:
                if entry['url'] in seen_urls:
                    continue
                seen_urls.add(entry['url'])
                new += 1
                yield entry

            logger.info(f"[{doc_format}] page {pages + 1}: {len(entries)} results ({new} new)")
            if not new:
                logger.warning(f"[{doc_format}] page {pages + 1} was entirely duplicates — "
                               f"pagination is not advancing, stopping")
                break

            next_link = soup.find('a', class_=re.compile(r'c-pagination__button--next'))
            url = urljoin(BASE_URL, next_link['href']) if next_link and next_link.get('href') else None
            pages += 1
            time.sleep(REQUEST_DELAY)

        logger.info(f"[{doc_format}] discovered {len(seen_urls)} documents")

    # --------------------------------------------------------------- content

    @staticmethod
    def _clean(text: str) -> str:
        text = html.unescape(text)
        text = text.replace('\xad', '')  # soft hyphens BaFin sprinkles into headings
        text = re.sub(r'[ \t]{2,}', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _html_text(self, soup: BeautifulSoup) -> str:
        article = soup.find('div', class_='l-article')
        if not article:
            article = soup.find('main') or soup.body
        if not article:
            return ''

        for junk in article.find_all(class_=re.compile(
                r'c-breadcrumb|c-feedback|c-socialbar|c-share|l-article__toc|c-skiplink')):
            junk.decompose()
        for tag in article.find_all(['script', 'style', 'nav', 'noscript']):
            tag.decompose()

        parts = []
        for elem in article.find_all(['h1', 'h2', 'h3', 'h4', 'p', 'li', 'td', 'th']):
            text = re.sub(r'\s+', ' ', elem.get_text(' ', strip=True))
            if text and text not in parts:
                parts.append(text)
        return self._clean('\n\n'.join(parts))

    def _fetch_document(self, entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        url = entry['url']
        try:
            response = self._request_with_backoff(url, timeout=60)
        except requests.RequestException as exc:
            logger.error(f"Error fetching {url}: {exc}")
            return None

        soup = BeautifulSoup(response.content, 'html.parser')

        title = entry.get('title')
        if not title:
            h1 = soup.find('h1')
            title = re.sub(r'\s+', ' ', h1.get_text(strip=True)) if h1 else entry['doc_id']
        title = title.replace('\xad', '')

        text = self._html_text(soup)
        pdf_url = None

        # Download landing pages carry only a stub; the body lives in the PDF.
        if len(text) < MIN_TEXT_CHARS or '/SharedDocs/Downloads/' in url:
            pdf_url = self._find_pdf(soup, url)
            if pdf_url and extract_pdf_markdown is not None:
                pdf_text = extract_pdf_markdown(
                    "DE/BaFin", entry['doc_id'], pdf_url=pdf_url, table="doctrine",
                )
                if pdf_text and len(pdf_text) > len(text):
                    text = self._clean(pdf_text)

        date = entry.get('date') or self._page_date(soup, text)

        return {
            'doc_id': entry['doc_id'],
            'url': url,
            'pdf_url': pdf_url,
            'title': title,
            'date': date,
            'doc_format': entry.get('doc_format'),
            'description': entry.get('description'),
            'text': text,
            'reference': self._reference(title, text),
        }

    @staticmethod
    def _find_pdf(soup: BeautifulSoup, page_url: str) -> Optional[str]:
        # Prefer a PDF inside the article body over anything in the chrome.
        for scope in (soup.find('div', class_='l-article'), soup):
            if scope is None:
                continue
            for a in scope.find_all('a', href=True):
                if '.pdf' in a['href'].lower():
                    return urljoin(page_url, a['href'])
        return None

    @staticmethod
    def _page_date(soup: BeautifulSoup, text: str) -> Optional[str]:
        meta = soup.find('meta', attrs={'name': 'date'})
        if meta and meta.get('content'):
            return meta['content']
        match = re.search(r'(?:Datum|Erscheinung|Stand)\D{0,10}(\d{2}\.\d{2}\.\d{4})', text)
        if match:
            return match.group(1)
        match = re.search(r'\b(\d{2}\.\d{2}\.\d{4})\b', text[:2000])
        return match.group(1) if match else None

    @staticmethod
    def _reference(title: str, text: str) -> Optional[str]:
        # e.g. "Rundschreiben 06/2026 (BA)" or "Merkblatt 01/2024 (GW)"
        match = re.search(r'(\d{1,2}\s*/\s*\d{4})\s*\(([A-Z]{2,3})\)', title)
        if match:
            return f"{match.group(1).replace(' ', '')} ({match.group(2)})"
        match = re.search(r'\b(\d{1,2}/\d{4})\b', title)
        return match.group(1) if match else None

    # ------------------------------------------------------------------- API

    def fetch_all(self, limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        """Yield every BaFin administrative-practice document with full text."""
        total = 0
        skipped = 0
        seen = set()

        for doc_format in FORMATS:
            for entry in self._discover(doc_format):
                if limit and total >= limit:
                    logger.info(f"Reached limit of {limit} documents")
                    return
                if entry['url'] in seen:
                    continue
                seen.add(entry['url'])

                doc = self._fetch_document(entry)
                time.sleep(REQUEST_DELAY)

                if not doc or len(doc.get('text') or '') < MIN_TEXT_CHARS:
                    skipped += 1
                    logger.warning(f"Skipping {entry['url']} — insufficient text")
                    continue

                total += 1
                logger.info(f"[{total}] {doc['title'][:60]} ({len(doc['text']):,} chars)")
                yield doc

        logger.info(f"Fetched {total} documents with full text ({skipped} skipped)")

    def fetch_updates(self, since: Any = None) -> Iterator[Dict[str, Any]]:
        """Yield documents published on or after `since`.

        The search is sorted newest-first per format, so we can stop walking a
        format once we cross the cutoff instead of re-crawling the corpus.
        """
        cutoff = _coerce_date(since)
        for doc_format in FORMATS:
            for entry in self._discover(doc_format):
                entry_date = _parse_date(entry.get('date'))
                if cutoff and entry_date and entry_date < cutoff:
                    logger.info(f"[{doc_format}] reached cutoff {cutoff} — stopping")
                    break
                doc = self._fetch_document(entry)
                time.sleep(REQUEST_DELAY)
                if not doc or len(doc.get('text') or '') < MIN_TEXT_CHARS:
                    continue
                doc_date = _parse_date(doc.get('date'))
                if cutoff and doc_date and doc_date < cutoff:
                    continue
                yield doc

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw document into the standard schema."""
        return {
            '_id': f"bafin_{raw_doc.get('doc_id', '')}",
            '_source': 'DE/BaFin',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'title': raw_doc.get('title') or raw_doc.get('doc_id', ''),
            'text': raw_doc.get('text', ''),
            'date': _parse_date(raw_doc.get('date')),
            'url': raw_doc.get('url', ''),
            'pdf_url': raw_doc.get('pdf_url'),
            'topic': raw_doc.get('doc_format'),
            'description': raw_doc.get('description'),
            'reference': raw_doc.get('reference'),
            'authority': 'BaFin',
            'language': 'de',
        }


def _parse_date(date_str: Optional[str]) -> Optional[str]:
    """Convert a BaFin date string to ISO 8601."""
    if not date_str:
        return None
    date_str = date_str.strip()
    if re.match(r'^\d{4}-\d{2}-\d{2}', date_str):
        return date_str[:10]
    match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', date_str)
    if match:
        try:
            return datetime(int(match.group(3)), int(match.group(2)),
                            int(match.group(1))).strftime('%Y-%m-%d')
        except ValueError:
            return None
    return None


def _coerce_date(since: Any) -> Optional[str]:
    """Accept a datetime, date or ISO string — the fleet passes all three."""
    if since is None:
        return None
    if isinstance(since, str):
        return _parse_date(since)
    if hasattr(since, 'strftime'):
        return since.strftime('%Y-%m-%d')
    return None


def _write_records(fetcher: BaFinFetcher, docs: Iterator[Dict[str, Any]],
                   sample_dir: Path, data_path: Optional[Path],
                   target_count: Optional[int]) -> int:
    sample_dir.mkdir(exist_ok=True)
    jsonl = None
    if data_path is not None:
        data_path.parent.mkdir(parents=True, exist_ok=True)
        jsonl = data_path.open('w', encoding='utf-8')

    count = 0
    try:
        for raw in docs:
            normalized = fetcher.normalize(raw)
            if len(normalized.get('text', '')) < MIN_TEXT_CHARS:
                continue

            if jsonl is not None:
                jsonl.write(json.dumps(normalized, ensure_ascii=False) + '\n')
                jsonl.flush()

            if count < (target_count or 15):
                filename = normalized['_id'].replace('/', '_').replace(':', '_') + '.json'
                with (sample_dir / filename).open('w', encoding='utf-8') as f:
                    json.dump(normalized, f, indent=2, ensure_ascii=False)

            count += 1
            if target_count and count >= target_count and jsonl is None:
                break
    finally:
        if jsonl is not None:
            jsonl.close()
    return count


def main():
    command = sys.argv[1] if len(sys.argv) > 1 else 'test'
    fetcher = BaFinFetcher()
    root = Path(__file__).parent
    sample_dir = root / 'sample'

    if command == 'bootstrap':
        sample_only = '--sample' in sys.argv
        target = 15 if sample_only else None
        data_path = None if sample_only else root / 'data' / 'records.jsonl'

        logger.info("Starting bootstrap (%s)", "sample" if sample_only else "full")
        count = _write_records(
            fetcher, fetcher.fetch_all(limit=target), sample_dir, data_path, target or 15,
        )

        print("\n=== SUMMARY ===")
        print(f"Records written: {count}")
        if data_path is not None:
            print(f"JSONL: {data_path}")
        files = sorted(sample_dir.glob('*.json'))
        total_chars = 0
        for f in files:
            total_chars += len(json.loads(f.read_text(encoding='utf-8')).get('text', ''))
        print(f"Sample files: {len(files)}")
        print(f"Average chars/doc: {total_chars // max(len(files), 1):,}")

        if count == 0:
            logger.error("No records written")
            sys.exit(1)

    elif command == 'update':
        since = sys.argv[2] if len(sys.argv) > 2 else None
        data_path = root / 'data' / 'records.jsonl'
        count = _write_records(fetcher, fetcher.fetch_updates(since), sample_dir, data_path, None)
        print(f"Updated records: {count}")

    else:
        print("Testing BaFin fetcher...")
        for i, raw in enumerate(fetcher.fetch_all(limit=3), 1):
            doc = fetcher.normalize(raw)
            print(f"\n--- Document {i} ---")
            print(f"ID: {doc['_id']}")
            print(f"Title: {doc['title'][:100]}")
            print(f"Topic: {doc['topic']}  Date: {doc['date']}  Ref: {doc['reference']}")
            print(f"URL: {doc['url']}")
            print(f"Text length: {len(doc['text']):,}")
            print(f"Preview: {doc['text'][:400]}...")


if __name__ == '__main__':
    # `bootstrap-fast` is the fleet runner's entry point; alias it onto the
    # full bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
