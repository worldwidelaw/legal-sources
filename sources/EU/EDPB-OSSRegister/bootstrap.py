#!/usr/bin/env python3
"""
EDPB Article 60 One-Stop-Shop Register — Data Fetcher

Fetches final decisions issued by EU national data protection authorities (DPAs)
under the GDPR cooperation mechanism (Article 60). Each entry carries structured
metadata (EDPBI identifier, lead SA, concerned SAs, main legal reference, relevant
topics, outcome) plus one or more decision PDFs, which are downloaded and text-
extracted to satisfy the full-text requirement.

Source: https://www.edpb.europa.eu/registers/register-of-final-one-stop-shop-decisions_en
Access: HTML scraping of the server-rendered register view + PDF download
Auth: None required

2026-08-20 (issue #1463): the register moved from
  /our-work-tools/consistency-findings/register-for-article-60-final-decisions_en
to
  /registers/register-of-final-one-stop-shop-decisions_en
and the Drupal markup was rebuilt: `div.node--type-edpb-article-60-final-decision`
rows are gone, replaced by `div.foss-decision-teaser`. The old selectors matched
nothing, so the crawl silently produced an empty corpus. Enumeration now targets the
new markup and fails loud when page 0 yields no rows.
"""

import argparse
import io
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

# PDF extraction backends (any one is enough)
try:
    import fitz  # PyMuPDF
    HAS_FITZ = True
except ImportError:
    HAS_FITZ = False

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from common.pdf_extract import extract_pdf_markdown
    HAS_COMMON_PDF = True
except ImportError:
    HAS_COMMON_PDF = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SOURCE_ID = "EU/EDPB-OSSRegister"
BASE_URL = "https://www.edpb.europa.eu"
REGISTER_PATH = "/registers/register-of-final-one-stop-shop-decisions_en"
# Kept for reference: the pre-2026-08 path, which now 301s to REGISTER_PATH.
LEGACY_REGISTER_PATH = "/our-work-tools/consistency-findings/register-for-article-60-final-decisions_en"

MIN_TEXT_CHARS = 50


class EDPBOSSRegisterFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/xhtml+xml,application/pdf,*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            # The EDPB CDN truncates chunked+compressed PDF streams ("Response ended
            # prematurely"); identity encoding and a non-kept-alive socket are reliable.
            'Accept-Encoding': 'identity',
            'Connection': 'close',
        })

    def _request(self, url: str, timeout: int = 90, retries: int = 4) -> Optional[requests.Response]:
        for attempt in range(retries):
            try:
                resp = self.session.get(url, timeout=timeout, stream=True)
                resp.raise_for_status()
                # Force a full read here so a truncated stream is retried rather than
                # silently yielding a short body.
                resp._body_bytes = b"".join(resp.iter_content(65536))
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed ({attempt + 1}/{retries}) {url}: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
        return None

    def _download(self, url: str, timeout: int = 180, attempts: int = 6) -> bytes:
        """Download a binary asset, resuming with Range on a truncated stream.

        The EDPB CDN intermittently ends a chunked PDF stream early
        ("Response ended prematurely"), which would otherwise drop born-digital
        decisions from the corpus. It does honour Range requests, so pick up where
        the broken stream stopped instead of restarting from zero.
        """
        data = b''
        for attempt in range(attempts):
            headers = {'Range': f'bytes={len(data)}-'} if data else {}
            got = 0
            try:
                resp = self.session.get(url, timeout=timeout, stream=True, headers=headers)
                if resp.status_code == 416:  # range past EOF => nothing left to fetch
                    return data
                resp.raise_for_status()
                for chunk in resp.iter_content(65536):
                    data += chunk
                    got += len(chunk)
                # The stream ended cleanly, so the asset is complete. A %%EOF check is
                # not a substitute: PDFs with incremental updates carry several of
                # them, so a truncated body can still end on one.
                return data
            except requests.exceptions.RequestException as e:
                logger.warning(f"Download interrupted ({attempt + 1}/{attempts}) {url}: "
                               f"{e} — {len(data):,} bytes so far")

            if attempt and got == 0:
                break
            time.sleep(min(2 ** attempt, 8))

        if data:
            logger.warning(f"{url}: returning {len(data):,} bytes without a %%EOF trailer")
        return data

    @staticmethod
    def _body(resp: requests.Response) -> bytes:
        # NB: don't use getattr(..., resp.content) — the default is evaluated eagerly
        # and a streamed response raises "content already consumed".
        if hasattr(resp, '_body_bytes'):
            return resp._body_bytes
        return resp.content

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF bytes. Returns '' for scanned/image-only PDFs."""
        if HAS_FITZ:
            try:
                with fitz.open(stream=pdf_bytes, filetype='pdf') as doc:
                    text = "\n\n".join(page.get_text() for page in doc).strip()
                if len(text) >= MIN_TEXT_CHARS:
                    return text
            except Exception as e:
                logger.warning(f"PyMuPDF extraction failed: {e}")

        if HAS_PDFPLUMBER:
            try:
                with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                    pages = [p.extract_text() or "" for p in pdf.pages]
                    text = "\n\n".join(pages).strip()
                if len(text) >= MIN_TEXT_CHARS:
                    return text
            except Exception as e:
                logger.warning(f"pdfplumber extraction failed: {e}")

        if HAS_COMMON_PDF:
            try:
                return extract_pdf_markdown(
                    source=SOURCE_ID, source_id="", pdf_bytes=pdf_bytes, table="case_law"
                ) or ""
            except Exception as e:
                logger.warning(f"common.pdf_extract failed: {e}")

        return ""

    @staticmethod
    def _clean(text: str) -> str:
        return re.sub(r'\s+', ' ', text or '').strip()

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse a date string (ISO datetime or human-readable) to ISO 8601 date."""
        date_str = (date_str or '').strip()
        if not date_str:
            return None
        m = re.match(r'(\d{4})-(\d{2})-(\d{2})', date_str)
        if m:
            return m.group(0)
        for fmt in ['%d %B %Y', '%d %b %Y', '%B %d, %Y', '%d/%m/%Y']:
            try:
                return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
        return None

    @staticmethod
    def _texts(node, class_name: str) -> List[str]:
        return [EDPBOSSRegisterFetcher._clean(el.get_text(' ', strip=True))
                for el in node.find_all(class_=class_name)]

    def _parse_listing_page(self, html: str) -> List[Dict[str, Any]]:
        """Parse a register listing page and extract decision metadata."""
        soup = BeautifulSoup(html, 'html.parser')
        decisions = []

        for teaser in soup.find_all('div', class_='foss-decision-teaser'):
            try:
                id_el = teaser.find(class_='foss-decision-foss-decision-teaser__id')
                case_id = self._clean(id_el.get_text()) if id_el else None
                if not case_id:
                    continue

                date_el = teaser.find(class_='foss-decision-foss-decision-teaser__date-of-decision')
                date_str = ''
                if date_el:
                    time_el = date_el.find('time')
                    date_str = (time_el.get('datetime') if time_el and time_el.get('datetime')
                                else self._clean(date_el.get_text()))

                lsa_el = teaser.find(class_='foss-decision-foss-decision-teaser__lead-sa')
                lsa = ''
                if lsa_el:
                    code_el = lsa_el.find(class_='member-country-token__code')
                    lsa = self._clean(code_el.get_text()).upper() if code_el else self._clean(lsa_el.get_text())

                csa_el = teaser.find(class_='foss-decision-teaser__concerned-sa-value')
                csa = self._texts(csa_el, 'member-state-token__name') if csa_el else []

                ref_el = teaser.find(class_='foss-decision-teaser__main-legel-ref-value')
                legal_refs = [self._clean(ref_el.get_text(' ', strip=True))] if ref_el else []
                legal_refs = [r for r in legal_refs if r]

                topics_el = teaser.find(class_='foss-decision-teaser__relevant-topics-value')
                keywords = (self._texts(topics_el, 'foss-decision-teaser__relevant-topics-list-item-link')
                            if topics_el else [])

                out_el = teaser.find(class_='foss-decision-teaser__outcome-value')
                outcomes = [self._clean(out_el.get_text(' ', strip=True))] if out_el else []
                outcomes = [o for o in outcomes if o]

                pdf_urls, seen = [], set()
                for a in teaser.find_all('a', href=True):
                    href = a['href']
                    if '.pdf' not in href.lower():
                        continue
                    full = urljoin(BASE_URL, href)
                    if full not in seen:
                        seen.add(full)
                        pdf_urls.append(full)

                decisions.append({
                    'case_id': case_id,
                    'date_str': date_str,
                    'lsa': lsa,
                    'csa': csa,
                    'legal_references': legal_refs,
                    'keywords': keywords,
                    'outcomes': outcomes,
                    'pdf_urls': pdf_urls,
                    'pdf_url': pdf_urls[0] if pdf_urls else None,
                })

            except Exception as e:
                logger.warning(f"Failed to parse decision teaser: {e}")

        return decisions

    @staticmethod
    def _total_items(html: str) -> Optional[int]:
        m = re.search(r'role="status"\s*>\s*([\d,\s]+?)\s*items', html)
        if m:
            try:
                return int(m.group(1).replace(',', '').replace(' ', ''))
            except ValueError:
                return None
        return None

    def fetch_all(self, max_docs: int = None, since: str = None) -> Iterator[Dict[str, Any]]:
        """Fetch decisions from the register with full text extracted from the PDFs."""
        page = 0
        fetched = 0
        consecutive_empty = 0
        seen_ids = set()
        total_items = None

        while True:
            if max_docs and fetched >= max_docs:
                return

            url = f"{BASE_URL}{REGISTER_PATH}?page={page}"
            logger.info(f"Fetching page {page}...")

            resp = self._request(url)
            if not resp:
                if page == 0:
                    raise RuntimeError(
                        f"Could not load the OSS register listing at {url} — the host may be "
                        f"unreachable or blocking this vantage (failing loud rather than "
                        f"emitting an empty corpus)"
                    )
                consecutive_empty += 1
                if consecutive_empty >= 2:
                    break
                page += 1
                continue

            html = self._body(resp).decode(resp.encoding or 'utf-8', errors='replace')

            if total_items is None:
                total_items = self._total_items(html)
                if total_items:
                    logger.info(f"Register reports {total_items:,} decisions")

            decisions = self._parse_listing_page(html)

            if not decisions and page == 0:
                raise RuntimeError(
                    "No decision rows parsed on page 0 of the OSS register — the EDPB "
                    "Drupal markup may have changed again (expected div.foss-decision-teaser) "
                    "or the request was blocked (failing loud rather than emitting an empty corpus)"
                )

            if not decisions:
                consecutive_empty += 1
                if consecutive_empty >= 2:
                    logger.info(f"No more decisions (page {page})")
                    break
                page += 1
                continue

            consecutive_empty = 0

            for dec in decisions:
                if max_docs and fetched >= max_docs:
                    return
                if dec['case_id'] in seen_ids:
                    continue
                seen_ids.add(dec['case_id'])

                date_iso = self._parse_date(dec.get('date_str', ''))
                if since and date_iso and date_iso < since:
                    # The register is sorted newest-first; once we cross the cutoff we are done.
                    logger.info(f"Reached cutoff {since} at {dec['case_id']} ({date_iso})")
                    return

                texts = []
                for pdf_url in dec['pdf_urls']:
                    body = self._download(pdf_url)
                    if body[:4] == b'%PDF':
                        extracted = self._extract_pdf_text(body)
                        if extracted:
                            texts.append(extracted)
                    elif body:
                        logger.warning(f"{pdf_url} did not return a PDF ({len(body)} bytes)")
                    time.sleep(1.0)

                dec['text'] = "\n\n".join(texts).strip()
                yield dec
                fetched += 1

            page += 1
            time.sleep(1.5)

        logger.info(f"Total fetched: {fetched}")

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Fetch decisions with a decision date on or after `since` (ISO date)."""
        yield from self.fetch_all(since=since)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize to standard schema."""
        case_id = raw['case_id']
        lsa = raw.get('lsa', '')
        date = self._parse_date(raw.get('date_str', ''))

        title_parts = [f"GDPR Art. 60 Decision {case_id}"]
        if lsa:
            title_parts.append(f"(LSA: {lsa})")
        legal_refs = raw.get('legal_references', [])
        if legal_refs:
            title_parts.append(f"— {'; '.join(legal_refs[:2])}")
        title = " ".join(title_parts)

        return {
            '_id': case_id,
            '_source': SOURCE_ID,
            '_type': 'case_law',
            '_fetched_at': datetime.utcnow().isoformat(),
            'case_id': case_id,
            'title': title,
            'text': raw.get('text', ''),
            'date': date,
            'url': f"{BASE_URL}{REGISTER_PATH}",
            'lsa': lsa,
            'csa': raw.get('csa', []),
            'legal_references': raw.get('legal_references', []),
            'keywords': raw.get('keywords', []),
            'outcome': raw.get('outcomes', []),
            'pdf_url': raw.get('pdf_url') or '',
        }


def main():
    parser = argparse.ArgumentParser(description='EDPB Art. 60 OSS Register fetcher')
    parser.add_argument('command', choices=['bootstrap', 'bootstrap-fast', 'updates'])
    parser.add_argument('--sample', action='store_true')
    parser.add_argument('--full', action='store_true')
    parser.add_argument('--since', help='ISO date for the updates command')
    args = parser.parse_args()

    if not (HAS_FITZ or HAS_PDFPLUMBER or HAS_COMMON_PDF):
        logger.error("No PDF extraction backend available. Install PyMuPDF or pdfplumber.")
        sys.exit(1)

    fetcher = EDPBOSSRegisterFetcher()
    script_dir = Path(__file__).parent
    sample_dir = script_dir / 'sample'
    data_dir = script_dir / 'data'

    if args.command == 'updates':
        since = args.since or datetime.utcnow().strftime('%Y-01-01')
        for raw in fetcher.fetch_updates(since):
            normalized = fetcher.normalize(raw)
            if len(normalized.get('text', '')) >= MIN_TEXT_CHARS:
                print(json.dumps(normalized, ensure_ascii=False))
        return

    sample_mode = args.sample or not args.full

    if sample_mode:
        sample_dir.mkdir(exist_ok=True)
        target = 15
        # Roughly half the register's PDFs are scanned (0 extractable chars), so over-fetch.
        max_docs = target * 5
        logger.info(f"Fetching sample (target {target}) decisions...")

        count = 0
        skipped = 0
        for raw in fetcher.fetch_all(max_docs=max_docs):
            normalized = fetcher.normalize(raw)
            if len(normalized.get('text', '')) < MIN_TEXT_CHARS:
                skipped += 1
                logger.warning(
                    f"Skipped {normalized['case_id']} — insufficient text "
                    f"({len(normalized.get('text', ''))} chars, likely a scanned PDF)"
                )
                continue

            filename = normalized['_id'].replace(':', '_').replace('/', '_') + '.json'
            with open(sample_dir / filename, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            count += 1
            logger.info(f"[{count}] {normalized['case_id']} — {len(normalized['text']):,} chars")
            if count >= target:
                break

        logger.info(f"Done. Saved {count}, skipped {skipped} (insufficient text).")
        if count == 0:
            logger.error("No records written!")
            sys.exit(1)
        return

    # Full run: stream every record to data/records.jsonl for the ingest pipeline.
    data_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = data_dir / 'records.jsonl'
    count = 0
    skipped = 0
    with open(jsonl_path, 'w', encoding='utf-8') as f:
        for raw in fetcher.fetch_all():
            normalized = fetcher.normalize(raw)
            if len(normalized.get('text', '')) < MIN_TEXT_CHARS:
                skipped += 1
                continue
            f.write(json.dumps(normalized, ensure_ascii=False) + "\n")
            count += 1
            if count % 25 == 0:
                logger.info(f"Progress: {count} records written ({skipped} skipped)")

    logger.info(f"bootstrap_fast complete: {count} written, {skipped} skipped -> {jsonl_path}")
    if count == 0:
        logger.error("No records written!")
        sys.exit(1)


if __name__ == '__main__':
    main()
