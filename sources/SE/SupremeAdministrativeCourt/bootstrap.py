#!/usr/bin/env python3
"""
Swedish Supreme Administrative Court (Högsta förvaltningsdomstolen - HFD) Data Fetcher

Data source: Domstolsverket's Open Data API
API: https://rattspraxis.etjanst.domstol.se/api/v1
API docs: https://rattspraxis.etjanst.domstol.se/openapi/puh-openapi.yaml

This fetcher uses the official API to retrieve HFD decisions with full text content.
The API provides:
- Full text in HTML format via the 'innehall' field
- PDF attachments for official document versions
- Comprehensive metadata including case numbers, dates, keywords, legal provisions

Coverage: ~1,300+ decisions from the API (March 2025+ new decisions, plus historical referat)

No authentication required. Public domain court decisions.

Usage:
  python bootstrap.py bootstrap            # Full pull, streams to data/records.jsonl
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample decisions
  python bootstrap.py bootstrap-fast       # Full pull, concurrent normalize
  python bootstrap.py update               # Incremental pull
  python bootstrap.py test                 # Connectivity test
"""

import html
import json
import logging
import re
import sys
import time
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Generator, Optional, List, Dict, Union
from urllib.parse import quote

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SE.SupremeAdministrativeCourt")

API_BASE = "https://rattspraxis.etjanst.domstol.se/api/v1"
SOURCE_ID = "SE/SupremeAdministrativeCourt"
COURT_CODE = "HFD"
COURT_NAME = "Högsta förvaltningsdomstolen"

# The API held ~1,300 HFD publications as of 2026-08. An empty first page means
# the endpoint changed or is refusing us, not that the court stopped deciding
# cases — so a full sweep must fail loud rather than exit 0.
KNOWN_CORPUS_FLOOR = 100

# Shortest body we accept as full text.
MIN_TEXT_CHARS = 200

PUB_TYPES = {
    'DOM_ELLER_BESLUT': 'Judgment or decision',
    'RATTSFALL': 'Case report (RÅ)',
    'PROVNINGSTILLSTAND': 'Leave to appeal decision',
    'FORHANDSAVGORANDE': 'Preliminary ruling request',
}


class HFDUnavailable(RuntimeError):
    """The Domstolsverket API did not return a usable result set."""


def _as_date_str(value: Union[str, date, datetime, None]) -> Optional[str]:
    """Render a datetime/date/str as YYYY-MM-DD, so date comparisons are total."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    return text[:10] if text else None


def html_to_text(html_content: str) -> str:
    """Convert HTML to plain text, preserving paragraph structure."""
    if not html_content:
        return ""

    # Replace common block elements with newlines
    text = re.sub(r'</(p|div|h[1-6]|li|tr)>', '\n', html_content, flags=re.IGNORECASE)
    text = re.sub(r'<(br|hr)\s*/?>', '\n', text, flags=re.IGNORECASE)

    # Remove all remaining HTML tags
    text = re.sub(r'<[^>]+>', '', text)

    # Decode HTML entities
    text = html.unescape(text)

    # Clean up whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)
    text = re.sub(r'^\s+', '', text, flags=re.MULTILINE)

    return text.strip()


class HFDScraper(BaseScraper):
    """
    Scraper for SE/SupremeAdministrativeCourt — Högsta förvaltningsdomstolen.

    fetch_all()/fetch_updates() yield RAW API publications; normalize(raw)
    resolves the full text (HTML content, else PDF attachment, else summary)
    and maps the record onto the standard schema.
    """

    def __init__(self):
        super().__init__(Path(__file__).parent)
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'LegalDataHunter/1.0 (research; https://github.com/ZachLaik/LegalDataHunter)',
        })

    # ---------------------------------------------------------------- fetching

    def fetch_publications(
        self,
        page: int = 0,
        pagesize: int = 100,
        pub_types: str = None,
        sort_asc: bool = False,
    ) -> List[Dict]:
        """Fetch one page of HFD publications from the API."""
        params = {
            'domstolkod': COURT_CODE,
            'page': page,
            'pagesize': pagesize,
            'sortorder': 'avgorandedatum',
            'asc': 'true' if sort_asc else 'false',
        }
        if pub_types:
            params['publiceringstyper'] = pub_types

        resp = self.session.get(f"{API_BASE}/publiceringar", params=params, timeout=30)
        if resp.status_code != 200:
            raise HFDUnavailable(
                f"{API_BASE}/publiceringar returned HTTP {resp.status_code} for page "
                f"{page}: {resp.text[:200]}"
            )
        return resp.json()

    def download_attachment(self, storage_id: str) -> bytes:
        """Download an attachment (PDF) from the API."""
        # The bilagor endpoint returns 406 Not Acceptable for
        # application/octet-stream; it only serves application/pdf (see #1214).
        resp = self.session.get(
            f"{API_BASE}/bilagor/{quote(storage_id, safe='')}",
            headers={'Accept': 'application/pdf'},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.content

    def _iter_publications(self, stop_before: Optional[str] = None) -> Generator[Dict, None, None]:
        """
        Page through HFD publications newest-first, yielding RAW records.

        If stop_before is a YYYY-MM-DD string, stop as soon as a decision older
        than it is reached — results are date-sorted descending, so everything
        after that point is already held.
        """
        page = 0
        pagesize = 100
        yielded = 0
        empty_pages = 0

        while True:
            publications = self.fetch_publications(page=page, pagesize=pagesize)

            if not publications:
                empty_pages += 1
                if empty_pages >= 3:
                    logger.info("No more publications (3 empty pages)")
                    break
                page += 1
                continue

            empty_pages = 0

            for pub in publications:
                if stop_before:
                    decided = _as_date_str(pub.get('avgorandedatum'))
                    if decided and decided < stop_before:
                        logger.info(
                            f"Reached {decided} (older than {stop_before}) — "
                            f"stopping after {yielded} records"
                        )
                        return
                yield pub
                yielded += 1

            page += 1
            time.sleep(1.0)

        if stop_before is None and yielded < KNOWN_CORPUS_FLOOR:
            raise HFDUnavailable(
                f"Full sweep yielded only {yielded} publications, below the known "
                f"floor of {KNOWN_CORPUS_FLOOR} — the API changed or is refusing us"
            )

    def fetch_all(self) -> Generator[Dict, None, None]:
        """Yield every HFD publication as a RAW API record."""
        yield from self._iter_publications()

    def fetch_updates(self, since: Union[str, date, datetime]) -> Generator[Dict, None, None]:
        """
        Yield publications decided on or after `since`.

        Accepts a datetime, date or string, since the refresh runner passes
        whichever it has.
        """
        since_str = _as_date_str(since)
        logger.info(f"Fetching HFD decisions since {since_str}")
        yield from self._iter_publications(stop_before=since_str)

    # ------------------------------------------------------------- normalizing

    def _resolve_text(self, raw: Dict) -> tuple:
        """
        Resolve the full text of a publication.

        Returns (text, text_source, pdf_filename). Tries the HTML 'innehall'
        field first, then the PDF attachment, then the summary.
        """
        html_content = raw.get('innehall', '')
        if html_content:
            text = html_to_text(html_content)
            if text and len(text) >= MIN_TEXT_CHARS:
                return text, 'html', ''

        for attachment in raw.get('bilagaLista', []) or []:
            storage_id = attachment.get('fillagringId', '')
            filename = attachment.get('filnamn', '')
            if not storage_id:
                continue
            try:
                pdf_bytes = self.download_attachment(storage_id)
                text = extract_pdf_markdown(
                    source=SOURCE_ID,
                    source_id=raw.get('id', ''),
                    pdf_bytes=pdf_bytes,
                    table="case_law",
                ) or ""
                if text and len(text) >= MIN_TEXT_CHARS:
                    return text, 'pdf', filename
            except Exception as e:
                logger.warning(f"PDF download/extraction failed for {filename}: {e}")

        summary = raw.get('sammanfattning', '')
        if summary and len(summary) >= MIN_TEXT_CHARS:
            return summary, 'summary', ''

        return '', '', ''

    def normalize(self, raw: Dict) -> Optional[Dict]:
        """Transform a raw API publication into the standard schema."""
        pub_id = raw.get('id', '')

        text, text_source, pdf_filename = self._resolve_text(raw)
        if not text:
            logger.warning(f"No usable text for publication {pub_id}")
            return None

        case_numbers = raw.get('malNummerLista', [])
        primary_case = case_numbers[0] if case_numbers else pub_id

        decision_date = _as_date_str(raw.get('avgorandedatum'))

        title_parts = []
        if raw.get('benamning'):
            title_parts.append(raw['benamning'].strip())
        if primary_case:
            title_parts.append(f"Mål: {primary_case}")
        title = ' - '.join(title_parts) if title_parts else f"Mål: {primary_case}"

        provisions = raw.get('lagrumLista', []) or []
        sfs_refs = [p.get('referens', '') for p in provisions if p.get('referens')]
        sfs_numbers = [p.get('sfsNummer', '') for p in provisions if p.get('sfsNummer')]

        pub_type = raw.get('typ', '')
        is_precedent = raw.get('arVagledande', False)

        # Key on the API's own publication id: case numbers repeat across the
        # several publications a single case can produce, and are blank on some
        # records, so a case-number key would collide (same failure as #1437).
        doc_id = f"HFD-{pub_id}" if pub_id else f"HFD-{primary_case}"

        return {
            '_id': doc_id,
            '_source': SOURCE_ID,
            '_type': 'case_law',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': title,
            'text': text,
            'date': decision_date,
            'url': f"https://rattspraxis.etjanst.domstol.se/sok/?id={pub_id}",
            'court': COURT_NAME,
            'court_code': COURT_CODE,
            'case_numbers': case_numbers,
            'case_number': primary_case,
            'publication_type': pub_type,
            'publication_type_label': PUB_TYPES.get(pub_type, pub_type),
            'is_precedent': is_precedent,
            'document_type': 'precedent' if is_precedent else 'decision',
            'published_at': raw.get('publiceringstid', ''),
            'reference_numbers': raw.get('referatNummerLista', []),
            'legal_provisions': sfs_refs,
            'sfs_numbers': sfs_numbers,
            'keywords': raw.get('nyckelordLista', []),
            'summary': raw.get('sammanfattning', ''),
            'text_source': text_source,
            'pdf_filename': pdf_filename,
            'language': 'sv',
        }

    # -------------------------------------------------------------------- test

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Domstolsverket HFD API...")
        pubs = self.fetch_publications(page=0, pagesize=5)
        print(f"Found {len(pubs)} publications")
        if not pubs:
            raise HFDUnavailable("API returned an empty first page")

        pub = pubs[0]
        print("First publication:")
        print(f"  ID: {pub.get('id')}")
        print(f"  Case numbers: {pub.get('malNummerLista', [])}")
        print(f"  Date: {pub.get('avgorandedatum')}")
        print(f"  Type: {pub.get('typ')}")
        print(f"  Has HTML content: {bool(pub.get('innehall'))}")
        print(f"  Attachments: {len(pub.get('bilagaLista', []) or [])}")

        record = self.normalize(pub)
        if not record:
            raise HFDUnavailable("Could not extract text from the newest publication")
        print(f"  Text: {len(record['text']):,} chars ({record['text_source']})")
        print("\nTest complete!")


def main():
    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        sample_size = int(sys.argv[sys.argv.index("--sample-size") + 1])

    scraper = HFDScraper()

    try:
        if command == "test":
            scraper.test_connection()
            return

        # "bootstrap-fast" is what the fleet wrapper invokes; route it to the
        # full bootstrap so an unrecognised command can't fall back to samples.
        if command in ("bootstrap", "bootstrap-fast"):
            if sample_mode:
                stats = scraper.run_sample(n=sample_size)
                print(
                    f"\nSample complete: "
                    f"{stats.get('sample_records_saved', 0)} records saved to sample/"
                )
                written = stats.get("sample_records_saved", 0)
            else:
                stats = scraper.bootstrap()
                print(
                    f"\nBootstrap complete: {stats['records_new']} new, "
                    f"{stats['records_updated']} updated, "
                    f"{stats['records_skipped']} skipped"
                )
                written = stats.get("records_new", 0) + stats.get("records_updated", 0)

        elif command == "update":
            stats = scraper.update()
            print(
                f"\nUpdate complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated"
            )
            written = stats.get("records_new", 0) + stats.get("records_updated", 0)

        else:
            print(f"Unknown command: {command}")
            sys.exit(1)

    except HFDUnavailable as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)

    print(json.dumps(stats, indent=2))

    if stats.get("error_message"):
        print(f"\nERROR: {stats['error_message']}", file=sys.stderr)
        sys.exit(1)
    if command != "update" and not written:
        print("\nERROR: no records written", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
