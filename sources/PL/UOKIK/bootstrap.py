#!/usr/bin/env python3
"""
PL/UOKIK -- Polish Office of Competition and Consumer Protection (UOKiK) Decisions

Fetches competition and consumer protection decisions from the UOKiK decisions portal:
  https://decyzje.uokik.gov.pl

Data source: Lotus Domino database (dec_prez.nsf)
Strategy:
  1. ExpandView listing returns all ~9,900 decision UNIDs in one request
  2. editDocument endpoint provides structured metadata (hidden form fields)
  3. OpenDocument page contains PDF attachment link with full decision text
  4. PDF text extracted via pdfplumber
License: Public domain (official government decisions)
Coverage: ~9,900 decisions (2000-2026)

Usage:
  python bootstrap.py bootstrap --sample     # Fetch sample records
  python bootstrap.py bootstrap              # Full fetch
  python bootstrap.py test-api               # Connectivity test
"""

import argparse
import io
import json
import logging
import re
import sys
import time
import urllib3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


# Suppress SSL warnings for decyzje.uokik.gov.pl
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://decyzje.uokik.gov.pl"
SOURCE_ID = "PL/UOKIK"
VIEW_ID = "0b335ee1820a0883c1257876002d1521"
SAMPLE_DIR = Path(__file__).parent / "sample"
DATA_DIR = Path(__file__).parent / "data"
RECORDS_FILE = DATA_DIR / "records.jsonl"
CHECKPOINT_FILE = DATA_DIR / "checkpoint.json"


class UOKIKFetcher:
    """Fetcher for UOKiK competition/consumer protection decisions."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pl-PL,pl;q=0.9,en;q=0.5',
        })
        self.session.verify = False

    def fetch_all_unids(self) -> List[Tuple[str, str]]:
        """Fetch all decision UNIDs from the ExpandView listing.

        Returns list of (view_path, unid) tuples.
        """
        url = f"{BASE_URL}/bp/dec_prez.nsf/UOKiK?OpenForm&Start=1&Count=10000&ExpandView"
        logger.info("Fetching complete decision index...")
        resp = self.session.get(url, timeout=120)
        resp.raise_for_status()

        # Extract UNID and view path from links
        pattern = r'/bp/dec_prez\.nsf/([a-f0-9]{32})/([a-f0-9]{32})\?OpenDocument'
        matches = re.findall(pattern, resp.text, re.IGNORECASE)
        # Deduplicate by UNID
        seen = set()
        results = []
        for view_id, unid in matches:
            uid = unid.upper()
            if uid not in seen:
                seen.add(uid)
                results.append((view_id, uid))

        logger.info(f"Found {len(results)} unique decisions")
        return results

    def fetch_metadata(self, unid: str) -> Dict[str, str]:
        """Fetch structured metadata from editDocument view."""
        url = f"{BASE_URL}/bp/dec_prez.nsf/0/{unid}"
        params = {'editDocument': '', 'act': 'Decyzja'}
        try:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch metadata for {unid}: {e}")
            return {}

        meta = {}
        # Extract hidden input fields
        for match in re.finditer(r'<input[^>]+name="([^"]+)"[^>]+value="([^"]*)"', resp.text):
            name, value = match.group(1), match.group(2)
            if value.strip():
                meta[name] = value.strip()
        return meta

    def fetch_pdf_link(self, view_id: str, unid: str) -> Optional[str]:
        """Find PDF attachment URL from OpenDocument page."""
        url = f"{BASE_URL}/bp/dec_prez.nsf/0/{unid.lower()}?OpenDocument"
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch document page for {unid}: {e}")
            return None

        # Find PDF link pattern: /bp/dec_prez.nsf/viewid/unid/$FILE/name.pdf
        pdf_pattern = r'href="(/bp/dec_prez\.nsf/[^"]+/\$FILE/[^"]+\.pdf)"'
        match = re.search(pdf_pattern, resp.text, re.IGNORECASE)
        if match:
            return urljoin(BASE_URL, match.group(1))

        # Also try broader pattern
        pdf_pattern2 = r'(/bp/dec_prez\.nsf/[^"\s]+\$FILE/[^"\s]+\.pdf)'
        match = re.search(pdf_pattern2, resp.text, re.IGNORECASE)
        if match:
            return urljoin(BASE_URL, match.group(1))

        return None

    def extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="PL/UOKIK",
            source_id="",
            pdf_url=pdf_url,
            table="doctrine",
        ) or ""

    def normalize(self, unid: str, meta: Dict[str, str], full_text: str,
                  pdf_url: Optional[str]) -> Dict[str, Any]:
        """Normalize a decision record to standard schema."""
        case_number = meta.get('decyzja_nr', meta.get('decyzja_nr_1', ''))
        signature = meta.get('Sygnatura', '')
        date_raw = meta.get('Data_decyzji', '')
        parties = meta.get('linki_kontrah_www1', '')
        practice_type = meta.get('Rodzaj_praktyki', '')
        penalty = meta.get('Kary', '')
        industry = meta.get('XBranza', '')
        region = meta.get('Region', '')
        appeal = meta.get('Odwolanie', '')

        # Parse date (multiple formats from Domino: MM/DD/YYYY, DD/MM/YYYY, or YYYY-MM-DD)
        date_iso = None
        if date_raw:
            for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y'):
                try:
                    dt = datetime.strptime(date_raw, fmt)
                    date_iso = dt.strftime('%Y-%m-%d')
                    break
                except ValueError:
                    continue

        doc_id = case_number.replace('.', '_').replace('/', '_').replace(' ', '') if case_number else unid
        doc_url = f"{BASE_URL}/bp/dec_prez.nsf/0/{unid}?OpenDocument"

        return {
            '_id': f"PL_UOKIK_{doc_id}",
            '_source': SOURCE_ID,
            '_type': 'doctrine',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': f"Decyzja Prezesa UOKiK nr {case_number}" if case_number else f"Decyzja UOKiK {unid}",
            'text': full_text,
            'date': date_iso,
            'url': doc_url,
            'unid': unid,
            'case_number': case_number,
            'signature': signature,
            'parties': parties,
            'practice_type': practice_type,
            'penalty': penalty == 'Tak',
            'industry': industry,
            'region': region,
            'appeal': appeal == 'Tak',
        }

    def fetch_all(self, sample: bool = False,
                  done: Optional[Set[str]] = None) -> Iterator[Dict[str, Any]]:
        """Fetch all UOKiK decisions with full text.

        `done` holds UNIDs already written by a previous run; they are skipped
        with no network calls so a restarted fleet slot advances monotonically
        instead of re-walking the corpus from the top (issue #1285).
        """
        entries = self.fetch_all_unids()

        if sample:
            entries = entries[:15]
            logger.info(f"Sample mode: processing {len(entries)} decisions")
        elif done:
            before = len(entries)
            entries = [e for e in entries if e[1] not in done]
            logger.info(f"Resuming: skipping {before - len(entries)} already-fetched "
                        f"decisions, {len(entries)} remaining")

        for i, (view_id, unid) in enumerate(entries):
            logger.info(f"[{i+1}/{len(entries)}] Fetching {unid}...")

            # Get metadata
            meta = self.fetch_metadata(unid)
            case_nr = meta.get('decyzja_nr', 'unknown')
            logger.info(f"  Decision: {case_nr}")

            # Find and download PDF
            pdf_url = self.fetch_pdf_link(view_id, unid)
            if not pdf_url:
                logger.warning(f"  No PDF found for {case_nr}, skipping")
                continue

            full_text = self.extract_pdf_text(pdf_url)
            if not full_text or len(full_text) < 50:
                logger.warning(f"  Empty/short PDF text for {case_nr}, skipping")
                continue

            logger.info(f"  Extracted {len(full_text)} chars from PDF")
            time.sleep(1.0)

            record = self.normalize(unid, meta, full_text, pdf_url)
            yield record

    def test_api(self) -> bool:
        """Test connectivity to the UOKiK decisions portal."""
        logger.info("Testing UOKiK API connectivity...")

        # Test listing
        try:
            url = f"{BASE_URL}/bp/dec_prez.nsf/UOKiK?OpenForm&Start=1&Count=5&ExpandView"
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            links = re.findall(r'/bp/dec_prez\.nsf/[a-f0-9]+/([A-Fa-f0-9]{32})\?OpenDocument', resp.text)
            logger.info(f"  Listing endpoint: OK ({len(links)} decisions on page)")
        except Exception as e:
            logger.error(f"  Listing endpoint: FAILED - {e}")
            return False

        if not links:
            logger.error("  No decision links found")
            return False

        # Test metadata for first decision
        unid = links[0].upper()
        view_match = re.search(r'/bp/dec_prez\.nsf/([a-f0-9]{32})/' + unid, resp.text, re.IGNORECASE)
        view_id = view_match.group(1) if view_match else '0'

        try:
            meta = self.fetch_metadata(unid)
            case_nr = meta.get('decyzja_nr', 'N/A')
            logger.info(f"  Metadata endpoint: OK (case: {case_nr})")
        except Exception as e:
            logger.error(f"  Metadata endpoint: FAILED - {e}")
            return False

        # Test PDF download
        try:
            pdf_url = self.fetch_pdf_link(view_id, unid)
            if pdf_url:
                text = self.extract_pdf_text(pdf_url)
                if text and len(text) > 100:
                    logger.info(f"  PDF extraction: OK ({len(text)} chars)")
                else:
                    logger.error(f"  PDF extraction: Too short or empty")
                    return False
            else:
                logger.error(f"  No PDF link found for {unid}")
                return False
        except Exception as e:
            logger.error(f"  PDF extraction: FAILED - {e}")
            return False

        logger.info("All API tests passed!")
        return True


def bootstrap_sample(fetcher: UOKIKFetcher):
    """Fetch sample records and save to sample/ directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    for record in fetcher.fetch_all(sample=True):
        fname = SAMPLE_DIR / f"{record['_id']}.json"
        with open(fname, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        text_len = len(record.get('text', ''))
        logger.info(f"  Saved {record['_id']} ({text_len} chars)")
        count += 1
    logger.info(f"Sample complete: {count} records saved to {SAMPLE_DIR}")
    return count


def _load_checkpoint() -> Set[str]:
    """UNIDs already streamed to records.jsonl by an earlier run."""
    if not CHECKPOINT_FILE.exists():
        return set()
    try:
        with open(CHECKPOINT_FILE, encoding='utf-8') as f:
            return set(json.load(f).get('done', []))
    except (OSError, ValueError) as e:
        logger.warning(f"Ignoring unreadable checkpoint {CHECKPOINT_FILE}: {e}")
        return set()


def _save_checkpoint(done: Set[str]):
    tmp = CHECKPOINT_FILE.with_suffix('.tmp')
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump({'done': sorted(done)}, f)
    tmp.replace(CHECKPOINT_FILE)


def bootstrap_full(fetcher: UOKIKFetcher):
    """Fetch all records, streaming to data/records.jsonl for pipeline ingest."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    done = _load_checkpoint()
    count = 0
    # Append so a resumed run keeps what the previous slot already wrote; the
    # checkpoint guarantees we never re-append the same decision.
    with open(RECORDS_FILE, 'a', encoding='utf-8') as out:
        for record in fetcher.fetch_all(sample=False, done=done):
            out.write(json.dumps(record, ensure_ascii=False) + '\n')
            out.flush()
            done.add(record['unid'])
            count += 1
            if count % 25 == 0:
                _save_checkpoint(done)
                logger.info(f"  Progress: {count} records written")
    _save_checkpoint(done)
    logger.info(f"Full bootstrap complete: {count} records written to {RECORDS_FILE}")
    return count


def main():
    parser = argparse.ArgumentParser(description='PL/UOKIK Competition Decisions Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'bootstrap-fast', 'update', 'test-api'],
                        help='Command to run')
    parser.add_argument('--sample', action='store_true',
                        help='Only fetch sample records (15 decisions)')
    parser.add_argument('--since', type=str,
                        help='Fetch updates since date (YYYY-MM-DD)')
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    fetcher = UOKIKFetcher()

    if args.command == 'test-api':
        success = fetcher.test_api()
        sys.exit(0 if success else 1)

    # The fleet wrapper invokes `bootstrap-fast`; route it to the full path so it
    # streams the whole corpus instead of falling back to the committed samples.
    elif args.command in ('bootstrap', 'bootstrap-fast'):
        if args.sample:
            count = bootstrap_sample(fetcher)
        else:
            count = bootstrap_full(fetcher)
        if count == 0:
            if not args.sample and _load_checkpoint():
                logger.info("No new records — corpus already fetched (checkpoint complete)")
            else:
                logger.error("No records fetched!")
                sys.exit(1)

    elif args.command == 'update':
        logger.info("Update not supported for Domino-based source; use full bootstrap")
        sys.exit(1)


if __name__ == '__main__':
    main()
