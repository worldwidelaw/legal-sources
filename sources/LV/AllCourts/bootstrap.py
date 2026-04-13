#!/usr/bin/env python3
"""
Latvian Courts (elieta.lv) Data Fetcher

Official anonymized court decisions from the Latvian Courts Portal.
https://www.elieta.lv/web/

Uses the gateway.elieta.lv REST API to search decisions and download
full-text PDFs. Text extracted with PyMuPDF (fitz).

420,000+ decisions covering all Latvian courts since 2007.
No authentication required.
"""

import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional

import fitz  # PyMuPDF

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
API_BASE = "https://gateway.elieta.lv/api/v1"
SEARCH_URL = f"{API_BASE}/PublicMaterial"
DOWNLOAD_URL = f"{API_BASE}/PublicMaterialDownload"


class LatvianCourtsFetcher:
    """Fetcher for Latvian court decisions from elieta.lv"""

    def __init__(self, slow_mode: bool = False):
        self.slow_mode = slow_mode
        self.doc_delay = 3.0 if slow_mode else 1.5
        self.page_delay = 5.0 if slow_mode else 2.0

        if slow_mode:
            logger.info("Running in SLOW MODE")

    def _curl_post(self, url: str, body: dict, max_attempts: int = 3) -> Optional[dict]:
        """POST JSON via curl"""
        body_json = json.dumps(body)
        for attempt in range(max_attempts):
            try:
                result = subprocess.run(
                    ['curl', '-s', '--max-time', '60',
                     '-X', 'POST', url,
                     '-H', 'Content-Type: application/json',
                     '-H', 'Accept: application/json',
                     '-d', body_json],
                    capture_output=True, text=True, timeout=70
                )
                if result.returncode == 0 and result.stdout:
                    return json.loads(result.stdout)
                delay = min(5 * (2 ** attempt), 60)
                logger.warning(f"POST failed attempt {attempt+1}, waiting {delay}s...")
                time.sleep(delay)
            except (subprocess.TimeoutExpired, json.JSONDecodeError) as e:
                delay = min(5 * (2 ** attempt), 60)
                logger.warning(f"POST error attempt {attempt+1}: {e}, waiting {delay}s...")
                time.sleep(delay)
            except Exception as e:
                logger.warning(f"POST unexpected error: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(5)
                else:
                    return None
        return None

    def _download_pdf(self, file_id: str) -> Optional[bytes]:
        """Download a PDF file by materialFileId"""
        url = f"{DOWNLOAD_URL}/{file_id}"
        for attempt in range(3):
            try:
                result = subprocess.run(
                    ['curl', '-s', '--max-time', '60', '-o', '-', url],
                    capture_output=True, timeout=70
                )
                if result.returncode == 0 and result.stdout and len(result.stdout) > 100:
                    return result.stdout
                delay = min(5 * (2 ** attempt), 30)
                logger.warning(f"PDF download failed attempt {attempt+1}, waiting {delay}s...")
                time.sleep(delay)
            except subprocess.TimeoutExpired:
                delay = min(5 * (2 ** attempt), 30)
                logger.warning(f"PDF timeout attempt {attempt+1}, waiting {delay}s...")
                time.sleep(delay)
            except Exception as e:
                logger.warning(f"PDF download error: {e}")
                if attempt < 2:
                    time.sleep(5)
                else:
                    return None
        return None

    def _extract_text_from_pdf(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="LV/AllCourts",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="case_law",
        ) or ""

    def _search_decisions(self, page: int = 1, limit: int = 50,
                          year_month: str = None) -> Optional[dict]:
        """Search for court decisions"""
        body = {
            "page": page,
            "limit": limit,
            "orderBy": "registrationDate",
            "sortOrderDescending": True,
            "institutionSourceRegistryCode": "TIS_COURTS"
        }
        if year_month:
            body["registrationDateYearMonth"] = year_month
        return self._curl_post(SEARCH_URL, body)

    def _parse_timestamp(self, ts) -> Optional[str]:
        """Parse Unix timestamp (ms) to ISO date"""
        if not ts:
            return None
        try:
            if isinstance(ts, str):
                ts = int(ts)
            # Could be ms or seconds
            if ts > 1e12:
                ts = ts / 1000
            return datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
        except (ValueError, OSError):
            return None

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """Fetch all Latvian court decisions with full text"""
        page = 1
        count = 0
        total = None
        consecutive_failures = 0

        while True:
            logger.info(f"Fetching search page {page}...")
            data = self._search_decisions(page=page)

            if not data:
                consecutive_failures += 1
                if consecutive_failures >= 5:
                    logger.error("Too many consecutive search failures, stopping")
                    break
                time.sleep(10)
                continue

            consecutive_failures = 0

            if total is None:
                total = data.get('totalResults', 0)
                total_pages = data.get('totalPages', 0)
                logger.info(f"Total decisions: {total}, pages: {total_pages}")

            items = data.get('items', [])
            if not items:
                logger.info("No more items")
                break

            for item in items:
                files = item.get('materialFiles', [])
                if not files:
                    logger.warning(f"No files for {item.get('caseNumber', '?')}")
                    continue

                file_id = files[0].get('id')
                if not file_id:
                    continue

                logger.info(f"[{count+1}] Downloading {item.get('ecliCode', item.get('caseNumber', '?'))}...")
                pdf_bytes = self._download_pdf(file_id)

                if pdf_bytes:
                    text = self._extract_text_from_pdf(pdf_bytes)
                    if text and len(text) > 100:
                        item['_full_text'] = text
                        yield item
                        count += 1

                        if limit and count >= limit:
                            return
                    else:
                        logger.warning(f"Text too short for {item.get('caseNumber', '?')}")
                else:
                    logger.warning(f"Failed to download PDF for {item.get('caseNumber', '?')}")

                time.sleep(self.doc_delay)

            page += 1
            time.sleep(self.page_delay)

        logger.info(f"Fetched {count} decisions total")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch decisions registered since a given date"""
        # Generate year-month filters from since to now
        now = datetime.now()
        months = []
        current = since.replace(day=1)
        while current <= now:
            months.append(current.strftime('%m.%Y'))
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)

        year_month_filter = ','.join(months)
        page = 1
        count = 0

        while True:
            data = self._search_decisions(page=page, year_month=year_month_filter)
            if not data:
                break

            items = data.get('items', [])
            if not items:
                break

            for item in items:
                files = item.get('materialFiles', [])
                if not files:
                    continue

                file_id = files[0].get('id')
                if not file_id:
                    continue

                pdf_bytes = self._download_pdf(file_id)
                if pdf_bytes:
                    text = self._extract_text_from_pdf(pdf_bytes)
                    if text and len(text) > 100:
                        item['_full_text'] = text
                        yield item
                        count += 1

                time.sleep(self.doc_delay)

            total_pages = data.get('totalPages', 0)
            if page >= total_pages:
                break
            page += 1
            time.sleep(self.page_delay)

        logger.info(f"Fetched {count} updated decisions")

    def normalize(self, raw_item: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a decision to the standard schema"""
        ecli = raw_item.get('ecliCode', '')
        case_number = raw_item.get('caseNumber', '')
        doc_id = ecli or raw_item.get('id', '')

        # Institution
        institution = raw_item.get('institution', {})
        court_name = institution.get('name', '')
        department = institution.get('departmentName', '')

        # Process type
        process_type = raw_item.get('processType', {})
        process_name = process_type.get('name', '')

        # Material type (judgment, ruling, etc.)
        material_type = raw_item.get('materialType', {})
        material_name = material_type.get('name', '')

        # Status
        material_status = raw_item.get('materialStatus', {})
        status_name = material_status.get('name', '')

        # Date
        reg_date = self._parse_timestamp(raw_item.get('registrationDate'))

        # Title: construct from material type + case number
        title = f"{material_name} - {case_number}" if material_name else case_number
        if court_name:
            title = f"{court_name}: {title}"

        # URL
        url = f"https://www.elieta.lv/web/"

        return {
            '_id': doc_id,
            '_source': 'LV/AllCourts',
            '_type': 'case_law',
            '_fetched_at': datetime.now().isoformat(),
            'title': title,
            'text': raw_item.get('_full_text', ''),
            'date': reg_date,
            'url': url,
            'language': 'lv',
            'ecli': ecli,
            'case_number': case_number,
            'court': court_name,
            'department': department,
            'process_type': process_name,
            'material_type': material_name,
            'status': status_name,
        }


def main():
    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        is_fast = '--fast' in sys.argv
        slow_mode = not is_fast and ('--slow' in sys.argv or os.environ.get('VPS_MODE') == '1')
        fetcher = LatvianCourtsFetcher(slow_mode=slow_mode)

        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap...")

        sample_count = 0
        target_count = 15 if '--sample' in sys.argv else 100

        for raw_item in fetcher.fetch_all(limit=target_count + 10):
            if sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_item)
            text_len = len(normalized.get('text', ''))

            if text_len < 100:
                continue

            doc_id = str(normalized['_id']).replace('/', '_').replace(':', '-')
            filename = f"{doc_id}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{sample_count+1}/{target_count}]: {normalized.get('ecli', '')} ({text_len} chars)")
            sample_count += 1

        logger.info(f"Bootstrap complete. Saved {sample_count} documents to {sample_dir}")

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

    elif len(sys.argv) > 1 and sys.argv[1] == 'updates':
        since_str = None
        for i, arg in enumerate(sys.argv):
            if arg == '--since' and i + 1 < len(sys.argv):
                since_str = sys.argv[i + 1]
        if not since_str:
            print("Usage: bootstrap.py updates --since YYYY-MM-DD")
            sys.exit(1)

        since = datetime.strptime(since_str, '%Y-%m-%d')
        fetcher = LatvianCourtsFetcher()
        for raw_item in fetcher.fetch_updates(since):
            normalized = fetcher.normalize(raw_item)
            print(f"{normalized['ecli']}: {normalized['title'][:60]} ({len(normalized.get('text', ''))} chars)")

    elif len(sys.argv) > 1 and sys.argv[1] == 'validate':
        sample_dir = Path(__file__).parent / 'sample'
        files = list(sample_dir.glob('*.json'))
        if not files:
            print("No sample files found. Run bootstrap --sample first.")
            sys.exit(1)

        print(f"Validating {len(files)} sample files...")
        issues = 0
        for f in files:
            with open(f, 'r', encoding='utf-8') as fp:
                data = json.load(fp)
            text = data.get('text', '')
            title = data.get('title', '')
            if not text or len(text) < 100:
                print(f"  FAIL: {f.name} — text too short ({len(text)} chars)")
                issues += 1
            if not title:
                print(f"  WARN: {f.name} — no title")
            if '<' in text and '>' in text and re.search(r'<[a-z]+[^>]*>', text):
                print(f"  WARN: {f.name} — possible HTML in text")
                issues += 1

        print(f"\nValidation: {len(files)} files, {issues} issues")
        sys.exit(1 if issues > 0 else 0)

    else:
        print("Usage:")
        print("  bootstrap.py bootstrap --sample   Fetch 15 sample decisions")
        print("  bootstrap.py bootstrap             Fetch 100 decisions")
        print("  bootstrap.py updates --since DATE  Fetch updates since DATE")
        print("  bootstrap.py validate              Validate sample data")


if __name__ == '__main__':
    main()
