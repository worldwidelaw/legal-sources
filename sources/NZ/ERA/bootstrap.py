#!/usr/bin/env python3
"""
NZ/ERA -- Employment Relations Authority Determinations

Fetches employment dispute determinations from the NZ ERA database.

Strategy:
  - Enumerate determination pages by sequential ID (1 to ~21207)
  - Parse metadata from HTML (title, reference, date, member, jurisdiction, parties)
  - Download PDF and extract full text via common/pdf_extract
  - PDFs available since 2005; pre-2005 records included with summary text if available

Source: https://determinations.era.govt.nz/determinations (NZ Government, open access)
Rate limit: 1 req/sec

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NZ.ERA")

BASE_URL = "https://determinations.era.govt.nz"

# Approximate highest known ID (will be discovered dynamically)
MAX_KNOWN_ID = 21210


class ERAScraper(BaseScraper):
    """
    Scraper for NZ/ERA -- Employment Relations Authority.
    Country: NZ
    URL: https://determinations.era.govt.nz/determinations
    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,*/*",
            },
            timeout=60,
        )

    def _discover_max_id(self) -> int:
        """Find the current highest determination ID from the recent page."""
        try:
            resp = self.client.get(f"{BASE_URL}/determinations/recent", timeout=30)
            if resp and resp.status_code == 200:
                ids = re.findall(r'/determination/view/(\d+)', resp.text)
                if ids:
                    return max(int(i) for i in ids)
        except Exception as e:
            logger.warning(f"Could not discover max ID: {e}")
        return MAX_KNOWN_ID

    def _parse_determination_page(self, det_id: int) -> Optional[dict]:
        """Fetch and parse a determination page by ID."""
        url = f"{BASE_URL}/determination/view/{det_id}"
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url, timeout=30)
            if resp is None or resp.status_code != 200:
                return None

            html = resp.text

            # Title
            title_m = re.search(
                r'class="determination__title"[^>]*>(.*?)</h1>',
                html, re.DOTALL
            )
            title = re.sub(r'<[^>]+>', '', title_m.group(1)).strip() if title_m else ""
            if not title:
                return None

            # Extract table rows: <th>Label</th> ... <td>Value</td>
            metadata = {}
            rows = re.findall(
                r'<th[^>]*scope="row"[^>]*>(.*?)</th>\s*<td>(.*?)</td>',
                html, re.DOTALL
            )
            for label, value in rows:
                label = re.sub(r'<[^>]+>', '', label).strip().rstrip(':')
                value = re.sub(r'<[^>]+>', ' ', value).strip()
                value = re.sub(r'\s+', ' ', value).strip()
                metadata[label] = value

            # PDF link
            pdf_m = re.search(r'href="([^"]*elawpdf[^"]*\.pdf)"', html)
            pdf_path = pdf_m.group(1) if pdf_m else None
            pdf_url = None
            if pdf_path:
                if pdf_path.startswith('http'):
                    pdf_url = pdf_path
                else:
                    pdf_url = f"{BASE_URL}{pdf_path}"

            # Parse date
            date_str = metadata.get('Determination date', '')
            date_iso = None
            if date_str:
                for fmt in ('%d %B %Y', '%d %b %Y'):
                    try:
                        date_iso = datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
                        break
                    except ValueError:
                        continue

            ref_no = metadata.get('Reference No', '')

            return {
                'det_id': det_id,
                'title': title,
                'reference_no': ref_no,
                'date': date_iso,
                'date_raw': date_str,
                'member': metadata.get('Member', ''),
                'jurisdiction': metadata.get('Jurisdiction', ''),
                'parties': metadata.get('Parties', title),
                'location': metadata.get('Location', ''),
                'hearing_date': metadata.get('Hearing date', ''),
                'representation': metadata.get('Representation', ''),
                'summary': metadata.get('Summary', ''),
                'result': metadata.get('Result', ''),
                'main_category': metadata.get('Main Category', ''),
                'restrictions': metadata.get('Restrictions', ''),
                'pdf_url': pdf_url,
                'page_url': url,
            }

        except Exception as e:
            logger.debug(f"Error parsing determination {det_id}: {e}")
            return None

    def _extract_full_text(self, raw: dict) -> str:
        """Download and extract text from PDF, fall back to summary."""
        pdf_url = raw.get('pdf_url')
        if pdf_url:
            try:
                text = extract_pdf_markdown(
                    source="NZ/ERA",
                    source_id=str(raw['det_id']),
                    pdf_url=pdf_url,
                    table="case_law",
                    force=True,
                )
                if text and len(text) > 100:
                    return text
            except Exception as e:
                logger.debug(f"PDF extraction failed for {raw['det_id']}: {e}")

        # Fallback to summary
        summary = raw.get('summary', '')
        if summary and len(summary) > 50:
            return summary
        return ""

    def normalize(self, raw: dict) -> dict:
        """Transform raw determination data into standard schema."""
        det_id = raw['det_id']
        ref_no = raw.get('reference_no', '')
        _id = ref_no if ref_no else f"ERA-{det_id}"

        return {
            '_id': _id,
            '_source': 'NZ/ERA',
            '_type': 'case_law',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': raw.get('title', ''),
            'text': raw.get('text', ''),
            'date': raw.get('date'),
            'url': raw.get('page_url', f"{BASE_URL}/determination/view/{det_id}"),
            'reference_no': ref_no,
            'member': raw.get('member', ''),
            'jurisdiction': raw.get('jurisdiction', ''),
            'parties': raw.get('parties', ''),
            'pdf_url': raw.get('pdf_url', ''),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all determinations by iterating through sequential IDs."""
        max_id = self._discover_max_id()
        logger.info(f"Fetching determinations from ID 1 to {max_id}")

        consecutive_misses = 0
        for det_id in range(max_id, 0, -1):  # newest first
            raw = self._parse_determination_page(det_id)
            if raw is None:
                consecutive_misses += 1
                if consecutive_misses > 50:
                    logger.info(f"50 consecutive misses at ID {det_id}, stopping")
                    break
                continue

            consecutive_misses = 0
            text = self._extract_full_text(raw)
            if not text:
                logger.debug(f"No text for determination {det_id}, skipping")
                continue

            raw['text'] = text
            record = self.normalize(raw)
            yield record

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch determinations from the recent page."""
        # Start from the highest ID and work backwards until we pass the date
        max_id = self._discover_max_id()
        for det_id in range(max_id, max(1, max_id - 500), -1):
            raw = self._parse_determination_page(det_id)
            if raw is None:
                continue
            if raw.get('date') and raw['date'] < since:
                break

            text = self._extract_full_text(raw)
            if not text:
                continue
            raw['text'] = text
            yield self.normalize(raw)

    def test_api(self) -> bool:
        """Test connectivity to ERA site."""
        try:
            resp = self.client.get(f"{BASE_URL}/determinations/recent", timeout=15)
            if resp and resp.status_code == 200:
                ids = re.findall(r'/determination/view/(\d+)', resp.text)
                logger.info(f"ERA site OK — found {len(ids)} recent determinations")
                return True
            logger.error(f"ERA site returned {resp.status_code if resp else 'None'}")
            return False
        except Exception as e:
            logger.error(f"ERA connectivity test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="NZ/ERA bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    parser.add_argument("--full", action="store_true", help="Full fetch (all records)")
    args = parser.parse_args()

    scraper = ERAScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        limit = 15 if args.sample else None
        count = 0

        for record in scraper.fetch_all():
            count += 1
            if args.sample:
                fname = sample_dir / f"{count:04d}.json"
                fname.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                logger.info(
                    f"[{count}] {record['_id']} — {record['title'][:60]} "
                    f"({len(record.get('text', ''))} chars)"
                )
            else:
                scraper.save_record(record)
                if count % 100 == 0:
                    logger.info(f"Saved {count} records")

            if limit and count >= limit:
                break

        logger.info(f"Done: {count} records fetched")


if __name__ == "__main__":
    main()
