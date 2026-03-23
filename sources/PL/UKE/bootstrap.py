#!/usr/bin/env python3
"""
PL/UKE -- Polish Office of Electronic Communications Official Journal

Fetches doctrine documents from the Official Journal (Dziennik Urzędowy) of UKE:
  - Zarządzenia (orders)
  - Obwieszczenia (announcements)
  - Informacje (information notices)
  - Komunikaty (communications)
  - Decyzje (decisions)

Data source: https://edziennik.uke.gov.pl
API: REST JSON (SearchLegalAct) + PDF downloads (GetActPdf.ashx)
License: Public domain (official government journal)
Coverage: 937 acts (2012-2026)

Usage:
  python bootstrap.py bootstrap --sample     # Fetch sample records
  python bootstrap.py bootstrap              # Full fetch
  python bootstrap.py test-api               # Connectivity test
"""

import argparse
import hashlib
import io
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import pdfplumber
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://edziennik.uke.gov.pl"
SOURCE_ID = "PL/UKE"
SAMPLE_DIR = Path(__file__).parent / "sample"


class UKEFetcher:
    """Fetcher for UKE Official Journal documents."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'pl-PL,pl;q=0.9,en;q=0.5',
        })

    def fetch_all_acts(self) -> List[Dict[str, Any]]:
        """Fetch the complete index of all legal acts via SearchLegalAct API."""
        url = f"{BASE_URL}/api/SearchLegalAct"
        all_acts = []
        page = 0
        page_size = 500

        while True:
            logger.info(f"Fetching act index page {page}...")
            try:
                resp = self.session.post(
                    url,
                    json={"pageSize": page_size, "pageIndex": page},
                    headers={"Content-Type": "application/json"},
                    timeout=30
                )
                resp.raise_for_status()
                data = resp.json()
                acts = data.get('LegalActs', [])
                total = data.get('TotalItems', 0)
                all_acts.extend(acts)
                logger.info(f"  Retrieved {len(acts)} acts (total so far: {len(all_acts)}/{total})")

                if len(all_acts) >= total or not acts:
                    break
                page += 1
                time.sleep(1)
            except requests.RequestException as e:
                logger.error(f"Failed to fetch act index page {page}: {e}")
                break

        logger.info(f"Total acts retrieved: {len(all_acts)}")
        return all_acts

    def fetch_pdf_text(self, year: int, position: int) -> Optional[str]:
        """Download a PDF and extract its text content."""
        url = f"{BASE_URL}/GetActPdf.ashx?year={year}&book=0&position={position}"
        try:
            resp = self.session.get(url, timeout=60)
            if resp.status_code != 200:
                logger.warning(f"HTTP {resp.status_code} for PDF: year={year}, pos={position}")
                return None
            if 'application/pdf' not in resp.headers.get('Content-Type', ''):
                logger.warning(f"Not a PDF response for year={year}, pos={position}")
                return None
        except requests.RequestException as e:
            logger.warning(f"Failed to download PDF year={year}, pos={position}: {e}")
            return None

        try:
            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                pages_text = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
                full_text = "\n\n".join(pages_text)
                return full_text if len(full_text) > 50 else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for year={year}, pos={position}: {e}")
            return None

    def _parse_date(self, date_str: Optional[str]) -> Optional[str]:
        """Parse API date string to ISO 8601."""
        if not date_str:
            return None
        try:
            dt = datetime.fromisoformat(date_str.split('.')[0])
            return dt.strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            return None

    def normalize(self, act: Dict[str, Any], text: str) -> Dict[str, Any]:
        """Normalize an act into standard schema."""
        year = act.get('Year', 0)
        position = act.get('Position', 0)
        doc_id = f"{year}-{position}"

        act_date = self._parse_date(act.get('ActDate'))
        pub_date = self._parse_date(act.get('PublicationDate'))
        date = act_date or pub_date

        title = act.get('Title') or act.get('Subject') or ''
        act_type = act.get('LegalActType', '')
        pdf_url = f"{BASE_URL}/GetActPdf.ashx?year={year}&book=0&position={position}"

        return {
            '_id': f"PL-UKE-{doc_id}",
            '_source': SOURCE_ID,
            '_type': 'doctrine',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': title,
            'text': text,
            'date': date,
            'url': pdf_url,
            'act_type': act_type,
            'year': year,
            'position': position,
            'publisher': act.get('PublishersListFlat', ''),
            'language': 'pol',
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Yield all normalized documents."""
        acts = self.fetch_all_acts()
        for i, act in enumerate(acts):
            year = act.get('Year', 0)
            position = act.get('Position', 0)
            logger.info(f"Fetching PDF {i+1}/{len(acts)}: year={year}, pos={position}")

            text = self.fetch_pdf_text(year, position)
            if not text:
                logger.warning(f"  No text extracted, skipping")
                continue

            yield self.normalize(act, text)
            time.sleep(1.5)

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Yield documents published since a given date."""
        since_dt = datetime.fromisoformat(since)
        acts = self.fetch_all_acts()
        for act in acts:
            pub_date = self._parse_date(act.get('PublicationDate'))
            if pub_date:
                try:
                    act_dt = datetime.fromisoformat(pub_date)
                    if act_dt < since_dt:
                        continue
                except (ValueError, TypeError):
                    pass

            year = act.get('Year', 0)
            position = act.get('Position', 0)
            text = self.fetch_pdf_text(year, position)
            if text:
                yield self.normalize(act, text)
            time.sleep(1.5)

    def bootstrap_sample(self, n: int = 15) -> List[Dict[str, Any]]:
        """Fetch a diverse sample of documents for testing."""
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

        acts = self.fetch_all_acts()
        if not acts:
            logger.error("No acts found in index")
            return []

        # Group by type and pick diverse samples
        by_type: Dict[str, list] = {}
        for act in acts:
            act_type = act.get('LegalActType', 'Unknown')
            by_type.setdefault(act_type, []).append(act)

        selected = []
        types = list(by_type.keys())
        per_type = max(1, n // len(types)) if types else n
        for act_type in types:
            for act in by_type[act_type][:per_type]:
                selected.append(act)
            if len(selected) >= n:
                break

        if len(selected) < n:
            for act_type in sorted(types, key=lambda t: len(by_type[t]), reverse=True):
                for act in by_type[act_type][per_type:]:
                    if len(selected) >= n:
                        break
                    selected.append(act)
                if len(selected) >= n:
                    break

        selected = selected[:n]
        results = []

        for i, act in enumerate(selected):
            year = act.get('Year', 0)
            position = act.get('Position', 0)
            logger.info(f"Sample {i+1}/{len(selected)}: {act.get('LegalActType')} year={year} pos={position}")

            text = self.fetch_pdf_text(year, position)
            if not text:
                logger.warning(f"  No text, skipping")
                continue

            normalized = self.normalize(act, text)
            results.append(normalized)

            fname = f"{normalized['_id']}.json"
            with open(SAMPLE_DIR / fname, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            logger.info(f"  Saved: {fname} ({len(text)} chars)")

            time.sleep(1.5)

        return results

    def test_api(self):
        """Quick connectivity test."""
        print("Testing UKE Official Journal API...")

        acts = self.fetch_all_acts()
        print(f"  Total acts: {len(acts)}")

        if acts:
            act = acts[0]
            year = act.get('Year', 0)
            position = act.get('Position', 0)
            print(f"  Latest: year={year}, pos={position}, type={act.get('LegalActType')}")
            print(f"  Title: {act.get('Title', '')[:80]}")

            text = self.fetch_pdf_text(year, position)
            if text:
                print(f"  PDF text: {len(text)} chars")
                print(f"  Preview: {text[:150]}...")
            else:
                print("  PDF text extraction failed!")

        # Show type distribution
        by_type: Dict[str, int] = {}
        for act in acts:
            t = act.get('LegalActType', 'Unknown')
            by_type[t] = by_type.get(t, 0) + 1
        print("\n  Act types:")
        for t, c in sorted(by_type.items(), key=lambda x: -x[1]):
            print(f"    {t}: {c}")

        print("\nTest complete!")


def main():
    parser = argparse.ArgumentParser(description="PL/UKE fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--sample-size", type=int, default=15)

    args = parser.parse_args()
    fetcher = UKEFetcher()

    if args.command == "test-api":
        fetcher.test_api()
    elif args.command == "bootstrap":
        if args.sample:
            results = fetcher.bootstrap_sample(n=args.sample_size)
            print(f"\nSample complete: {len(results)} records saved to sample/")
        else:
            data_dir = Path(__file__).parent / "data"
            data_dir.mkdir(exist_ok=True)
            count = 0
            for record in fetcher.fetch_all():
                fname = f"{record['_id']}.json"
                with open(data_dir / fname, 'w', encoding='utf-8') as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
                if count % 100 == 0:
                    logger.info(f"Saved {count} documents")
            print(f"\nBootstrap complete: {count} documents saved")
    elif args.command == "update":
        since = (datetime.now() - __import__('datetime').timedelta(days=30)).isoformat()
        count = 0
        data_dir = Path(__file__).parent / "data"
        data_dir.mkdir(exist_ok=True)
        for record in fetcher.fetch_updates(since):
            fname = f"{record['_id']}.json"
            with open(data_dir / fname, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        print(f"\nUpdate complete: {count} documents saved")


if __name__ == "__main__":
    main()
