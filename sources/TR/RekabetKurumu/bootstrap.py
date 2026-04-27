#!/usr/bin/env python3
"""
TR/RekabetKurumu -- Turkish Competition Authority Board Decisions

Fetches competition decisions from Rekabet Kurumu via HTML scraping of the
search results page and PDF download for full text.

Strategy:
  - GET /tr/Kararlar?page=N returns 10 decisions per page (server-rendered HTML)
  - Each decision has a UUID, metadata in table rows, and a PDF link
  - GET /Karar?kararId={UUID} returns the PDF directly (application/pdf)
  - Extract text from PDFs via common/pdf_extract

URL patterns:
  - Search: https://www.rekabet.gov.tr/tr/Kararlar?page={N}
  - Decision PDF: https://www.rekabet.gov.tr/Karar?kararId={UUID}
  - No auth required

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TR.RekabetKurumu")

BASE_URL = "https://www.rekabet.gov.tr"
SEARCH_URL = "/tr/Kararlar"

# Decision type mapping (Turkish -> English)
DECISION_TYPES = {
    "Birleşme ve Devralma": "Mergers & Acquisitions",
    "Rekabet İhlali": "Competition Violation",
    "Menfi Tespit ve Muafiyet": "Negative Assessment & Exemption",
    "Özelleştirme": "Privatization",
    "Diğer": "Other",
}


def parse_tr_date(date_str: str) -> Optional[str]:
    """Parse Turkish date format 'DD.MM.YYYY' to ISO 8601."""
    if not date_str:
        return None
    date_str = date_str.strip()
    try:
        dt = datetime.strptime(date_str, "%d.%m.%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def parse_search_page(html: str) -> tuple[List[Dict], int]:
    """Parse a search results page. Returns (decisions, total_count)."""
    decisions = []

    # Extract total count
    total_match = re.search(r'Toplam\s*:\s*(\d+)', html)
    total = int(total_match.group(1)) if total_match else 0

    # Extract each decision table
    tables = re.findall(
        r'<table[^>]*class="[^"]*equalDivide[^"]*"[^>]*>(.*?)</table>',
        html, re.DOTALL
    )

    for table_html in tables:
        decision = {}

        # Extract UUID from link
        uuid_match = re.search(r'kararId=([a-f0-9-]+)', table_html)
        if not uuid_match:
            continue
        decision["uuid"] = uuid_match.group(1)

        # Extract title
        title_match = re.search(
            r'<a[^>]*href="/Karar\?kararId=[^"]*"[^>]*>([^<]+)</a>',
            table_html
        )
        if title_match:
            decision["title"] = title_match.group(1).strip()

        # Extract all th/td pairs for metadata
        rows = re.findall(
            r'<th[^>]*>([^<]*)</th>\s*<td[^>]*>([^<]*)</td>',
            table_html
        )
        for label, value in rows:
            label = label.strip().rstrip(" :")
            value = value.strip()
            if "Yayımlanma Tarihi" in label:
                decision["publication_date"] = value
            elif "Karar Tarihi" in label:
                decision["decision_date"] = value
            elif "Karar Sayısı" in label:
                decision["decision_number"] = value
            elif "Karar Türü" in label:
                decision["decision_type_tr"] = value
                decision["decision_type"] = DECISION_TYPES.get(value, value)

        decisions.append(decision)

    return decisions, total


class RekabetKurumuScraper(BaseScraper):
    """Scraper for TR/RekabetKurumu -- Turkish Competition Authority decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/worldwidelaw/legal-sources)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
            },
            timeout=60,
        )

    def _fetch_search_page(self, page: int) -> tuple[List[Dict], int]:
        """Fetch one page of search results."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(f"{SEARCH_URL}?page={page}")
            if not resp or resp.status_code != 200:
                logger.error(f"Search page {page} error: {resp.status_code if resp else 'no response'}")
                return [], 0
            return parse_search_page(resp.text)
        except Exception as e:
            logger.error(f"Error fetching search page {page}: {e}")
            return [], 0

    def _extract_pdf_text(self, uuid: str, doc_id: str) -> Optional[str]:
        """Download decision PDF and extract text."""
        pdf_url = f"{BASE_URL}/Karar?kararId={uuid}"
        text = extract_pdf_markdown(
            source="TR/RekabetKurumu",
            source_id=doc_id,
            pdf_url=pdf_url,
            table="case_law",
        )
        return text

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all decisions by paginating through search results."""
        page = 1
        total = None
        seen_uuids = set()

        while True:
            decisions, count = self._fetch_search_page(page)
            if total is None:
                total = count
                logger.info(f"Total decisions: {total}")

            if not decisions:
                break

            new_count = 0
            for d in decisions:
                uuid = d.get("uuid")
                if uuid and uuid not in seen_uuids:
                    seen_uuids.add(uuid)
                    new_count += 1
                    yield d

            logger.info(f"Page {page}: {new_count} new decisions (total seen: {len(seen_uuids)})")

            if total and len(seen_uuids) >= total:
                break

            page += 1

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Fetch decisions published since a given date."""
        page = 1
        seen_uuids = set()

        while True:
            decisions, _ = self._fetch_search_page(page)
            if not decisions:
                break

            found_old = False
            for d in decisions:
                uuid = d.get("uuid")
                if uuid and uuid not in seen_uuids:
                    pub_date = parse_tr_date(d.get("publication_date", ""))
                    if pub_date:
                        try:
                            dt = datetime.strptime(pub_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                            if dt < since:
                                found_old = True
                                continue
                        except ValueError:
                            pass
                    seen_uuids.add(uuid)
                    yield d

            if found_old:
                break
            page += 1

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw decision metadata into standard schema, fetching PDF text."""
        uuid = raw.get("uuid", "")
        title = raw.get("title", "").strip()
        decision_number = raw.get("decision_number", "").strip()
        decision_date = parse_tr_date(raw.get("decision_date", ""))
        publication_date = parse_tr_date(raw.get("publication_date", ""))
        decision_type = raw.get("decision_type", "")
        decision_type_tr = raw.get("decision_type_tr", "")

        if not uuid or not title:
            return None

        doc_id = decision_number if decision_number else f"RK-{uuid[:12]}"
        doc_id = re.sub(r'[^a-zA-Z0-9._-]', '_', doc_id)

        # Extract full text from PDF
        text = self._extract_pdf_text(uuid, doc_id)
        if not text or len(text) < 50:
            logger.warning(f"Insufficient text for {doc_id}: {len(text) if text else 0} chars")
            return None

        record = {
            "_id": doc_id,
            "_source": "TR/RekabetKurumu",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": decision_date or publication_date,
            "url": f"{BASE_URL}/Karar?kararId={uuid}",
            "decision_number": decision_number,
            "decision_date": decision_date,
            "publication_date": publication_date,
            "decision_type": decision_type,
            "decision_type_original": decision_type_tr,
            "jurisdiction": "TR",
            "language": "tr",
        }

        return record

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Rekabet Kurumu search page...")

        decisions, total = self._fetch_search_page(1)
        if not decisions:
            print("FAILED: No decisions returned from search page")
            return

        print(f"Total decisions in database: {total}")
        print(f"Decisions on page 1: {len(decisions)}")

        for d in decisions[:3]:
            print(f"\n  UUID: {d.get('uuid', '')[:20]}...")
            print(f"  Number: {d.get('decision_number', '')}")
            print(f"  Title: {d.get('title', '')[:80]}")
            print(f"  Decision Date: {d.get('decision_date', '')}")
            print(f"  Publication Date: {d.get('publication_date', '')}")
            print(f"  Type: {d.get('decision_type', '')}")

        # Test PDF extraction
        d = decisions[0]
        uuid = d.get("uuid", "")
        doc_id = d.get("decision_number", f"test-{uuid[:8]}")
        doc_id = re.sub(r'[^a-zA-Z0-9._-]', '_', doc_id)
        print(f"\nTesting PDF extraction for {doc_id}...")
        text = self._extract_pdf_text(uuid, doc_id)
        if text:
            print(f"  Extracted {len(text)} chars")
            print(f"  Sample: {text[:200]}...")
        else:
            print("  FAILED: No text extracted from PDF")

        print("\nTest complete!")


def main():
    scraper = RekabetKurumuScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, {stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
