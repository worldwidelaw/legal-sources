#!/usr/bin/env python3
"""
IT/CassazioneCivile -- Italian Supreme Court (Corte di Cassazione) Data Fetcher

Fetches Italian Supreme Court case law from the SentenzeWeb Solr API.

API Details:
  - Base URL: https://www.italgiure.giustizia.it/sncass/
  - Solr endpoint: /isapi/hc.dll/sn.solr/sn-collection/select
  - Authentication: None required (public access)
  - Coverage: 1.87M+ documents total
    - Civil cases (snciv): 186,000+
    - Criminal cases (snpen): 237,000+
    - Civil/Penal registry (sic): 1.4M+

Data Fields:
  - id: Unique document ID
  - ocr: Full text (OCR extracted from PDF)
  - kind: Document type (snciv=civil, snpen=criminal)
  - numdec: Decision number
  - anno: Year
  - datdep: Date of deposit (YYYYMMDD)
  - datdec: Date of decision (YYYYMMDD)
  - tipoprov: Type (Sentenza, Ordinanza)
  - szdec: Section number
  - presidente: President judge
  - relatore: Reporting judge
  - materia: Subject matter

License: Open Government Data (Italian IODL)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urlencode

try:
    import requests
except ImportError:
    print("ERROR: requests is required. Install with: pip install requests")
    sys.exit(1)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.cassazione")

# API Configuration
BASE_URL = "https://www.italgiure.giustizia.it"
SOLR_ENDPOINT = "/sncass/isapi/hc.dll/sn.solr/sn-collection/select"

# Solr fields to retrieve
SOLR_FIELDS = [
    "id", "ocr", "kind", "numdec", "anno", "datdep", "datdec",
    "tipoprov", "szdec", "presidente", "relatore", "materia",
    "filename", "ocrdis", "ssz"
]

# Document types to fetch
DOC_TYPES = ["snciv", "snpen"]  # Civil and Criminal decisions

# Pagination
PAGE_SIZE = 50  # Conservative to avoid timeouts


class CassazioneCivileScraper(BaseScraper):
    """
    Scraper for IT/CassazioneCivile -- Italian Supreme Court Case Law.
    Country: IT
    URL: https://www.cortedicassazione.it

    Data types: case_law
    Auth: none (Open access via SentenzeWeb)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        # Session for Solr queries
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json",
        })
        # SSL verification disabled due to certificate issues on italgiure.giustizia.it
        self._session.verify = False

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 LegalDataHunter/1.0",
                "Accept": "application/json",
            },
            timeout=60,
        )

    def _solr_query(self, query: str, start: int = 0, rows: int = PAGE_SIZE,
                    sort: str = "pd desc") -> Dict[str, Any]:
        """
        Execute a Solr query and return the response.

        Returns dict with 'numFound', 'start', and 'docs' keys.
        """
        params = {
            "q": query,
            "start": start,
            "rows": rows,
            "wt": "json",
            "fl": ",".join(SOLR_FIELDS),
            "sort": sort,
        }

        url = f"{BASE_URL}{SOLR_ENDPOINT}?{urlencode(params)}"

        try:
            self.rate_limiter.wait()
            resp = self._session.get(url, timeout=60)
            resp.raise_for_status()

            data = resp.json()
            return data.get("response", {"numFound": 0, "start": 0, "docs": []})

        except requests.exceptions.Timeout:
            logger.warning(f"Solr query timeout: start={start}")
            return {"numFound": 0, "start": 0, "docs": []}
        except Exception as e:
            logger.error(f"Solr query failed: {e}")
            return {"numFound": 0, "start": 0, "docs": []}

    def _clean_text(self, text: str) -> str:
        """Clean and normalize OCR text."""
        if not text:
            return ""

        # Decode HTML entities
        text = html.unescape(text)

        # Remove excessive whitespace but preserve paragraph breaks
        text = re.sub(r' +', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)

        # Remove common OCR artifacts
        text = re.sub(r'[^\S\n]+', ' ', text)  # Normalize whitespace except newlines

        return text.strip()

    def _format_date(self, date_str: str) -> Optional[str]:
        """
        Convert date from YYYYMMDD to ISO 8601 format.
        Returns None if date is invalid.
        """
        if not date_str or len(date_str) < 8:
            return None

        try:
            # Handle both YYYYMMDD and YYYY-MM-DD formats
            if '-' in date_str:
                dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
            else:
                dt = datetime.strptime(date_str[:8], "%Y%m%d")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return None

    def _get_list_value(self, value: Any) -> str:
        """Extract string from list or return string directly."""
        if isinstance(value, list):
            return value[0] if value else ""
        return str(value) if value else ""

    def _build_ecli(self, doc: Dict[str, Any]) -> str:
        """
        Build ECLI identifier for Italian Cassation decisions.
        Format: ECLI:IT:CASS:{YEAR}:{ID}
        """
        year = doc.get("anno", "")
        doc_id = doc.get("id", "")

        if year and doc_id:
            return f"ECLI:IT:CASS:{year}:{doc_id}"
        return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all case law documents from the Court of Cassation.

        Iterates through civil and criminal cases using Solr pagination.
        """
        for doc_type in DOC_TYPES:
            type_name = "civil" if doc_type == "snciv" else "criminal"
            logger.info(f"Fetching {type_name} decisions (kind:{doc_type})...")

            query = f"kind:{doc_type}"

            # Get total count
            result = self._solr_query(query, start=0, rows=1)
            total = result.get("numFound", 0)
            logger.info(f"Total {type_name} documents: {total:,}")

            # Paginate through results
            start = 0
            while start < total:
                result = self._solr_query(query, start=start, rows=PAGE_SIZE)
                docs = result.get("docs", [])

                if not docs:
                    logger.warning(f"No documents returned at offset {start}")
                    break

                for doc in docs:
                    # Skip if no full text
                    ocr = self._get_list_value(doc.get("ocr", ""))
                    if not ocr or len(ocr) < 100:
                        continue

                    yield doc

                start += len(docs)

                if start % 500 == 0:
                    logger.info(f"Progress: {start:,}/{total:,} ({100*start/total:.1f}%)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents deposited since the given date.

        Uses Solr range query on 'pd' field (deposit date in YYYYMMDD format).
        """
        since_str = since.strftime("%Y%m%d")
        now_str = datetime.now().strftime("%Y%m%d")

        for doc_type in DOC_TYPES:
            type_name = "civil" if doc_type == "snciv" else "criminal"
            logger.info(f"Fetching {type_name} updates since {since_str}...")

            # Range query on deposit date
            query = f"kind:{doc_type} AND pd:[{since_str} TO {now_str}]"

            # Get total count
            result = self._solr_query(query, start=0, rows=1)
            total = result.get("numFound", 0)
            logger.info(f"Found {total:,} {type_name} updates")

            # Paginate
            start = 0
            while start < total:
                result = self._solr_query(query, start=start, rows=PAGE_SIZE)
                docs = result.get("docs", [])

                if not docs:
                    break

                for doc in docs:
                    ocr = self._get_list_value(doc.get("ocr", ""))
                    if not ocr or len(ocr) < 100:
                        continue
                    yield doc

                start += len(docs)

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw Solr document into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        doc_id = raw.get("id", "")
        kind = raw.get("kind", "")

        # Extract and clean text
        ocr = self._get_list_value(raw.get("ocr", ""))
        full_text = self._clean_text(ocr)

        # Get metadata
        numdec = raw.get("numdec", "")
        anno = raw.get("anno", "")
        tipoprov = raw.get("tipoprov", "")
        szdec = raw.get("szdec", "")

        # Dates
        datdep = self._get_list_value(raw.get("datdep", ""))
        datdec = raw.get("datdec", "")
        date_deposit = self._format_date(datdep)
        date_decision = self._format_date(datdec)

        # Judges
        presidente = self._get_list_value(raw.get("presidente", ""))
        relatore = self._get_list_value(raw.get("relatore", ""))

        # Subject matter
        materia = self._get_list_value(raw.get("materia", ""))

        # Build title
        case_type = "Civile" if kind == "snciv" else "Penale"
        title = f"Cassazione {case_type}, {tipoprov} n. {numdec}/{anno}"
        if materia:
            title += f" - {materia}"

        # Build URL
        filename = self._get_list_value(raw.get("filename", ""))
        if filename:
            # Convert ./20210722/snciv@s50@a2021@n21018@tO.pdf to full URL
            url = f"https://www.italgiure.giustizia.it/sncass/{filename.lstrip('./')}"
        else:
            url = f"https://www.cortedicassazione.it"

        # Build ECLI
        ecli = self._build_ecli(raw)

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "IT/CassazioneCivile",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_deposit or date_decision or "",
            "url": url,
            # Case metadata
            "ecli": ecli,
            "court": "Corte Suprema di Cassazione",
            "jurisdiction": "IT",
            "case_type": case_type.lower(),
            "decision_type": tipoprov,
            "decision_number": numdec,
            "section": szdec,
            "year": anno,
            # Dates
            "date_deposit": date_deposit,
            "date_decision": date_decision,
            # Judges
            "president": presidente,
            "reporter": relatore,
            # Subject
            "subject_matter": materia,
            # Language
            "language": "it",
        }

    def test_connection(self):
        """Quick connectivity test."""
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        print("Testing Court of Cassation SentenzeWeb API...")

        # Test Solr connection
        print("\n1. Testing Solr endpoint...")
        try:
            result = self._solr_query("*:*", start=0, rows=1)
            total = result.get("numFound", 0)
            print(f"   Total documents: {total:,}")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        # Test civil cases
        print("\n2. Testing civil cases (snciv)...")
        try:
            result = self._solr_query("kind:snciv", start=0, rows=2)
            total = result.get("numFound", 0)
            print(f"   Civil cases: {total:,}")

            docs = result.get("docs", [])
            if docs:
                doc = docs[0]
                ocr = self._get_list_value(doc.get("ocr", ""))
                print(f"   Sample ID: {doc.get('id')}")
                print(f"   Type: {doc.get('tipoprov')}")
                print(f"   Text length: {len(ocr):,} chars")
                if ocr:
                    print(f"   Text preview: {ocr[:200]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test criminal cases
        print("\n3. Testing criminal cases (snpen)...")
        try:
            result = self._solr_query("kind:snpen", start=0, rows=1)
            total = result.get("numFound", 0)
            print(f"   Criminal cases: {total:,}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test with full text
        print("\n4. Testing full document fetch...")
        try:
            result = self._solr_query("kind:snciv", start=0, rows=1)
            docs = result.get("docs", [])
            if docs:
                raw_doc = docs[0]
                normalized = self.normalize(raw_doc)
                print(f"   Title: {normalized['title']}")
                print(f"   Date: {normalized['date']}")
                print(f"   Text length: {len(normalized['text']):,} chars")
                print(f"   ECLI: {normalized['ecli']}")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    scraper = CassazioneCivileScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
