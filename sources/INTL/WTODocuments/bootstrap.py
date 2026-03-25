#!/usr/bin/env python3
"""
INTL/WTODocuments — WTO Dispute Settlement Documents

Fetches WTO dispute settlement documents (panel reports, Appellate Body reports,
arbitration awards, requests for consultations) from docs.wto.org.

Strategy:
  - Use the XML search API (GetXMLResults.aspx) to discover documents
  - Download PDFs via directdoc.aspx and extract full text with pdfplumber
  - ~10,000 dispute settlement documents

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update --since 2024-01-01
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
import pdfplumber

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.WTODocuments")

XML_API_URL = "https://docs.wto.org/dol2fe/Pages/SS/GetXMLResults.aspx"
DIRECT_DOC_BASE = "https://docs.wto.org/dol2fe/Pages/SS/directdoc.aspx"
NS = {"autn": "http://schemas.autonomy.com/aci/"}

# Document type classification based on symbol patterns
CASE_LAW_PATTERNS = [
    r"/R\b",      # Panel reports
    r"/AB/R",     # Appellate Body reports
    r"/ARB",      # Arbitration awards
    r"/RW",       # Compliance panel reports
    r"/AB/RW",    # Compliance AB reports
]

PAGE_SIZE = 100  # Max results per XML API page


class WTODocumentsScraper(BaseScraper):
    """
    Scraper for INTL/WTODocuments — WTO Documents Online.
    Country: INTL
    URL: https://docs.wto.org/
    Data types: case_law, legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/xml,application/xml,*/*;q=0.8",
        })

    def _classify_document(self, symbol: str) -> str:
        """Classify a WTO document as case_law or legislation based on its symbol."""
        for pattern in CASE_LAW_PATTERNS:
            if re.search(pattern, symbol):
                return "case_law"
        if "/DS" in symbol:
            return "case_law"
        return "legislation"

    def _extract_text_from_pdf(self, pdf_content: bytes) -> str:
        """Extract text from PDF bytes using pdfplumber."""
        try:
            with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                pages_text = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
                return "\n\n".join(pages_text)
        except Exception as e:
            logger.warning(f"PDF text extraction failed: {e}")
            return ""

    def _download_pdf(self, url: str, retries: int = 3) -> Optional[bytes]:
        """Download a PDF from the given URL with retries."""
        for attempt in range(retries):
            try:
                time.sleep(2)  # Rate limit
                resp = self.session.get(url, timeout=120)
                if resp.status_code == 200 and len(resp.content) > 1000:
                    content_type = resp.headers.get("Content-Type", "")
                    if "html" in content_type and "not published" in resp.text.lower():
                        logger.debug(f"Document not published: {url}")
                        return None
                    return resp.content
                logger.warning(f"Unexpected response {resp.status_code} for {url} (size={len(resp.content)})")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Download attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(5 * (attempt + 1))
        return None

    def _xml_search(self, query: str, max_hits: int = 0) -> Generator[dict, None, None]:
        """
        Search the WTO XML API and yield parsed document metadata dicts.
        If max_hits=0, fetch all results.
        """
        start = 0
        total = None
        fetched = 0

        while True:
            params = {
                "DataSource": "Cat",
                "query": query,
                "Language": "English",
                "XMLMaxHitsPerPage": str(PAGE_SIZE),
                "XMLStart": str(start),
            }
            try:
                time.sleep(1)
                resp = self.session.get(XML_API_URL, params=params, timeout=60)
                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.error(f"XML API request failed at offset {start}: {e}")
                break

            root = ET.fromstring(resp.text)
            if total is None:
                total_el = root.find(".//autn:totalhits", NS)
                total = int(total_el.text) if total_el is not None else 0
                logger.info(f"XML search total hits: {total}")
                if total == 0:
                    return

            hits = root.findall(".//autn:hit", NS)
            if not hits:
                break

            for hit in hits:
                doc = self._parse_xml_hit(hit)
                if doc:
                    yield doc
                    fetched += 1
                    if max_hits > 0 and fetched >= max_hits:
                        return

            start += len(hits)
            if start >= total:
                break

            if start % 500 == 0:
                logger.info(f"Fetched {start}/{total} metadata records")

    def _parse_xml_hit(self, hit) -> Optional[dict]:
        """Parse a single XML hit element into a metadata dict."""
        title_el = hit.find("autn:title", NS)
        title = title_el.text if title_el is not None else ""

        doc_el = hit.find("autn:content/DOCUMENT", NS)
        if doc_el is None:
            # Try without namespace
            doc_el = hit.find(".//DOCUMENT")
        if doc_el is None:
            return None

        symbol_el = doc_el.find("SYMBOL")
        if symbol_el is None:
            # Try CATTITLE or title for symbol
            cat_title = doc_el.find("CATTITLE")
            symbol = ""
        else:
            symbol = (symbol_el.text or "").strip()

        # Extract primary symbol (first one if multiple separated by #)
        if "#" in symbol:
            symbol = symbol.split("#")[0].strip()

        if not symbol:
            return None

        # Get PDF path
        pdf_path_el = doc_el.find("FILENAMESA")
        pdf_path = (pdf_path_el.text or "").strip() if pdf_path_el is not None else ""

        pdf_url = None
        if pdf_path:
            pdf_url = f"{DIRECT_DOC_BASE}?filename={pdf_path}&Open=True"

        # Get date
        date_el = doc_el.find("ISSUINGDATE")
        date_str = None
        if date_el is not None and date_el.text:
            try:
                dt = datetime.strptime(date_el.text.strip().split(" ")[0], "%d/%m/%Y")
                date_str = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Get other metadata
        bodies_el = doc_el.find("BODIES")
        bodies = (bodies_el.text or "").strip() if bodies_el is not None else ""

        types_el = doc_el.find("TYPES")
        doc_types = (types_el.text or "").strip() if types_el is not None else ""

        countries_el = doc_el.find("CONCERNEDCOUNTRIES")
        countries = (countries_el.text or "").strip() if countries_el is not None else ""

        subjects_el = doc_el.find("SUBJECTLIST")
        subjects = (subjects_el.text or "").strip() if subjects_el is not None else ""

        contents_el = doc_el.find("CONTENTS")
        contents = (contents_el.text or "").strip() if contents_el is not None else ""

        restriction_el = doc_el.find("RESTRICTIONTYPENAME")
        restriction = (restriction_el.text or "").strip() if restriction_el is not None else ""

        cat_title_el = doc_el.find("CATTITLE")
        cat_title = (cat_title_el.text or "").strip() if cat_title_el is not None else ""

        return {
            "symbol": symbol,
            "title": cat_title or title or symbol,
            "date": date_str,
            "pdf_url": pdf_url,
            "bodies": bodies,
            "doc_types": doc_types,
            "countries": countries,
            "subjects": subjects,
            "contents": contents,
            "restriction": restriction,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all WTO dispute settlement documents."""
        yield from self._xml_search("@Symbol= WT/DS*")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch documents modified since the given date."""
        date_str = since.strftime("%d/%m/%Y")
        yield from self._xml_search(f"(@Symbol= WT/DS*) AND (@meta_Date>={date_str})")

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw search result into standardized schema."""
        symbol = raw.get("symbol", "")
        if not symbol:
            return None

        # Download PDF and extract text
        text = ""
        pdf_url = raw.get("pdf_url")
        if pdf_url:
            pdf_content = self._download_pdf(pdf_url)
            if pdf_content:
                text = self._extract_text_from_pdf(pdf_content)

        if not text or len(text) < 50:
            logger.debug(f"No/short text for {symbol}, skipping")
            return None

        doc_type = self._classify_document(symbol)

        return {
            "_id": f"WTO-{symbol.replace('/', '-')}",
            "_source": "INTL/WTODocuments",
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", symbol),
            "text": text,
            "date": raw.get("date"),
            "url": pdf_url or "https://docs.wto.org/",
            "document_symbol": symbol,
            "bodies": raw.get("bodies", ""),
            "countries": raw.get("countries", ""),
            "subjects": raw.get("subjects", ""),
        }

    # ── CLI ────────────────────────────────────────────────────────────

    def run_sample(self, count: int = 15):
        """Fetch sample records for validation."""
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(parents=True, exist_ok=True)

        records = []
        seen_ids = set()

        # Search for panel reports first (most substantial text)
        queries = [
            ("(@Symbol= WT/DS*/R) AND (@Types= Report*)", "panel reports"),
            ("@Symbol= WT/DS*/AB/R", "AB reports"),
            ("@Symbol= WT/DS*/1", "consultation requests"),
        ]

        for query, label in queries:
            if len(records) >= count:
                break
            logger.info(f"Searching {label}...")
            for raw in self._xml_search(query, max_hits=count * 2):
                if len(records) >= count:
                    break
                _id = f"WTO-{raw['symbol'].replace('/', '-')}"
                if _id in seen_ids:
                    continue
                seen_ids.add(_id)

                record = self.normalize(raw)
                if record and len(record.get("text", "")) > 200:
                    records.append(record)
                    logger.info(f"Sample {len(records)}/{count}: {raw['symbol']} ({len(record['text'])} chars)")

        # Save samples
        for i, record in enumerate(records):
            path = sample_dir / f"record_{i:04d}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(record, f, indent=2, ensure_ascii=False)

        all_path = sample_dir / "all_samples.json"
        with open(all_path, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, ensure_ascii=False)

        return records

    def run_test(self):
        """Quick connectivity test."""
        logger.info("Testing WTO Documents Online connectivity...")

        # Test XML API
        for raw in self._xml_search("@Symbol= WT/DS2/R", max_hits=1):
            logger.info(f"XML API test: found {raw['symbol']} - {raw['title']}")

            # Test PDF download
            if raw.get("pdf_url"):
                pdf_content = self._download_pdf(raw["pdf_url"])
                if pdf_content:
                    text = self._extract_text_from_pdf(pdf_content)
                    logger.info(f"PDF test: {len(pdf_content)} bytes, {len(text)} chars text")
                    return True
                else:
                    logger.error("PDF download test FAILED")
                    return False

        logger.error("XML API search returned no results")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="INTL/WTODocuments data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample records only")
    parser.add_argument("--count", type=int, default=15, help="Number of sample records")
    parser.add_argument("--since", type=str, help="Fetch updates since date (YYYY-MM-DD)")

    args = parser.parse_args()
    scraper = WTODocumentsScraper()

    if args.command == "test":
        success = scraper.run_test()
        return 0 if success else 1

    if args.command == "bootstrap":
        if args.sample:
            records = scraper.run_sample(args.count)
            logger.info(f"\nFetched {len(records)} sample records")

            texts = [r for r in records if len(r.get("text", "")) > 100]
            logger.info(f"Records with substantial text: {len(texts)}/{len(records)}")
            if texts:
                avg_len = sum(len(r["text"]) for r in texts) / len(texts)
                logger.info(f"Average text length: {avg_len:,.0f} chars")

            if len(records) >= 10 and len(texts) >= 10:
                logger.info("VALIDATION PASSED")
                return 0
            else:
                logger.error("VALIDATION FAILED - not enough records with text")
                return 1
        else:
            stats = scraper.bootstrap()
            logger.info(f"Bootstrap complete: {stats}")
            return 0

    if args.command == "update":
        if not args.since:
            logger.error("--since required for update")
            return 1
        since = datetime.fromisoformat(args.since)
        stats = scraper.bootstrap()
        logger.info(f"Update complete: {stats}")
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
