#!/usr/bin/env python3
"""
INTL/UN-ModelTaxConvention -- UN Tax Committee Documents (E/C.18)

Fetches documents from the UN Committee of Experts on International
Cooperation in Tax Matters via the UN Digital Library + ODS.

Strategy:
  - Search digitallibrary.un.org Invenio JSON API for E/C.18 documents
  - Extract document symbol from MARC-style filenames
  - Download DOCX from documents.un.org/api/symbol/access?t=doc
  - Extract text from DOCX (stdlib zipfile — no external deps)
  - Fall back to PDF text extraction via common/pdf_extract if DOCX fails

Data: ~190 documents. No auth required.
Rate limit: 1 req/sec to documents.un.org, 2 req/sec to digitallibrary.un.org.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import io
import re
import json
import time
import logging
import zipfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests as _requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.UN-ModelTaxConvention")


def _make_session() -> _requests.Session:
    """Create a requests session that works with UN Digital Library.

    The UN server returns 202 if it detects default Python requests headers.
    We clear all defaults and use minimal headers.
    """
    s = _requests.Session()
    s.headers.clear()
    s.headers["User-Agent"] = "curl/8.7.1"
    s.headers["Accept"] = "*/*"
    retry = Retry(total=3, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


SEARCH_API = "https://digitallibrary.un.org/search"
ODS_API = "https://documents.un.org/api/symbol/access"
PAGE_SIZE = 50


def extract_symbol_from_filename(filename: str) -> Optional[str]:
    """Extract UN document symbol from PDF filename.

    Examples:
      E_C.18_2025_2-EN.pdf -> E/C.18/2025/2
      E_C.18_2024_CRP.41-EN.pdf -> E/C.18/2024/CRP.41
      E_2026_45--E_C.18_2025_5-EN.pdf -> E/C.18/2025/5
    """
    # Handle double-symbol filenames (E_2026_45--E_C.18_2025_5-EN.pdf)
    # Take the E/C.18 part
    if "--" in filename:
        parts = filename.split("--")
        for part in parts:
            if "C.18" in part:
                filename = part + ".pdf" if not part.endswith(".pdf") else part
                break

    m = re.match(r"(E_C\.18_.+?)-(?:AR|ZH|EN|FR|RU|ES)\.pdf", filename)
    if not m:
        # Try broader pattern for E/YYYY/NN style (session reports)
        m = re.match(r"(E_\d{4}_\d+.*?)-(?:AR|ZH|EN|FR|RU|ES)\.pdf", filename)
        if not m:
            return None
    raw = m.group(1)
    # Clean up suffixes
    raw = re.sub(r"\^[A-Z]\^--.*", "", raw)
    parts = raw.split("_")
    return "/".join(parts)


def extract_text_from_docx(data: bytes) -> Optional[str]:
    """Extract plain text from a DOCX file using stdlib only."""
    if len(data) < 4 or data[:2] != b"PK":
        return None
    try:
        z = zipfile.ZipFile(io.BytesIO(data))
        if "word/document.xml" not in z.namelist():
            return None
        with z.open("word/document.xml") as doc:
            xml_content = doc.read().decode("utf-8", errors="replace")
        text = re.sub(r"<[^>]+>", "", xml_content)
        text = re.sub(r"\s+", " ", text).strip()
        return text if len(text) > 50 else None
    except (zipfile.BadZipFile, KeyError, Exception) as e:
        logger.debug(f"DOCX extraction failed: {e}")
        return None


class UNModelTaxScraper(BaseScraper):
    """
    Scraper for INTL/UN-ModelTaxConvention -- UN Tax Committee Documents.
    Country: INTL
    URL: https://digitallibrary.un.org

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = _make_session()

    def _search_documents(self, offset: int = 1, count: int = PAGE_SIZE) -> list:
        """Search for E/C.18 tax committee documents via Invenio JSON API."""
        params = {
            "p": '"E/C.18"',
            "f": "191__a",
            "of": "recjson",
            "rg": str(count),
            "jrec": str(offset),
            "sf": "year",
            "so": "d",
        }

        resp = self.session.get(SEARCH_API, params=params, timeout=60)
        if resp is None or resp.status_code != 200:
            logger.error(f"Search failed at offset {offset}: {resp.status_code if resp else 'None'}")
            return []

        try:
            data = resp.json()
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.error(f"JSON parse error: {e}")
            return []

    def _download_docx(self, symbol: str) -> Optional[bytes]:
        """Download DOCX from UN ODS for the given document symbol."""
        params = {"s": symbol, "l": "en", "t": "doc"}
        try:
            resp = self.session.get(ODS_API, params=params, timeout=120, allow_redirects=True)
            if resp is None or resp.status_code != 200:
                return None
            return resp.content
        except Exception as e:
            logger.debug(f"DOCX download failed for {symbol}: {e}")
            return None

    def _download_pdf_text(self, symbol: str) -> Optional[str]:
        """Download PDF from ODS and extract text as fallback."""
        params = {"s": symbol, "l": "en"}
        try:
            resp = self.session.get(ODS_API, params=params, timeout=120, allow_redirects=True)
            if resp is None or resp.status_code != 200:
                return None
            # Try common pdf_extract
            try:
                from common.pdf_extract import extract_pdf_markdown
                text = extract_pdf_markdown(resp.content)
                return text if text and len(text) > 50 else None
            except ImportError:
                logger.debug("pdf_extract not available for PDF fallback")
                return None
        except Exception as e:
            logger.debug(f"PDF download failed for {symbol}: {e}")
            return None

    def _extract_record_info(self, rec: dict) -> Optional[dict]:
        """Extract key info from a search result record."""
        recid = rec.get("recid")
        if not recid:
            return None

        # Find the document symbol from filenames or other fields
        filenames = rec.get("filenames", [])
        symbol = None

        # Try English PDF filename first
        for fn in filenames:
            if "-EN" in fn and fn.endswith(".pdf") and "C.18" in fn:
                symbol = extract_symbol_from_filename(fn)
                if symbol:
                    break

        # If no C.18-specific file, try any English PDF
        if not symbol:
            for fn in filenames:
                if "-EN" in fn and fn.endswith(".pdf"):
                    symbol = extract_symbol_from_filename(fn)
                    if symbol:
                        break

        # Try to extract symbol from the record's title or other fields
        if not symbol:
            # Check for symbol in 'report_number' or similar fields
            report_num = rec.get("report_number", [])
            if isinstance(report_num, list):
                for rn in report_num:
                    if isinstance(rn, str) and "C.18" in rn:
                        symbol = rn
                        break
                    elif isinstance(rn, dict) and "C.18" in str(rn.get("value", "")):
                        symbol = rn.get("value", "")
                        break

        if not symbol:
            return None

        # Extract date
        date_str = None
        prepub = rec.get("prepublication", {})
        if isinstance(prepub, dict):
            date_str = prepub.get("date") or prepub.get("place")
        imprint = rec.get("imprint", {})
        if not date_str and isinstance(imprint, dict):
            date_str = imprint.get("date", "")

        # Extract title
        title = rec.get("comment", "") or rec.get("title", {})
        if isinstance(title, dict):
            title = title.get("title", "") or str(title)
        if not title:
            title = f"UN Tax Committee Document {symbol}"

        # Extract subjects
        subjects = []
        for s in rec.get("subject", []):
            if isinstance(s, dict) and s.get("term"):
                subjects.append(s["term"])

        # Extract document type
        doc_type = "document"
        doc_types = rec.get("document_type", [])
        if doc_types and isinstance(doc_types, list):
            doc_type = doc_types[0].lower() if doc_types[0] else "document"

        return {
            "recid": recid,
            "symbol": symbol,
            "title": title,
            "date": date_str,
            "subjects": subjects,
            "doc_type": doc_type,
        }

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw record into standard schema."""
        text = raw.get("_text", "")
        if not text or len(text) < 50:
            return None

        symbol = raw.get("symbol", "")
        doc_id = f"UN-TAX-{symbol.replace('/', '-')}" if symbol else f"UN-TAX-{raw.get('recid', 'unknown')}"

        # Parse date to ISO 8601
        date_str = raw.get("date", "")
        if date_str:
            m = re.search(r"(\d{4}-\d{2}-\d{2})", str(date_str))
            if m:
                date_str = m.group(1)
            else:
                m = re.search(r"(\d{4})", str(date_str))
                date_str = m.group(1) if m else None

        return {
            "_id": doc_id,
            "_source": "INTL/UN-ModelTaxConvention",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "title": raw.get("title", f"UN Tax Committee Document {symbol}"),
            "text": text,
            "date": date_str or None,
            "url": f"https://digitallibrary.un.org/record/{raw.get('recid', '')}",
            "body": "Committee of Experts on International Cooperation in Tax Matters",
            "doc_type": raw.get("doc_type", "document"),
            "subjects": raw.get("subjects", []),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all E/C.18 tax committee documents with full text."""
        offset = 1
        total_yielded = 0
        total_skipped = 0
        consecutive_failures = 0

        while True:
            logger.info(f"Searching offset={offset}, yielded={total_yielded}, skipped={total_skipped}")
            results = self._search_documents(offset=offset, count=PAGE_SIZE)

            if not results:
                logger.info(f"No more results at offset {offset}")
                break

            for rec in results:
                info = self._extract_record_info(rec)
                if not info:
                    total_skipped += 1
                    continue

                # Rate limit ODS requests
                time.sleep(1.5)

                # Try DOCX first
                text = None
                docx_data = self._download_docx(info["symbol"])
                if docx_data:
                    text = extract_text_from_docx(docx_data)

                # Fall back to PDF if DOCX failed
                if not text:
                    logger.debug(f"No DOCX text for {info['symbol']}, trying PDF...")
                    time.sleep(1.0)
                    text = self._download_pdf_text(info["symbol"])

                if not text:
                    logger.debug(f"No text for {info['symbol']}")
                    total_skipped += 1
                    consecutive_failures += 1
                    if consecutive_failures > 20:
                        logger.warning("Too many consecutive failures, moving to next page")
                        break
                    continue

                consecutive_failures = 0
                total_yielded += 1
                yield {
                    "recid": info["recid"],
                    "symbol": info["symbol"],
                    "title": info["title"],
                    "date": info["date"],
                    "subjects": info["subjects"],
                    "doc_type": info["doc_type"],
                    "_text": text,
                }

            offset += PAGE_SIZE
            time.sleep(0.5)

        logger.info(f"Done: {total_yielded} documents with text, {total_skipped} skipped")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents from recent years."""
        since_year = since.year
        for raw in self.fetch_all():
            date_str = raw.get("date", "")
            if date_str:
                m = re.search(r"(\d{4})", str(date_str))
                if m and int(m.group(1)) >= since_year:
                    yield raw


# -- CLI ----------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="INTL/UN-ModelTaxConvention Data Fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = UNModelTaxScraper()

    if args.command == "test-api":
        logger.info("Testing UN Digital Library search API for E/C.18 documents...")
        results = scraper._search_documents(offset=1, count=3)
        if results:
            logger.info(f"Search OK: {len(results)} records returned")
            for rec in results:
                filenames = rec.get("filenames", [])
                en = [f for f in filenames if "-EN" in f and f.endswith(".pdf")]
                logger.info(f"  recid={rec.get('recid')} files={en[:2]}")
        else:
            logger.error("Search failed")
            return

        logger.info("\nTesting ODS DOCX endpoint with E/C.18/2025/2...")
        data = scraper._download_docx("E/C.18/2025/2")
        if data:
            text = extract_text_from_docx(data)
            if text:
                logger.info(f"DOCX OK: {len(text)} chars extracted")
                logger.info(f"  Preview: {text[:200]}...")
            else:
                logger.error("DOCX downloaded but text extraction failed")
        else:
            logger.error("DOCX download failed")
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=365)
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
