#!/usr/bin/env python3
"""
UN/DigitalLibrary -- UN General Assembly Resolutions

Fetches GA resolutions with full text from the UN Digital Library + ODS.

Strategy:
  - Search digitallibrary.un.org Invenio JSON API for GA resolutions (field 191:A/RES/)
  - Extract document symbol from PDF filenames
  - Download DOCX from documents.un.org/api/symbol/access?t=doc
  - Extract text from DOCX (stdlib zipfile — no external deps)
  - Older documents (pre-2015) may be OLE2 .doc format — skipped

Data: ~21,000+ GA resolutions. No auth required.
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
from xml.etree import ElementTree

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
logger = logging.getLogger("legal-data-hunter.UN.DigitalLibrary")


def _make_session() -> _requests.Session:
    """Create a requests session that works with UN Digital Library.

    The UN server returns 202 if it detects default Python requests headers
    (Accept-Encoding, Connection). We clear all defaults and use minimal headers.
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
PAGE_SIZE = 50  # Records per search page


def extract_symbol_from_filename(filename: str) -> Optional[str]:
    """Extract UN document symbol from PDF filename.

    Examples:
      A_RES_78_199-EN.pdf -> A/RES/78/199
      A_RES_ES-11_10-EN.pdf -> A/RES/ES-11/10
      A_RES_80_236_A-B-EN.pdf -> A/RES/80/236/A-B
    """
    m = re.match(r"(A_RES_.+?)-(?:AR|ZH|EN|FR|RU|ES)\.pdf", filename)
    if not m:
        return None
    raw = m.group(1)
    # Remove ^X^-- suffixes (agenda item variants)
    raw = re.sub(r"\^[A-Z]\^--.*", "", raw)
    parts = raw.split("_")
    return "/".join(parts)


def extract_text_from_docx(data: bytes) -> Optional[str]:
    """Extract plain text from a DOCX file using stdlib only.

    Returns None if the data is not a valid DOCX (ZIP) file.
    """
    if len(data) < 4 or data[:2] != b"PK":
        return None
    try:
        z = zipfile.ZipFile(io.BytesIO(data))
        if "word/document.xml" not in z.namelist():
            return None
        with z.open("word/document.xml") as doc:
            xml_content = doc.read().decode("utf-8", errors="replace")
        # Strip XML tags to get text
        text = re.sub(r"<[^>]+>", "", xml_content)
        text = re.sub(r"\s+", " ", text).strip()
        return text if len(text) > 50 else None
    except (zipfile.BadZipFile, KeyError, Exception) as e:
        logger.debug(f"DOCX extraction failed: {e}")
        return None


class DigitalLibraryScraper(BaseScraper):
    """
    Scraper for UN/DigitalLibrary -- UN General Assembly Resolutions.
    Country: UN
    URL: https://digitallibrary.un.org

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = _make_session()

    def _search_resolutions(self, offset: int = 1, count: int = PAGE_SIZE,
                            sort_desc: bool = True) -> list:
        """Search for GA resolutions via Invenio JSON API."""
        params = {
            "p": "191:A/RES/",
            "of": "recjson",
            "rg": str(count),
            "jrec": str(offset),
        }
        if sort_desc:
            params["sf"] = "year"
            params["so"] = "d"

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
        resp = self.session.get(ODS_API, params=params, timeout=120, allow_redirects=True)
        if resp is None or resp.status_code != 200:
            return None
        return resp.content

    def _extract_record_info(self, rec: dict) -> Optional[dict]:
        """Extract key info from a search result record."""
        recid = rec.get("recid")
        if not recid:
            return None

        # Find English PDF filename to derive symbol
        filenames = rec.get("filenames", [])
        symbol = None
        for fn in filenames:
            if "-EN" in fn and fn.endswith(".pdf"):
                symbol = extract_symbol_from_filename(fn)
                if symbol:
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

        # Extract title from comment or construct from symbol
        title = rec.get("comment", "") or f"General Assembly Resolution {symbol}"

        # Extract subjects
        subjects = []
        for s in rec.get("subject", []):
            if isinstance(s, dict) and s.get("term"):
                subjects.append(s["term"])

        return {
            "recid": recid,
            "symbol": symbol,
            "title": title,
            "date": date_str,
            "subjects": subjects,
        }

    # -- Normalize ----------------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw record into standard schema."""
        text = raw.get("_text", "")
        if not text or len(text) < 50:
            return None

        symbol = raw.get("symbol", "")
        doc_id = f"UN-GA-{symbol.replace('/', '-')}" if symbol else f"UN-GA-{raw.get('recid', 'unknown')}"

        # Parse date to ISO 8601
        date_str = raw.get("date", "")
        if date_str:
            # Try to extract YYYY-MM-DD pattern
            m = re.search(r"(\d{4}-\d{2}-\d{2})", str(date_str))
            if m:
                date_str = m.group(1)
            else:
                # Try to extract just year
                m = re.search(r"(\d{4})", str(date_str))
                date_str = m.group(1) if m else None

        return {
            "_id": doc_id,
            "_source": "UN/DigitalLibrary",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "title": raw.get("title", f"GA Resolution {symbol}"),
            "text": text,
            "date": date_str or None,
            "url": f"https://digitallibrary.un.org/record/{raw.get('recid', '')}",
            "body": "General Assembly",
            "doc_type": "resolution",
            "subjects": raw.get("subjects", []),
        }

    # -- Fetch methods ------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all GA resolutions with full text (DOCX extraction)."""
        offset = 1
        total_yielded = 0
        total_skipped = 0
        consecutive_failures = 0

        while True:
            logger.info(f"Searching offset={offset}, yielded={total_yielded}, skipped={total_skipped}")
            results = self._search_resolutions(offset=offset, count=PAGE_SIZE)

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

                # Download DOCX
                docx_data = self._download_docx(info["symbol"])
                if not docx_data:
                    logger.debug(f"No DOCX for {info['symbol']}")
                    total_skipped += 1
                    consecutive_failures += 1
                    if consecutive_failures > 20:
                        logger.warning("Too many consecutive DOCX failures, moving to next page")
                        break
                    continue

                # Extract text
                text = extract_text_from_docx(docx_data)
                if not text:
                    logger.debug(f"DOCX not parseable for {info['symbol']} (likely OLE2 .doc)")
                    total_skipped += 1
                    continue

                consecutive_failures = 0
                total_yielded += 1
                yield {
                    "recid": info["recid"],
                    "symbol": info["symbol"],
                    "title": info["title"],
                    "date": info["date"],
                    "subjects": info["subjects"],
                    "_text": text,
                }

            offset += PAGE_SIZE

            # Rate limit between search pages
            time.sleep(0.5)

        logger.info(f"Done: {total_yielded} resolutions with text, {total_skipped} skipped")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield resolutions from recent sessions."""
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

    parser = argparse.ArgumentParser(description="UN/DigitalLibrary Data Fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    args = parser.parse_args()

    scraper = DigitalLibraryScraper()

    if args.command == "test-api":
        logger.info("Testing UN Digital Library search API...")
        results = scraper._search_resolutions(offset=1, count=2)
        if results:
            logger.info(f"Search OK: {len(results)} records returned")
            for rec in results:
                filenames = rec.get("filenames", [])
                en = [f for f in filenames if "-EN" in f and f.endswith(".pdf")]
                logger.info(f"  recid={rec.get('recid')} files={en[:1]}")
        else:
            logger.error("Search failed")
            return

        logger.info("\nTesting ODS DOCX endpoint...")
        data = scraper._download_docx("A/RES/78/1")
        if data:
            text = extract_text_from_docx(data)
            if text:
                logger.info(f"DOCX OK: {len(text)} chars extracted")
                logger.info(f"  Preview: {text[:150]}...")
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
