#!/usr/bin/env python3
"""
FK/Legislation -- Falkland Islands Legislation

Fetches legislation from the official Falkland Islands Government portal
(legislation.gov.fk), powered by TeraText for Legislation (TTFL).

Uses the public JSON API:
  - TTFL-BrowseDataSource: lists all documents by DocType
  - TTFL-FragViewDataSource: fetches full text HTML fragments per document

Document types: fiord (Ordinances), fisl (Subsidiary Legislation), fiproc (Proclamations)

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 12+ sample records
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import re
import logging
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FK.Legislation")

BASE_URL = "https://www.legislation.gov.fk"
GETDATA_URL = f"{BASE_URL}/getdata"

DOC_TYPES = [
    ("fiord", "Ordinance"),
    ("fisl", "Subsidiary Legislation"),
    ("fiproc", "Proclamation"),
]


def _strip_html(html: str) -> str:
    """Strip HTML tags and decode entities, preserving structure."""
    # Replace block-level tags with newlines
    text = re.sub(r'<br\s*/?\s*>', '\n', html, flags=re.IGNORECASE)
    text = re.sub(r'</(?:p|div|h[1-6]|li|tr|td|th|blockquote|section|article)>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<(?:p|div|h[1-6]|li|tr|td|th|blockquote|section|article)[^>]*>', '\n', text, flags=re.IGNORECASE)
    # Remove remaining tags
    text = re.sub(r'<[^>]+>', '', text)
    text = unescape(text)
    # Collapse whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class FalklandsLegislationScraper(BaseScraper):
    """Scraper for FK/Legislation using TeraText JSON API."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = None

    def _get_session(self):
        if self.session is None:
            import requests
            self.session = requests.Session()
            self.session.headers.update({
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json",
            })
        return self.session

    def _api_get(self, params: dict) -> dict:
        """Make a GET request to the TTFL API."""
        self.rate_limiter.wait()
        sess = self._get_session()
        resp = sess.get(GETDATA_URL, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _browse_docs(self, doc_type: str, start: int = 1, count: int = 1000) -> dict:
        """Browse documents of a given type."""
        return self._api_get({
            "ds": "TTFL-BrowseDataSource",
            "collection": "TTFL.toc",
            "subset": "browse",
            "expression": f'DocType="{doc_type}"',
            "start": str(start),
            "count": str(count),
            "sortField": "sort.title",
            "sortDirection": "asc",
        })

    def _get_fragments(self, vs_id: str, vd_id: str) -> List[str]:
        """Fetch full text fragments for a document."""
        data = self._api_get({
            "ds": "TTFL-FragViewDataSource",
            "collection": "TTFL.fragment",
            "subset": "F",
            "expression": f'VersionSeriesId="{vs_id}" AND VersionDescId="{vd_id}"',
            "start": "1",
            "count": "500",
        })
        fragments = []
        rows = data.get("rows", data.get("data", []))
        if isinstance(rows, list):
            for row in rows:
                html = ""
                if isinstance(row, dict):
                    html = row.get("frag.html", row.get("fragHtml", row.get("html", "")))
                elif isinstance(row, str):
                    html = row
                if html:
                    fragments.append(html)
        return fragments

    def _extract_doc_fields(self, row: dict) -> dict:
        """Extract relevant fields from a browse API row."""
        # Handle different possible field name formats
        title = (row.get("title", "") or row.get("Title", "") or "").strip()
        year = row.get("year", "") or row.get("Year", "") or ""
        number = row.get("no", "") or row.get("number", "") or ""
        vs_id = row.get("version.series.id", "") or row.get("versionSeriesId", "") or ""
        vd_id = row.get("version.desc.id", "") or row.get("versionDescId", "") or ""
        doc_type = row.get("doc.type", "") or row.get("docType", "") or ""
        doc_status = row.get("doc.status", "") or row.get("docStatus", "") or ""
        pub_date = row.get("publication.date", "") or row.get("publicationDate", "") or ""
        doc_id = row.get("id", "") or row.get("Id", "") or ""

        return {
            "title": title,
            "year": str(year),
            "number": str(number),
            "vs_id": vs_id,
            "vd_id": vd_id,
            "doc_type": doc_type,
            "doc_status": doc_status,
            "pub_date": pub_date,
            "doc_id": doc_id,
        }

    def normalize(self, raw: dict) -> dict:
        title = raw.get("title", "Unknown")
        year = raw.get("year", "")
        doc_type_code = raw.get("doc_type", "")
        doc_id = raw.get("doc_id", "")

        # Map doc type codes to readable labels
        type_map = {"fiord": "Ordinance", "fisl": "Subsidiary Legislation", "fiproc": "Proclamation",
                     "ukpga": "UK Act", "uksi": "UK Statutory Instrument", "uksro": "UK Statutory Rules"}
        doc_type_label = type_map.get(doc_type_code, doc_type_code)

        # Build URL
        vs_id = raw.get("vs_id", "")
        url = f"{BASE_URL}/#/view/{vs_id}" if vs_id else BASE_URL

        # Build date
        pub_date = raw.get("pub_date", "")
        if pub_date and len(pub_date) >= 8:
            # Might be YYYYMMDD format
            try:
                if len(pub_date) == 8 and pub_date.isdigit():
                    date_str = f"{pub_date[:4]}-{pub_date[4:6]}-{pub_date[6:8]}"
                elif "-" in pub_date:
                    date_str = pub_date[:10]
                else:
                    date_str = f"{year}-01-01" if year else ""
            except Exception:
                date_str = f"{year}-01-01" if year else ""
        elif year:
            date_str = f"{year}-01-01"
        else:
            date_str = ""

        # Stable ID from doc_id or vs_id
        stable_id = doc_id or vs_id or hashlib.md5(title.encode()).hexdigest()[:12]

        return {
            "_id": f"FK/Legislation/{stable_id}",
            "_source": "FK/Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("_prefetched_text", ""),
            "date": date_str,
            "url": url,
            "year": year,
            "number": raw.get("number", ""),
            "doc_type": doc_type_label,
            "doc_status": raw.get("doc_status", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        limit = 15 if sample else None
        count = 0

        for doc_type_code, doc_type_label in DOC_TYPES:
            if limit and count >= limit:
                break

            logger.info(f"Browsing {doc_type_label} (DocType={doc_type_code})...")

            try:
                data = self._browse_docs(doc_type_code, start=1, count=1000)
            except Exception as e:
                logger.error(f"Failed to browse {doc_type_code}: {e}")
                continue

            rows = data.get("rows", data.get("data", []))
            if not rows:
                logger.warning(f"No rows returned for {doc_type_code}")
                continue

            total = len(rows)
            logger.info(f"  Found {total} {doc_type_label} documents")

            # If more than 1000, paginate
            total_count = data.get("totalCount", data.get("total", total))
            if isinstance(total_count, str):
                total_count = int(total_count)
            while len(rows) < total_count:
                try:
                    more = self._browse_docs(doc_type_code, start=len(rows) + 1, count=1000)
                    more_rows = more.get("rows", more.get("data", []))
                    if not more_rows:
                        break
                    rows.extend(more_rows)
                except Exception as e:
                    logger.error(f"Pagination error for {doc_type_code}: {e}")
                    break

            for row in rows:
                if limit and count >= limit:
                    break

                fields = self._extract_doc_fields(row)
                if not fields["vs_id"] or not fields["vd_id"]:
                    logger.warning(f"  Skipping {fields['title'][:50]} - no version IDs")
                    continue

                # Fetch full text fragments
                try:
                    fragments = self._get_fragments(fields["vs_id"], fields["vd_id"])
                except Exception as e:
                    logger.warning(f"  Failed to get fragments for {fields['title'][:50]}: {e}")
                    continue

                if not fragments:
                    logger.warning(f"  No fragments for {fields['title'][:50]}")
                    continue

                # Combine and clean HTML fragments
                full_html = "\n".join(fragments)
                text = _strip_html(full_html)

                if not text or len(text) < 50:
                    logger.warning(f"  Skipping {fields['title'][:50]} - text too short ({len(text)} chars)")
                    continue

                fields["_prefetched_text"] = text
                yield fields
                count += 1
                logger.info(f"  [{count}] {fields['title'][:60]} ({len(text)} chars)")

        logger.info(f"Total records yielded: {count}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        yield from self.fetch_all()


if __name__ == "__main__":
    scraper = FalklandsLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        print("Testing Falkland Islands legislation API access...")
        data = scraper._browse_docs("fiord", start=1, count=5)
        rows = data.get("rows", data.get("data", []))
        print(f"API returned {len(rows)} ordinance rows")
        if rows:
            fields = scraper._extract_doc_fields(rows[0])
            print(f"  First: {fields['title'][:60]}")
            print(f"  VS ID: {fields['vs_id']}")
            print(f"  VD ID: {fields['vd_id']}")
            if fields["vs_id"] and fields["vd_id"]:
                frags = scraper._get_fragments(fields["vs_id"], fields["vd_id"])
                total_html = sum(len(f) for f in frags)
                text = _strip_html("\n".join(frags))
                print(f"  Fragments: {len(frags)}, HTML: {total_html} chars, Text: {len(text)} chars")
        print("Test PASSED")
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
