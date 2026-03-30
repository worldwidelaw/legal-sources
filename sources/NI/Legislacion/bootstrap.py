#!/usr/bin/env python3
"""
NI/Legislacion -- Nicaragua National Assembly Legislation (Domino/Notes)

Fetches all national legislation from the Asamblea Nacional de Nicaragua's
Domino database. 26,000+ documents from 1808-2026.

Strategy:
  - Enumerate document UNIDs via ReadViewEntries JSON endpoint (paginated)
  - Fetch full text from XPages document pages
  - Extract text from <span id="view:_id1:computedField1"> container

API:
  - Base: http://legislacion.asamblea.gob.ni/Normaweb.nsf
  - Listing: /bbe90a5bb646d50906257265005d21f8?ReadViewEntries&Count=200&Start=N&outputformat=JSON
  - Document: /xpNorma.xsp?documentId={UNID}&action=openDocument
  - No auth required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch ~15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as htmlmod
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NI.Legislacion")

BASE_URL = "http://legislacion.asamblea.gob.ni"
NSF_PATH = "/Normaweb.nsf"
# Flat view listing all documents
VIEW_ID = "bbe90a5bb646d50906257265005d21f8"
PAGE_SIZE = 200


def clean_html_text(html_str: str) -> str:
    """Strip HTML tags and clean text."""
    if not html_str:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_str, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_str, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<(?:p|div|br|h[1-6]|li|tr|blockquote)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</(?:p|div|li|tr|blockquote|ol|ul|table)>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = htmlmod.unescape(text)
    lines = [line.strip() for line in text.split('\n')]
    lines = [line for line in lines if line]
    return '\n'.join(lines).strip()


def _extract_entry_text(entry: dict) -> dict:
    """Extract title and other fields from a Domino view entry."""
    result = {"unid": entry.get("@unid", "")}
    entry_data = entry.get("entrydata", [])
    for col in entry_data:
        name = col.get("@name", "")
        # Text values
        text_val = col.get("text", {})
        if isinstance(text_val, dict):
            text_val = text_val.get("0", "")
        elif isinstance(text_val, list):
            text_val = text_val[0].get("0", "") if text_val else ""
        # DateTime values
        dt_val = col.get("datetime", {})
        if isinstance(dt_val, dict):
            dt_val = dt_val.get("0", "")
        elif isinstance(dt_val, list):
            dt_val = dt_val[0].get("0", "") if dt_val else ""

        if name == "NJTitulo":
            result["title"] = text_val
        elif name == "NJNumeroLD":
            result["number"] = text_val
        elif name == "NJFechaPublicacion":
            result["pub_date"] = dt_val if dt_val else text_val
        elif name == "CodigoNorma":
            result["code"] = text_val

    return result


class NILegislacionScraper(BaseScraper):
    """Scraper for NI/Legislacion -- Nicaraguan national legislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/worldwidelaw/legal-sources)",
                "Accept": "text/html,application/xhtml+xml,application/json,*/*",
                "Accept-Language": "es-NI,es;q=0.9,en;q=0.5",
            },
            timeout=60,
        )

    def _list_documents(self, max_entries: int = 0) -> List[Dict]:
        """Enumerate all documents via ReadViewEntries JSON endpoint."""
        entries = []
        start = 1

        while True:
            self.rate_limiter.wait()
            url = f"{NSF_PATH}/{VIEW_ID}?ReadViewEntries&Count={PAGE_SIZE}&Start={start}&outputformat=JSON"
            try:
                resp = self.client.get(url)
                if not resp or resp.status_code != 200:
                    logger.warning(f"Failed to fetch view at start={start}")
                    break

                # Domino JSON can have encoding issues; try to parse
                text = resp.text
                # Fix common Domino JSON issues: sometimes returns with BOM or wrapping
                text = text.lstrip('\ufeff')
                data = json.loads(text)

                view_entries = data.get("viewentry", [])
                if not view_entries:
                    break

                for entry in view_entries:
                    parsed = _extract_entry_text(entry)
                    if parsed.get("unid") and parsed.get("title"):
                        entries.append(parsed)

                logger.info(f"Listed {len(entries)} entries so far (start={start})")

                if max_entries and len(entries) >= max_entries:
                    entries = entries[:max_entries]
                    break

                # Next page
                if len(view_entries) < PAGE_SIZE:
                    break
                start += PAGE_SIZE

            except json.JSONDecodeError as e:
                logger.warning(f"JSON parse error at start={start}: {e}")
                break
            except Exception as e:
                logger.warning(f"Error listing at start={start}: {e}")
                break

        return entries

    def _fetch_document_text(self, unid: str) -> Optional[str]:
        """Fetch full text from XPages document page."""
        self.rate_limiter.wait()
        try:
            url = f"{NSF_PATH}/xpNorma.xsp?documentId={unid}&action=openDocument"
            resp = self.client.get(url)
            if not resp or resp.status_code != 200:
                return None

            html = resp.text

            # Pattern 1: XPages computedField1
            match = re.search(
                r'<span[^>]*id="view:_id1:computedField1"[^>]*>(.*?)</span>',
                html, re.DOTALL
            )
            if match:
                text = clean_html_text(match.group(1))
                if len(text) >= 50:
                    return text

            # Pattern 2: justifyText div (classic Domino fallback)
            match = re.search(
                r'<div\s+class="justifyText"[^>]*>(.*?)</div>',
                html, re.DOTALL
            )
            if match:
                text = clean_html_text(match.group(1))
                if len(text) >= 50:
                    return text

            # Pattern 3: broader content extraction
            match = re.search(
                r'<div[^>]*class="[^"]*lotusWidget[^"]*"[^>]*>(.*?)</div>\s*</div>\s*</div>',
                html, re.DOTALL
            )
            if match:
                text = clean_html_text(match.group(1))
                if len(text) >= 50:
                    return text

            return None

        except Exception as e:
            logger.warning(f"Error fetching document {unid}: {e}")
            return None

    def _fetch_document_text_classic(self, unid: str) -> Optional[str]:
        """Fallback: fetch via classic Domino URL (ISO-8859-1)."""
        self.rate_limiter.wait()
        try:
            url = f"{NSF_PATH}/0/{unid}?OpenDocument"
            resp = self.client.get(url)
            if not resp or resp.status_code != 200:
                return None

            # Handle ISO-8859-1 encoding
            if 'charset=iso-8859-1' in resp.headers.get('content-type', '').lower():
                html = resp.content.decode('iso-8859-1', errors='replace')
            else:
                html = resp.text

            match = re.search(
                r'<div\s+class="justifyText"[^>]*>(.*?)</div>',
                html, re.DOTALL
            )
            if match:
                text = clean_html_text(match.group(1))
                if len(text) >= 50:
                    return text

            return None

        except Exception as e:
            logger.warning(f"Error fetching classic document {unid}: {e}")
            return None

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse Domino date string to ISO 8601."""
        if not date_str:
            return None
        # Domino format: YYYYMMDDTHHMMSS or YYYY-MM-DD
        match = re.match(r'(\d{4})(\d{2})(\d{2})', date_str.replace('-', '').replace('T', ''))
        if match:
            y, m, d = match.groups()
            return f"{y}-{m}-{d}"
        return None

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all document entries from the listing view."""
        entries = self._list_documents()
        for entry in entries:
            yield entry

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Re-fetch all (no reliable date filtering in view)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema, fetching full text."""
        unid = raw.get("unid", "")
        title = raw.get("title", "")

        if not unid or not title:
            return None

        # Try XPages first, fall back to classic Domino
        text = self._fetch_document_text(unid)
        if not text:
            text = self._fetch_document_text_classic(unid)
        if not text:
            return None

        date_str = self._parse_date(raw.get("pub_date", ""))

        return {
            "_id": f"NI-LEG-{unid}",
            "_source": "NI/Legislacion",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": f"{BASE_URL}{NSF_PATH}/xpNorma.xsp?documentId={unid}&action=openDocument",
            "number": raw.get("number", ""),
            "jurisdiction": "NI",
            "language": "es",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing NI/Legislacion...")

        # Test listing
        entries = self._list_documents(max_entries=5)
        print(f"Listing: {len(entries)} entries retrieved")

        if entries:
            for i, entry in enumerate(entries[:3]):
                print(f"\n--- Entry {i+1} ---")
                print(f"  UNID: {entry['unid']}")
                print(f"  Title: {entry.get('title', 'N/A')[:80]}")
                print(f"  Number: {entry.get('number', 'N/A')}")

                text = self._fetch_document_text(entry['unid'])
                if not text:
                    text = self._fetch_document_text_classic(entry['unid'])
                if text:
                    print(f"  Full text: {len(text)} chars")
                    print(f"  Sample: {text[:150]}...")
                else:
                    print("  FAILED: No text extracted")

        print("\nTest complete!")


def main():
    scraper = NILegislacionScraper()

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
