#!/usr/bin/env python3
"""
BG/NAP-TaxDoctrine -- Bulgarian National Revenue Agency Tax Opinions Fetcher

Fetches tax opinions (становища) from the Bulgarian National Revenue Agency (НАП)
by using kik-info.com as an index to discover MD5 hashes and metadata, then
downloading Word documents from the NRA server and extracting full text.

Source index: https://kik-info.com/stanovishta-na-nap/
Full text: https://nraapp02.nra.bg/cms5/apps/wqreg/get/{hash}
Volume: 13,182+ opinions covering VAT, PIT, CIT, social security, health insurance,
        tax procedure, local taxes, and double taxation treaties.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental (newest first)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BG.NAP-TaxDoctrine")

INDEX_BASE = "https://kik-info.com"
INDEX_URL = f"{INDEX_BASE}/stanovishta-na-nap/"
NRA_BASE = "https://nraapp02.nra.bg"
NRA_DOWNLOAD = f"{NRA_BASE}/cms5/apps/wqreg/get"
DELAY = 1.5  # seconds between requests

LAW_CODES = {
    "ЗДДС": "VAT Act",
    "ЗДДФЛ": "Personal Income Tax Act",
    "ЗКПО": "Corporate Income Tax Act",
    "КСО": "Social Security Code",
    "ЗЗО": "Health Insurance Act",
    "ДОПК": "Tax-Insurance Procedure Code",
    "ЗМДТ": "Local Taxes and Fees Act",
    "СИДДО": "Double Taxation Avoidance Agreement",
    "ППЗДДС": "VAT Implementing Regulations",
}


def strip_html(raw_html: str) -> str:
    """Strip HTML tags and decode entities to plain text."""
    text = re.sub(r'<(style|script)[^>]*>.*?</\1>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<(br|p|div|h[1-6]|li|tr)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_text_from_doc(doc_bytes: bytes) -> str:
    """Extract text from a .doc (OLE2) file using UTF-16LE decoding.

    Bulgarian NRA .doc files store text as UTF-16LE within the OLE2 binary.
    We decode and extract meaningful text chunks.
    """
    text_utf16 = doc_bytes.decode('utf-16-le', errors='replace')
    # Match Bulgarian + Latin + digits + common punctuation
    chunks = re.findall(
        r'[А-Яа-яЁёA-Za-z0-9.,;:!?()  №/\-\n\r\t"\'ьъ%€$&@+*=<>{}[\]]{15,}',
        text_utf16
    )
    # Filter out XML/markup/theme artifacts
    filtered = []
    skip_patterns = ['http', 'schema', 'xml', 'theme', 'PK', 'Content_Types',
                     'rels/', 'drawingml', 'officedocument', 'openxml', 'microsoft']
    for chunk in chunks:
        chunk_lower = chunk.lower()
        if any(pat in chunk_lower for pat in skip_patterns):
            continue
        # Skip chunks that are mostly repeated characters
        if len(set(chunk)) < 5:
            continue
        filtered.append(chunk.strip())

    return '\n'.join(filtered)


class NAPTaxDoctrine(BaseScraper):
    SOURCE_ID = "BG/NAP-TaxDoctrine"

    def __init__(self):
        self.index_http = HttpClient(
            base_url=INDEX_BASE,
            headers={
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "bg,en;q=0.5",
                "User-Agent": "LegalDataHunter/1.0 (academic research; open legal data)",
            },
        )
        self.nra_http = HttpClient(
            base_url=NRA_BASE,
            headers={
                "Accept": "application/msword,*/*",
                "User-Agent": "LegalDataHunter/1.0 (academic research; open legal data)",
            },
        )

    def fetch_index_page(self, page: int) -> List[Dict[str, str]]:
        """Fetch a listing page from kik-info.com and extract opinion metadata.

        Uses schema.org Article markup:
        - itemprop="name" for title
        - itemprop="datePublished" content="YYYY-MM-DD" for date
        - Вх.№ reference / date for reference number
        - Office name after building icon
        - Law codes in a separate div
        """
        resp = self.index_http.get(f"/stanovishta-na-nap/index.php?pageID={page}")
        time.sleep(DELAY)
        if resp is None or resp.status_code != 200:
            logger.warning("Failed to fetch index page %d (status=%s)",
                           page, resp.status_code if resp else "None")
            return []

        html = resp.text
        entries = []

        # Split by <article> tags
        articles = re.findall(
            r'<article\s+class="article[^"]*"[^>]*>(.*?)</article>',
            html, re.DOTALL
        )

        for article_html in articles:
            entry = {}

            # Extract hash from link
            hash_match = re.search(r'/stanovishta-na-nap/([a-f0-9]{32})', article_html)
            if not hash_match:
                continue
            entry["hash"] = hash_match.group(1)

            # Extract title from itemprop="name"
            title_match = re.search(r'itemprop="name">(.*?)</h3>', article_html, re.DOTALL)
            if title_match:
                entry["title"] = strip_html(title_match.group(1)).strip()

            # Extract date from itemprop="datePublished" content="YYYY-MM-DD"
            date_match = re.search(r'itemprop="datePublished"\s+content="(\d{4}-\d{2}-\d{2})"', article_html)
            if date_match:
                entry["iso_date"] = date_match.group(1)

            # Extract reference number: Вх.№ XXXXX / DD.MM.YYYY
            ref_match = re.search(r'Вх\.?\s*№\s*([^\s/<]+)\s*/\s*(\d{2}\.\d{2}\.\d{4})', article_html)
            if ref_match:
                entry["reference_number"] = ref_match.group(1)
                entry["date_raw"] = ref_match.group(2)

            # Extract issuing office (after building icon)
            office_match = re.search(r'fa-building[^>]*>\s*</i>\s*(.*?)</span>', article_html, re.DOTALL)
            if office_match:
                entry["office"] = strip_html(office_match.group(1)).strip()

            # Extract law references from the second attr div
            # Pattern: <div class="mb-2"><span class="attr text-muted"><i ...></i> LAWS</span></div>
            law_match = re.search(r'fa-file-alt[^>]*>\s*</i>\s*((?:ЗДДС|ЗДДФЛ|ЗКПО|КСО|ЗЗО|ДОПК|ЗМДТ|СИДДО|ППЗДДС|РЕГЛАМЕНТ|НАРЕДБА)[^<]*)', article_html)
            if law_match:
                entry["laws"] = strip_html(law_match.group(1)).strip()

            # Extract summary from itemprop="headline"
            headline_match = re.search(r'itemprop="headline">(.*?)</div>', article_html, re.DOTALL)
            if headline_match:
                entry["summary"] = strip_html(headline_match.group(1)).strip()

            entries.append(entry)

        # Deduplicate by hash
        seen = set()
        unique = []
        for e in entries:
            if e["hash"] not in seen:
                seen.add(e["hash"])
                unique.append(e)

        return unique

    def download_opinion(self, md5_hash: str) -> Optional[str]:
        """Download the .doc file from NRA and extract text."""
        resp = self.nra_http.get(f"/cms5/apps/wqreg/get/{md5_hash}")
        time.sleep(DELAY)
        if resp is None or resp.status_code != 200:
            logger.warning("Failed to download doc for hash %s (status=%s)",
                           md5_hash, resp.status_code if resp else "None")
            return None

        doc_bytes = resp.content
        if len(doc_bytes) < 100:
            logger.warning("Doc too small for hash %s (%d bytes)", md5_hash, len(doc_bytes))
            return None

        text = extract_text_from_doc(doc_bytes)
        if len(text) < 50:
            logger.warning("Extracted text too short for hash %s (%d chars)", md5_hash, len(text))
            return None

        return text

    def parse_date(self, date_str: str) -> Optional[str]:
        """Parse DD.MM.YYYY to ISO format."""
        if not date_str:
            return None
        try:
            dt = datetime.strptime(date_str.strip(), "%d.%m.%Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return date_str

    def normalize(self, entry: Dict[str, str], text: str) -> Dict[str, Any]:
        """Normalize a raw opinion into the standard schema."""
        iso_date = entry.get("iso_date") or self.parse_date(entry.get("date_raw", ""))

        return {
            "_id": f"BG-NAP-{entry['hash'][:16]}",
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": entry.get("title", ""),
            "text": text,
            "date": iso_date,
            "url": f"{NRA_DOWNLOAD}/{entry['hash']}",
            "language": "bg",
            "reference_number": entry.get("reference_number", ""),
            "issuing_office": entry.get("office", ""),
            "law_references": entry.get("laws", ""),
            "md5_hash": entry["hash"],
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all opinions by paginating through the kik-info.com index."""
        total_yielded = 0
        sample_limit = 15 if sample else None
        page = 1
        max_pages = 3 if sample else 1500  # ~1319 expected pages
        consecutive_empty = 0

        while page <= max_pages:
            if sample_limit and total_yielded >= sample_limit:
                break

            logger.info("Fetching index page %d...", page)
            entries = self.fetch_index_page(page)

            if not entries:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    logger.info("3 consecutive empty pages at page %d, stopping.", page)
                    break
                page += 1
                continue
            consecutive_empty = 0

            for entry in entries:
                if sample_limit and total_yielded >= sample_limit:
                    break

                text = self.download_opinion(entry["hash"])
                if not text:
                    logger.warning("No text for %s: %s", entry["hash"][:12], entry.get("title", "")[:50])
                    continue

                record = self.normalize(entry, text)
                yield record
                total_yielded += 1

                if total_yielded % 50 == 0:
                    logger.info("  Progress: %d opinions fetched", total_yielded)

            page += 1

        logger.info("Fetch complete. Total opinions: %d", total_yielded)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch opinions published since a given date (YYYY-MM-DD)."""
        page = 1
        found_older = False

        while not found_older:
            logger.info("Checking page %d for updates since %s...", page, since)
            entries = self.fetch_index_page(page)
            if not entries:
                break

            for entry in entries:
                date_raw = entry.get("date_raw") or entry.get("pub_date", "")
                iso_date = self.parse_date(date_raw)
                if iso_date and iso_date < since:
                    found_older = True
                    break

                text = self.download_opinion(entry["hash"])
                if not text:
                    continue

                record = self.normalize(entry, text)
                yield record

            page += 1
            if page > 1500:
                break

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            entries = self.fetch_index_page(1)
            logger.info("Test: found %d entries on index page 1", len(entries))
            if not entries:
                return False
            text = self.download_opinion(entries[0]["hash"])
            if text and len(text) > 50:
                logger.info("Test passed: opinion %s has %d chars of text",
                            entries[0]["hash"][:12], len(text))
                return True
            logger.error("Test failed: no text extracted")
            return False
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


# === CLI entry point ===

def main():
    import argparse

    parser = argparse.ArgumentParser(description="BG/NAP-TaxDoctrine bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10-15 sample records")
    parser.add_argument("--since", type=str, help="Date for incremental update (YYYY-MM-DD)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NAPTaxDoctrine()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            safe_name = re.sub(r'[^\w\-.]', '_', record['_id'])
            out_file = sample_dir / f"{safe_name}.json"
            out_file.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            count += 1
            text_len = len(record.get("text", ""))
            logger.info(
                "  [%d] %s | %s | text=%d chars",
                count, record["date"], record["title"][:60], text_len
            )

        logger.info("Bootstrap complete: %d records saved to sample/", count)
        sys.exit(0 if count >= 10 else 1)

    if args.command == "update":
        since = args.since or "2026-01-01"
        count = 0
        for record in scraper.fetch_updates(since):
            count += 1
            logger.info("  [%d] %s: %s", count, record["date"], record["title"][:60])
        logger.info("Update complete: %d new records since %s", count, since)


if __name__ == "__main__":
    main()
