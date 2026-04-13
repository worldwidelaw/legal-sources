#!/usr/bin/env python3
"""
CA/AB-QP -- Alberta King's Printer Legislation Fetcher

Fetches Alberta statutes and regulations from the King's Printer website.

Data source: https://kings-printer.alberta.ca/legislation.aspx
License: Crown Copyright, reproduction permitted without charge.

Strategy:
  - Browse alphabetical listings (1, A-Y) to enumerate all ISBNs
  - For each ISBN, fetch detail page to get page/leg_type params
  - Download HTML full text via 1266.cfm endpoint
  - Parse and clean Word-generated HTML

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap            # Full bootstrap
  python bootstrap.py test-api             # Quick API connectivity test
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "CA/AB-QP"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CA.AB-QP")

BASE_URL = "https://kings-printer.alberta.ca"
ALPHA_URL = f"{BASE_URL}/570.cfm"
DETAIL_URL = f"{BASE_URL}/570.cfm"
FULLTEXT_URL = f"{BASE_URL}/1266.cfm"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (academic research)",
    "Accept": "text/html",
}

# Letters used in alphabetical browse (1 = numeric, then A-Y)
ALPHA_LETTERS = ["1"] + [chr(c) for c in range(ord("A"), ord("Z"))]


def clean_html(text: str) -> str:
    """Strip HTML/XML tags and clean text, extracting only legislation body."""
    if not text:
        return ""
    # Extract from first WordSection div onwards (skip site navigation chrome)
    ws_match = re.search(r'<div\s+class=WordSection\d', text, re.I)
    if ws_match:
        text = text[ws_match.start():]
    text = unescape(text)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.S | re.I)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.S | re.I)
    # Strip footer chrome (copyright, nav links)
    text = re.sub(r'<div\s+id="?footer"?.*', "", text, flags=re.S | re.I)
    text = re.sub(r'©\s*\d{4}\s*-\s*\d{4}\s*Government of Alberta.*', "", text, flags=re.S | re.I)
    # Replace block tags with newlines
    text = re.sub(r"<(?:p|div|br|h[1-6]|li|tr)[^>]*>", "\n", text, flags=re.I)
    # Remove all remaining tags
    text = re.sub(r"<[^>]+>", " ", text)
    # Clean whitespace
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class AlbertaKPFetcher:
    """Fetcher for Alberta King's Printer legislation."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.request_count = 0

    def _rate_limit(self):
        self.request_count += 1
        if self.request_count % 10 == 0:
            time.sleep(3.0)
        else:
            time.sleep(1.5)

    def _get(self, url: str, params: dict = None) -> Optional[str]:
        """Fetch URL and return response text."""
        self._rate_limit()
        try:
            r = self.session.get(url, params=params, timeout=30)
            r.raise_for_status()
            return r.text
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None

    def list_by_letter(self, letter: str) -> list:
        """List all legislation entries for a given letter."""
        html = self._get(ALPHA_URL, {"search_by": "alpha", "letter": letter})
        if not html:
            return []

        entries = []
        # Find all frm_isbn links: 570.cfm?frm_isbn=XXXX&search_by=link
        pattern = r'frm_isbn=(\d+)&amp;search_by=link[^"]*"[^>]*class="cartList"[^>]*>([^<]+)<'
        for m in re.finditer(pattern, html, re.I):
            isbn = m.group(1)
            title = unescape(m.group(2)).strip()
            entries.append({"isbn": isbn, "title": title})

        if not entries:
            # Try alternative pattern (the class might appear differently)
            pattern2 = r'frm_isbn=(\d+)[^"]*search_by=link[^"]*"[^>]*>([^<]+)<'
            for m in re.finditer(pattern2, html, re.I):
                isbn = m.group(1)
                title = unescape(m.group(2)).strip()
                entries.append({"isbn": isbn, "title": title})

        logger.info(f"Letter {letter}: {len(entries)} entries")
        return entries

    def get_detail(self, isbn: str) -> Optional[dict]:
        """Get detail page for an ISBN to extract page/leg_type params."""
        html = self._get(DETAIL_URL, {"frm_isbn": isbn, "search_by": "link"})
        if not html:
            return None

        # Extract 1266.cfm link params: page=XXX.cfm&leg_type=Acts|Regs
        pattern = r'1266\.cfm\?page=([^&]+)&(?:amp;)?leg_type=(\w+)&(?:amp;)?isbncln=(\d+)'
        m = re.search(pattern, html, re.I)
        if not m:
            logger.warning(f"No full text link found for ISBN {isbn}")
            return None

        page = m.group(1)
        leg_type = m.group(2)

        # Extract citation (e.g., "A-1 RSA 2000" or "272/1996")
        citation = None
        cit_pattern = r'<td[^>]*>\s*(?:Chapter|Regulation)\s*</td>\s*<td[^>]*>\s*([^<]+)'
        cm = re.search(cit_pattern, html, re.I)
        if cm:
            citation = cm.group(1).strip()

        # Try to extract consolidation date (e.g., "1/17/2006")
        date = None
        date_match = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', html)
        if date_match:
            m_val, d_val, y_val = date_match.group(1), date_match.group(2), date_match.group(3)
            date = f"{y_val}-{int(m_val):02d}-{int(d_val):02d}"
        else:
            # Fallback: year from citation
            year_match = re.search(r'\b(19\d{2}|20\d{2})\b', citation or "")
            if year_match:
                date = f"{year_match.group(1)}-01-01"

        return {
            "page": page,
            "leg_type": leg_type,
            "isbn": isbn,
            "citation": citation,
            "date": date,
        }

    def fetch_full_text(self, page: str, leg_type: str, isbn: str) -> Optional[str]:
        """Fetch HTML full text of a legislation document."""
        html = self._get(FULLTEXT_URL, {
            "page": page,
            "leg_type": leg_type,
            "isbncln": isbn,
            "display": "html",
        })
        if not html:
            return None

        text = clean_html(html)
        return text if text and len(text) > 50 else None

    def fetch_document(self, isbn: str, title: str) -> Optional[dict]:
        """Fetch a single legislation document by ISBN."""
        detail = self.get_detail(isbn)
        if not detail:
            return None

        text = self.fetch_full_text(detail["page"], detail["leg_type"], detail["isbn"])
        if not text:
            logger.warning(f"No text for ISBN {isbn}: {title}")
            return None

        doc_type = "statute" if detail["leg_type"] == "Acts" else "regulation"

        return {
            "_id": f"AB-{isbn}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": detail.get("date"),
            "url": f"{BASE_URL}/1266.cfm?page={detail['page']}&leg_type={detail['leg_type']}&isbncln={isbn}&display=html",
            "doc_type": doc_type,
            "isbn": isbn,
            "citation": detail.get("citation"),
            "jurisdiction": "CA-AB",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all Alberta legislation documents."""
        for letter in ALPHA_LETTERS:
            entries = self.list_by_letter(letter)
            for entry in entries:
                result = self.fetch_document(entry["isbn"], entry["title"])
                if result:
                    yield result

    def fetch_sample(self, count: int = 15) -> list:
        """Fetch a sample of legislation documents."""
        results = []

        # Sample from a few different letters
        sample_letters = ["A", "B", "C", "E", "L"]
        for letter in sample_letters:
            if len(results) >= count:
                break

            entries = self.list_by_letter(letter)
            for entry in entries[:4]:
                if len(results) >= count:
                    break

                result = self.fetch_document(entry["isbn"], entry["title"])
                if result:
                    results.append(result)
                    logger.info(
                        f"  [{len(results)}/{count}] {result['title'][:60]} "
                        f"({len(result['text'])} chars)"
                    )

        return results

    def normalize(self, raw: dict) -> dict:
        """Normalize to standard schema."""
        return {
            "_id": raw.get("_id", ""),
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": raw.get("_fetched_at", datetime.now(timezone.utc).isoformat()),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "doc_type": raw.get("doc_type", "statute"),
            "isbn": raw.get("isbn", ""),
            "citation": raw.get("citation"),
            "jurisdiction": "CA-AB",
        }


def test_api():
    """Quick API connectivity test."""
    fetcher = AlbertaKPFetcher()

    print("Testing Alberta King's Printer...")
    entries = fetcher.list_by_letter("A")
    print(f"  Letter A entries: {len(entries)}")

    if entries:
        print(f"\nFetching detail for: {entries[0]['title']}...")
        detail = fetcher.get_detail(entries[0]["isbn"])
        if detail:
            print(f"  Page: {detail['page']}, Type: {detail['leg_type']}")
            print(f"\nFetching full text...")
            text = fetcher.fetch_full_text(detail["page"], detail["leg_type"], detail["isbn"])
            if text:
                print(f"  Text length: {len(text)} chars")
                print(f"  First 200 chars: {text[:200]}")
                return True
            else:
                print("  FAILED: No text returned")
        else:
            print("  FAILED: No detail found")
    return False


def bootstrap(sample: bool = False, full: bool = False):
    """Run the bootstrap process."""
    SAMPLE_DIR.mkdir(exist_ok=True)
    fetcher = AlbertaKPFetcher()

    if sample:
        records = fetcher.fetch_sample(15)
    else:
        records = list(fetcher.fetch_all())

    saved = 0
    for rec in records:
        normalized = fetcher.normalize(rec)
        out_path = SAMPLE_DIR / f"{normalized['_id']}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(normalized, f, ensure_ascii=False, indent=2)
        saved += 1

    print(f"\nSaved {saved} records to {SAMPLE_DIR}/")

    # Validation
    texts = [r.get("text", "") for r in records]
    non_empty = sum(1 for t in texts if t and len(t) > 100)
    print(f"Records with substantial text: {non_empty}/{len(records)}")

    if non_empty < 10 and not sample:
        print("WARNING: Fewer than 10 records with full text!")
    elif sample and non_empty >= 10:
        print("PASS: Sample validation successful")
    elif sample:
        print(f"WARNING: Only {non_empty} records with full text (need 10+)")

    return records


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CA/AB-QP Bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Fetch legislation")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--full", action="store_true", help="Full bootstrap")

    sub.add_parser("test-api", help="Test API connectivity")

    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample, full=args.full)
    else:
        parser.print_help()
