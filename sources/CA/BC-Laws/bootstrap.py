#!/usr/bin/env python3
"""
CA/BC-Laws -- British Columbia Laws Data Fetcher

Fetches BC legislation from the official CiviX XML API.

Data source: https://www.bclaws.gov.bc.ca
License: King's Printer Licence (open access)

Strategy:
  - Browse /civix/content/ to enumerate all statutes and regulations
  - Fetch full text via /civix/document/ endpoint
  - Parse XML/HTML content and extract clean text

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
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

# Setup
SOURCE_ID = "CA/BC-Laws"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CA.BC-Laws")

BASE_URL = "https://www.bclaws.gov.bc.ca/civix"
CONTENT_URL = f"{BASE_URL}/content/complete/statreg"
DOCUMENT_URL = f"{BASE_URL}/document/id/complete/statreg"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (academic research)",
    "Accept": "application/xml",
}


def clean_html(text: str) -> str:
    """Strip HTML/XML tags and clean text."""
    if not text:
        return ""
    text = unescape(text)
    # Remove script and style blocks
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.S | re.I)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.S | re.I)
    # Replace block tags with newlines
    text = re.sub(r"<(?:p|div|br|h[1-6]|li|tr)[^>]*>", "\n", text, flags=re.I)
    # Remove all remaining tags
    text = re.sub(r"<[^>]+>", " ", text)
    # Clean whitespace
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class BCLawsFetcher:
    """Fetcher for BC Laws CiviX API."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.request_count = 0

    def _rate_limit(self):
        self.request_count += 1
        if self.request_count % 10 == 0:
            time.sleep(2.0)
        else:
            time.sleep(0.5)

    def _get_xml(self, url: str) -> Optional[ET.Element]:
        """Fetch URL and parse XML response."""
        self._rate_limit()
        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
            return ET.fromstring(r.content)
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None

    def list_letter_dirs(self) -> list:
        """List top-level letter directories (A-Z)."""
        root = self._get_xml(f"{CONTENT_URL}/")
        if root is None:
            return []

        dirs = []
        for elem in root:
            if elem.tag == "dir":
                title = elem.findtext("CIVIX_DOCUMENT_TITLE", "")
                doc_id = elem.findtext("CIVIX_DOCUMENT_ID", "")
                if doc_id and title.startswith("--"):
                    dirs.append({"id": doc_id, "title": title})
        return dirs

    def list_acts_in_dir(self, dir_id: str) -> list:
        """List all acts/regulations in a letter directory."""
        root = self._get_xml(f"{CONTENT_URL}/{dir_id}/")
        if root is None:
            return []

        acts = []
        for elem in root:
            if elem.tag == "dir":
                title = elem.findtext("CIVIX_DOCUMENT_TITLE", "")
                doc_id = elem.findtext("CIVIX_DOCUMENT_ID", "")
                if doc_id and title:
                    acts.append({"id": doc_id, "title": title})
        return acts

    def list_documents_in_act(self, act_id: str) -> list:
        """List documents within an act directory."""
        root = self._get_xml(f"{CONTENT_URL}/{act_id}/")
        if root is None:
            return []

        docs = []
        for elem in root:
            if elem.tag == "document":
                title = elem.findtext("CIVIX_DOCUMENT_TITLE", "")
                doc_id = elem.findtext("CIVIX_DOCUMENT_ID", "")
                ext = elem.findtext("CIVIX_DOCUMENT_EXT", "")
                if doc_id and ext == "xml":
                    docs.append({"id": doc_id, "title": title, "ext": ext})
        return docs

    def fetch_document(self, doc_id: str, act_title: str = "") -> Optional[dict]:
        """Fetch full text of a document."""
        url = f"{DOCUMENT_URL}/{doc_id}"
        self._rate_limit()

        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch document {doc_id}: {e}")
            return None

        raw_html = r.text
        title = act_title

        # Try to extract title from HTML
        m = re.search(r"<title>([^<]+)</title>", raw_html, re.I)
        if m:
            title = unescape(m.group(1)).strip()

        text = clean_html(raw_html)
        if not text or len(text) < 50:
            logger.warning(f"Document {doc_id} has insufficient text ({len(text)} chars)")
            return None

        return {
            "_id": f"BC-{doc_id}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": None,  # BC Laws doesn't have a simple date per act
            "url": f"https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/{doc_id}",
            "doc_type": "statute",
            "jurisdiction": "CA-BC",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all BC legislation documents."""
        letter_dirs = self.list_letter_dirs()
        logger.info(f"Found {len(letter_dirs)} letter directories")

        for letter_dir in letter_dirs:
            acts = self.list_acts_in_dir(letter_dir["id"])
            logger.info(f"  {letter_dir['title']}: {len(acts)} acts")

            for act in acts:
                docs = self.list_documents_in_act(act["id"])
                for doc in docs:
                    result = self.fetch_document(doc["id"], act["title"])
                    if result:
                        yield result

    def fetch_sample(self, count: int = 15) -> list:
        """Fetch a sample of legislation documents."""
        results = []
        letter_dirs = self.list_letter_dirs()

        if not letter_dirs:
            logger.error("No letter directories found")
            return []

        # Sample from different letters
        sample_dirs = letter_dirs[:5]  # A through E

        for letter_dir in sample_dirs:
            if len(results) >= count:
                break

            acts = self.list_acts_in_dir(letter_dir["id"])
            logger.info(f"  {letter_dir['title']}: {len(acts)} acts")

            for act in acts[:4]:  # Max 4 per letter
                if len(results) >= count:
                    break

                docs = self.list_documents_in_act(act["id"])
                for doc in docs:
                    if doc["ext"] == "xml":
                        result = self.fetch_document(doc["id"], act["title"])
                        if result:
                            results.append(result)
                            logger.info(
                                f"  [{len(results)}/{count}] {result['title'][:60]} "
                                f"({len(result['text'])} chars)"
                            )
                        break  # One doc per act

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
            "jurisdiction": raw.get("jurisdiction", "CA-BC"),
        }


def test_api():
    """Quick API connectivity test."""
    fetcher = BCLawsFetcher()

    print("Testing BC Laws CiviX API...")
    try:
        dirs = fetcher.list_letter_dirs()
        print(f"  Letter directories: {len(dirs)}")
    except Exception as e:
        print(f"  FAILED: {e}")
        return False

    if dirs:
        print(f"\nListing acts under {dirs[0]['title']}...")
        acts = fetcher.list_acts_in_dir(dirs[0]["id"])
        print(f"  Acts found: {len(acts)}")
        if acts:
            print(f"  First: {acts[0]['title']}")

            print(f"\nListing documents in {acts[0]['title']}...")
            docs = fetcher.list_documents_in_act(acts[0]["id"])
            print(f"  Documents: {len(docs)}")

            xml_docs = [d for d in docs if d["ext"] == "xml"]
            if xml_docs:
                print(f"\nFetching full text of {xml_docs[0]['title']}...")
                result = fetcher.fetch_document(xml_docs[0]["id"], acts[0]["title"])
                if result:
                    print(f"  Title: {result['title']}")
                    print(f"  Text length: {len(result['text'])} chars")
                    print(f"  Preview: {result['text'][:150]}...")
                else:
                    print("  FAILED: No text retrieved")
                    return False

    print("\nAll tests passed!")
    return True


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    fetcher = BCLawsFetcher()

    if sample:
        logger.info("Running sample bootstrap (15 records)...")
        documents = fetcher.fetch_sample(count=15)
    else:
        logger.info("Running full bootstrap...")
        documents = list(fetcher.fetch_all())

    if not documents:
        logger.error("No documents fetched!")
        return

    SAMPLE_DIR.mkdir(exist_ok=True)

    for doc in documents:
        normalized = fetcher.normalize(doc)
        filename = SAMPLE_DIR / f"{normalized['_id']}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(normalized, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(documents)} records to {SAMPLE_DIR}")

    has_text = sum(1 for d in documents if d.get("text"))
    has_title = sum(1 for d in documents if d.get("title"))
    avg_text_len = (
        sum(len(d.get("text", "")) for d in documents) / len(documents)
        if documents
        else 0
    )

    print(f"\n{'='*60}")
    print(f"CA/BC-Laws Bootstrap Summary")
    print(f"{'='*60}")
    print(f"Total records: {len(documents)}")
    print(f"With full text: {has_text}/{len(documents)}")
    print(f"With title: {has_title}/{len(documents)}")
    print(f"Avg text length: {avg_text_len:.0f} chars")
    print(f"Saved to: {SAMPLE_DIR}")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="CA/BC-Laws Data Fetcher")
    subparsers = parser.add_subparsers(dest="command")

    boot = subparsers.add_parser("bootstrap", help="Run bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")

    subparsers.add_parser("test-api", help="Test API connectivity")

    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
