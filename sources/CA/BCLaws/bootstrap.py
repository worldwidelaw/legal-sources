#!/usr/bin/env python3
"""
CA/BCLaws -- British Columbia Laws Data Fetcher

Fetches BC acts and regulations from the official CIVIX REST API.
Structured XML with full text. No auth required.

Strategy:
  - Browse directory tree at /civix/content/complete/statreg/
  - Each letter directory (A-Z) contains act directories
  - Each act directory contains the document + regulations
  - Fetch XML for each document and extract full text

API: https://www.bclaws.gov.bc.ca/civix/
Docs: https://github.com/bcgov/bc-laws-api

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py test-api             # Quick API connectivity test
"""

import sys
import json
import logging
import time
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from xml.etree import ElementTree as ET

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CA.BCLaws")

API_BASE = "https://www.bclaws.gov.bc.ca/civix"


class BCLawsScraper(BaseScraper):
    """
    Scraper for CA/BCLaws -- British Columbia Laws.
    Country: CA
    URL: https://www.bclaws.gov.bc.ca

    Data types: legislation
    Auth: none (Queen's Printer Licence)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=60,
        )

    # -- XML helpers --------------------------------------------------------

    def _get_xml(self, path):
        """Fetch and parse XML from a CIVIX path."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(path)
            resp.raise_for_status()
            return ET.fromstring(resp.text)
        except Exception as e:
            logger.error(f"Failed to fetch {path}: {e}")
            return None

    def _extract_text(self, root):
        """Recursively extract all text content from an XML element."""
        parts = []
        if root.text:
            parts.append(root.text)
        for child in root:
            parts.append(self._extract_text(child))
            if child.tail:
                parts.append(child.tail)
        return " ".join(parts)

    def _clean_text(self, text):
        """Clean extracted text."""
        # Collapse whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        # Remove XML artifacts
        text = re.sub(r'<[^>]+>', '', text)
        return text

    def _browse_directory(self, dir_id):
        """Browse a CIVIX directory and return child entries."""
        path = f"/content/complete/statreg/{dir_id}"
        root = self._get_xml(path)
        if root is None:
            return []

        entries = []
        for elem in root:
            entry = {
                "title": "",
                "doc_id": "",
                "doc_type": elem.tag,  # 'dir' or 'document'
            }
            for child in elem:
                tag = child.tag
                if tag == "CIVIX_DOCUMENT_TITLE":
                    entry["title"] = child.text or ""
                elif tag == "CIVIX_DOCUMENT_ID":
                    entry["doc_id"] = child.text or ""
                elif tag == "CIVIX_DOCUMENT_TYPE":
                    entry["doc_type"] = child.text or ""
            entries.append(entry)
        return entries

    def _fetch_document_xml(self, doc_id):
        """Fetch the XML content of a specific document."""
        path = f"/document/id/complete/statreg/{doc_id}/xml"
        return self._get_xml(path)

    # -- Document discovery -------------------------------------------------

    def _discover_documents(self, max_acts=None):
        """
        Walk the CIVIX directory tree to discover all act/regulation documents.
        Yields (doc_id, title, doc_type_hint) tuples.
        """
        # Get top-level letter directories
        top_entries = self._browse_directory("")
        act_count = 0

        for letter_entry in top_entries:
            if letter_entry["doc_type"] != "dir":
                continue

            letter_title = letter_entry["title"]
            letter_id = letter_entry["doc_id"]
            logger.info(f"Browsing {letter_title}...")

            # Get acts under this letter
            act_entries = self._browse_directory(letter_id)

            for act_entry in act_entries:
                if act_entry["doc_type"] != "dir":
                    # Direct document at letter level (rare)
                    if act_entry["doc_type"] == "document":
                        yield act_entry["doc_id"], act_entry["title"], "act"
                        act_count += 1
                    continue

                act_id = act_entry["doc_id"]
                act_title = act_entry["title"]

                # Browse into the act directory to find the main document
                # and any regulations
                act_contents = self._browse_directory(act_id)

                for item in act_contents:
                    if item["doc_type"] == "document":
                        # Main act document
                        yield item["doc_id"], item["title"] or act_title, "act"
                        act_count += 1
                    elif item["doc_type"] == "dir" and "Regulations" == item["title"]:
                        # Regulations directory
                        reg_entries = self._browse_directory(item["doc_id"])
                        for reg in reg_entries:
                            if reg["doc_type"] == "dir":
                                # Each regulation has its own directory
                                reg_contents = self._browse_directory(reg["doc_id"])
                                for reg_doc in reg_contents:
                                    if reg_doc["doc_type"] == "document":
                                        yield reg_doc["doc_id"], reg_doc["title"] or reg["title"], "regulation"
                                        act_count += 1

                if max_acts and act_count >= max_acts:
                    logger.info(f"Reached max_acts={max_acts}")
                    return

    # -- BaseScraper interface ----------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all BC acts and regulations with full text."""
        for doc_id, title, doc_type_hint in self._discover_documents():
            xml_root = self._fetch_document_xml(doc_id)
            if xml_root is None:
                continue

            raw_text = self._extract_text(xml_root)
            text = self._clean_text(raw_text)

            if not text or len(text) < 50:
                logger.warning(f"Skipping {doc_id}: text too short ({len(text)} chars)")
                continue

            # Extract metadata from XML
            ns = {"act": "http://www.gov.bc.ca/2013/legislation/act",
                  "reg": "http://www.gov.bc.ca/2013/legislation/regulation"}

            year = ""
            for ns_prefix in ns.values():
                year_elem = xml_root.find(f"{{{ns_prefix}}}yearenacted")
                if year_elem is not None and year_elem.text:
                    year = year_elem.text
                    break

            title_elem = xml_root.find(f"{{{ns['act']}}}title")
            if title_elem is None:
                title_elem = xml_root.find(f"{{{ns['reg']}}}title")
            if title_elem is not None and title_elem.text:
                title = title_elem.text

            yield {
                "doc_id": doc_id,
                "title": title,
                "text": text,
                "year": year,
                "doc_type_hint": doc_type_hint,
                "url": f"https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/{doc_id}",
            }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch updated documents. BCLaws has no date-based filtering,
        so this re-fetches all and relies on dedup."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw BCLaws document into standard schema."""
        date_str = raw.get("year", "")
        if date_str and len(date_str) == 4:
            date_str = f"{date_str}-01-01"

        return {
            "_id": f"BC/{raw.get('doc_id', '')}",
            "_source": "CA/BCLaws",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date_str or None,
            "url": raw.get("url", ""),
            "doc_id": raw.get("doc_id", ""),
            "legislation_type": raw.get("doc_type_hint", "act"),
            "jurisdiction": "British Columbia",
            "country": "CA",
        }

    # -- Sample mode --------------------------------------------------------

    def _fetch_sample(self) -> list:
        """Fetch sample records for validation."""
        samples = []
        count = 0

        for doc_id, title, doc_type_hint in self._discover_documents(max_acts=15):
            xml_root = self._fetch_document_xml(doc_id)
            if xml_root is None:
                continue

            raw_text = self._extract_text(xml_root)
            text = self._clean_text(raw_text)

            if not text or len(text) < 50:
                continue

            raw = {
                "doc_id": doc_id,
                "title": title,
                "text": text,
                "year": "",
                "doc_type_hint": doc_type_hint,
                "url": f"https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/{doc_id}",
            }

            # Try to get year from XML
            ns_act = "http://www.gov.bc.ca/2013/legislation/act"
            ns_reg = "http://www.gov.bc.ca/2013/legislation/regulation"
            for ns in [ns_act, ns_reg]:
                year_elem = xml_root.find(f"{{{ns}}}yearenacted")
                if year_elem is not None and year_elem.text:
                    raw["year"] = year_elem.text
                    break

            normalized = self.normalize(raw)
            samples.append(normalized)
            logger.info(f"  {doc_id}: {title} ({len(text)} chars)")
            count += 1

            if count >= 12:
                break

        return samples


def main():
    import argparse
    parser = argparse.ArgumentParser(description="CA/BCLaws data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Sample mode: fetch small set for validation")
    args = parser.parse_args()

    scraper = BCLawsScraper()

    if args.command == "test-api":
        print("Testing BCLaws CIVIX API connectivity...")
        entries = scraper._browse_directory("")
        if entries:
            print(f"OK: {len(entries)} top-level directories")
            for e in entries[:5]:
                print(f"  {e['title']} (id={e['doc_id']})")
        else:
            print("FAIL: Could not reach API")
            sys.exit(1)
        return

    if args.command == "bootstrap":
        if args.sample:
            print("Running sample mode...")
            samples = scraper._fetch_sample()
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)

            for i, record in enumerate(samples):
                fname = sample_dir / f"sample_{i+1:03d}.json"
                with open(fname, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"\nSaved {len(samples)} sample records to sample/")
            if samples:
                texts = [s["text"] for s in samples if s.get("text")]
                avg_len = sum(len(t) for t in texts) // max(len(texts), 1)
                print(f"Average text length: {avg_len} chars")
                print(f"Types: {[s['legislation_type'] for s in samples]}")
                for s in samples:
                    assert s.get("text"), f"Missing text: {s['_id']}"
                    assert s.get("title"), f"Missing title: {s['_id']}"
                print("All validation checks passed!")
            return

        result = scraper.bootstrap()
        print(f"Bootstrap complete: {result}")

    elif args.command == "update":
        result = scraper.update()
        print(f"Update complete: {result}")


if __name__ == "__main__":
    main()
