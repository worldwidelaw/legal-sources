#!/usr/bin/env python3
"""
DK/Retsinformation -- Denmark Official Law Database Fetcher

Fetches Danish legislation via harvest API + ELI XML full text.

Strategy:
  - Query harvest API for documents changed per day
  - Iterate backwards over last 10 days
  - Download XML for each document via ELI accession URL
  - Parse XML for title, text, and metadata
  - Normalize into standard schema

Usage:
  python bootstrap.py bootstrap          # Fetch docs from last 10 days
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import time
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DK.Retsinformation")

API_BASE = "https://api.retsinformation.dk/v1/Documents"
XML_BASE = "http://retsinformation.dk/eli/accn/{accession}/xml"


class RetsinformationScraper(BaseScraper):
    """Scraper for DK/Retsinformation -- Danish legislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self._last_request = 0

    def _rate_limit(self):
        """Enforce 10-second gap between API requests."""
        elapsed = time.time() - self._last_request
        if elapsed < 10:
            time.sleep(10 - elapsed)
        self._last_request = time.time()

    def _http_get(self, url: str) -> Optional[str]:
        """HTTP GET with rate limiting."""
        import urllib.request
        self._rate_limit()
        for attempt in range(3):
            try:
                req = urllib.request.Request(url, headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/json, application/xml, text/xml",
                })
                with urllib.request.urlopen(req, timeout=30) as resp:
                    return resp.read().decode("utf-8", errors="replace")
            except Exception as e:
                if "429" in str(e):
                    logger.warning("Rate limited, waiting 15 seconds")
                    time.sleep(15)
                elif "404" in str(e):
                    return None
                else:
                    logger.warning(f"Attempt {attempt+1} failed for {url[:80]}: {e}")
                    time.sleep(10)
        return None

    def _fetch_documents_for_date(self, date_str: str) -> list:
        """Fetch documents changed on a given date."""
        url = f"{API_BASE}?date={date_str}"
        text = self._http_get(url)
        if not text:
            return []
        try:
            data = json.loads(text, strict=False)
            return data if isinstance(data, list) else []
        except json.JSONDecodeError:
            return []

    def _fetch_xml(self, accession: str) -> Optional[str]:
        """Fetch XML document via ELI accession URL."""
        url = XML_BASE.format(accession=accession)
        return self._http_get(url)

    def _parse_xml(self, xml_content: str) -> Dict[str, str]:
        """Parse XML document for title and text."""
        result = {"title": "", "text": "", "year": "", "number": "",
                  "status": "", "document_type": ""}

        try:
            # Remove namespaces for simpler parsing
            xml_clean = re.sub(r'\sxmlns[^"]*"[^"]*"', '', xml_content, count=5)
            root = ET.fromstring(xml_clean)
        except ET.ParseError:
            # Fallback: regex extraction
            return self._regex_parse_xml(xml_content)

        # Extract metadata from Meta element
        meta = root.find(".//Meta")
        if meta is not None:
            title_elem = meta.find("DocumentTitle")
            if title_elem is not None and title_elem.text:
                result["title"] = title_elem.text.strip()
            year_elem = meta.find("Year")
            if year_elem is not None and year_elem.text:
                result["year"] = year_elem.text.strip()
            num_elem = meta.find("Number")
            if num_elem is not None and num_elem.text:
                result["number"] = num_elem.text.strip()
            status_elem = meta.find("Status")
            if status_elem is not None and status_elem.text:
                result["status"] = status_elem.text.strip()
            doctype_elem = meta.find("DocumentType")
            if doctype_elem is not None and doctype_elem.text:
                result["document_type"] = doctype_elem.text.strip()

        # Extract all text from document body
        parts = []
        for elem in root.iter():
            if elem.text and elem.text.strip():
                tag = elem.tag
                if tag not in ("Meta", "DocumentType", "Rank", "AccessionNumber",
                               "DocumentId", "UniqueDocumentId", "SchemaLocation"):
                    parts.append(elem.text.strip())
            if elem.tail and elem.tail.strip():
                parts.append(elem.tail.strip())

        result["text"] = "\n".join(parts)
        return result

    def _regex_parse_xml(self, xml_content: str) -> Dict[str, str]:
        """Fallback XML parsing with regex."""
        title_m = re.search(r'<DocumentTitle[^>]*>(.*?)</DocumentTitle>', xml_content, re.DOTALL)
        title = title_m.group(1).strip() if title_m else ""

        # Strip all tags for text
        text = re.sub(r'<[^>]+>', ' ', xml_content)
        text = re.sub(r'\s+', ' ', text).strip()

        return {"title": title, "text": text, "year": "", "number": "",
                "status": "", "document_type": ""}

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        accession = raw.get("accession_number", "")
        change_date = raw.get("change_date", "")
        title = raw.get("title", "")
        text = raw.get("text", "")
        doc_type = raw.get("document_type", "")

        url = f"https://www.retsinformation.dk/eli/accn/{accession}" if accession else ""

        return {
            "_id": f"DK-RI-{accession}",
            "_source": "DK/Retsinformation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": change_date,
            "url": url,
            "accession_number": accession,
            "document_type": doc_type,
            "year": raw.get("year", ""),
            "number": raw.get("number", ""),
            "status": raw.get("status", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch documents from the last 10 days via harvest API."""
        count = 0
        today = datetime.now()

        for days_back in range(1, 11):
            date = today - timedelta(days=days_back)
            date_str = date.strftime("%Y-%m-%d")

            docs = self._fetch_documents_for_date(date_str)
            if not docs:
                logger.info(f"No documents for {date_str}")
                continue

            logger.info(f"{date_str}: {len(docs)} documents")

            for doc in docs:
                accession = doc.get("accessionsnummer", "")
                href = doc.get("href", "")
                if not accession or not href:
                    continue

                xml_content = self._fetch_xml(accession)
                if not xml_content:
                    continue

                parsed = self._parse_xml(xml_content)
                if not parsed.get("text") or len(parsed["text"]) < 100:
                    continue

                raw = {
                    "accession_number": accession,
                    "change_date": doc.get("changeDate", date_str),
                    "title": parsed["title"],
                    "text": parsed["text"],
                    "document_type": parsed["document_type"],
                    "year": parsed["year"],
                    "number": parsed["number"],
                    "status": parsed["status"],
                }
                count += 1
                yield self.normalize(raw)

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent updates."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        today = datetime.now()
        for days_back in range(1, 5):
            date = today - timedelta(days=days_back)
            date_str = date.strftime("%Y-%m-%d")
            docs = self._fetch_documents_for_date(date_str)
            if docs:
                logger.info(f"API OK: {len(docs)} docs for {date_str}")
                accession = docs[0].get("accessionsnummer", "")
                if accession:
                    xml = self._fetch_xml(accession)
                    if xml:
                        parsed = self._parse_xml(xml)
                        logger.info(f"XML OK: {parsed['title'][:60]} ({len(parsed['text'])} chars)")
                        return True
                return True
        logger.error("No documents found in last 4 days")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="DK/Retsinformation data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    args = parser.parse_args()

    scraper = RetsinformationScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        max_records = 15 if args.sample else None
        count = 0

        for record in scraper.fetch_all():
            out_path = sample_dir / f"{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            text_len = len(record.get("text", ""))
            logger.info(
                f"[{count + 1}] {record.get('title', '?')[:80]} "
                f"({text_len:,} chars)"
            )

            count += 1
            if max_records and count >= max_records:
                break

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_updates():
            out_path = sample_dir / f"update_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    main()
