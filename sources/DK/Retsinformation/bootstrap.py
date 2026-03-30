#!/usr/bin/env python3
"""
DK/Retsinformation -- Denmark Official Law Database Fetcher

Fetches Danish legislation via sitemap discovery + ELI XML full text.

Strategy:
  - Parse sitemap index (21 pages, ~20K documents)
  - Extract ELI URIs for all documents
  - Download XML for each via {eli_url}/xml
  - Parse XML for title, text, and metadata
  - For updates: use harvest API (last 10 days)

Usage:
  python bootstrap.py bootstrap          # Full bootstrap via sitemap
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
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DK.Retsinformation")

SITEMAP_INDEX = "https://retsinformation.dk/sitemap.xml"
HARVEST_API = "https://api.retsinformation.dk/v1/Documents"


class RetsinformationScraper(BaseScraper):
    """Scraper for DK/Retsinformation -- Danish legislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self._last_request = 0

    def _rate_limit(self, min_gap: float = 2.0):
        """Enforce gap between requests."""
        elapsed = time.time() - self._last_request
        if elapsed < min_gap:
            time.sleep(min_gap - elapsed)
        self._last_request = time.time()

    def _http_get(self, url: str, min_gap: float = 2.0) -> Optional[str]:
        """HTTP GET with rate limiting and retries."""
        import urllib.request
        self._rate_limit(min_gap)
        for attempt in range(3):
            try:
                req = urllib.request.Request(url, headers={
                    "User-Agent": "LegalDataHunter/1.0 (open-data-research)",
                    "Accept": "application/xml, text/xml, application/json, */*",
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
                    time.sleep(5)
        return None

    def _get_sitemap_urls(self) -> List[str]:
        """Parse sitemap index and all sitemap pages to get all ELI URLs."""
        eli_urls = []

        # Fetch sitemap index
        index_xml = self._http_get(SITEMAP_INDEX, min_gap=1.0)
        if not index_xml:
            logger.error("Failed to fetch sitemap index")
            return []

        # Parse sitemap index for page URLs
        sitemap_pages = re.findall(r'<loc>(https?://[^<]+sitemap\.xml\?page=\d+)</loc>', index_xml)
        if not sitemap_pages:
            # Try without query params
            sitemap_pages = re.findall(r'<loc>(https?://[^<]+)</loc>', index_xml)

        logger.info(f"Found {len(sitemap_pages)} sitemap pages")

        for page_url in sitemap_pages:
            page_xml = self._http_get(page_url, min_gap=1.0)
            if not page_xml:
                continue

            # Extract ELI URLs from sitemap page
            urls = re.findall(r'<loc>(https?://retsinformation\.dk/eli/[^<]+)</loc>', page_xml)
            eli_urls.extend(urls)
            logger.info(f"  {page_url}: {len(urls)} ELI URLs")

        logger.info(f"Total ELI URLs from sitemap: {len(eli_urls)}")
        return eli_urls

    def _fetch_eli_xml(self, eli_url: str) -> Optional[str]:
        """Fetch XML document via ELI URL + /xml suffix."""
        xml_url = eli_url.rstrip("/") + "/xml"
        return self._http_get(xml_url)

    def _parse_xml(self, xml_content: str) -> Dict[str, str]:
        """Parse XML document for title and text."""
        result = {"title": "", "text": "", "year": "", "number": "",
                  "status": "", "document_type": "", "accession_number": ""}

        try:
            xml_clean = re.sub(r'\sxmlns[^"]*"[^"]*"', '', xml_content, count=5)
            root = ET.fromstring(xml_clean)
        except ET.ParseError:
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
            accn_elem = meta.find("AccessionNumber")
            if accn_elem is not None and accn_elem.text:
                result["accession_number"] = accn_elem.text.strip()

        # Extract date - try multiple fields
        for date_tag in ["StartDate", "SignatureDate", "EndDate"]:
            date_elem = root.find(f".//{date_tag}")
            if date_elem is not None and date_elem.text and date_elem.text.strip():
                result["date"] = date_elem.text.strip()
                break

        # Extract all text from document body
        parts = []
        skip_tags = {"Meta", "DocumentType", "Rank", "AccessionNumber",
                     "DocumentId", "UniqueDocumentId", "SchemaLocation",
                     "SignatureDate", "Year", "Number", "Status"}
        for elem in root.iter():
            if elem.text and elem.text.strip():
                tag = elem.tag
                if tag not in skip_tags:
                    parts.append(elem.text.strip())
            if elem.tail and elem.tail.strip():
                parts.append(elem.tail.strip())

        result["text"] = "\n".join(parts)
        return result

    def _regex_parse_xml(self, xml_content: str) -> Dict[str, str]:
        """Fallback XML parsing with regex."""
        title_m = re.search(r'<DocumentTitle[^>]*>(.*?)</DocumentTitle>', xml_content, re.DOTALL)
        title = title_m.group(1).strip() if title_m else ""
        accn_m = re.search(r'<AccessionNumber[^>]*>(.*?)</AccessionNumber>', xml_content, re.DOTALL)
        accession = accn_m.group(1).strip() if accn_m else ""

        text = re.sub(r'<[^>]+>', ' ', xml_content)
        text = re.sub(r'\s+', ' ', text).strip()

        return {"title": title, "text": text, "year": "", "number": "",
                "status": "", "document_type": "", "accession_number": accession}

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        accession = raw.get("accession_number", "")
        eli_url = raw.get("eli_url", "")
        title = raw.get("title", "")
        text = raw.get("text", "")
        date = raw.get("date", raw.get("change_date", ""))

        url = eli_url if eli_url else (
            f"https://www.retsinformation.dk/eli/accn/{accession}" if accession else ""
        )

        doc_id = accession if accession else eli_url.split("/eli/")[-1].replace("/", "-") if eli_url else ""

        return {
            "_id": f"DK-RI-{doc_id}",
            "_source": "DK/Retsinformation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "accession_number": accession,
            "document_type": raw.get("document_type", ""),
            "year": raw.get("year", ""),
            "number": raw.get("number", ""),
            "status": raw.get("status", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all documents via sitemap discovery + ELI XML."""
        eli_urls = self._get_sitemap_urls()
        if not eli_urls:
            logger.error("No ELI URLs found in sitemap")
            return

        count = 0
        skipped = 0

        for i, eli_url in enumerate(eli_urls):
            if i % 100 == 0 and i > 0:
                logger.info(f"Progress: {i}/{len(eli_urls)} processed, {count} yielded, {skipped} skipped")

            xml_content = self._fetch_eli_xml(eli_url)
            if not xml_content:
                skipped += 1
                continue

            parsed = self._parse_xml(xml_content)
            if not parsed.get("text") or len(parsed["text"]) < 100:
                skipped += 1
                continue

            raw = {
                "accession_number": parsed.get("accession_number", ""),
                "eli_url": eli_url,
                "date": parsed.get("date", ""),
                "title": parsed["title"],
                "text": parsed["text"],
                "document_type": parsed["document_type"],
                "year": parsed["year"],
                "number": parsed["number"],
                "status": parsed["status"],
            }
            count += 1
            yield self.normalize(raw)

        logger.info(f"Completed: {count} documents fetched, {skipped} skipped")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent updates via harvest API (last 10 days)."""
        today = datetime.now()

        for days_back in range(1, 11):
            date = today - timedelta(days=days_back)
            date_str = date.strftime("%Y-%m-%d")

            url = f"{HARVEST_API}?date={date_str}"
            text = self._http_get(url, min_gap=10.0)
            if not text:
                continue

            try:
                data = json.loads(text, strict=False)
                docs = data if isinstance(data, list) else []
            except json.JSONDecodeError:
                continue

            logger.info(f"{date_str}: {len(docs)} documents")

            for doc in docs:
                accession = doc.get("accessionsnummer", "")
                if not accession:
                    continue

                xml_url = f"http://retsinformation.dk/eli/accn/{accession}/xml"
                xml_content = self._http_get(xml_url, min_gap=10.0)
                if not xml_content:
                    continue

                parsed = self._parse_xml(xml_content)
                if not parsed.get("text") or len(parsed["text"]) < 100:
                    continue

                raw = {
                    "accession_number": accession,
                    "eli_url": f"https://retsinformation.dk/eli/accn/{accession}",
                    "change_date": doc.get("changeDate", date_str),
                    "title": parsed["title"],
                    "text": parsed["text"],
                    "document_type": parsed["document_type"],
                    "year": parsed["year"],
                    "number": parsed["number"],
                    "status": parsed["status"],
                }
                yield self.normalize(raw)

    def test(self) -> bool:
        """Quick connectivity test."""
        # Test sitemap
        index_xml = self._http_get(SITEMAP_INDEX, min_gap=1.0)
        if not index_xml:
            logger.error("Cannot fetch sitemap index")
            return False
        logger.info("Sitemap index OK")

        # Test one ELI XML
        page_xml = self._http_get(f"{SITEMAP_INDEX}?page=2", min_gap=1.0)
        if not page_xml:
            logger.error("Cannot fetch sitemap page 2")
            return False

        urls = re.findall(r'<loc>(https?://retsinformation\.dk/eli/[^<]+)</loc>', page_xml)
        if not urls:
            logger.error("No ELI URLs in sitemap page 2")
            return False

        xml_content = self._fetch_eli_xml(urls[0])
        if not xml_content:
            logger.error(f"Cannot fetch XML for {urls[0]}")
            return False

        parsed = self._parse_xml(xml_content)
        logger.info(f"XML OK: {parsed['title'][:60]} ({len(parsed['text'])} chars)")
        return True


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
