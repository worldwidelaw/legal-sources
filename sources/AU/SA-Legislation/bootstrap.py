#!/usr/bin/env python3
"""
AU/SA-Legislation -- South Australia Legislation Fetcher

Fetches SA Acts and Regulations from the data.sa.gov.au CKAN open data portal.

Strategy:
  - Query CKAN API for fortnightly XML update packages
  - Download ZIP files, extract inner A.zip (Acts) and R.zip (Regulations)
  - Parse SAOPC Exchange DTD XML for full text
  - No auth required; CC BY 4.0 license

Data:
  - SA Acts and Regulations in structured XML
  - Full text with parts/divisions/sections
  - Language: English

Usage:
  python bootstrap.py bootstrap          # Full initial pull (all packages)
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch latest packages
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import json
import logging
import re
import time
import zipfile
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AU.SA-Legislation")

CKAN_API = "https://data.sa.gov.au/data/api/3/action/package_show"
CKAN_PACKAGE_ID = "database-update-package-xml"
DATASET_URL = "https://data.sa.gov.au/data/dataset/database-update-package-xml"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
}


def _fetch_url(url: str, timeout: int = 120) -> Optional[bytes]:
    """Fetch a URL with error handling."""
    req = Request(url, headers=HEADERS)
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except (URLError, HTTPError) as e:
        logger.warning(f"Failed to fetch {url}: {e}")
        return None


def _get_ckan_resources() -> List[Dict[str, Any]]:
    """Get all resources from the CKAN package."""
    url = f"{CKAN_API}?id={CKAN_PACKAGE_ID}"
    data = _fetch_url(url)
    if not data:
        logger.error("Failed to fetch CKAN package metadata")
        return []

    result = json.loads(data)
    if not result.get("success"):
        logger.error("CKAN API returned failure")
        return []

    resources = result["result"]["resources"]
    # Filter to ZIP files only and sort by creation date (newest first)
    zips = [r for r in resources if r.get("url", "").endswith(".zip")]
    zips.sort(key=lambda r: r.get("created", ""), reverse=True)
    return zips


def _extract_text_from_xml(xml_bytes: bytes) -> Dict[str, Any]:
    """Extract metadata and full text from SAOPC Exchange DTD XML.

    Returns dict with title, text, date, year, number, doc_class.
    """
    try:
        xml_str = xml_bytes.decode("utf-8", errors="replace")
    except Exception:
        xml_str = xml_bytes.decode("us-ascii", errors="replace")

    result = {
        "title": "",
        "text": "",
        "date": None,
        "year": None,
        "number": None,
        "doc_class": "act",
    }

    # Extract attributes from <exdoc> root element
    exdoc_match = re.search(
        r'<exdoc\s([^>]+)>', xml_str[:2000], re.DOTALL
    )
    if exdoc_match:
        attrs = exdoc_match.group(1)

        title_m = re.search(r'title="([^"]+)"', attrs)
        if title_m:
            raw_title = title_m.group(1)
            # Double-decode: XML has &amp;#x00A0; which needs two passes
            raw_title = html.unescape(raw_title)  # &amp; -> &
            raw_title = html.unescape(raw_title)  # &#x00A0; -> \xa0
            raw_title = re.sub(r'&#x([0-9a-fA-F]+);', lambda m: chr(int(m.group(1), 16)), raw_title)
            result["title"] = raw_title.replace("\xa0", " ").strip()

        year_m = re.search(r'year="(\d{4})"', attrs)
        if year_m:
            result["year"] = year_m.group(1)

        number_m = re.search(r'number="(\d+)"', attrs)
        if number_m:
            result["number"] = number_m.group(1)

        date_m = re.search(r'enact\.or\.made\.date="(\d{4}-\d{2}-\d{2})"', attrs)
        if date_m:
            result["date"] = date_m.group(1)

        fvd_m = re.search(r'first\.valid\.date="(\d{4}-\d{2}-\d{2})"', attrs)
        if fvd_m and not result["date"]:
            result["date"] = fvd_m.group(1)

        class_m = re.search(r'doc\.class="([^"]+)"', attrs)
        if class_m:
            result["doc_class"] = class_m.group(1)

    # If no date from attributes, try metadata section
    if not result["date"] and result["year"]:
        result["date"] = f"{result['year']}-01-01"

    # Extract full text: strip XML tags, clean up
    text = xml_str
    # Remove XML declaration and DOCTYPE
    text = re.sub(r'<\?xml[^>]*\?>', ' ', text)
    text = re.sub(r'<!DOCTYPE[^>]*>', ' ', text)
    # Remove comments
    text = re.sub(r'<!--.*?-->', ' ', text, flags=re.DOTALL)
    # Remove all XML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # Decode HTML entities
    text = html.unescape(text)
    # Replace non-breaking spaces
    text = text.replace('\xa0', ' ')
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()

    result["text"] = text
    return result


def _extract_xmls_from_inner_zip(inner_zip_bytes: bytes) -> Generator[tuple, None, None]:
    """Extract XML files from an inner ZIP (A.zip or R.zip).

    Yields (filename, xml_bytes) tuples.
    """
    try:
        with zipfile.ZipFile(io.BytesIO(inner_zip_bytes)) as zf:
            for name in zf.namelist():
                if name.lower().endswith(".xml"):
                    try:
                        yield (name, zf.read(name))
                    except Exception as e:
                        logger.debug(f"Failed to read {name}: {e}")
    except zipfile.BadZipFile as e:
        logger.warning(f"Bad inner ZIP: {e}")


class SouthAustraliaLegislationScraper(BaseScraper):
    """
    Scraper for AU/SA-Legislation -- South Australia Legislation.
    Country: AU
    URL: https://data.sa.gov.au/data/dataset/database-update-package-xml

    Data types: legislation
    Auth: none (Open Data, CC BY 4.0)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _process_outer_zip(self, zip_bytes: bytes) -> Generator[Dict[str, Any], None, None]:
        """Process an outer ZIP file and yield raw document records."""
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as outer_zf:
                inner_zips = [n for n in outer_zf.namelist()
                              if n.lower().endswith(".zip")]

                for inner_name in inner_zips:
                    logger.info(f"  Processing inner ZIP: {inner_name}")
                    inner_bytes = outer_zf.read(inner_name)

                    for xml_name, xml_bytes in _extract_xmls_from_inner_zip(inner_bytes):
                        parsed = _extract_text_from_xml(xml_bytes)

                        if not parsed["text"] or len(parsed["text"]) < 200:
                            logger.debug(f"Skipping {xml_name}: text too short")
                            continue

                        # Build doc_id from year.number
                        doc_id = None
                        if parsed["year"] and parsed["number"]:
                            doc_id = f"{parsed['year']}.{parsed['number']}"
                        else:
                            # Fallback: extract from filename (e.g., 1974.15.xml)
                            fn_match = re.search(r'(\d{4}\.\d+)\.xml$', xml_name)
                            if fn_match:
                                doc_id = fn_match.group(1)

                        if not doc_id:
                            logger.debug(f"Skipping {xml_name}: no doc_id")
                            continue

                        yield {
                            "doc_id": doc_id,
                            "title": parsed["title"] or xml_name.split("/")[0],
                            "text": parsed["text"],
                            "date": parsed["date"],
                            "year": parsed["year"],
                            "number": parsed["number"],
                            "doc_class": parsed["doc_class"],
                            "xml_filename": xml_name,
                        }

        except zipfile.BadZipFile as e:
            logger.warning(f"Bad outer ZIP: {e}")

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record to standard schema."""
        doc_class = raw.get("doc_class", "act")
        return {
            "_id": raw["doc_id"],
            "_source": "AU/SA-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", raw["doc_id"]),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": DATASET_URL,
            "doc_id": raw["doc_id"],
            "doc_class": doc_class,
            "year": raw.get("year"),
            "number": raw.get("number"),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all SA legislation documents from all CKAN packages."""
        resources = _get_ckan_resources()
        if not resources:
            logger.error("No CKAN resources found")
            return

        logger.info(f"Found {len(resources)} ZIP resources to process")
        seen_ids = set()

        for i, resource in enumerate(resources):
            url = resource["url"]
            name = resource.get("name", f"resource-{i}")
            logger.info(f"Downloading package {i+1}/{len(resources)}: {name}")

            zip_bytes = _fetch_url(url, timeout=180)
            if not zip_bytes:
                logger.warning(f"Failed to download {name}")
                continue

            for doc in self._process_outer_zip(zip_bytes):
                if doc["doc_id"] not in seen_ids:
                    seen_ids.add(doc["doc_id"])
                    yield doc

            time.sleep(1)

        logger.info(f"Total unique documents: {len(seen_ids)}")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Fetch recently updated documents from latest CKAN packages."""
        resources = _get_ckan_resources()
        if not resources:
            return

        # Get the 3 most recent packages
        recent = resources[:3]
        seen_ids = set()

        for resource in recent:
            url = resource["url"]
            name = resource.get("name", "unknown")
            logger.info(f"Downloading recent package: {name}")

            zip_bytes = _fetch_url(url, timeout=180)
            if not zip_bytes:
                continue

            for doc in self._process_outer_zip(zip_bytes):
                if doc["doc_id"] not in seen_ids:
                    seen_ids.add(doc["doc_id"])
                    yield doc

            time.sleep(1)


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="AU/SA-Legislation data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    args = parser.parse_args()

    scraper = SouthAustraliaLegislationScraper()

    if args.command == "test":
        logger.info("Testing CKAN API access...")
        resources = _get_ckan_resources()
        if resources:
            logger.info(f"OK — {len(resources)} ZIP resources available")
            # Download the latest and check for XML content
            url = resources[0]["url"]
            logger.info(f"Downloading latest package: {resources[0].get('name', '?')}")
            zip_bytes = _fetch_url(url, timeout=120)
            if zip_bytes:
                count = 0
                for doc in scraper._process_outer_zip(zip_bytes):
                    count += 1
                    if count == 1:
                        logger.info(f"OK — '{doc['title']}' ({len(doc['text'])} chars)")
                logger.info(f"OK — {count} documents in latest package")
            else:
                logger.error("FAILED — could not download ZIP")
                sys.exit(1)
        else:
            logger.error("FAILED — no resources found")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
