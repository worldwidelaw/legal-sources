#!/usr/bin/env python3
"""
HK/eLegislation -- Hong Kong e-Legislation Fetcher

Fetches Hong Kong ordinances from the official e-Legislation bulk XML download.

Strategy:
  - Download ZIP file with all ordinance XML files
  - Parse each XML using namespace-aware ElementTree
  - Extract metadata from <meta> and full text from <main>

Data:
  - 1,372 ordinances (primary legislation)
  - Full text in structured XML (HKLM 1.0 schema)
  - Language: English
  - robots.txt allows access

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import zipfile
import io
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.HK.eLegislation")

BASE_URL = "https://www.elegislation.gov.hk"

# data.gov.hk mirrors — no JavaScript challenge, direct ZIP download
BULK_ZIP_URLS = [
    "https://resource.data.one.gov.hk/doj/data/hkel_c_leg_cap_1_cap_300_en.zip",
    "https://resource.data.one.gov.hk/doj/data/hkel_c_leg_cap_301_cap_600_en.zip",
    "https://resource.data.one.gov.hk/doj/data/hkel_c_leg_cap_601_cap_end_en.zip",
    "https://resource.data.one.gov.hk/doj/data/hkel_c_instruments_en.zip",
]
ZIP_CACHE_DIR = Path("/tmp/hk_leg_zips")

# XML namespaces
NS = {
    "hk": "http://www.xml.gov.hk/schemas/hklm/1.0",
    "dc": "http://purl.org/dc/elements/1.1/",
    "dcterms": "http://purl.org/dc/terms/",
    "xhtml": "http://www.w3.org/1999/xhtml",
}


class HongKongELegislationScraper(BaseScraper):
    """
    Scraper for HK/eLegislation -- Hong Kong e-Legislation.
    Country: HK
    URL: https://www.elegislation.gov.hk/

    Data types: legislation
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "*/*",
            },
            timeout=120,
        )

    def _download_zip(self, url: str) -> zipfile.ZipFile:
        """Download or use cached ZIP file."""
        ZIP_CACHE_DIR.mkdir(exist_ok=True)
        fname = url.rsplit("/", 1)[-1]
        cache_path = ZIP_CACHE_DIR / fname

        if cache_path.exists() and cache_path.stat().st_size > 1_000_000:
            logger.info(f"Using cached ZIP: {cache_path} ({cache_path.stat().st_size:,} bytes)")
            return zipfile.ZipFile(cache_path)

        logger.info(f"Downloading {fname}...")
        resp = self.client.session.get(url, timeout=600, stream=True)
        resp.raise_for_status()

        with open(cache_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)

        logger.info(f"Downloaded {cache_path.stat().st_size:,} bytes to {cache_path}")
        return zipfile.ZipFile(cache_path)

    def _extract_text(self, element: ET.Element) -> str:
        """Recursively extract text from an XML element."""
        parts = []
        if element.text:
            parts.append(element.text)

        for child in element:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag

            if tag == "marker":
                if child.get("role") == "blank-line":
                    parts.append("\n")
            elif tag in ("heading", "num"):
                parts.append(self._extract_text(child) + " ")
            elif tag in ("section", "part", "division", "subdivision", "schedule"):
                parts.append("\n" + self._extract_text(child))
            else:
                parts.append(self._extract_text(child))

            if child.tail:
                parts.append(child.tail)

        return "".join(parts)

    def _parse_xml(self, data: bytes, filename: str) -> Optional[Dict[str, Any]]:
        """Parse a single XML ordinance file."""
        try:
            root = ET.fromstring(data)
        except ET.ParseError as e:
            logger.warning(f"XML parse error in {filename}: {e}")
            return None

        meta = root.find("hk:meta", NS)
        if meta is None:
            return None

        doc_name = meta.findtext("hk:docName", "", NS)
        doc_type = meta.findtext("hk:docType", "", NS)
        doc_number = meta.findtext("hk:docNumber", "", NS)
        doc_status = meta.findtext("hk:docStatus", "", NS)
        date = meta.findtext("dc:date", "", NS)
        identifier = meta.findtext("dc:identifier", "", NS)

        main = root.find("hk:main", NS)
        if main is None:
            return None

        text = self._extract_text(main)
        lines = [line.strip() for line in text.split("\n") if line.strip()]
        text = "\n".join(lines)

        if len(text) < 20:
            return None

        # Extract long title if present
        long_title_el = main.find(".//hk:longTitle/hk:content", NS)
        long_title = ""
        if long_title_el is not None:
            long_title = self._extract_text(long_title_el).strip()

        # Build title
        title = doc_name
        if long_title:
            # Try to extract short title from text
            short_match = re.search(
                r"may be cited as the (.+?)(?:\.|$)", text[:1000]
            )
            if short_match:
                title = f"{doc_name} - {short_match.group(1).strip()}"

        # Build URL
        cap_clean = doc_number.replace(" ", "")
        url = f"{BASE_URL}/hk/cap{cap_clean}!en"

        return {
            "cap_number": doc_number,
            "doc_name": doc_name,
            "doc_type": doc_type,
            "doc_status": doc_status,
            "title": title,
            "long_title": long_title,
            "text": text,
            "date": date,
            "url": url,
            "identifier": identifier,
            "filename": filename,
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw document to standard schema."""
        return {
            "_id": f"HK/eLegislation:cap{raw['cap_number']}",
            "_source": "HK/eLegislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "cap_number": raw.get("cap_number", ""),
            "doc_status": raw.get("doc_status", ""),
            "jurisdiction": "HK",
        }

    def _iter_zip(self, url: str, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        """Yield normalized records from a single ZIP."""
        zf = self._download_zip(url)
        xml_files = sorted(
            [n for n in zf.namelist() if n.endswith(".xml")]
        )
        logger.info(f"Found {len(xml_files)} XML files in {url.rsplit('/', 1)[-1]}")

        count = 0
        errors = 0

        for i, name in enumerate(xml_files):
            try:
                data = zf.read(name)
                raw = self._parse_xml(data, name)
            except Exception as e:
                logger.warning(f"Error reading {name}: {e}")
                errors += 1
                continue

            if raw is None:
                errors += 1
                continue

            normalized = self.normalize(raw)
            count += 1
            yield normalized

            if max_records and count >= max_records:
                break

            if count % 100 == 0:
                logger.info(
                    f"Progress: {count} parsed, {errors} errors, "
                    f"{i+1}/{len(xml_files)} files"
                )

        zf.close()
        logger.info(f"ZIP done: {count} ordinances, {errors} errors")

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all ordinances from all bulk ZIPs."""
        for url in BULK_ZIP_URLS:
            yield from self._iter_zip(url)

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Re-download ZIP for updates (no incremental API)."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test using the smallest ZIP."""
        try:
            # Use the smallest ZIP (caps 601+) for testing
            zf = self._download_zip(BULK_ZIP_URLS[2])
            xml_files = [n for n in zf.namelist() if n.endswith(".xml")]
            if not xml_files:
                logger.error("No XML files in ZIP")
                return False
            data = zf.read(xml_files[0])
            raw = self._parse_xml(data, xml_files[0])
            zf.close()
            if raw and raw.get("text"):
                logger.info(
                    f"Test passed: parsed {raw['doc_name']} "
                    f"({len(raw['text']):,} chars)"
                )
                return True
            return False
        except Exception as e:
            logger.error(f"Test failed: {e}")
            return False


# === CLI ===
def main():
    import argparse

    parser = argparse.ArgumentParser(description="HK/eLegislation data fetcher")
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

    scraper = HongKongELegislationScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        max_records = 15 if args.sample else None
        count = 0

        if args.sample:
            # Use smallest ZIP for sample mode
            gen = scraper._iter_zip(BULK_ZIP_URLS[2], max_records=max_records)
        else:
            gen = scraper.fetch_all()

        for record in gen:
            out_path = sample_dir / f"{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            text_len = len(record.get("text", ""))
            logger.info(
                f"[{count + 1}] {record.get('title', 'unknown')[:80]} "
                f"({text_len:,} chars)"
            )

            count += 1
            if max_records and count >= max_records:
                break

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
