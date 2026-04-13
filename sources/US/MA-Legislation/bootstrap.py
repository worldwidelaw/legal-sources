#!/usr/bin/env python3
"""
US/MA-Legislation -- Massachusetts General Laws

Fetches codified statutes from the MassGenLaws GitHub repository, which contains
24,719 XML files in StateDecoded format covering all 5 parts of the Massachusetts
General Laws.

The official site malegislature.gov is unreachable from datacenter IPs, so this
uses the structured XML data as the primary source.

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap (all sections)
  python bootstrap.py update --since YYYY-MM-DD  # Re-fetch all (no incremental)
  python bootstrap.py test                  # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.MA-Legislation")

RAW_BASE = "https://raw.githubusercontent.com/morrissinger/MassGenLaws/master"
TREE_API = "https://api.github.com/repos/morrissinger/MassGenLaws/git/trees/master?recursive=1"
USER_AGENT = "LegalDataHunter/1.0 (legal research; open data collection)"

# Representative sample files across all 5 parts
SAMPLE_FILES = [
    "p1/p1-t1/p1-t1-c1-s1.xml",
    "p1/p1-t2/p1-t2-c6-s1.xml",
    "p1/p1-t3/p1-t3-c30A-s1.xml",
    "p1/p1-t7/p1-t7-c40-s1.xml",
    "p1/p1-t9/p1-t9-c62-s1.xml",
    "p1/p1-t12/p1-t12-c71-s1.xml",
    "p1/p1-t15/p1-t15-c93A-s1.xml",
    "p1/p1-t16/p1-t16-c106-s1-101.xml",
    "p1/p1-t21/p1-t21-c151B-s1.xml",
    "p2/p2-t1/p2-t1-c183-s1.xml",
    "p2/p2-t2/p2-t2-c190B-s1-101.xml",
    "p3/p3-t1/p3-t1-c211-s1.xml",
    "p3/p3-t4/p3-t4-c231-s1.xml",
    "p4/p4-t1/p4-t1-c263-s1.xml",
    "p5/p5-t1/p5-t1-c281-s1.xml",
]


class MALegislationScraper(BaseScraper):
    """
    Scraper for US/MA-Legislation — Massachusetts General Laws.
    Uses MassGenLaws GitHub repository (StateDecoded XML format).
    """

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/xml",
        })

    def _get_file_list(self) -> List[str]:
        """Get list of all XML files from the GitHub API tree."""
        logger.info("Fetching file tree from GitHub API...")
        self.session.headers["Accept"] = "application/json"
        try:
            resp = self.session.get(TREE_API, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            files = [
                t["path"] for t in data.get("tree", [])
                if t["path"].endswith(".xml") and t["type"] == "blob"
            ]
            logger.info(f"Found {len(files)} XML files")
            return sorted(files)
        except Exception as e:
            logger.error(f"Failed to fetch file tree: {e}")
            return []
        finally:
            self.session.headers["Accept"] = "application/xml"

    def _fetch_xml(self, path: str) -> Optional[str]:
        """Fetch a single XML file from GitHub raw."""
        url = f"{RAW_BASE}/{path}"
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=30)
                if resp.status_code == 404:
                    logger.warning(f"Not found: {path}")
                    return None
                if resp.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.text
            except requests.exceptions.Timeout:
                if attempt < 2:
                    time.sleep(2)
                    continue
                return None
            except Exception as e:
                logger.warning(f"Failed to fetch {path}: {e}")
                if attempt < 2:
                    time.sleep(1)
                    continue
                return None
        return None

    def _parse_xml(self, xml_text: str, file_path: str) -> Optional[Dict[str, Any]]:
        """Parse a StateDecoded XML file into a raw record."""
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            logger.warning(f"XML parse error in {file_path}: {e}")
            return None

        # Extract structure
        structure = root.find("structure")
        part_name = ""
        title_name = ""
        chapter_name = ""
        part_id = ""
        title_id = ""
        chapter_id = ""

        if structure is not None:
            for unit in structure.findall("unit"):
                label = unit.get("label", "")
                identifier = unit.get("identifier", "")
                text = unit.text or ""
                if label == "part":
                    part_name = text.strip()
                    part_id = identifier
                elif label == "title":
                    title_name = text.strip()
                    title_id = identifier
                elif label == "chapter":
                    chapter_name = text.strip()
                    chapter_id = identifier

        section_number = ""
        sn_elem = root.find("section_number")
        if sn_elem is not None and sn_elem.text:
            section_number = sn_elem.text.strip()

        catch_line = ""
        cl_elem = root.find("catch_line")
        if cl_elem is not None and cl_elem.text:
            catch_line = cl_elem.text.strip()

        text = ""
        text_elem = root.find("text")
        if text_elem is not None:
            # Get all text including child elements
            text = ET.tostring(text_elem, encoding="unicode", method="text")
            text = text.strip()
            # Clean up whitespace
            text = re.sub(r"\n\s*\n", "\n\n", text)
            text = re.sub(r"[ \t]+", " ", text)
            text = text.strip()

        if not text or len(text) < 10:
            logger.warning(f"No text in {file_path}")
            return None

        return {
            "file_path": file_path,
            "part_id": part_id,
            "part_name": part_name,
            "title_id": title_id,
            "title_name": title_name,
            "chapter_id": chapter_id,
            "chapter_name": chapter_name,
            "section_number": section_number,
            "catch_line": catch_line,
            "text": text,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Massachusetts General Laws sections."""
        files = self._get_file_list()
        if not files:
            logger.error("No files found")
            return

        total = 0
        for i, path in enumerate(files):
            delay = self.config.get("fetch", {}).get("delay", 0.5)
            time.sleep(delay)

            xml_text = self._fetch_xml(path)
            if not xml_text:
                continue

            raw = self._parse_xml(xml_text, path)
            if raw:
                total += 1
                yield raw

            if total % 100 == 0 and total > 0:
                logger.info(f"Progress: {total} records from {i+1}/{len(files)} files")

        logger.info(f"Total fetched: {total}")

    def fetch_sample(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch a representative sample of sections."""
        for path in SAMPLE_FILES:
            delay = self.config.get("fetch", {}).get("delay", 0.5)
            time.sleep(delay)

            xml_text = self._fetch_xml(path)
            if not xml_text:
                continue

            raw = self._parse_xml(xml_text, path)
            if raw:
                yield raw

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Re-fetch all sections (no incremental updates for static repo)."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw section record into the standard schema."""
        chapter_id = raw.get("chapter_id", "")
        section_number = raw.get("section_number", "")
        doc_id = f"MA-GL-c{chapter_id}-s{section_number}"

        catch_line = raw.get("catch_line", "")
        chapter_name = raw.get("chapter_name", "")
        title = f"Chapter {chapter_id}, Section {section_number}"
        if catch_line:
            title += f" — {catch_line}"

        return {
            "_id": doc_id,
            "_source": "US/MA-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": None,  # Static codification, no per-section date
            "url": f"https://malegislature.gov/Laws/GeneralLaws/Part{raw.get('part_id', '')}/Title{raw.get('title_id', '')}/Chapter{chapter_id}/Section{section_number}",
            "section_number": section_number,
            "chapter": chapter_id,
            "chapter_name": chapter_name,
            "part": raw.get("part_id", ""),
            "part_name": raw.get("part_name", ""),
            "title_number": raw.get("title_id", ""),
            "title_name": raw.get("title_name", ""),
            "catch_line": catch_line,
            "jurisdiction": "US-MA",
        }

    def test_connection(self) -> bool:
        """Test connectivity to GitHub raw content."""
        try:
            xml_text = self._fetch_xml("p1/p1-t1/p1-t1-c1-s1.xml")
            if xml_text:
                raw = self._parse_xml(xml_text, "p1/p1-t1/p1-t1-c1-s1.xml")
                if raw and raw.get("text"):
                    logger.info(f"Connection test passed: got {len(raw['text'])} chars")
                    return True
            logger.error("Connection test failed: no data")
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/MA-Legislation data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample for validation",
    )
    parser.add_argument(
        "--since",
        help="ISO date for incremental updates (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Full bootstrap (all sections)",
    )
    args = parser.parse_args()

    scraper = MALegislationScraper()

    if args.command == "test":
        success = scraper.test_connection()
        print(f"Connection test: {'PASSED' if success else 'FAILED'}")
        sys.exit(0 if success else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command == "bootstrap":
        count = 0
        target = 15 if args.sample else 999999

        gen = scraper.fetch_sample() if args.sample else scraper.fetch_all()
        for raw in gen:
            record = scraper.normalize(raw)

            safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            text_len = len(record.get("text", ""))
            logger.info(
                f"[{count + 1}] {record['_id']}: {record['title'][:60]} "
                f"({text_len} chars)"
            )
            count += 1

            if count >= target:
                break

        print(f"\nBootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        count = 0
        gen = scraper.fetch_updates(since=args.since)
        for raw in gen:
            record = scraper.normalize(raw)
            safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1

        print(f"\nUpdate complete: {count} records")


if __name__ == "__main__":
    main()
