#!/usr/bin/env python3
"""
US/DC-Code -- District of Columbia Code Fetcher

Fetches the DC Code from the DCCouncil/law-xml GitHub repository.
Each section is a self-contained XML file with full text, paragraph
structure, annotations, and legislative history.

Strategy:
  - Use GitHub API to list all section XML files in the repository tree
  - Fetch raw XML content from raw.githubusercontent.com
  - Parse XML to extract text, section number, heading, and annotations

Data:
  - 53 titles, ~21,163 sections
  - Format: XML (namespace: https://code.dccouncil.us/schemas/dc-library)
  - Language: English
  - License: Public Domain (US Government Work)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.DC-Code")

GITHUB_API = "https://api.github.com"
RAW_BASE = "https://raw.githubusercontent.com/DCCouncil/law-xml/main"
REPO = "DCCouncil/law-xml"
CODE_BASE_PATH = "us/dc/council/code/titles"
DC_NS = "https://code.dccouncil.us/schemas/dc-library"
WEB_BASE = "https://code.dccouncil.gov"


class DCCodeScraper(BaseScraper):
    """
    Scraper for US/DC-Code -- District of Columbia Code.
    Country: US
    URL: https://code.dccouncil.gov

    Data types: legislation
    Auth: none (GitHub public repository)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.raw_client = HttpClient(
            base_url=RAW_BASE,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/xml, text/xml, */*",
            },
            timeout=30,
        )

        self.api_client = HttpClient(
            base_url=GITHUB_API,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/vnd.github.v3+json",
            },
            timeout=60,
        )

    def _list_section_files(self) -> List[str]:
        """
        Use GitHub API to list all section XML files in the repository.
        Returns list of file paths relative to repo root.
        """
        logger.info("Fetching repository tree from GitHub API...")
        resp = self.api_client.get(
            f"/repos/{REPO}/git/trees/main",
            params={"recursive": "1"},
        )
        resp.raise_for_status()
        tree = resp.json()

        section_files = []
        for item in tree.get("tree", []):
            path = item.get("path", "")
            if (
                item.get("type") == "blob"
                and path.startswith(CODE_BASE_PATH)
                and "/sections/" in path
                and path.endswith(".xml")
            ):
                section_files.append(path)

        logger.info(f"Found {len(section_files)} section XML files")
        return sorted(section_files)

    def _fetch_section_xml(self, file_path: str) -> Optional[str]:
        """Fetch raw XML content for a section file from GitHub."""
        relative_path = f"/{file_path}"
        self.rate_limiter.wait()

        try:
            resp = self.raw_client.get(relative_path)
            if resp.status_code == 404:
                logger.debug(f"Not found: {file_path}")
                return None
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {file_path}: {e}")
            return None

    def _extract_text(self, root: ET.Element) -> str:
        """Extract plain text content from DC Code XML section."""
        text_parts = []

        def extract_recursive(elem, is_root_child=False):
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag

            if tag == "heading":
                text = "".join(elem.itertext()).strip()
                if text:
                    text_parts.append(f"\n{text}\n")
            elif tag == "num" and is_root_child:
                # Top-level section number handled separately
                pass
            elif tag == "text":
                text = "".join(elem.itertext()).strip()
                if text:
                    text_parts.append(text + "\n")
            elif tag == "num":
                text = "".join(elem.itertext()).strip()
                if text:
                    text_parts.append(f"{text} ")

            for child in elem:
                extract_recursive(child)

        # Process children of root (skip annotations)
        for child in root:
            child_tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if child_tag == "annotations":
                continue
            extract_recursive(child, is_root_child=True)

        full_text = "".join(text_parts)
        full_text = re.sub(r"\n{3,}", "\n\n", full_text)
        full_text = re.sub(r"[ \t]+", " ", full_text)
        return full_text.strip()

    def _extract_annotations(self, root: ET.Element) -> List[str]:
        """Extract annotation text from the annotations element."""
        annotations = []
        for ann_elem in root:
            ann_tag = ann_elem.tag.split("}")[-1] if "}" in ann_elem.tag else ann_elem.tag
            if ann_tag == "annotations":
                for ann in ann_elem:
                    text = "".join(ann.itertext()).strip()
                    if text:
                        ann_type = ann.get("type", "")
                        if ann_type:
                            annotations.append(f"[{ann_type}] {text}")
                        else:
                            annotations.append(text)
        return annotations

    def _path_to_metadata(self, file_path: str) -> Dict[str, str]:
        """Extract title number and section number from file path."""
        # Path: us/dc/council/code/titles/1/sections/1-1001.01.xml
        parts = file_path.split("/")
        title_num = None
        section_file = None

        for i, part in enumerate(parts):
            if part == "titles" and i + 1 < len(parts):
                title_num = parts[i + 1]
            if part == "sections" and i + 1 < len(parts):
                section_file = parts[i + 1]

        section_num = section_file.replace(".xml", "") if section_file else None

        return {
            "title_number": title_num,
            "section_number": section_num,
        }

    def _section_to_url(self, section_number: str) -> str:
        """Build the web URL for a DC Code section."""
        # Section format: 1-1001.01 -> title 1, path §1-1001.01
        return f"{WEB_BASE}/us/dc/council/code/sections/{section_number}.html"

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all DC Code sections."""
        section_files = self._list_section_files()

        for i, file_path in enumerate(section_files):
            if i > 0 and i % 100 == 0:
                logger.info(f"Progress: {i}/{len(section_files)} sections fetched")

            xml_content = self._fetch_section_xml(file_path)
            if not xml_content:
                continue

            yield {
                "file_path": file_path,
                "xml_content": xml_content,
            }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch sections updated since the given date using GitHub commits API."""
        since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info(f"Fetching commits since {since_str}")

        try:
            resp = self.api_client.get(
                f"/repos/{REPO}/commits",
                params={"since": since_str, "per_page": 100},
            )
            resp.raise_for_status()
            commits = resp.json()
        except Exception as e:
            logger.error(f"Failed to fetch commits: {e}")
            return

        changed_files = set()
        for commit in commits:
            sha = commit.get("sha", "")
            try:
                detail = self.api_client.get(f"/repos/{REPO}/commits/{sha}")
                detail.raise_for_status()
                for f in detail.json().get("files", []):
                    fname = f.get("filename", "")
                    if (
                        fname.startswith(CODE_BASE_PATH)
                        and "/sections/" in fname
                        and fname.endswith(".xml")
                    ):
                        changed_files.add(fname)
            except Exception:
                continue

        logger.info(f"Found {len(changed_files)} changed section files")
        for file_path in sorted(changed_files):
            xml_content = self._fetch_section_xml(file_path)
            if xml_content:
                yield {
                    "file_path": file_path,
                    "xml_content": xml_content,
                }

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw section XML into normalized record."""
        file_path = raw["file_path"]
        xml_content = raw["xml_content"]

        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            logger.warning(f"XML parse error for {file_path}: {e}")
            return None

        meta = self._path_to_metadata(file_path)
        section_num = meta.get("section_number", "")
        title_num = meta.get("title_number", "")

        # Extract heading
        ns = {"dc": DC_NS}
        heading_elem = root.find(f"{{{DC_NS}}}heading")
        if heading_elem is None:
            heading_elem = root.find("heading")
        heading = "".join(heading_elem.itertext()).strip() if heading_elem is not None else ""

        # Extract section number from XML
        num_elem = root.find(f"{{{DC_NS}}}num")
        if num_elem is None:
            num_elem = root.find("num")
        xml_section_num = num_elem.text.strip() if num_elem is not None and num_elem.text else section_num

        # Build title
        title = f"D.C. Code § {xml_section_num}"
        if heading:
            title = f"{title} — {heading}"

        # Extract text
        text = self._extract_text(root)
        if not text:
            logger.debug(f"Empty text for {file_path}")
            return None

        # Extract annotations
        annotations = self._extract_annotations(root)

        # Build date from annotation history
        date = None
        for ann in annotations:
            if "[History]" in ann:
                # Try to find a date pattern
                date_match = re.search(r"(\w+ \d{1,2}, \d{4})", ann)
                if date_match:
                    try:
                        parsed = datetime.strptime(date_match.group(1), "%B %d, %Y")
                        if date is None or parsed > date:
                            date = parsed
                    except ValueError:
                        pass
                # Also try short month
                date_match = re.search(r"(\w+\. \d{1,2}, \d{4})", ann)
                if date_match:
                    try:
                        parsed = datetime.strptime(date_match.group(1), "%b. %d, %Y")
                        if date is None or parsed > date:
                            date = parsed
                    except ValueError:
                        pass

        date_str = date.strftime("%Y-%m-%d") if date else None
        url = self._section_to_url(xml_section_num)

        # Add annotations to text if present
        full_text = text
        if annotations:
            full_text += "\n\n--- Annotations ---\n" + "\n".join(annotations)

        return {
            "_id": f"US/DC-Code/{xml_section_num}",
            "_source": "US/DC-Code",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": date_str,
            "url": url,
            "section_number": xml_section_num,
            "title_number": title_num,
            "heading": heading,
            "jurisdiction": "US-DC",
        }


# ── CLI ──────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/DC-Code scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (10 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap (all records)")
    args = parser.parse_args()

    scraper = DCCodeScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            resp = scraper.api_client.get(f"/repos/{REPO}")
            resp.raise_for_status()
            info = resp.json()
            logger.info(f"Repository: {info['full_name']}")
            logger.info(f"Description: {info['description']}")
            logger.info(f"Last updated: {info['updated_at']}")
            logger.info("Connectivity test PASSED")
        except Exception as e:
            logger.error(f"Connectivity test FAILED: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        sample_mode = args.sample or not args.full
        stats = scraper.bootstrap(sample_mode=sample_mode)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
