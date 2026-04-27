#!/usr/bin/env python3
"""
US/WY-Legislation -- Wyoming Statutes (wyoleg.gov)

Fetches all Wyoming Statutes with full text from the official Legislature
Rocket NXT (Folio) gateway.

Strategy:
  1. Use xmlcontents API to discover all titles and chapters
  2. For each chapter (leaf node), fetch full HTML document
  3. Parse sections from HTML using div.Section markers
  4. Normalize into standard schema

Data: Public domain (Wyoming government works). No auth required.

Usage:
  python bootstrap.py bootstrap            # Full pull (all chapters)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample sections
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import quote

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.WY-Legislation")

BASE_URL = "https://wyoleg.gov/NXT/gateway.dll"
VID = "Publish:10.1048/Enu"
YEAR = "2025"
ROOT_PATH = f"{YEAR} Wyoming Statutes/{YEAR} Titles"


def strip_html(text: str) -> str:
    """Strip HTML tags and clean up text."""
    if not text:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'</p>', '\n', text)
    text = re.sub(r'</div>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class WYLegislationScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
                "Accept": "*/*",
            },
            timeout=60,
        )
        self.delay = 2.0

    def _get(self, url: str) -> str:
        """Fetch URL with rate limiting."""
        time.sleep(self.delay)
        resp = self.http.get(url)
        return resp.text

    def _get_children(self, basepath: str, maxnodes: int = 200) -> str:
        """Fetch XML children of a path in the NXT tree."""
        url = (
            f"{BASE_URL}?f=xmlcontents"
            f"&vid={quote(VID, safe='')}"
            f"&command=getchildren"
            f"&basepathid={quote(basepath, safe='')}"
            f"&maxnodes={maxnodes}"
            f"&minnodesleft=5"
        )
        return self._get(url)

    def _get_document(self, doc_path: str) -> str:
        """Fetch the full HTML document for a given NXT path."""
        encoded_path = quote(doc_path, safe='')
        url = (
            f"{BASE_URL}/{encoded_path}"
            f"?f=templates&fn=document-frameset.htm"
            f"&vid={quote(VID, safe='')}"
        )
        return self._get(url)

    def test_api(self):
        """Test connectivity to WY Legislature NXT gateway."""
        logger.info("Testing WY Legislature NXT gateway...")
        try:
            xml_text = self._get_children(ROOT_PATH, maxnodes=5)
            if '<node' in xml_text or '<n ' in xml_text:
                logger.info("  XML contents: OK (got tree nodes)")
                logger.info("API test PASSED")
                return True
            else:
                logger.error(f"  XML contents: unexpected response: {xml_text[:200]}")
                return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def discover_chapters(self, basepath: str = None) -> list:
        """Recursively discover all leaf (chapter) document paths.

        Returns list of (doc_path, title_text) tuples.
        """
        if basepath is None:
            basepath = ROOT_PATH

        xml_text = self._get_children(basepath)

        # Parse the XML response
        # Wrap in root if needed
        if not xml_text.strip().startswith('<?xml') and not xml_text.strip().startswith('<nodes'):
            xml_text = f"<nodes>{xml_text}</nodes>"

        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            # Try wrapping
            try:
                root = ET.fromstring(f"<root>{xml_text}</root>")
            except ET.ParseError as e:
                logger.warning(f"Failed to parse XML for {basepath}: {e}")
                return []

        chapters = []
        for node in root.iter():
            if node.tag not in ('node', 'n'):
                continue

            node_id = node.get('id', '')
            node_title = node.get('t', '')
            content_type = node.get('ct', '')
            has_children = node.get('nc', '') == 'y'

            if not node_id:
                continue

            if content_type == 'application/morenode':
                # Pagination — need to fetch more
                continue

            if has_children or content_type == 'application/folder':
                # Recurse into folders
                sub_chapters = self.discover_chapters(node_id)
                chapters.extend(sub_chapters)
            elif content_type == 'text/xml':
                # Leaf document — this is a chapter
                chapters.append((node_id, node_title))

        return chapters

    def parse_chapter_document(self, doc_path: str, chapter_title: str) -> list:
        """Fetch a chapter document and parse individual sections.

        Returns list of raw section dicts.
        """
        html = self._get_document(doc_path)

        # Extract title number from the doc_path
        # Path format: "2025 Wyoming Statutes/2025 Titles/{title_num}/{chapter_idx}"
        path_parts = doc_path.split('/')
        title_num = ""
        if len(path_parts) >= 3:
            title_num = path_parts[2] if path_parts[2].replace('.', '').isdigit() else path_parts[2]

        # Split into sections using div.Section markers
        # Sections are marked with <div class="Section"> containing section number like "1-2-101. Title."
        section_pattern = re.compile(
            r'<div\s+class="Section"[^>]*>(.*?)</div>',
            re.DOTALL
        )

        # Alternative: split by section number pattern in the text
        # Section numbers follow pattern: {title}-{chapter}-{section}
        # e.g., "1-2-101.  Form."

        # First, try to find the document body
        # The NXT frameset might return a frameset, need the actual content
        if '<frame' in html.lower() or '<frameset' in html.lower():
            # Extract the content frame URL
            frame_match = re.search(r'src="([^"]*document-frame[^"]*)"', html)
            if frame_match:
                frame_url = frame_match.group(1)
                if not frame_url.startswith('http'):
                    frame_url = f"https://wyoleg.gov{frame_url}" if frame_url.startswith('/') else f"{BASE_URL}/{frame_url}"
                html = self._get(frame_url)

        sections = []

        # Try to find section divs
        section_matches = list(section_pattern.finditer(html))

        if section_matches:
            return self._parse_from_section_divs(html, section_matches, title_num, chapter_title, doc_path)

        # Fallback: parse by section number patterns in the text
        # Look for patterns like "1-2-101." at the start of lines/divs
        sec_num_pattern = re.compile(
            r'(\d+(?:\.\d+)?-\d+-\d+)\.\s+(.*?)(?=\d+(?:\.\d+)?-\d+-\d+\.\s|$)',
            re.DOTALL
        )

        clean_text = strip_html(html)
        for m in sec_num_pattern.finditer(clean_text):
            sec_id = m.group(1)
            content = m.group(2).strip()

            # Split content into title line and body
            lines = content.split('\n', 1)
            sec_title = lines[0].strip().rstrip('.')
            body = lines[1].strip() if len(lines) > 1 else ""

            if not body or len(body) < 10:
                continue

            full_title = f"Wyoming Statutes § {sec_id}"
            if sec_title:
                full_title = f"Wyoming Statutes § {sec_id} — {sec_title}"

            sections.append({
                "section_id": sec_id,
                "section_title": sec_title,
                "title": full_title,
                "text": body,
                "statute_title": title_num,
                "chapter_title": chapter_title,
                "doc_path": doc_path,
            })

        return sections

    def _parse_from_section_divs(self, html: str, matches: list, title_num: str,
                                  chapter_title: str, doc_path: str) -> list:
        """Parse sections from div.Section elements."""
        sections = []

        for i, match in enumerate(matches):
            section_text = strip_html(match.group(1))

            # Extract section number: e.g., "1-2-101.  Form."
            sec_match = re.match(r'(\d+(?:\.\d+)?-\d+-\d+)\.\s+(.*)', section_text)
            if not sec_match:
                continue

            sec_id = sec_match.group(1)
            remainder = sec_match.group(2)

            # The title is on the first line, body follows
            lines = remainder.split('\n', 1)
            sec_title = lines[0].strip().rstrip('.')
            body_start = match.end()

            # Body text: everything between this Section div and the next
            if i + 1 < len(matches):
                body_end = matches[i + 1].start()
            else:
                body_end = len(html)

            body_html = html[body_start:body_end]

            # Remove navigation elements
            body_html = re.sub(r'<div\s+class="Section"[^>]*>.*?</div>', '', body_html, flags=re.DOTALL)

            body = strip_html(body_html)
            if not body or len(body) < 5:
                # Try using the remainder as text
                body = '\n'.join(lines[1:]).strip() if len(lines) > 1 else ""

            if not body or len(body) < 5:
                continue

            full_title = f"Wyoming Statutes § {sec_id}"
            if sec_title:
                full_title = f"Wyoming Statutes § {sec_id} — {sec_title}"

            sections.append({
                "section_id": sec_id,
                "section_title": sec_title,
                "title": full_title,
                "text": body,
                "statute_title": title_num,
                "chapter_title": chapter_title,
                "doc_path": doc_path,
            })

        return sections

    def normalize(self, raw: dict) -> dict:
        """Transform raw section data into standard schema."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        return {
            "_id": raw["section_id"],
            "_source": "US/WY-Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": raw["title"],
            "text": raw["text"],
            "date": today,
            "url": f"https://wyoleg.gov/StateStatutes#section-{raw['section_id']}",
            "statute_title": raw["statute_title"],
            "chapter_title": raw.get("chapter_title", ""),
            "section_id": raw["section_id"],
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all WY statute sections."""
        chapters = self.discover_chapters()
        logger.info(f"Discovered {len(chapters)} chapters. Processing...")
        total = 0
        for doc_path, chap_title in chapters:
            try:
                sections = self.parse_chapter_document(doc_path, chap_title)
                for raw in sections:
                    yield raw
                    total += 1
                if sections:
                    logger.info(f"  {chap_title}: {len(sections)} sections (total: {total})")
            except Exception as e:
                logger.warning(f"  {chap_title}: failed ({e})")
        logger.info(f"Total sections fetched: {total}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch all sections (no incremental update supported)."""
        yield from self.fetch_all()

    def fetch_sample(self) -> Generator[dict, None, None]:
        """Fetch a small sample — first 3 chapters from Title 1."""
        logger.info("Fetching sample from Title 1...")

        # Get Title 1 children
        title1_path = f"{ROOT_PATH}/1"
        xml_text = self._get_children(title1_path)

        if not xml_text.strip().startswith('<?xml') and not xml_text.strip().startswith('<nodes'):
            xml_text = f"<nodes>{xml_text}</nodes>"

        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            root = ET.fromstring(f"<root>{xml_text}</root>")

        chapters = []
        for node in root.iter():
            if node.tag not in ('node', 'n'):
                continue
            node_id = node.get('id', '')
            node_title = node.get('t', '')
            content_type = node.get('ct', '')
            has_children = node.get('nc', '') == 'y'

            if not node_id:
                continue

            if has_children or content_type == 'application/folder':
                # Recurse one level
                sub_xml = self._get_children(node_id)
                if not sub_xml.strip().startswith('<?xml') and not sub_xml.strip().startswith('<nodes'):
                    sub_xml = f"<nodes>{sub_xml}</nodes>"
                try:
                    sub_root = ET.fromstring(sub_xml)
                except ET.ParseError:
                    sub_root = ET.fromstring(f"<root>{sub_xml}</root>")
                for sub_node in sub_root.iter():
                    if sub_node.tag not in ('node', 'n'):
                        continue
                    sub_id = sub_node.get('id', '')
                    sub_title = sub_node.get('t', '')
                    sub_ct = sub_node.get('ct', '')
                    if sub_id and sub_ct == 'text/xml':
                        chapters.append((sub_id, sub_title))
            elif content_type == 'text/xml':
                chapters.append((node_id, node_title))

        # Take first 3 chapters
        sample_chapters = chapters[:3]
        logger.info(f"Found {len(chapters)} chapters in Title 1, sampling {len(sample_chapters)}")

        count = 0
        for doc_path, chap_title in sample_chapters:
            try:
                sections = self.parse_chapter_document(doc_path, chap_title)
                for raw in sections:
                    yield raw
                    count += 1
                logger.info(f"  {chap_title}: {len(sections)} sections")
            except Exception as e:
                logger.warning(f"  {chap_title}: failed ({e})")
            if count >= 15:
                break
        logger.info(f"Sample complete: {count} sections fetched")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/WY-Legislation bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WYLegislationScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        if args.sample:
            gen = scraper.fetch_sample()
        else:
            gen = scraper.fetch_all()

        count = 0
        for record in gen:
            out_path = sample_dir / f"{record['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            if count <= 20 or count % 100 == 0:
                logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
