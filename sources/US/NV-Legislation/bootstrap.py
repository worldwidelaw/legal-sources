#!/usr/bin/env python3
"""
US/NV-Legislation -- Nevada Revised Statutes (NRS)

Fetches the full text of all NRS sections from nevada.public.law,
which mirrors the official Nevada Legislature statutes.

Strategy:
  - Download sitemaps to get URLs for all ~46K individual NRS sections
  - Fetch each section page and extract title + body text from HTML
  - Official source link preserved for each section

Data Coverage:
  - All 59 titles, ~820 chapters, ~46,000 sections of the Nevada Revised Statutes
  - Language: English
  - Open access, no authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Same as bootstrap (statutes are consolidated)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import gzip
import io
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from html import unescape

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NV-Legislation")

BASE_URL = "https://nevada.public.law"
SITEMAP_INDEX = f"{BASE_URL}/sitemaps/sitemap.xml.gz"
SECTION_PATTERN = re.compile(r"/statutes/nrs_(\d+[a-z]?)\.(\d+[a-z]?)$")

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research; open data collection)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}


def extract_section_text(html: str) -> Dict[str, str]:
    """Extract section number, title, body text, and source URL from a section page."""
    result = {"number": "", "title": "", "text": "", "source_url": ""}

    # Section number from NRS pattern in page
    num_match = re.search(r"NRS\s+(\d+[A-Za-z]?\.\d+[a-z]?)", html)
    if num_match:
        result["number"] = num_match.group(1)

    # Title from span#name
    name_match = re.search(r'<span\s+id="name">\s*([^<]+)', html)
    if name_match:
        result["title"] = unescape(name_match.group(1).strip())

    # Body text from leaf-statute-body div
    body_match = re.search(
        r'id="leaf-statute-body">(.*?)</div>\s*(?:</div>|<footer)',
        html,
        re.DOTALL,
    )
    if body_match:
        body_html = body_match.group(1)
        # Replace section breaks with newlines
        text = re.sub(r"</section>", "\n", body_html)
        text = re.sub(r"</h2>", " ", text)
        # Strip all tags
        text = re.sub(r"<[^>]+>", "", text)
        # Decode entities
        text = unescape(text)
        # Clean whitespace
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n\s*\n", "\n\n", text)
        text = text.strip()
        # Remove trailing source citation block
        text = re.sub(r"\n*Source:\s*\nSection.*$", "", text, flags=re.DOTALL)
        result["text"] = text

    # Official source URL
    src_match = re.search(r'footer-source-link[^>]*href="([^"]+)"', html)
    if src_match:
        result["source_url"] = src_match.group(1)

    return result


def get_section_urls_from_sitemaps(session: requests.Session) -> List[str]:
    """Download sitemaps and extract all NRS section URLs."""
    logger.info("Downloading sitemap index...")
    resp = session.get(SITEMAP_INDEX, timeout=30)
    resp.raise_for_status()
    index_xml = gzip.decompress(resp.content).decode("utf-8")

    # Find sub-sitemap URLs
    sub_urls = re.findall(r"<loc>(https://[^<]+\.xml\.gz)</loc>", index_xml)
    logger.info(f"Found {len(sub_urls)} sub-sitemaps")

    section_urls = []
    for sub_url in sub_urls:
        logger.info(f"Downloading {sub_url}...")
        resp = session.get(sub_url, timeout=30)
        resp.raise_for_status()
        sub_xml = gzip.decompress(resp.content).decode("utf-8")

        urls = re.findall(r"<loc>(https://nevada\.public\.law/statutes/nrs_[^<]+)</loc>", sub_xml)
        for url in urls:
            path = url.replace(BASE_URL, "")
            if SECTION_PATTERN.search(path):
                section_urls.append(url)

    section_urls.sort()
    logger.info(f"Total section URLs: {len(section_urls)}")
    return section_urls


class NVLegislationScraper(BaseScraper):
    """Scraper for US/NV-Legislation -- Nevada Revised Statutes."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all NRS sections."""
        section_urls = get_section_urls_from_sitemaps(self.session)
        delay = self.config.get("fetch", {}).get("delay", 1.5)

        for i, url in enumerate(section_urls):
            time.sleep(delay)

            try:
                resp = self.session.get(url, timeout=30)
                if resp.status_code == 404:
                    logger.debug(f"404 for {url}")
                    continue
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"Failed to fetch {url}: {e}")
                continue

            data = extract_section_text(resp.text)
            if not data["text"] or len(data["text"]) < 10:
                logger.debug(f"No text for {url}")
                continue

            # Parse chapter from URL
            match = SECTION_PATTERN.search(url)
            chapter = match.group(1) if match else ""

            data["url"] = url
            data["chapter"] = chapter
            yield data

            if (i + 1) % 100 == 0:
                logger.info(f"Progress: {i + 1}/{len(section_urls)}")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Statutes are consolidated; fetch_updates = fetch_all."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw section into the standard schema."""
        section_num = raw.get("number", "")
        chapter = raw.get("chapter", "")
        title = raw.get("title", "")

        doc_id = f"US-NV-NRS-{section_num}" if section_num else f"US-NV-NRS-ch{chapter}"

        display_title = f"NRS {section_num}" if section_num else f"NRS Chapter {chapter}"
        if title:
            display_title += f" - {title}"

        return {
            "_id": doc_id,
            "_source": "US/NV-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": display_title,
            "text": raw.get("text", ""),
            "date": None,  # Consolidated statutes, no single date
            "url": raw.get("source_url") or raw.get("url", ""),
            "section_number": section_num,
            "chapter": chapter,
            "jurisdiction": "US-NV",
        }

    def test_connection(self) -> bool:
        """Test connectivity by fetching a known section."""
        try:
            resp = self.session.get(f"{BASE_URL}/statutes/nrs_200.010", timeout=15)
            resp.raise_for_status()
            data = extract_section_text(resp.text)
            logger.info(f"Test: NRS {data['number']} - {data['title']} ({len(data['text'])} chars)")
            return len(data["text"]) > 50
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/NV-Legislation data fetcher")
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
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NVLegislationScraper()

    if args.command == "test":
        success = scraper.test_connection()
        print(f"Connection test: {'PASSED' if success else 'FAILED'}")
        sys.exit(0 if success else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command in ("bootstrap", "update"):
        count = 0
        target = 15 if args.sample else 999999

        for raw in scraper.fetch_all():
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


if __name__ == "__main__":
    main()
