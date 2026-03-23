#!/usr/bin/env python3
"""
SE/SKV -- Swedish Tax Agency (Skatteverket) Regulations Fetcher

Fetches SKVFS (Skatteverkets författningssamling) regulations from lagen.nu.

Strategy:
  - Use Atom feed to discover all SKVFS documents with metadata
  - Follow prev-archive links to get complete listing
  - For each document, fetch HTML page and extract full text from textbox elements
  - Text is OCR-extracted from PDF originals by lagen.nu's Ferenda system

Endpoints:
  - Atom feed: https://lagen.nu/dataset/myndfs/feed.atom?dcterms_publisher=publisher/skatteverket
  - HTML pages: https://lagen.nu/skvfs/{YEAR}:{NUMBER}
  - JSON metadata: https://lagen.nu/skvfs/{YEAR}:{NUMBER}.json

Data:
  - ~500+ SKVFS regulations from 2004 to present
  - Language: Swedish (SV)
  - Types: Tax regulations, guidelines, tables
  - Rate limit: 1 request/second

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as html_module
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SE.skv")

BASE_URL = "https://lagen.nu"
ATOM_FEED_URL = "https://lagen.nu/dataset/myndfs/feed.atom?dcterms_publisher=publisher/skatteverket"
ATOM_NS = "http://www.w3.org/2005/Atom"


class SwedishSKVScraper(BaseScraper):
    """
    Scraper for SE/SKV -- Swedish Tax Agency regulations (SKVFS).
    Country: SE
    URL: https://lagen.nu

    Data types: doctrine
    Auth: none (Open public access via lagen.nu)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "application/atom+xml, text/html, application/json",
                "Accept-Language": "sv,en",
            },
            timeout=60,
        )

    def _fetch_atom_entries(self) -> List[Dict[str, Any]]:
        """
        Fetch all SKVFS entries from the Atom feed, following prev-archive links.
        Returns list of dicts with: id, title, published, updated.
        """
        entries = []
        feed_url = ATOM_FEED_URL
        seen_archives = set()
        max_pages = 20  # Safety limit

        while feed_url and len(seen_archives) < max_pages:
            logger.info(f"Fetching Atom feed: {feed_url}")
            self.rate_limiter.wait()

            try:
                resp = self.client.session.get(feed_url, timeout=60, headers={
                    "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                    "Accept": "application/atom+xml, text/xml",
                })
                resp.raise_for_status()
            except Exception as e:
                logger.error(f"Failed to fetch Atom feed {feed_url}: {e}")
                break

            seen_archives.add(feed_url)

            try:
                root = ET.fromstring(resp.content)
            except ET.ParseError as e:
                logger.error(f"Failed to parse Atom XML: {e}")
                break

            # Parse entries
            for entry in root.findall(f"{{{ATOM_NS}}}entry"):
                entry_id = entry.findtext(f"{{{ATOM_NS}}}id", "")
                title_raw = entry.findtext(f"{{{ATOM_NS}}}title", "")
                published = entry.findtext(f"{{{ATOM_NS}}}published", "")
                updated = entry.findtext(f"{{{ATOM_NS}}}updated", "")

                # Extract SKVFS number from title (e.g., "SKVFS 2025:3: ...")
                skvfs_match = re.match(r"(SKVFS\s+\d{4}:\d+):\s*(.*)", title_raw)
                if skvfs_match:
                    skvfs_number = skvfs_match.group(1)
                    title = skvfs_match.group(2).strip()
                else:
                    skvfs_number = title_raw.split(":")[0].strip() if ":" in title_raw else title_raw
                    title = title_raw

                # Skip entries with missing title
                if "(Titel saknas)" in title:
                    title = skvfs_number  # Use SKVFS number as title

                entries.append({
                    "id": entry_id,
                    "skvfs_number": skvfs_number,
                    "title": title,
                    "published": published[:10] if published else "",
                    "updated": updated[:10] if updated else "",
                })

            # Follow prev-archive link
            feed_url = None
            for link in root.findall(f"{{{ATOM_NS}}}link"):
                if link.get("rel") == "prev-archive":
                    href = link.get("href", "")
                    if href:
                        # Resolve relative URL
                        if href.startswith("http"):
                            feed_url = href
                        else:
                            # Build absolute URL from the feed base
                            feed_url = f"https://lagen.nu/dataset/myndfs/{href}"
                        if feed_url in seen_archives:
                            feed_url = None
                        break

            logger.info(f"Collected {len(entries)} entries so far")

        logger.info(f"Total Atom entries: {len(entries)}")
        return entries

    def _fetch_document_html(self, doc_url: str) -> Optional[str]:
        """
        Fetch a document page and extract full text from textbox elements.
        """
        try:
            self.rate_limiter.wait()
            resp = self.client.session.get(doc_url, timeout=60, headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "text/html",
            })
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch document {doc_url}: {e}")
            return None

    def _extract_text_from_html(self, html_content: str) -> str:
        """
        Extract full text from lagen.nu HTML page.
        Text is in <p class="textbox ..."> elements within <div class="pdfpage"> sections.
        """
        # Extract all textbox paragraphs
        textboxes = re.findall(
            r'<p\s+class="textbox[^"]*"[^>]*>(.*?)</p>',
            html_content,
            re.DOTALL,
        )

        if not textboxes:
            # Fallback: try extracting from <article> content
            article_match = re.search(
                r'<article[^>]*>(.*?)</article>',
                html_content,
                re.DOTALL,
            )
            if article_match:
                text = re.sub(r'<[^>]+>', ' ', article_match.group(1))
                text = html_module.unescape(text)
                text = re.sub(r'\s+', ' ', text).strip()
                return text
            return ""

        # Clean each textbox: strip tags, decode entities
        lines = []
        for tb in textboxes:
            # Strip HTML tags
            text = re.sub(r'<[^>]+>', '', tb)
            text = html_module.unescape(text)
            text = text.strip()
            if text:
                lines.append(text)

        # Join with spaces, then clean up excessive whitespace
        full_text = "\n".join(lines)
        # Remove page numbers that appear alone on a line
        full_text = re.sub(r'\n\d+\s*\n', '\n', full_text)
        return full_text.strip()

    def _extract_metadata_from_html(self, html_content: str) -> Dict[str, str]:
        """Extract additional metadata from the HTML page."""
        metadata = {}

        # Extract title from <h2> in section#top
        h2_match = re.search(r'<section[^>]*id="top"[^>]*>.*?<h2>(.*?)</h2>', html_content, re.DOTALL)
        if h2_match:
            title = re.sub(r'<[^>]+>', '', h2_match.group(1))
            metadata["title"] = html_module.unescape(title).strip()

        # Extract source link to skatteverket.se
        source_match = re.search(
            r'href="(https://www4\.skatteverket\.se/[^"]*)"[^>]*>Källa',
            html_content,
        )
        if source_match:
            metadata["source_url"] = source_match.group(1)

        # Extract "Senast hämtad" date
        fetched_match = re.search(r'Senast hämtad:\s*(\d{4}-\d{2}-\d{2})', html_content)
        if fetched_match:
            metadata["last_fetched"] = fetched_match.group(1)

        return metadata

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all SKVFS documents."""
        entries = self._fetch_atom_entries()

        for i, entry in enumerate(entries):
            doc_url = entry["id"]  # e.g., https://lagen.nu/skvfs/2025:3
            logger.info(f"[{i+1}/{len(entries)}] Fetching {entry['skvfs_number']}: {doc_url}")

            html_content = self._fetch_document_html(doc_url)
            if not html_content:
                logger.warning(f"No HTML content for {doc_url}")
                continue

            text = self._extract_text_from_html(html_content)
            if not text or len(text) < 50:
                logger.warning(f"Insufficient text for {doc_url} ({len(text) if text else 0} chars)")
                continue

            html_metadata = self._extract_metadata_from_html(html_content)

            yield {
                "id": doc_url,
                "skvfs_number": entry["skvfs_number"],
                "title": html_metadata.get("title", entry["title"]),
                "text": text,
                "published": entry["published"],
                "updated": entry["updated"],
                "source_url": html_metadata.get("source_url", ""),
            }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents updated since the given date."""
        entries = self._fetch_atom_entries()
        since_str = since.strftime("%Y-%m-%d")

        for entry in entries:
            if entry.get("updated", "") >= since_str or entry.get("published", "") >= since_str:
                doc_url = entry["id"]
                logger.info(f"Update: Fetching {entry['skvfs_number']}")

                html_content = self._fetch_document_html(doc_url)
                if not html_content:
                    continue

                text = self._extract_text_from_html(html_content)
                if not text or len(text) < 50:
                    continue

                html_metadata = self._extract_metadata_from_html(html_content)

                yield {
                    "id": doc_url,
                    "skvfs_number": entry["skvfs_number"],
                    "title": html_metadata.get("title", entry["title"]),
                    "text": text,
                    "published": entry["published"],
                    "updated": entry["updated"],
                    "source_url": html_metadata.get("source_url", ""),
                }

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        skvfs_number = raw.get("skvfs_number", "")
        doc_id = f"SE_SKV_{skvfs_number.replace(' ', '_').replace(':', '_')}"

        return {
            "_id": doc_id,
            "_source": "SE/SKV",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", skvfs_number),
            "text": raw.get("text", ""),
            "date": raw.get("published", ""),
            "url": raw.get("id", ""),
            "skvfs_number": skvfs_number,
            "decision_date": raw.get("published", ""),
            "source_url": raw.get("source_url", ""),
            "language": "sv",
        }

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            resp = self.client.session.get(ATOM_FEED_URL, timeout=30, headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "application/atom+xml",
            })
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            entries = root.findall(f"{{{ATOM_NS}}}entry")
            logger.info(f"Connectivity OK: Atom feed has {len(entries)} entries on first page")

            # Test fetching one document
            if entries:
                first_id = entries[0].findtext(f"{{{ATOM_NS}}}id", "")
                if first_id:
                    html = self._fetch_document_html(first_id)
                    if html:
                        text = self._extract_text_from_html(html)
                        logger.info(f"Document text extraction OK: {len(text)} chars from {first_id}")
                        return True
            return True
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            return False


# ── CLI entry point ──────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="SE/SKV Skatteverket data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (10 records)")
    args = parser.parse_args()

    scraper = SwedishSKVScraper()

    if args.command == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample)
        print(json.dumps(stats, indent=2))
    elif args.command == "update":
        stats = scraper.update()
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
