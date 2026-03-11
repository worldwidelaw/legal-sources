#!/usr/bin/env python3
"""
PT/DiarioRepublica -- Portuguese Official Journal Data Fetcher

Fetches Portuguese legislation from the Diário da República via dre.tretas.org,
a community mirror that provides structured access to official gazette content.

Strategy:
  - Uses dre.tretas.org RSS feed for document discovery (recent updates).
  - Browse by date for historical document discovery.
  - Full text: HTML pages at /dre/{id}/{slug} contain complete document text.
  - JSON-LD metadata at /dre/{id}.jsonld for structured metadata.

Endpoints:
  - RSS feed: https://dre.tretas.org/dre/rss/
  - Browse by date: https://dre.tretas.org/dre/data/{yyyy}/{m}/{d}/
  - Document: https://dre.tretas.org/dre/{id}/{slug}
  - JSON-LD: https://dre.tretas.org/dre/{id}.jsonld

Data:
  - Legislation types: Decreto-Lei, Lei, Portaria, Resolução, etc.
  - Coverage: Series I (legislation) from 1910+
  - Official source: https://diariodarepublica.pt
  - License: Open Government Data (mirrored from official sources)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent RSS items)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List
import xml.etree.ElementTree as ET

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PT.diariorepublica")

# Base URL for tretas.org mirror (community archive with full text)
BASE_URL = "https://dre.tretas.org"

# Official ELI base URL (for reference links)
OFFICIAL_URL = "https://data.dre.pt"


class DiarioRepublicaScraper(BaseScraper):
    """
    Scraper for PT/DiarioRepublica -- Portuguese Official Journal.
    Country: PT
    URL: https://diariodarepublica.pt (official), https://dre.tretas.org (mirror)

    Data types: legislation
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept-Language": "pt,en",
            },
            timeout=60,
        )

    def _parse_rss_feed(self, xml_content: str) -> List[Dict[str, Any]]:
        """
        Parse the RSS feed to extract document references.

        Returns a list of dicts with: id, title, description, date, url
        """
        documents = []

        try:
            root = ET.fromstring(xml_content)
            channel = root.find("channel")
            if channel is None:
                return documents

            for item in channel.findall("item"):
                try:
                    title = item.findtext("title", "").strip()
                    link = item.findtext("link", "").strip()
                    description = item.findtext("description", "").strip()
                    pub_date = item.findtext("pubDate", "").strip()
                    guid = item.findtext("guid", "").strip()

                    # Extract creator (entity that issued the document)
                    creator = ""
                    for elem in item:
                        if elem.tag.endswith("creator"):
                            creator = elem.text or ""
                            break

                    # Extract document ID from link or guid
                    doc_id = guid if guid.isdigit() else ""
                    if not doc_id:
                        # Try to extract from link: /dre/{id}/{slug}
                        match = re.search(r'/dre/(\d+)/', link)
                        if match:
                            doc_id = match.group(1)

                    if not doc_id:
                        continue

                    # Parse date from pubDate
                    date_str = ""
                    if pub_date:
                        # Format: "Tue, 10 Feb 2026 00:00:00 +0000"
                        try:
                            dt = datetime.strptime(pub_date[:16], "%a, %d %b %Y")
                            date_str = dt.strftime("%Y-%m-%d")
                        except:
                            pass

                    documents.append({
                        "id": doc_id,
                        "title": title,
                        "description": description,
                        "date": date_str,
                        "url": link,
                        "creator": creator,
                    })

                except Exception as e:
                    logger.warning(f"Failed to parse RSS item: {e}")
                    continue

        except ET.ParseError as e:
            logger.error(f"Failed to parse RSS XML: {e}")

        return documents

    def _fetch_document_text(self, doc_id: str) -> Dict[str, Any]:
        """
        Fetch full text and metadata for a document by ID.

        Returns dict with: full_text, metadata from JSON-LD
        """
        result = {"full_text": "", "metadata": {}}

        try:
            # First, get JSON-LD for structured metadata
            self.rate_limiter.wait()
            jsonld_url = f"/dre/{doc_id}.jsonld"
            resp = self.client.get(jsonld_url)
            if resp.status_code == 200:
                result["metadata"] = resp.json()
        except Exception as e:
            logger.warning(f"Failed to fetch JSON-LD for {doc_id}: {e}")

        # Now fetch the HTML page for full text
        try:
            # Get document URL from metadata if available
            doc_url = result["metadata"].get("@id", "")
            if not doc_url or not doc_url.startswith(BASE_URL):
                # Try to find a working URL from metadata
                doc_url = f"/dre/{doc_id}/"
            else:
                doc_url = doc_url.replace(BASE_URL, "")

            self.rate_limiter.wait()
            resp = self.client.get(doc_url)
            resp.raise_for_status()

            content = resp.content.decode("utf-8", errors="replace")
            full_text = self._extract_text_from_html(content)
            result["full_text"] = full_text

        except Exception as e:
            logger.warning(f"Failed to fetch document HTML for {doc_id}: {e}")

        return result

    def _extract_text_from_html(self, html_content: str) -> str:
        """
        Extract full text from the document HTML page.

        The main content is in <div itemprop="articleBody" class="result_notes">
        """
        # Look for the article body
        article_match = re.search(
            r'<div itemprop="articleBody"[^>]*>(.*?)</div>\s*(?:<h2|<div class="only-mobile)',
            html_content,
            re.DOTALL | re.IGNORECASE
        )

        if article_match:
            article_html = article_match.group(1)
        else:
            # Fallback: look for text after "Texto do documento" heading
            text_section = re.search(
                r'<h2><a name="text">.*?</h2>\s*(.*?)(?:<h2>|<div class="only-mobile)',
                html_content,
                re.DOTALL | re.IGNORECASE
            )
            if text_section:
                article_html = text_section.group(1)
            else:
                return ""

        # Clean up HTML to extract plain text
        text = article_html

        # Remove internal links but keep text
        text = re.sub(r'<a[^>]*>([^<]*)</a>', r'\1', text)

        # Convert <br> and </p> to newlines
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)

        # Remove all remaining HTML tags
        text = re.sub(r'<[^>]+>', '', text)

        # Decode HTML entities
        text = html.unescape(text)

        # Clean up whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' +', ' ', text)

        # Strip lines
        lines = [line.strip() for line in text.split('\n')]
        text = '\n'.join(lines)

        return text.strip()

    def _browse_by_date(self, year: int, month: int, day: int) -> List[Dict[str, Any]]:
        """
        Browse documents published on a specific date.

        Returns list of document references.
        """
        documents = []
        url = f"/dre/data/{year}/{month}/{day}/"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)

            if resp.status_code == 404:
                return documents

            resp.raise_for_status()
            content = resp.content.decode("utf-8", errors="replace")

            # Extract document links: href="/dre/{id}/{slug}"
            for match in re.finditer(r'href="/dre/(\d+)/([^"]+)"', content):
                doc_id = match.group(1)
                slug = match.group(2)

                # Extract title from the link text (next in HTML)
                title_match = re.search(
                    rf'/dre/{doc_id}/{slug}"[^>]*>\s*(?:<[^>]+>)*([^<]+)',
                    content
                )
                title = ""
                if title_match:
                    title = title_match.group(1).strip()
                    title = html.unescape(title)

                documents.append({
                    "id": doc_id,
                    "slug": slug,
                    "title": title,
                    "date": f"{year:04d}-{month:02d}-{day:02d}",
                    "url": f"{BASE_URL}/dre/{doc_id}/{slug}",
                })

        except Exception as e:
            logger.warning(f"Failed to browse date {year}/{month}/{day}: {e}")

        # Remove duplicates by ID
        seen = set()
        unique = []
        for doc in documents:
            if doc["id"] not in seen:
                seen.add(doc["id"])
                unique.append(doc)

        return unique

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from the Portuguese Official Journal.

        Iterates through dates, fetching document listings and then
        full text for each document.
        """
        # Start from recent dates and work backwards
        end_date = datetime.now()
        start_date = datetime(2020, 1, 1)  # Start from 2020 for initial fetch

        current = end_date
        while current >= start_date:
            logger.info(f"Fetching documents from {current.date()}...")

            documents = self._browse_by_date(
                current.year, current.month, current.day
            )

            for doc in documents:
                # Fetch full text for each document
                doc_data = self._fetch_document_text(doc["id"])

                if not doc_data["full_text"]:
                    logger.warning(f"No full text for {doc['id']}, skipping")
                    continue

                doc["full_text"] = doc_data["full_text"]
                doc["metadata"] = doc_data["metadata"]
                yield doc

            # Move to previous day
            current -= timedelta(days=1)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents updated since the given date.

        Uses the RSS feed for recent updates.
        """
        logger.info("Fetching RSS feed for recent updates...")

        try:
            self.rate_limiter.wait()
            resp = self.client.get("/dre/rss/")
            resp.raise_for_status()

            documents = self._parse_rss_feed(resp.text)
            logger.info(f"Found {len(documents)} documents in RSS feed")

            for doc in documents:
                # Parse date and filter
                if doc.get("date"):
                    try:
                        doc_date = datetime.strptime(doc["date"], "%Y-%m-%d")
                        doc_date = doc_date.replace(tzinfo=timezone.utc)
                        if doc_date < since:
                            continue
                    except:
                        pass

                # Fetch full text
                doc_data = self._fetch_document_text(doc["id"])

                if not doc_data["full_text"]:
                    logger.warning(f"No full text for {doc['id']}, skipping")
                    continue

                doc["full_text"] = doc_data["full_text"]
                doc["metadata"] = doc_data["metadata"]
                yield doc

        except Exception as e:
            logger.error(f"Failed to fetch RSS feed: {e}")

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        doc_id = raw.get("id", "")
        title = raw.get("title", "")
        full_text = raw.get("full_text", "")
        date_str = raw.get("date", "")
        url = raw.get("url", f"{BASE_URL}/dre/{doc_id}/")
        metadata = raw.get("metadata", {})

        # Ensure metadata is a dict
        if not isinstance(metadata, dict):
            metadata = {}

        # Extract additional info from metadata
        doc_type = metadata.get("legislationType", "")
        if not doc_type and title:
            # Try to extract from title (e.g., "Decreto-Lei 34/2024")
            type_match = re.match(r'^([\w-]+)\s+\d', title)
            if type_match:
                doc_type = type_match.group(1)

        creator = raw.get("creator", "") or metadata.get("legislationPassedBy", "")
        summary = raw.get("description", "") or metadata.get("abstract", "")

        # Get official ELI URI if available
        eli_uri = ""
        encodings = metadata.get("encoding", [])
        for enc in encodings:
            if isinstance(enc, dict) and enc.get("legislationLegalValue", {}).get("@id") == "https://schema.org/OfficialLegalValue":
                eli_uri = enc.get("@id", "")
                break

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "PT/DiarioRepublica",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_str,
            "url": url,
            # Additional metadata
            "document_type": doc_type,
            "creator": creator,
            "summary": summary,
            "eli_uri": eli_uri,
            "official_url": f"{OFFICIAL_URL}/eli/{doc_id}" if doc_id else "",
            "language": "pt",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Portuguese Diário da República endpoints (via dre.tretas.org)...")

        # Test RSS feed
        print("\n1. Testing RSS feed...")
        try:
            resp = self.client.get("/dre/rss/")
            print(f"   Status: {resp.status_code}")
            documents = self._parse_rss_feed(resp.text)
            print(f"   Found {len(documents)} documents in feed")
            if documents:
                print(f"   Sample: {documents[0].get('title', 'N/A')[:60]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test browse by date
        print("\n2. Testing browse by date...")
        try:
            today = datetime.now()
            docs = self._browse_by_date(today.year, today.month, today.day)
            print(f"   Found {len(docs)} documents for today")
            if not docs:
                # Try yesterday
                yesterday = today - timedelta(days=1)
                docs = self._browse_by_date(yesterday.year, yesterday.month, yesterday.day)
                print(f"   Found {len(docs)} documents for yesterday")
            if docs:
                print(f"   Sample: {docs[0].get('title', 'N/A')[:60]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test document text fetch
        print("\n3. Testing document text fetch...")
        try:
            # Use a known document ID
            test_id = "5752809"  # Decreto-Lei 34/2024
            doc_data = self._fetch_document_text(test_id)
            print(f"   Text length: {len(doc_data['full_text'])} characters")
            if doc_data["full_text"]:
                print(f"   Sample: {doc_data['full_text'][:200]}...")
            if doc_data["metadata"]:
                print(f"   Metadata: {doc_data['metadata'].get('name', 'N/A')}")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = DiarioRepublicaScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
