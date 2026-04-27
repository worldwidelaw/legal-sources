#!/usr/bin/env python3
"""
MT/MFSA-Enforcement -- Malta Financial Services Authority Enforcement Actions

Fetches enforcement actions (administrative penalties, settlement notices,
regulatory sanctions) from the MFSA website.

Strategy:
  - Uses the WordPress RSS feed at /feed/?post_type=publication&paged=N
  - Full text available in <content:encoded> CDATA field
  - Filters publications by title/content patterns for enforcement-related entries
  - Also fetches the pre-2020 archive page via WP REST API (page id 40120)

Endpoints:
  - RSS feed: https://www.mfsa.mt/feed/?post_type=publication&paged={N}
  - Archive page: https://www.mfsa.mt/?rest_route=/wp/v2/pages/40120

Data:
  - Administrative penalties and measures
  - Settlement notices
  - Enforcement actions and sanctions
  - Regulatory warnings

License: Public regulatory data (Malta)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import html
import logging
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from email.utils import parsedate_to_datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MT.MFSA-Enforcement")

BASE_URL = "https://www.mfsa.mt"

# RSS feed for all publications
RSS_FEED_URL = "/feed/"

# Patterns that identify enforcement-related publications
ENFORCEMENT_PATTERNS = [
    r"administrative\s+(measures?\s+and\s+)?penalt",
    r"settlement\s+notice",
    r"regulatory\s+action",
    r"enforcement\s+action",
    r"penalty\s+notice",
    r"administrative\s+measure",
    r"sanction",
    r"\bfine\b.*imposed",
    r"supervisory\s+measure",
    r"precautionary\s+measure",
    r"suspension\s+of\s+licen[cs]e",
    r"revocation\s+of\s+licen[cs]e",
    r"withdrawal\s+of\s+licen[cs]e",
    r"cancellation\s+of\s+licen[cs]e",
    r"prohibition\s+order",
    r"cease\s+and\s+desist",
    r"reprimand",
    r"ref:\s*\d{4}-\d+",  # Reference numbers like Ref: 2024-06
    r"public\s+statement.*breach",
    r"directive\s+issued",
    r"warning.*(?:unauthori[sz]ed|unlicen[cs]ed)",
    r"investor\s+(?:alert|warning)",
    r"consumer\s+(?:alert|warning|notice)",
]

ENFORCEMENT_RE = re.compile("|".join(ENFORCEMENT_PATTERNS), re.IGNORECASE)

# Namespaces used in WordPress RSS
RSS_NS = {
    "content": "http://purl.org/rss/1.0/modules/content/",
    "dc": "http://purl.org/dc/elements/1.1/",
    "atom": "http://www.w3.org/2005/Atom",
    "wfw": "http://wellformedweb.org/CommentAPI/",
}


class MFSAEnforcementScraper(BaseScraper):
    """
    Scraper for MT/MFSA-Enforcement -- MFSA Enforcement Actions.
    Country: MT
    URL: https://www.mfsa.mt/enforcement/

    Data types: doctrine
    Auth: none (public regulatory data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/rss+xml, application/xml, text/xml",
            },
            timeout=60,
        )

    def _clean_html(self, text: str) -> str:
        """Remove HTML tags and clean up text."""
        if not text:
            return ""
        # Remove HTML tags
        text = re.sub(r"<[^>]+>", " ", text)
        # Decode HTML entities
        text = html.unescape(text)
        # Remove WordPress boilerplate "The post ... appeared first on MFSA."
        text = re.sub(r"The post\s+.*?appeared first on\s+MFSA\s*\.?\s*$", "", text, flags=re.DOTALL)
        # Normalize whitespace but preserve paragraph breaks
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
        return text.strip()

    def _extract_pdf_urls(self, html_content: str) -> list:
        """Extract PDF URLs from HTML content."""
        urls = re.findall(r'href=["\']([^"\']+\.pdf)["\']', html_content, re.IGNORECASE)
        return [u for u in urls if "mfsa.mt" in u or u.startswith("/")]

    def _fetch_pdf_text(self, pdf_url: str) -> str:
        """Download a PDF and extract text."""
        try:
            if pdf_url.startswith("/"):
                pdf_url = BASE_URL + pdf_url
            self.rate_limiter.wait()
            resp = self.client.get(pdf_url, headers={"Accept": "application/pdf"})
            if resp.status_code != 200:
                return ""
            return extract_pdf_markdown(
                source="MT/MFSA-Enforcement",
                source_id="",
                pdf_bytes=resp.content,
                table="doctrine",
            ) or ""
        except Exception as e:
            logger.debug(f"PDF extraction failed for {pdf_url}: {e}")
            return ""

    def _is_enforcement_related(self, title: str, content: str) -> bool:
        """Check if a publication is enforcement-related based on title/content."""
        combined = f"{title} {content[:2000]}"
        return bool(ENFORCEMENT_RE.search(combined))

    def _parse_rss_page(self, page: int) -> list:
        """Fetch and parse one page of the RSS feed. Returns list of raw items."""
        params = {"post_type": "publication", "paged": str(page)}
        try:
            self.rate_limiter.wait()
            resp = self.client.get(RSS_FEED_URL, params=params)
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch RSS page {page}: {e}")
            return []

        try:
            root = ET.fromstring(resp.content)
        except ET.ParseError as e:
            logger.warning(f"Failed to parse RSS page {page}: {e}")
            return []

        channel = root.find("channel")
        if channel is None:
            return []

        items = []
        for item in channel.findall("item"):
            title_el = item.find("title")
            link_el = item.find("link")
            pub_date_el = item.find("pubDate")
            desc_el = item.find("description")
            content_el = item.find("content:encoded", RSS_NS)

            title = title_el.text.strip() if title_el is not None and title_el.text else ""
            link = link_el.text.strip() if link_el is not None and link_el.text else ""
            pub_date = pub_date_el.text.strip() if pub_date_el is not None and pub_date_el.text else ""
            description = desc_el.text.strip() if desc_el is not None and desc_el.text else ""
            content_html = content_el.text.strip() if content_el is not None and content_el.text else ""

            # Get categories
            categories = []
            for cat_el in item.findall("category"):
                if cat_el.text:
                    categories.append(cat_el.text.strip())

            items.append({
                "title": title,
                "link": link,
                "pub_date": pub_date,
                "description": description,
                "content_html": content_html,
                "categories": categories,
            })

        return items

    def _fetch_archive_page(self) -> list:
        """Fetch the pre-2020 enforcement archive via WP REST API."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get("/?rest_route=/wp/v2/pages/40120")
            if resp.status_code != 200:
                logger.warning(f"Archive page returned {resp.status_code}")
                return []

            data = resp.json()
            content_html = data.get("content", {}).get("rendered", "")
            if not content_html:
                return []

            # The archive page contains multiple enforcement entries inline.
            # Split by <h3> or <h4> headings which separate entries.
            entries = []
            # Split on headings that look like enforcement entry titles
            parts = re.split(r"<h[34][^>]*>", content_html)

            for i, part in enumerate(parts):
                if i == 0:
                    continue  # Skip intro text before first heading

                # Extract the heading text (it was split off)
                heading_match = re.match(r"(.*?)</h[34]>", part, re.DOTALL)
                if not heading_match:
                    continue

                heading = self._clean_html(heading_match.group(1))
                body = part[heading_match.end():]

                # Get text until next heading or end
                body_text = self._clean_html(body)

                if len(body_text) < 50:
                    continue

                if not self._is_enforcement_related(heading, body_text):
                    continue

                entries.append({
                    "title": heading,
                    "link": f"https://www.mfsa.mt/enforcement/administrative-penalties/archive/#{i}",
                    "pub_date": "",
                    "description": body_text[:500],
                    "content_html": body,
                    "categories": ["Archive", "Enforcement"],
                    "text_clean": f"{heading}\n\n{body_text}",
                    "is_archive": True,
                })

            logger.info(f"Found {len(entries)} enforcement entries in archive page")
            return entries

        except Exception as e:
            logger.warning(f"Failed to fetch archive page: {e}")
            return []

    def _iterate_rss(
        self, sample_mode: bool = False, sample_size: int = 12
    ) -> Generator[Dict[str, Any], None, None]:
        """Iterate through all RSS feed pages, yielding enforcement-related items."""
        count = 0
        page = 1
        max_pages = 500  # Safety limit
        empty_pages = 0

        while page <= max_pages:
            logger.info(f"Fetching RSS page {page}...")
            items = self._parse_rss_page(page)

            if not items:
                empty_pages += 1
                if empty_pages >= 3:
                    logger.info(f"Stopping after {empty_pages} consecutive empty pages")
                    break
                page += 1
                continue

            empty_pages = 0

            for item in items:
                title = item["title"]
                content_html = item["content_html"]
                content_text = self._clean_html(content_html)

                # Filter for enforcement-related content
                if not self._is_enforcement_related(title, content_text):
                    continue

                # If inline text is thin, try to extract from linked PDF
                if len(content_text) < 200:
                    pdf_urls = self._extract_pdf_urls(content_html)
                    for pdf_url in pdf_urls[:1]:  # Only try first PDF
                        pdf_text = self._fetch_pdf_text(pdf_url)
                        if pdf_text and len(pdf_text) > len(content_text):
                            content_text = pdf_text
                            break

                if len(content_text) < 50:
                    logger.debug(f"Skipping thin content: {title}")
                    continue

                item["text_clean"] = content_text
                count += 1
                yield item

                if sample_mode and count >= sample_size:
                    logger.info(f"Sample mode: collected {count} records")
                    return

            page += 1

        # Also fetch the pre-2020 archive
        if not sample_mode or count < sample_size:
            logger.info("Fetching pre-2020 archive page...")
            archive_items = self._fetch_archive_page()
            for item in archive_items:
                count += 1
                yield item
                if sample_mode and count >= sample_size:
                    return

        logger.info(f"Total enforcement items found: {count}")

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all enforcement action documents."""
        for doc in self._iterate_rss(sample_mode=False):
            yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents published since the given date."""
        for doc in self._iterate_rss(sample_mode=False):
            pub_date_str = doc.get("pub_date", "")
            if pub_date_str:
                try:
                    pub_date = parsedate_to_datetime(pub_date_str)
                    if pub_date >= since:
                        yield doc
                    else:
                        # RSS is reverse-chronological; if we've passed the date, stop
                        return
                except Exception:
                    yield doc
            else:
                yield doc

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw RSS item into standard schema."""
        title = self._clean_html(raw.get("title", ""))
        if not title:
            return None

        # Get full text
        text = raw.get("text_clean", "")
        if not text:
            content_html = raw.get("content_html", "")
            text = self._clean_html(content_html)

        if len(text) < 50:
            logger.warning(f"Insufficient text for: {title}")
            return None

        # Parse date
        date_str = ""
        pub_date = raw.get("pub_date", "")
        if pub_date:
            try:
                dt = parsedate_to_datetime(pub_date)
                date_str = dt.strftime("%Y-%m-%d")
            except Exception:
                date_str = ""

        # Build stable ID from URL or title
        link = raw.get("link", "")
        if link:
            # Extract slug from URL
            slug = link.rstrip("/").split("/")[-1]
            doc_id = slug
        else:
            doc_id = re.sub(r"[^a-zA-Z0-9]+", "_", title)[:100]

        # Determine category
        categories = raw.get("categories", [])
        category = "enforcement"
        for cat in categories:
            cat_lower = cat.lower()
            if "settlement" in cat_lower:
                category = "settlement_notice"
            elif "penalt" in cat_lower:
                category = "administrative_penalty"
            elif "warning" in cat_lower:
                category = "warning"

        url = link or "https://www.mfsa.mt/enforcement/"

        return {
            "_id": doc_id,
            "_source": "MT/MFSA-Enforcement",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": url,
            "category": category,
            "description": self._clean_html(raw.get("description", ""))[:500],
            "language": "en",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing MFSA enforcement endpoints...")

        print("\n1. Testing RSS feed (page 1)...")
        try:
            items = self._parse_rss_page(1)
            print(f"   Items on page 1: {len(items)}")
            if items:
                print(f"   First title: {items[0]['title'][:80]}")
                content = self._clean_html(items[0].get("content_html", ""))
                print(f"   Content length: {len(content)} chars")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\n2. Testing enforcement filter on page 1...")
        enforcement_count = 0
        for item in items:
            title = item["title"]
            content = self._clean_html(item.get("content_html", ""))
            if self._is_enforcement_related(title, content):
                enforcement_count += 1
                print(f"   [MATCH] {title[:80]}")
        print(f"   Enforcement items on page 1: {enforcement_count}/{len(items)}")

        print("\n3. Testing archive page (pre-2020)...")
        try:
            archive = self._fetch_archive_page()
            print(f"   Archive entries: {len(archive)}")
            if archive:
                print(f"   First: {archive[0]['title'][:80]}")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = MFSAEnforcementScraper()

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
