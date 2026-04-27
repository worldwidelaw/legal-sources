#!/usr/bin/env python3
"""
FI/Vero-Guidance -- Finnish Tax Authority Detailed Guidance Fetcher

Fetches tax guidance, advance rulings, decisions, and statements from
Verohallinto (Finnish Tax Administration) at vero.fi.

Strategy:
  - Parse sitemap.xml to find all URLs under /syventavat-vero-ohjeet/
  - Fetch each page and extract full text from <article id="content-main">
  - ~1,780 documents total

Categories (by URL path):
  - ohje-hakusivu: Detailed guidance (~472)
  - ennakkoratkaisut: Advance rulings/KVL decisions (~1,134)
  - paatokset: Tax admin decisions (~118)
  - kannanotot: Tax admin statements (~55)

Usage:
  python bootstrap.py bootstrap          # Full fetch
  python bootstrap.py bootstrap --sample # Fetch 15 samples
  python bootstrap.py test               # Connectivity test
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
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FI.Vero-Guidance")

SITEMAP_URL = "https://www.vero.fi/sitemap.xml"
DELAY = 2.5

# URL path prefixes for guidance content
GUIDANCE_PREFIXES = [
    "/syventavat-vero-ohjeet/ohje-hakusivu/",
    "/syventavat-vero-ohjeet/ennakkoratkaisut/",
    "/syventavat-vero-ohjeet/paatokset/",
    "/syventavat-vero-ohjeet/kannanotot/",
]

CATEGORY_MAP = {
    "ohje-hakusivu": "guidance",
    "ennakkoratkaisut": "advance_ruling",
    "paatokset": "decision",
    "kannanotot": "statement",
}


def strip_html(raw_html: str) -> str:
    """Strip HTML tags and decode entities to plain text."""
    text = re.sub(r'<(style|script|noscript)[^>]*>.*?</\1>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<(br|p|div|h[1-6]|li|tr)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class VeroGuidance(BaseScraper):
    SOURCE_ID = "FI/Vero-Guidance"

    def __init__(self):
        self.http = HttpClient(base_url="https://www.vero.fi")

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return raw

    def get_guidance_urls(self) -> List[Dict[str, str]]:
        """Parse sitemap.xml to find all guidance document URLs."""
        resp = self.http.get(SITEMAP_URL)
        if not resp or resp.status_code != 200:
            logger.error("Failed to fetch sitemap")
            return []

        urls = []
        try:
            root = ET.fromstring(resp.text)
        except ET.ParseError:
            logger.error("Failed to parse sitemap XML")
            return []

        ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        for url_elem in root.findall('.//sm:url', ns):
            loc = url_elem.findtext('sm:loc', '', ns)
            lastmod = url_elem.findtext('sm:lastmod', '', ns)

            for prefix in GUIDANCE_PREFIXES:
                if prefix in loc:
                    urls.append({"url": loc, "lastmod": lastmod[:10] if lastmod else ""})
                    break

        logger.info("Found %d guidance URLs in sitemap", len(urls))
        return urls

    def fetch_and_parse(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch a guidance page and extract content."""
        try:
            resp = self.http.get(url)
            time.sleep(DELAY)
            if not resp or resp.status_code != 200:
                return None
        except Exception as e:
            logger.warning("Error fetching %s: %s", url, e)
            return None

        html_text = resp.text

        # Extract title from <h1>
        title = ""
        m = re.search(r'<h1[^>]*>(.*?)</h1>', html_text, re.DOTALL | re.IGNORECASE)
        if m:
            title = strip_html(m.group(1))

        # Extract main content from article or main-body div
        content = ""
        # Try article#content-main first
        m = re.search(r'<article[^>]*id=["\']content-main["\'][^>]*>(.*?)</article>', html_text, re.DOTALL | re.IGNORECASE)
        if m:
            content = strip_html(m.group(1))
        else:
            # Try div.main-body
            m = re.search(r'<div[^>]*class=["\'][^"\']*main-body[^"\']*["\'][^>]*>(.*?)</div>\s*(?:<div\s+class=["\'](?:footer|sidebar)|</main|</article)', html_text, re.DOTALL | re.IGNORECASE)
            if m:
                content = strip_html(m.group(1))
            else:
                # Broadest: main tag
                m = re.search(r'<main[^>]*>(.*?)</main>', html_text, re.DOTALL | re.IGNORECASE)
                if m:
                    content = strip_html(m.group(1))

        # Extract modified date
        date = ""
        m = re.search(r'class=["\'][^"\']*page-modified[^"\']*["\'][^>]*>.*?(\d{1,2}\.\d{1,2}\.\d{4})', html_text, re.DOTALL)
        if m:
            # Convert DD.MM.YYYY to YYYY-MM-DD
            parts = m.group(1).split('.')
            if len(parts) == 3:
                date = f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
        if not date:
            m = re.search(r'<time[^>]*datetime=["\']([^"\']+)', html_text)
            if m:
                date = m.group(1)[:10]

        # Determine category from URL
        category = "guidance"
        for key, cat in CATEGORY_MAP.items():
            if key in url:
                category = cat
                break

        return {
            "title": title,
            "text": content,
            "date": date,
            "category": category,
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all guidance documents."""
        urls = self.get_guidance_urls()
        if not urls:
            logger.error("No URLs found in sitemap")
            return

        sample_limit = 15 if sample else len(urls)
        total_yielded = 0

        for entry in urls:
            if total_yielded >= sample_limit:
                break

            url = entry["url"]
            lastmod = entry["lastmod"]

            result = self.fetch_and_parse(url)
            if not result or not result["text"] or len(result["text"]) < 50:
                logger.warning("No content from %s", url)
                continue

            # Create URL slug as ID
            slug = url.rstrip("/").split("/")[-1] if "/" in url else url
            # Include numeric ID if present in URL
            parts = url.rstrip("/").split("/")
            url_id = ""
            for p in parts:
                if p.isdigit():
                    url_id = p
                    break

            record = {
                "_id": f"vero-{url_id}-{slug}" if url_id else f"vero-{slug}",
                "_source": self.SOURCE_ID,
                "_type": "doctrine",
                "_fetched_at": datetime.now(timezone.utc).isoformat(),
                "title": result["title"],
                "text": result["text"],
                "date": result["date"] or lastmod,
                "url": url,
                "language": "fi",
                "category": result["category"],
                "url_slug": slug,
            }

            yield record
            total_yielded += 1

            if total_yielded % 50 == 0:
                logger.info("  Progress: %d/%d documents", total_yielded, len(urls))

        logger.info("Fetch complete. %d documents yielded from %d URLs", total_yielded, len(urls))

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch documents modified since a given date."""
        urls = self.get_guidance_urls()
        recent = [u for u in urls if u["lastmod"] >= since]
        logger.info("Found %d documents modified since %s", len(recent), since)

        for entry in recent:
            result = self.fetch_and_parse(entry["url"])
            if result and result["text"]:
                slug = entry["url"].rstrip("/").split("/")[-1]
                yield {
                    "_id": f"vero-{slug}",
                    "_source": self.SOURCE_ID,
                    "_type": "doctrine",
                    "_fetched_at": datetime.now(timezone.utc).isoformat(),
                    **result,
                    "url": entry["url"],
                    "language": "fi",
                    "url_slug": slug,
                }

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            urls = self.get_guidance_urls()
            if urls:
                logger.info("Test passed: found %d guidance URLs in sitemap", len(urls))
                return True
            logger.error("Test failed: no URLs found")
            return False
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="FI/Vero-Guidance bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = VeroGuidance()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            safe_name = re.sub(r'[^\w\-.]', '_', record['_id'])[:100]
            out_file = sample_dir / f"{safe_name}.json"
            out_file.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            count += 1
            text_len = len(record.get("text", ""))
            logger.info("  [%d] %s | %s | text=%d chars",
                        count, record["category"], record["title"][:60], text_len)

        logger.info("Bootstrap complete: %d records saved to sample/", count)
        sys.exit(0 if count >= 10 else 1)

    if args.command == "update":
        since = args.since if hasattr(args, 'since') and args.since else "2026-01-01"
        count = sum(1 for _ in scraper.fetch_updates(since))
        logger.info("Update complete: %d documents since %s", count, since)


if __name__ == "__main__":
    main()
