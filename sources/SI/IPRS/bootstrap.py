#!/usr/bin/env python3
"""
SI/IPRS -- Slovenian Data Protection Authority (Informacijski pooblaščenec)

Fetches data protection opinions from ip-rs.si.

Collections:
  - mnenja-gdpr: GDPR opinions (~4,248)
  - mnenja-zvop-2: ZVOP-2 (Slovenian DPA law) opinions (~2,124)
  - iskalnik-po-odlocbah: Freedom of information decisions (~6,004)

Strategy:
  - Paginate search results (offset-based, 20 per page for opinions, 10 for decisions)
  - Extract links from HTML table rows
  - Fetch individual opinion pages for full text
  - Clean HTML to plain text

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 10+ sample records
  python bootstrap.py bootstrap --full     # Fetch all records
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.parse import urlencode, quote, urljoin

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SI.IPRS")

BASE_URL = "https://www.ip-rs.si"
RATE_LIMIT_SECONDS = 2

# Collections to scrape
COLLECTIONS = [
    {
        "name": "mnenja-gdpr",
        "label": "GDPR Opinions",
        "path": "/mnenja-gdpr/",
        "per_page": 20,
        "doc_type": "doctrine",
    },
    {
        "name": "mnenja-zvop-2",
        "label": "ZVOP-2 Opinions",
        "path": "/mnenja-zvop-2/",
        "per_page": 20,
        "doc_type": "doctrine",
    },
    {
        "name": "odlocbe-ijz",
        "label": "Freedom of Information Decisions",
        "path": "/informacije-javnega-znacaja/iskalnik-po-odlocbah/",
        "per_page": 10,
        "doc_type": "doctrine",
    },
]


class SlovenianDPAScraper(BaseScraper):
    """
    Scraper for SI/IPRS -- Slovenian Information Commissioner.
    Country: SI
    URL: https://www.ip-rs.si
    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "sl,en",
            },
            timeout=60,
        )

    def _fetch_page(self, path: str, offset: int = 0) -> str:
        """Fetch a listing page with offset pagination."""
        sep = "&" if "?" in path else "?"
        url = f"{BASE_URL}{path}{sep}offset={offset}"
        logger.info(f"Fetching listing: {url}")

        time.sleep(RATE_LIMIT_SECONDS)
        resp = self.client.session.get(url, timeout=60)
        resp.raise_for_status()
        return resp.text

    def _parse_listing(self, html_content: str, collection: dict) -> Tuple[List[dict], int]:
        """
        Parse a listing page to extract result entries and total count.
        Returns (list of result dicts, total count).
        """
        results = []
        total = 0

        # Extract total count from "X - Y / TOTAL" pattern
        total_match = re.search(r'(\d[\d\s.]*)\s*-\s*\d[\d\s.]*\s*/\s*(\d[\d\s.]*)', html_content)
        if total_match:
            total_str = total_match.group(2).replace(".", "").replace(" ", "").strip()
            try:
                total = int(total_str)
            except ValueError:
                pass

        # Extract table rows - each row has: date, title (with link), reference, categories
        # The links use /go?u=... redirect pattern or direct hrefs
        # Pattern: <td>DATE</td> ... <a href="...">TITLE</a> ... <td>REFERENCE</td> ... <td>CATEGORIES</td>
        row_pattern = re.compile(
            r'<tr[^>]*>\s*'
            r'<td[^>]*>\s*(\d{1,2}\.\d{1,2}\.\d{4})\s*</td>\s*'  # date
            r'<td[^>]*>\s*<a\s+href="([^"]+)"[^>]*>([^<]+)</a>\s*</td>\s*'  # link + title
            r'<td[^>]*>\s*([^<]*)</td>\s*'  # reference number
            r'<td[^>]*>\s*([^<]*)</td>',  # categories
            re.DOTALL
        )

        for match in row_pattern.finditer(html_content):
            date_str = match.group(1).strip()
            link = match.group(2).strip()
            title = html_module.unescape(match.group(3).strip())
            ref = html_module.unescape(match.group(4).strip())
            categories = html_module.unescape(match.group(5).strip())

            # Resolve the link - may be /go?u=... redirect or direct
            if "/go?u=" in link:
                # Extract the actual URL from redirect
                actual_match = re.search(r'/go\?u=([^&"]+)', link)
                if actual_match:
                    from urllib.parse import unquote
                    link = unquote(actual_match.group(1))

            # Make absolute URL
            if link.startswith("/"):
                link = f"{BASE_URL}{link}"
            elif not link.startswith("http"):
                link = f"{BASE_URL}/{link}"

            # Parse date
            iso_date = ""
            try:
                parts = date_str.split(".")
                if len(parts) == 3:
                    iso_date = f"{parts[2].strip()}-{parts[1].strip().zfill(2)}-{parts[0].strip().zfill(2)}"
            except Exception:
                pass

            results.append({
                "title": title,
                "url": link,
                "date": iso_date,
                "reference": ref,
                "categories": categories,
                "collection": collection["name"],
            })

        return results, total

    def _fetch_opinion_text(self, url: str) -> str:
        """Fetch full text from an individual opinion page."""
        time.sleep(RATE_LIMIT_SECONDS)
        try:
            resp = self.client.session.get(url, timeout=60)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch opinion page {url}: {e}")
            return ""

        html_content = resp.text

        # The opinion text is in the main content area after the heading
        # Try to find the main content block
        # Common patterns: <article>, <div class="content">, or text after <h1>

        # Strategy 1: Find content between known markers
        # Look for the main body content - usually after navigation and before footer
        text = ""

        # Try to extract from article or main content div
        # The site uses TYPO3 CMS, content is usually in a div with class like "frame-type-text"
        content_patterns = [
            # TYPO3 content elements
            r'<div[^>]*class="[^"]*frame-type-text[^"]*"[^>]*>(.*?)</div>\s*(?:<div[^>]*class="[^"]*frame|<footer|</main)',
            # Generic article content
            r'<article[^>]*>(.*?)</article>',
            # After h1 heading until footer or sidebar
            r'<h1[^>]*>[^<]*</h1>\s*(.*?)(?:<footer|<aside|<div[^>]*class="[^"]*sidebar)',
        ]

        for pattern in content_patterns:
            match = re.search(pattern, html_content, re.DOTALL)
            if match:
                text = match.group(1)
                break

        if not text:
            # Fallback: extract the largest text block from body
            # Remove script, style, nav, header, footer
            cleaned = re.sub(r'<(script|style|nav|header|footer)[^>]*>.*?</\1>', '', html_content, flags=re.DOTALL)
            # Remove head section
            cleaned = re.sub(r'<head>.*?</head>', '', cleaned, flags=re.DOTALL)
            # Find the longest paragraph sequence
            paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', cleaned, re.DOTALL)
            if paragraphs:
                # Join paragraphs that look like content (>50 chars)
                content_paras = [p for p in paragraphs if len(re.sub(r'<[^>]+>', '', p).strip()) > 50]
                text = "\n\n".join(content_paras)

        if not text:
            return ""

        # Clean HTML tags
        text = re.sub(r'<br\s*/?>', '\n', text)
        text = re.sub(r'</?p[^>]*>', '\n\n', text)
        text = re.sub(r'</?li[^>]*>', '\n- ', text)
        text = re.sub(r'</?[uo]l[^>]*>', '\n', text)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = html_module.unescape(text)
        # Normalize whitespace
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
        text = text.strip()

        return text

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all opinions/decisions from all collections."""
        for collection in COLLECTIONS:
            logger.info(f"\n{'='*60}")
            logger.info(f"Fetching {collection['label']} from {collection['path']}")
            logger.info(f"{'='*60}")

            # Get first page to discover total
            html = self._fetch_page(collection["path"], offset=0)
            results, total = self._parse_listing(html, collection)
            logger.info(f"Total {collection['label']}: {total:,}")

            if total == 0:
                logger.warning(f"No results found for {collection['label']}")
                continue

            # Yield results from first page
            for result in results:
                full_text = self._fetch_opinion_text(result["url"])
                if full_text:
                    result["text"] = full_text
                    yield result
                else:
                    logger.warning(f"No text extracted from {result['url']}")

            # Paginate through remaining pages
            per_page = collection["per_page"]
            offset = per_page
            while offset < total:
                html = self._fetch_page(collection["path"], offset=offset)
                results, _ = self._parse_listing(html, collection)

                if not results:
                    logger.info(f"No more results at offset {offset}")
                    break

                for result in results:
                    full_text = self._fetch_opinion_text(result["url"])
                    if full_text:
                        result["text"] = full_text
                        yield result

                offset += per_page

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents modified since the given date."""
        since_str = since.strftime("%Y-%m-%d")

        for collection in COLLECTIONS:
            html = self._fetch_page(collection["path"], offset=0)
            results, total = self._parse_listing(html, collection)

            # Fetch recent pages (results are sorted by date desc)
            per_page = collection["per_page"]
            offset = 0
            found_old = False

            while offset < total and not found_old:
                if offset > 0:
                    html = self._fetch_page(collection["path"], offset=offset)
                    results, _ = self._parse_listing(html, collection)

                if not results:
                    break

                for result in results:
                    if result.get("date", "") < since_str:
                        found_old = True
                        break

                    full_text = self._fetch_opinion_text(result["url"])
                    if full_text:
                        result["text"] = full_text
                        yield result

                offset += per_page

    def normalize(self, raw: dict) -> dict:
        """Transform raw opinion into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 50:
            return None

        # Build ID from reference number or URL slug
        ref = raw.get("reference", "")
        if ref:
            safe_ref = ref.replace("/", "-").replace(" ", "_")
            doc_id = f"SI_IPRS_{safe_ref}"
        else:
            # Use URL slug as fallback
            slug = raw["url"].rstrip("/").split("/")[-1]
            doc_id = f"SI_IPRS_{slug}"

        return {
            "_id": doc_id,
            "_source": "SI/IPRS",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
            "reference_number": raw.get("reference", ""),
            "categories": raw.get("categories", ""),
            "collection": raw.get("collection", ""),
            "language": "sl",
        }

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            html = self._fetch_page("/mnenja-gdpr/", offset=0)
            results, total = self._parse_listing(html, COLLECTIONS[0])
            logger.info(f"Connectivity OK: {total:,} GDPR opinions, {len(results)} on first page")

            if results:
                text = self._fetch_opinion_text(results[0]["url"])
                if text:
                    logger.info(f"Text extraction OK: {len(text)} chars from {results[0]['title']}")
                    return True
                else:
                    logger.error("Failed to extract text from first opinion")
                    return False
            return True
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            return False


# ── CLI entry point ──────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="SI/IPRS data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (10 records)")
    args = parser.parse_args()

    scraper = SlovenianDPAScraper()

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
