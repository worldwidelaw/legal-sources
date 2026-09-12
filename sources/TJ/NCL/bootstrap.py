"""
Legal Data Hunter — TJ/NCL (National Center of Legislation)

Scrapes Tajikistan's official legislation database from mmk.tj.
The site is a Drupal 7 CMS with law content pages containing full text
in the body field, plus metadata in custom fields (classification, number,
authority, status).

Strategy:
  1. Discover law URLs from taxonomy term 760 (paginated listing)
  2. Fetch each content page and parse HTML for full text + metadata
"""

import re
import sys
import json
import html as html_mod
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter")

BASE_URL = "https://mmk.tj"
TAXONOMY_URL = f"{BASE_URL}/taxonomy/term/760"


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.I)
    text = re.sub(r'<p[^>]*>', '\n', text, flags=re.I)
    text = re.sub(r'</p>', '\n', text, flags=re.I)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_mod.unescape(text)
    text = re.sub(r'\xa0', ' ', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_field(html: str, field_name: str) -> str:
    """Extract a Drupal field value from HTML."""
    pattern = rf'class="field field-name-{field_name}[^"]*".*?class="field-item even"[^>]*>(.*?)</div>'
    m = re.search(pattern, html, re.DOTALL)
    if m:
        return strip_html(m.group(1))
    return ""


def extract_body(html: str) -> str:
    """Extract the body field (main text content) from a Drupal page."""
    # Find the body field section
    body_start = re.search(r'class="field field-name-body[^"]*"', html)
    if not body_start:
        return ""

    # Find where the next field starts (or end of node-content)
    rest = html[body_start.start():]
    # Look for the next field-name- div or end of article
    next_field = re.search(r'<div[^>]*class="field field-name-(?!body)', rest[50:])
    end_article = re.search(r'</article', rest[50:])

    if next_field:
        body_html = rest[:50 + next_field.start()]
    elif end_article:
        body_html = rest[:50 + end_article.start()]
    else:
        body_html = rest[:100000]  # cap at 100K

    # Extract text from the field-item even within the body section
    item_match = re.search(r'class="field-item even"[^>]*>(.*)', body_html, re.DOTALL)
    if item_match:
        text = strip_html(item_match.group(1))
        # Remove trailing div closing tags that leaked in
        text = re.sub(r'\s*$', '', text)
        if len(text) > 50:
            return text

    # Fallback: strip all HTML from body section
    text = strip_html(body_html)
    if len(text) > 100:
        return text
    return ""


def extract_title(html: str) -> str:
    """Extract the page title."""
    m = re.search(r'<h1[^>]*class="[^"]*title[^"]*"[^>]*>(.*?)</h1>', html, re.DOTALL)
    if m:
        return strip_html(m.group(1))
    m = re.search(r'<title>(.*?)\s*\|', html)
    if m:
        return html_mod.unescape(m.group(1)).strip()
    return ""


def extract_file_urls(html: str) -> list:
    """Extract legislation file download URLs."""
    return re.findall(
        r'href="(https?://mmk\.tj/system/files/Legislation/[^"]+)"',
        html,
    )


class SourceScraper(BaseScraper):
    """
    Scraper for: National Center of Legislation (MMK)
    Country: TJ
    URL: https://mmk.tj

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url="",
            headers={"User-Agent": "LegalDataHunter/1.0 (research)"},
        )

    def _discover_content_urls(self) -> list:
        """Discover all content URLs from taxonomy listing pages."""
        urls = []
        seen = set()
        page = 0

        while True:
            url = f"{TAXONOMY_URL}?page={page}" if page > 0 else TAXONOMY_URL
            logger.info(f"Fetching listing page {page}: {url}")
            try:
                resp = self.client.get(url)
                html = resp.text
            except Exception as e:
                logger.warning(f"Error fetching page {page}: {e}")
                break

            # Extract content links
            links = re.findall(
                r'href="(https?://mmk\.tj/content/[^"?#]+)"',
                html,
            )
            new_count = 0
            for link in links:
                if link not in seen:
                    seen.add(link)
                    urls.append(link)
                    new_count += 1

            if new_count == 0 and page > 0:
                break

            # Check if there's a next page
            next_page = re.search(rf'page={page + 1}', html)
            if not next_page:
                break

            page += 1
            time.sleep(1)

        # Also search for legislation using the search endpoint
        search_terms = [
            "%D2%9B%D0%BE%D0%BD%D1%83%D0%BD%D0%B8",  # Қонуни (Law)
            "%D0%BA%D0%BE%D0%B4%D0%B5%D0%BA%D1%81",   # кодекс (Code)
        ]
        for term in search_terms:
            for sp in range(3):  # up to 3 search pages
                search_url = f"{BASE_URL}/search/node/{term}"
                if sp > 0:
                    search_url += f"?page={sp}"
                try:
                    resp = self.client.get(search_url)
                    links = re.findall(
                        r'href="(https?://mmk\.tj/content/[^"?#]+)"',
                        resp.text,
                    )
                    new = 0
                    for link in links:
                        if link not in seen:
                            seen.add(link)
                            urls.append(link)
                            new += 1
                    if new == 0:
                        break
                except Exception:
                    break
                time.sleep(1)

        logger.info(f"Discovered {len(urls)} unique content URLs")
        return urls

    def _is_legislation(self, html: str) -> bool:
        """Check if a content page is actual legislation (has classification field)."""
        return bool(re.search(r'field-name-field-classification', html))

    def _parse_law_page(self, url: str, html: str) -> dict:
        """Parse a legislation content page into a raw document dict."""
        title = extract_title(html)
        body = extract_body(html)
        classification = extract_field(html, "field-classification")
        number = extract_field(html, "field-number")
        authority = extract_field(html, "field-authority")
        status = extract_field(html, "field-status")
        file_urls = extract_file_urls(html)

        # Extract URL slug as ID
        slug = url.rstrip("/").split("/content/")[-1] if "/content/" in url else url
        slug = unquote(slug)

        # Try to extract date from the body or page
        date_match = re.search(
            r'(\d{1,2})\s+(?:январ[ья]|феврал[ья]|март[а]?|апрел[ья]|ма[йя]|июн[ья]|июл[ья]|август[а]?|сентябр[ья]|октябр[ья]|ноябр[ья]|декабр[ья])\s+(?:соли\s+)?(\d{4})',
            body,
            re.I,
        )
        date_str = None
        if date_match:
            # Don't parse the month name, just store year
            date_str = date_match.group(2)

        return {
            "url": url,
            "url_slug": slug,
            "title": title,
            "text": body,
            "classification": classification,
            "number": number,
            "authority": authority,
            "status": status,
            "date_raw": date_str,
            "file_urls": file_urls,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislation documents from mmk.tj."""
        content_urls = self._discover_content_urls()

        for i, url in enumerate(content_urls):
            logger.info(f"Fetching [{i+1}/{len(content_urls)}]: {unquote(url)[:80]}")
            try:
                resp = self.client.get(url)
                html = resp.text
            except Exception as e:
                logger.warning(f"Error fetching {url}: {e}")
                continue

            if not self._is_legislation(html):
                logger.debug(f"Skipping non-legislation: {url}")
                continue

            doc = self._parse_law_page(url, html)
            if not doc["text"]:
                logger.warning(f"No text found for: {url}")
                continue

            yield doc
            time.sleep(1)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No incremental update support — full re-fetch."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform a raw document into the standard schema."""
        slug = raw.get("url_slug", "")
        return {
            "_id": slug,
            "_source": "TJ/NCL",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "url_slug": slug,
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "classification": raw.get("classification", ""),
            "number": raw.get("number", ""),
            "authority": raw.get("authority", ""),
            "status": raw.get("status", ""),
            "date": raw.get("date_raw"),
            "url": raw.get("url", ""),
            "file_urls": raw.get("file_urls", []),
            "language": "tg",
        }


# ── CLI Entry Point ───────────────────────────────────────────────

def main():
    scraper = SourceScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, {stats['records_updated']} updated, {stats['records_skipped']} skipped")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
