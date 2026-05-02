#!/usr/bin/env python3
"""
MY/MyCC -- Malaysia Competition Commission

Fetches enforcement decisions and media releases from mycc.gov.my.

Strategy:
  - Scrape paginated media release listing at /media-release?page=N
  - Fetch each article page for full text (HTML paragraphs)
  - Also scrape /case page for PDF links and extract text
  - English language content

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MY.MyCC")

BASE_URL = "https://www.mycc.gov.my"

# Regex patterns
RELEASE_LINK_RE = re.compile(r'href="(/media-release/[^"]+)"')
DATE_RE = re.compile(
    r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{4})',
    re.IGNORECASE,
)
CASE_PDF_RE = re.compile(
    r'<td[^>]*>(.*?)</td>.*?'
    r'<td[^>]*>(.*?)</td>.*?'
    r'href="([^"]*\.pdf)"',
    re.DOTALL,
)


def _clean_html(text: str) -> str:
    """Strip HTML tags and decode entities."""
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def _parse_date(date_str: str) -> Optional[str]:
    """Parse date strings like '11 Feb 2026' or '29 December 2025'."""
    date_str = _clean_html(date_str).strip()
    for fmt in ("%d %B %Y", "%d %b %Y", "%B %d, %Y"):
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    m = DATE_RE.search(date_str)
    if m:
        for fmt in ("%d %B %Y", "%d %b %Y"):
            try:
                dt = datetime.strptime(m.group(1), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
    return None


def _extract_slug(url: str) -> str:
    """Extract slug from /media-release/some-title-slug."""
    parts = url.rstrip('/').split('/')
    return parts[-1] if parts else url


class MyccScraper(BaseScraper):
    """
    Scraper for MY/MyCC -- Malaysia Competition Commission.
    Country: MY
    URL: https://www.mycc.gov.my/

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (open-data research project)",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        })

    def _fetch_page(self, url: str) -> str:
        """Fetch a page and return HTML."""
        if not url.startswith("http"):
            url = BASE_URL + url
        r = self.session.get(url, timeout=30)
        r.raise_for_status()
        return r.text

    def _get_media_release_urls(self, max_pages: int = 10) -> list[str]:
        """Scrape paginated media release listing for article URLs."""
        all_urls = []
        seen = set()

        for page in range(0, max_pages):
            url = f"{BASE_URL}/media-release?page={page}"
            try:
                html = self._fetch_page(url)
            except requests.HTTPError:
                break

            links = RELEASE_LINK_RE.findall(html)
            new_count = 0
            for l in links:
                if l not in seen:
                    seen.add(l)
                    all_urls.append(l)
                    new_count += 1

            if new_count == 0:
                break

            logger.info(f"[media-release] page {page}: {new_count} new links (total: {len(all_urls)})")
            time.sleep(1.5)

        return all_urls

    def _extract_article_content(self, html: str) -> dict:
        """Extract title, date, and body text from an article page."""
        # Title - try h1 first, then og:title
        title = ""
        m = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
        if m:
            title = _clean_html(m.group(1))
        if not title:
            m = re.search(r'og:title["\s]*content="([^"]+)"', html)
            if m:
                title = unescape(m.group(1))

        # Date
        date = None
        m = re.search(r'datetime="(\d{4}-\d{2}-\d{2})', html)
        if m:
            date = m.group(1)
        if not date:
            m = re.search(r'field--name-field-date.*?<time[^>]*>(.*?)</time>', html, re.DOTALL)
            if m:
                date = _parse_date(m.group(1))

        # Body text - extract from all <p> tags, filter noise
        paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', html, re.DOTALL)
        content_parts = []
        for p in paragraphs:
            clean = _clean_html(p)
            if (len(clean) > 20
                and 'cookie' not in clean.lower()
                and 'copyright' not in clean.lower()
                and 'all rights reserved' not in clean.lower()
                and not clean.startswith('{')):
                content_parts.append(clean)

        text = '\n\n'.join(content_parts)
        return {"title": title, "date": date, "text": text}

    def _scrape_case_pdfs(self) -> list[dict]:
        """Scrape the /case page for PDF links."""
        html = self._fetch_page("/case")

        pdf_links = re.findall(r'href="([^"]*\.pdf)"', html)
        results = []
        seen = set()

        for link in pdf_links:
            if link in seen:
                continue
            seen.add(link)

            if not link.startswith("http"):
                link = BASE_URL + link

            # Skip the leniency form
            if "leniency" in link.lower():
                continue

            # Extract a title from the filename
            filename = link.split("/")[-1]
            filename = requests.utils.unquote(filename)
            title = filename.replace(".pdf", "").replace("%20", " ")
            title = re.sub(r'[_\-]+', ' ', title).strip()

            results.append({
                "slug": f"case-{_extract_slug(link)}",
                "title": title,
                "pdf_url": link,
                "doc_type": "case_decision",
            })

        logger.info(f"[case] Found {len(results)} case PDFs")
        return results

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw article/PDF data into standard schema."""
        text = raw.get("text", "").strip()
        if not text or len(text) < 50:
            return None

        title = raw.get("title", "").strip()
        if not title:
            return None

        slug = raw.get("slug", "")
        doc_type = raw.get("doc_type", "media_release")

        return {
            "_id": f"MY-MyCC-{slug}",
            "_source": "MY/MyCC",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "doc_type": doc_type,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all MyCC media releases and case decisions."""
        # 1) Media releases
        urls = self._get_media_release_urls(max_pages=10)
        logger.info(f"Fetching {len(urls)} media release articles")

        for i, rel_url in enumerate(urls):
            slug = _extract_slug(rel_url)
            full_url = BASE_URL + rel_url

            try:
                html = self._fetch_page(full_url)
                article = self._extract_article_content(html)
            except Exception as e:
                logger.warning(f"Failed to fetch {rel_url}: {e}")
                continue

            if article["text"] and len(article["text"]) >= 50:
                yield {
                    "slug": slug,
                    "title": article["title"],
                    "text": article["text"],
                    "date": article["date"],
                    "url": full_url,
                    "doc_type": "media_release",
                }

            if (i + 1) % 20 == 0:
                logger.info(f"[media-release] {i+1}/{len(urls)} fetched")

            time.sleep(1.5)

        # 2) Case PDFs
        case_docs = self._scrape_case_pdfs()
        for i, doc in enumerate(case_docs):
            pdf_url = doc["pdf_url"]
            slug = doc["slug"]

            text = extract_pdf_markdown(
                source="MY/MyCC",
                source_id=slug,
                pdf_url=pdf_url,
                table="doctrine",
            ) or ""

            if text and len(text) > 500_000:
                text = text[:500_000]

            if not text or len(text) < 50:
                continue

            yield {
                "slug": slug,
                "title": doc["title"],
                "text": text,
                "date": None,
                "url": pdf_url,
                "doc_type": "case_decision",
            }

            time.sleep(2)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recently added media releases."""
        since_str = since.strftime("%Y-%m-%d")
        logger.info(f"Fetching updates since {since_str}")

        urls = self._get_media_release_urls(max_pages=3)
        for rel_url in urls:
            slug = _extract_slug(rel_url)
            full_url = BASE_URL + rel_url

            try:
                html = self._fetch_page(full_url)
                article = self._extract_article_content(html)
            except Exception:
                continue

            if article["date"] and article["date"] >= since_str:
                if article["text"] and len(article["text"]) >= 50:
                    yield {
                        "slug": slug,
                        "title": article["title"],
                        "text": article["text"],
                        "date": article["date"],
                        "url": full_url,
                        "doc_type": "media_release",
                    }
                    time.sleep(1.5)


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="MY/MyCC data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = MyccScraper()

    if args.command == "test":
        logger.info("Testing MyCC connectivity...")
        try:
            urls = scraper._get_media_release_urls(max_pages=1)
            logger.info(f"OK: Found {len(urls)} media release URLs on page 0")
            if urls:
                html = scraper._fetch_page(BASE_URL + urls[0])
                article = scraper._extract_article_content(html)
                logger.info(f"Title: {article['title'][:80]}")
                logger.info(f"Date: {article['date']}")
                logger.info(f"Text: {len(article['text'])} chars")
                if article['text']:
                    logger.info(f"Preview: {article['text'][:200]}")
            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
