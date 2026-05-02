#!/usr/bin/env python3
"""
MC/CCAF -- Monaco Commission for Financial Activities Control

Fetches regulatory publications from CCAF via WordPress REST API.
English translations available via WPML ?lang=en parameter.
Also extracts text from linked PDF sanction decisions.

Usage:
  python bootstrap.py bootstrap          # Full bootstrap
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import logging
import time
from html import unescape
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MC.CCAF")

API_BASE = "https://ccaf.mc/wp-json/wp/v2"
PER_PAGE = 100


class CCAFScraper(BaseScraper):
    """Scraper for MC/CCAF — Monaco CCAF publications via WP REST API."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
        })

    @staticmethod
    def _strip_html(html: str) -> str:
        """Remove HTML tags and decode entities."""
        text = re.sub(r'<br\s*/?>|</p>|</li>|</div>', '\n', html)
        text = re.sub(r'<[^>]+>', '', text)
        text = unescape(text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _extract_pdf_text(self, url: str) -> Optional[str]:
        """Download and extract text from a PDF URL."""
        try:
            from common.pdf_extract import extract_pdf_markdown
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            if len(resp.content) > 50_000_000:
                logger.warning(f"PDF too large: {len(resp.content)} bytes")
                return None
            text = extract_pdf_markdown("MC/CCAF", "MC-CCAF", pdf_bytes=resp.content)
            if text and len(text.strip()) > 50:
                return text.strip()
        except ImportError:
            logger.warning("common.pdf_extract not available, skipping PDF")
        except Exception as e:
            logger.warning(f"PDF extraction failed for {url}: {e}")
        return None

    def _find_pdf_links(self, html_content: str) -> list:
        """Extract PDF URLs from post content HTML."""
        return re.findall(r'href="([^"]+\.pdf)"', html_content)

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all English posts from WP REST API."""
        page = 1
        total_fetched = 0

        while True:
            url = f"{API_BASE}/posts?per_page={PER_PAGE}&page={page}&lang=en&orderby=date&order=desc"
            logger.info(f"Fetching page {page}: {url}")

            try:
                resp = self.session.get(url, timeout=30)
                if resp.status_code == 400:
                    break
                resp.raise_for_status()
            except requests.RequestException as e:
                logger.error(f"Request failed: {e}")
                break

            posts = resp.json()
            if not posts:
                break

            for post in posts:
                post_id = post.get("id")
                title_html = post.get("title", {}).get("rendered", "")
                content_html = post.get("content", {}).get("rendered", "")
                date_str = post.get("date", "")
                link = post.get("link", "")

                title = self._strip_html(title_html)
                text = self._strip_html(content_html)

                # Try to extract text from linked PDFs for richer content
                pdf_links = self._find_pdf_links(content_html)
                pdf_texts = []
                for pdf_url in pdf_links:
                    # Skip list PDFs (authorized firms lists)
                    if "LISTE-SOCIETES" in pdf_url.upper():
                        continue
                    logger.info(f"Extracting PDF: {pdf_url[:80]}")
                    pdf_text = self._extract_pdf_text(pdf_url)
                    if pdf_text:
                        pdf_texts.append(pdf_text)
                    time.sleep(1)

                # Combine post text with PDF text
                if pdf_texts:
                    combined = text + "\n\n---\n\n" + "\n\n---\n\n".join(pdf_texts)
                    text = combined

                if len(text) < 20:
                    logger.debug(f"Skipping empty post {post_id}: {title[:50]}")
                    continue

                yield {
                    "wp_id": post_id,
                    "title": title,
                    "text": text,
                    "date": date_str[:10] if date_str else None,
                    "url": link,
                    "pdf_urls": pdf_links if pdf_links else None,
                    "categories": post.get("categories", []),
                }
                total_fetched += 1

            total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
            if page >= total_pages:
                break
            page += 1
            time.sleep(2)

        logger.info(f"Fetched {total_fetched} posts total")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch posts modified after the given date."""
        since_iso = since.strftime("%Y-%m-%dT%H:%M:%S")
        url = f"{API_BASE}/posts?per_page={PER_PAGE}&lang=en&after={since_iso}&orderby=date&order=desc"

        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            posts = resp.json()
        except Exception as e:
            logger.error(f"Update fetch failed: {e}")
            return

        for post in posts:
            yield post

    def normalize(self, raw: dict) -> Optional[dict]:
        """Normalize a raw post into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 20:
            return None

        wp_id = raw.get("wp_id", "")
        return {
            "_id": f"MC-CCAF-{wp_id}",
            "_source": "MC/CCAF",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "pdf_urls": raw.get("pdf_urls"),
        }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="MC/CCAF Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CCAFScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")
        if fetched == 0:
            logger.error("No records fetched!")
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    import json
    main()
