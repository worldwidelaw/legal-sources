#!/usr/bin/env python3
"""
KY/DITC-TaxGuidance -- Cayman Islands DITC Tax Cooperation Guidance

Fetches official guidance, regulations, enforcement guidelines, and FAQs from
the Cayman Islands Department for International Tax Cooperation (DITC).

Strategy:
  1. WordPress REST API (/wp-json/wp/v2/posts) — 69+ posts with content
  2. Framework FAQ pages — rich text content on CRS, FATCA, ES, CBCR, CARF
  3. PDFs linked from posts — full guidance notes, regulations, enforcement docs

Coverage:
  - CRS (Common Reporting Standard)
  - FATCA (Foreign Account Tax Compliance Act)
  - ES (Economic Substance)
  - CBCR (Country-by-Country Reporting)
  - CARF (Crypto-Asset Reporting Framework)
  - EOIR (Exchange of Information on Request)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample records
  python bootstrap.py bootstrap             # Full extraction
  python bootstrap.py test-api              # Test connectivity
"""

import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from html import unescape

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KY.DITC-TaxGuidance")

BASE_URL = "https://www.ditc.ky"
WP_API = f"{BASE_URL}/wp-json/wp/v2"

# Framework FAQ pages with substantial content
FAQ_PAGES = [
    {"url": f"{BASE_URL}/news-updates/faqs/", "framework": "general", "title": "DITC General FAQs"},
    {"url": f"{BASE_URL}/crs/crs-faqs/", "framework": "crs", "title": "CRS FAQs"},
    {"url": f"{BASE_URL}/fatca/fatca-faqs/", "framework": "fatca", "title": "FATCA FAQs"},
    {"url": f"{BASE_URL}/es/es-faqs/", "framework": "es", "title": "Economic Substance FAQs"},
    {"url": f"{BASE_URL}/cbcr/cbcr-faqs/", "framework": "cbcr", "title": "CBCR FAQs"},
    {"url": f"{BASE_URL}/carf/carf-faqs/", "framework": "carf", "title": "CARF FAQs"},
]

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
    "Accept": "text/html,application/json",
}


class DITCTaxGuidanceScraper(BaseScraper):
    """Scraper for Cayman Islands DITC tax cooperation guidance."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._categories = None

    def _get_categories(self) -> dict:
        """Fetch WP categories mapping id -> name."""
        if self._categories is not None:
            return self._categories
        self._categories = {}
        try:
            resp = self.session.get(
                f"{WP_API}/categories",
                params={"per_page": 100},
                timeout=15,
            )
            resp.raise_for_status()
            for cat in resp.json():
                self._categories[cat["id"]] = cat["name"]
        except Exception as e:
            logger.warning(f"Failed to fetch categories: {e}")
        return self._categories

    def _fetch_wp_posts(self) -> Generator[dict, None, None]:
        """Fetch all posts from WP REST API."""
        categories = self._get_categories()
        page = 1
        while True:
            self.rate_limiter.wait()
            try:
                resp = self.session.get(
                    f"{WP_API}/posts",
                    params={
                        "per_page": 100,
                        "page": page,
                        "_fields": "id,title,content,link,date,categories",
                    },
                    timeout=20,
                )
                if resp.status_code == 400:
                    break
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"WP API page {page} failed: {e}")
                break

            posts = resp.json()
            if not posts:
                break

            for post in posts:
                content_html = post.get("content", {}).get("rendered", "")
                soup = BeautifulSoup(content_html, "html.parser")
                text = soup.get_text(separator="\n", strip=True)

                # Extract PDF links from content
                pdf_urls = []
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if ".pdf" in href.lower():
                        if not href.startswith("http"):
                            href = BASE_URL + href
                        pdf_urls.append(href)

                # Get category names
                cat_ids = post.get("categories", [])
                cat_names = [categories.get(c, "") for c in cat_ids if categories.get(c)]

                title = unescape(post.get("title", {}).get("rendered", ""))

                raw = {
                    "doc_type": "post",
                    "wp_id": post["id"],
                    "title": title,
                    "text": text,
                    "date": post.get("date", ""),
                    "url": post.get("link", ""),
                    "categories": cat_names,
                    "pdf_urls": pdf_urls,
                }

                # Skip posts with no meaningful content
                if len(text) < 50 and not pdf_urls:
                    continue

                yield raw

            page += 1
            time.sleep(0.5)

    def _fetch_faq_pages(self) -> Generator[dict, None, None]:
        """Fetch FAQ pages with substantial text content."""
        for faq in FAQ_PAGES:
            self.rate_limiter.wait()
            try:
                resp = self.session.get(faq["url"], timeout=20)
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"Failed to fetch {faq['url']}: {e}")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            main = soup.find("div", id="main-content") or soup.find("article")
            if not main:
                continue

            text = main.get_text(separator="\n", strip=True)

            if len(text) < 200:
                logger.info(f"  Skipping {faq['framework']} FAQ — too short ({len(text)} chars)")
                continue

            logger.info(f"  {faq['framework']} FAQ: {len(text)} chars")

            yield {
                "doc_type": "faq",
                "title": faq["title"],
                "text": text,
                "date": "",
                "url": faq["url"],
                "categories": [faq["framework"]],
                "pdf_urls": [],
            }

            time.sleep(1)

    def _extract_pdf_text(self, pdf_url: str, doc_id: str) -> str:
        """Download a PDF and extract text."""
        try:
            text = extract_pdf_markdown(
                source="KY/DITC-TaxGuidance",
                source_id=doc_id,
                pdf_url=pdf_url,
                table="doctrine",
            )
            return text or ""
        except Exception as e:
            logger.debug(f"PDF extraction failed for {pdf_url}: {e}")
            return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all DITC guidance documents."""
        # 1. WordPress posts
        logger.info("Fetching WP REST API posts...")
        post_count = 0
        for raw in self._fetch_wp_posts():
            post_count += 1
            # If post has PDF links, try to enrich with PDF text
            if raw["pdf_urls"] and len(raw["text"]) < 500:
                for pdf_url in raw["pdf_urls"][:2]:
                    self.rate_limiter.wait()
                    pdf_text = self._extract_pdf_text(pdf_url, f"post-{raw['wp_id']}")
                    if pdf_text and len(pdf_text) > len(raw["text"]):
                        raw["text"] = pdf_text
                        raw["url"] = pdf_url
                        break
                    time.sleep(1)

            yield raw

        logger.info(f"WP posts: {post_count} yielded")

        # 2. FAQ pages
        logger.info("Fetching FAQ pages...")
        faq_count = 0
        for raw in self._fetch_faq_pages():
            faq_count += 1
            yield raw

        logger.info(f"FAQ pages: {faq_count} yielded")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents modified since a date."""
        for raw in self.fetch_all():
            date_str = raw.get("date", "")
            if date_str:
                try:
                    doc_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                    if doc_date.replace(tzinfo=None) >= since.replace(tzinfo=None):
                        yield raw
                except (ValueError, TypeError):
                    yield raw
            else:
                yield raw

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw DITC record into standard schema."""
        text = raw.get("text", "").strip()
        if not text or len(text) < 50:
            return None

        title = raw.get("title", "").strip()
        if not title:
            return None

        doc_type = raw.get("doc_type", "post")
        wp_id = raw.get("wp_id", "")
        url = raw.get("url", "")

        # Build unique ID
        if doc_type == "faq":
            _id = f"KY_DITC_faq_{url.split('/')[-2] if url.endswith('/') else url.split('/')[-1]}"
        else:
            _id = f"KY_DITC_post_{wp_id}"

        # Parse date
        date_str = raw.get("date", "")
        if date_str:
            try:
                dt = datetime.fromisoformat(date_str)
                date_str = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                date_str = ""

        # Determine framework from categories
        categories = raw.get("categories", [])
        framework = ""
        for cat in categories:
            cat_lower = cat.lower()
            if "crs" in cat_lower:
                framework = "CRS"
            elif "fatca" in cat_lower:
                framework = "FATCA"
            elif "economic substance" in cat_lower or cat_lower == "es":
                framework = "Economic Substance"
            elif "cbcr" in cat_lower or "country-by-country" in cat_lower:
                framework = "CBCR"
            elif "carf" in cat_lower or "crypto" in cat_lower:
                framework = "CARF"
            elif "eoir" in cat_lower:
                framework = "EOIR"
            if framework:
                break

        return {
            "_id": _id,
            "_source": "KY/DITC-TaxGuidance",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str or None,
            "url": url,
            "category": ", ".join(categories) if categories else None,
            "framework": framework or None,
            "doc_type": doc_type,
            "language": "en",
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="KY/DITC-TaxGuidance bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true")
    args = parser.parse_args()

    scraper = DITCTaxGuidanceScraper()

    if args.command == "test-api":
        logger.info("Testing DITC WP REST API...")
        try:
            resp = scraper.session.get(f"{WP_API}/posts?per_page=1", timeout=15)
            resp.raise_for_status()
            total = resp.headers.get("X-WP-Total", "?")
            logger.info(f"OK: {total} posts available via WP REST API")
            post = resp.json()[0]
            logger.info(f"Sample: {post['title']['rendered'][:80]}")
        except Exception as e:
            logger.error(f"FAIL: {e}")
            sys.exit(1)
    elif args.command == "bootstrap":
        if args.sample:
            stats = scraper.run_sample(n=15)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, {stats['records_updated']} updated, {stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
