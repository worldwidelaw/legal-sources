#!/usr/bin/env python3
"""
TT/TTSEC -- Trinidad & Tobago Securities Exchange Commission

Fetches enforcement orders, regulatory notices, guidelines, circulars,
and investor alerts from the TTSEC website.

Strategy:
  - Enumerate posts via WP REST API by category
  - Extract PDF URLs from post content
  - Download PDFs and extract full text via common/pdf_extract
  - Fall back to HTML content when no PDF is available

Endpoints:
  - Posts: https://www.ttsec.org.tt/wp-json/wp/v2/posts?categories={cats}&per_page=100

Key categories:
  9   = Orders (parent: contravention, admin fines, settlement, delisting)
  37  = Notices
  126 = Notices of Hearing
  175 = Investor Alerts
  381 = Decisions/Settlements
  1202 = Acts
  122  = Legislation
  1203 = Bye-Laws
  81   = Rules
  289  = Guidelines
  102  = Circulars
  1194 = Exemption Orders
  1193 = Delegated Orders

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import logging
import json
import time
import html as html_mod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TT.TTSEC")

BASE_URL = "https://www.ttsec.org.tt"
POSTS_URL = f"{BASE_URL}/wp-json/wp/v2/posts"

# Categories grouped by document type
ENFORCEMENT_CATS = [9, 37, 126, 175, 381]   # Orders, Notices, Hearings, Alerts, Decisions
LEGISLATION_CATS = [1202, 122, 1203, 81, 1194, 1193]  # Acts, Legislation, Bye-Laws, Rules, Orders
DOCTRINE_CATS = [289, 102]  # Guidelines, Circulars

ALL_CATS = ENFORCEMENT_CATS + LEGISLATION_CATS + DOCTRINE_CATS

# Map category IDs to document types
CAT_TYPE_MAP = {}
for c in ENFORCEMENT_CATS:
    CAT_TYPE_MAP[c] = "doctrine"  # enforcement actions classified as doctrine
for c in LEGISLATION_CATS:
    CAT_TYPE_MAP[c] = "doctrine"
for c in DOCTRINE_CATS:
    CAT_TYPE_MAP[c] = "doctrine"


def _strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_mod.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


class TTSECScraper(BaseScraper):
    """Scraper for TT/TTSEC -- Trinidad & Tobago Securities Exchange Commission."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
                "Accept": "application/json",
            },
            timeout=60,
        )

    def _get_json(self, url: str, params: dict = None) -> Optional[Any]:
        """GET JSON from WP API with retry."""
        for attempt in range(3):
            try:
                resp = self.http.session.get(url, params=params, timeout=60)
                if resp.status_code == 400:
                    return None  # past last page
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    def _extract_pdf_urls(self, html_content: str) -> list[str]:
        """Extract PDF URLs from post HTML content."""
        patterns = [
            r'file=([^"&]+\.pdf)',
            r'href="([^"]+\.pdf[^"]*)"',
            r'src="([^"]+\.pdf[^"]*)"',
        ]
        urls = []
        for pat in patterns:
            for m in re.finditer(pat, html_content):
                url = m.group(1).split('"')[0]  # clean trailing chars
                if not url.startswith("http"):
                    url = BASE_URL + url
                if url not in urls:
                    urls.append(url)
        return urls

    def _extract_pdf_text(self, url: str, doc_id: str) -> Optional[str]:
        """Download a PDF and extract text."""
        try:
            text = extract_pdf_markdown(
                "TT/TTSEC",
                doc_id,
                pdf_url=url,
                table="doctrine",
                force=True,
            )
            if text and len(text.strip()) > 100:
                return text.strip()
            return None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {url}: {e}")
            return None

    def _classify_post(self, post: dict) -> str:
        """Classify post as doctrine (all TTSEC docs are regulatory doctrine)."""
        return "doctrine"

    def _fetch_category_posts(self, cats: list[int]) -> Generator[dict, None, None]:
        """Fetch all posts for given category IDs."""
        cats_str = ",".join(str(c) for c in cats)
        page = 1
        while True:
            data = self._get_json(POSTS_URL, params={
                "per_page": 50,
                "categories": cats_str,
                "page": page,
                "_fields": "id,title,date,link,content,categories,slug",
            })
            if not data:
                break
            for post in data:
                yield post
            if len(data) < 50:
                break
            page += 1
            time.sleep(1)

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all enforcement and regulatory documents with full text."""
        seen_ids = set()

        logger.info("Fetching posts from enforcement & regulatory categories...")
        total = 0

        for post in self._fetch_category_posts(ALL_CATS):
            post_id = str(post.get("id", ""))
            if post_id in seen_ids:
                continue
            seen_ids.add(post_id)

            title = _strip_html(post.get("title", {}).get("rendered", ""))
            content_html = post.get("content", {}).get("rendered", "")
            date_str = post.get("date", "")
            link = post.get("link", "")

            # Try PDF extraction first
            text = None
            pdf_urls = self._extract_pdf_urls(content_html)
            pdf_url = None
            for purl in pdf_urls:
                logger.info(f"Downloading PDF for: {title}")
                text = self._extract_pdf_text(purl, post_id)
                if text:
                    pdf_url = purl
                    break
                time.sleep(1)

            # Fall back to HTML content
            if not text:
                html_text = _strip_html(content_html)
                if len(html_text) >= 200:
                    text = html_text

            if not text:
                logger.debug(f"Skipping post with no substantial content: {title}")
                continue

            total += 1
            yield {
                "id": post_id,
                "title": title,
                "text": text,
                "date": date_str,
                "url": pdf_url or link,
                "link": link,
                "categories": post.get("categories", []),
            }
            time.sleep(1)

        logger.info(f"Total: {total} documents with full text")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents modified since the given date."""
        since_iso = since.strftime("%Y-%m-%dT%H:%M:%S")
        cats_str = ",".join(str(c) for c in ALL_CATS)
        page = 1
        seen_ids = set()
        while True:
            data = self._get_json(POSTS_URL, params={
                "per_page": 50,
                "categories": cats_str,
                "after": since_iso,
                "orderby": "date",
                "order": "desc",
                "page": page,
                "_fields": "id,title,date,link,content,categories,slug",
            })
            if not data:
                break
            for post in data:
                post_id = str(post.get("id", ""))
                if post_id in seen_ids:
                    continue
                seen_ids.add(post_id)

                title = _strip_html(post.get("title", {}).get("rendered", ""))
                content_html = post.get("content", {}).get("rendered", "")
                text = None
                pdf_urls = self._extract_pdf_urls(content_html)
                pdf_url = None
                for purl in pdf_urls:
                    text = self._extract_pdf_text(purl, post_id)
                    if text:
                        pdf_url = purl
                        break
                    time.sleep(1)
                if not text:
                    html_text = _strip_html(content_html)
                    if len(html_text) >= 200:
                        text = html_text
                if not text:
                    continue
                yield {
                    "id": post_id,
                    "title": title,
                    "text": text,
                    "date": post.get("date", ""),
                    "url": pdf_url or post.get("link", ""),
                    "link": post.get("link", ""),
                    "categories": post.get("categories", []),
                }
                time.sleep(1)
            if len(data) < 50:
                break
            page += 1

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        date_str = raw.get("date", "")
        if date_str:
            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                date_str = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                pass

        return {
            "_id": raw.get("id", ""),
            "_source": "TT/TTSEC",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date_str,
            "url": raw.get("url", ""),
            "link": raw.get("link", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = TTSECScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        logger.info("Testing connectivity to TTSEC WP API...")
        data = scraper._get_json(POSTS_URL, params={"per_page": 1})
        if data:
            logger.info(f"OK — got {len(data)} post(s)")
            print("Test passed: WP REST API accessible")
        else:
            logger.error("Failed to reach WP REST API")
            sys.exit(1)

    elif command == "bootstrap":
        sample = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample, sample_size=15)
        print(json.dumps(result, indent=2, default=str))

    elif command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=30)
        result = scraper.bootstrap(sample_mode=False)
        print(json.dumps(result, indent=2, default=str))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
