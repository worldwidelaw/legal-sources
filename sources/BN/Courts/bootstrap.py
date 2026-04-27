#!/usr/bin/env python3
"""
BN/Courts -- Brunei Judiciary Court Decisions

Fetches court judgments from the Brunei Judiciary WordPress site via its
REST API. Full text is extracted from linked PDFs.

Endpoints:
  - WP REST API: https://www.judiciary.gov.bn/wp-json/wp/v2/posts
  - Categories: 182 (Court of Appeal), 183 (High Court),
                184 (Intermediate Court), 185 (Magistrate's Court)
  - PDFs under: https://www.judiciary.gov.bn/wp-content/uploads/...

Data:
  - ~500+ judgments with full text from PDFs
  - Courts: Court of Appeal, High Court, Intermediate, Magistrate's

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import html as html_mod
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BN.Courts")

BASE_URL = "https://www.judiciary.gov.bn"
API_URL = f"{BASE_URL}/wp-json/wp/v2/posts"

# Court category IDs from WordPress
COURT_CATEGORIES = {
    182: "Court of Appeal",
    183: "High Court",
    184: "Intermediate Court",
    185: "Magistrate's Court",
}

# Extract PDF URLs from post HTML content
# PDFs are in Divi builder link_option_url attributes with smart quotes (&#8221; = \u201d)
PDF_RE = re.compile(
    r'link_option_url\s*=\s*["\u201c\u201d\u2033\']+\s*(/wp-content/uploads/[^\s"\';\u201c\u201d\u2033]+\.pdf)',
    re.IGNORECASE,
)
# Also match standard href
HREF_PDF_RE = re.compile(
    r'href\s*=\s*["\']([^"\']*\.pdf)["\']',
    re.IGNORECASE,
)

# Strip HTML tags
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


DIVI_RE = re.compile(r'\[/?et_pb_[^\]]*\]', re.IGNORECASE)


def strip_html(s: str) -> str:
    text = html_mod.unescape(s)
    # Remove Divi builder shortcodes
    text = DIVI_RE.sub(" ", text)
    text = TAG_RE.sub(" ", text)
    return WS_RE.sub(" ", text).strip()


def extract_case_number(title: str) -> str:
    """Try to extract a case reference number from the title."""
    # Common patterns: COACV 14/2024, CM 30/2024, etc.
    m = re.search(r'([A-Z]+\s*\d+[/-]\d{4})', title)
    if m:
        return m.group(1)
    return ""


class BNCourtsScraper(BaseScraper):
    """Scraper for BN/Courts -- Brunei Judiciary decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        import requests
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
        })

    def _get(self, url: str, **kwargs) -> "requests.Response":
        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=120, **kwargs)
        resp.raise_for_status()
        return resp

    def _get_json(self, url: str, params: dict = None) -> Any:
        self.rate_limiter.wait()
        resp = self.session.get(url, params=params, timeout=120)
        resp.raise_for_status()
        return resp.json(), resp.headers

    def _identify_court(self, category_ids: List[int]) -> str:
        """Identify the court from post category IDs."""
        for cat_id, court_name in COURT_CATEGORIES.items():
            if cat_id in category_ids:
                return court_name
        return "Unknown Court"

    def _extract_pdf_url(self, content_html: str) -> Optional[str]:
        """Extract the first PDF URL from post content HTML."""
        # First decode HTML entities so smart quotes become unicode
        decoded = html_mod.unescape(content_html)
        # Try Divi link_option_url first (most common)
        match = PDF_RE.search(decoded)
        if match:
            path = match.group(1).strip()
            return BASE_URL + path
        # Fallback to standard href
        match = HREF_PDF_RE.search(decoded)
        if match:
            url = match.group(1)
            if not url.startswith("http"):
                url = BASE_URL + url if url.startswith("/") else BASE_URL + "/" + url
            return url
        return None

    def _fetch_posts_paginated(self, sample: bool = False) -> List[Dict[str, Any]]:
        """Fetch all court decision posts via WP REST API."""
        all_posts = []
        seen_ids = set()

        # Fetch from all court categories
        for cat_id, court_name in COURT_CATEGORIES.items():
            page = 1
            while True:
                params = {
                    "categories": cat_id,
                    "per_page": 100,
                    "page": page,
                    "_fields": "id,title,date,link,content,categories",
                }
                logger.info(f"Fetching {court_name} page {page}...")
                try:
                    data, headers = self._get_json(API_URL, params=params)
                except Exception as e:
                    if "400" in str(e):
                        break  # No more pages
                    raise

                if not data:
                    break

                for post in data:
                    post_id = post["id"]
                    if post_id not in seen_ids:
                        seen_ids.add(post_id)
                        all_posts.append(post)

                # Check if more pages
                total_pages = int(headers.get("X-WP-TotalPages", 1))
                if page >= total_pages:
                    break
                page += 1

                if sample and len(all_posts) >= 25:
                    break

            if sample and len(all_posts) >= 25:
                break

        logger.info(f"Total unique posts fetched: {len(all_posts)}")
        return all_posts

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        post_id = raw.get("post_id", 0)
        title = raw.get("title", "")
        return {
            "_id": f"BN/Courts/{post_id}",
            "_source": "BN/Courts",
            "_type": "case_law",
            "_fetched_at": now,
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("date", None),
            "url": raw.get("url", ""),
            "post_id": post_id,
            "court": raw.get("court", ""),
            "case_number": extract_case_number(title),
            "pdf_url": raw.get("pdf_url", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        limit = 15 if sample else None
        count = 0

        posts = self._fetch_posts_paginated(sample=sample)
        if sample:
            posts = posts[:25]

        for post in posts:
            if limit and count >= limit:
                break

            post_id = post["id"]
            title = strip_html(post["title"].get("rendered", ""))
            content_html = post["content"].get("rendered", "")
            date_str = post.get("date", "")[:10]  # YYYY-MM-DD
            link = post.get("link", "")
            court = self._identify_court(post.get("categories", []))

            pdf_url = self._extract_pdf_url(content_html)
            if not pdf_url:
                logger.warning(f"  [{post_id}] No PDF found for: {title[:60]}")
                # Try to use the HTML content as text fallback
                text = strip_html(content_html)
                if len(text.strip()) < 500:
                    logger.warning(f"    Skipping {post_id} - no PDF and HTML too short ({len(text.strip())} chars)")
                    continue
            else:
                logger.info(f"  [{count+1}] {title[:60]}...")
                try:
                    text = extract_pdf_markdown(
                        source="BN/Courts",
                        source_id=str(post_id),
                        pdf_url=pdf_url,
                        table="case_law",
                    )
                except Exception as e:
                    logger.warning(f"    PDF extraction failed for {post_id}: {e}")
                    # Fallback to HTML content
                    text = strip_html(content_html)

            if not text or len(text.strip()) < 50:
                logger.warning(f"    Skipping {post_id} - no/short text")
                continue

            record = self.normalize({
                "title": title,
                "text": text,
                "date": date_str if date_str else None,
                "url": link,
                "post_id": post_id,
                "court": court,
                "pdf_url": pdf_url or "",
            })
            yield record
            count += 1
            logger.info(f"    OK ({len(text)} chars)")

        logger.info(f"Total records yielded: {count}")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch posts modified after a given date using WP API."""
        logger.info(f"Fetching updates since {since}")
        # WP API supports after parameter for date filtering
        all_posts = []
        seen_ids = set()

        for cat_id in COURT_CATEGORIES:
            page = 1
            while True:
                params = {
                    "categories": cat_id,
                    "per_page": 100,
                    "page": page,
                    "after": f"{since}T00:00:00",
                    "_fields": "id,title,date,link,content,categories",
                }
                try:
                    data, headers = self._get_json(API_URL, params=params)
                except Exception:
                    break
                if not data:
                    break
                for post in data:
                    if post["id"] not in seen_ids:
                        seen_ids.add(post["id"])
                        all_posts.append(post)
                total_pages = int(headers.get("X-WP-TotalPages", 1))
                if page >= total_pages:
                    break
                page += 1

        # Process same as fetch_all
        for post in all_posts:
            post_id = post["id"]
            title = strip_html(post["title"].get("rendered", ""))
            content_html = post["content"].get("rendered", "")
            date_str = post.get("date", "")[:10]
            link = post.get("link", "")
            court = self._identify_court(post.get("categories", []))
            pdf_url = self._extract_pdf_url(content_html)

            if pdf_url:
                try:
                    text = extract_pdf_markdown(
                        source="BN/Courts",
                        source_id=str(post_id),
                        pdf_url=pdf_url,
                        table="case_law",
                    )
                except Exception:
                    text = strip_html(content_html)
            else:
                text = strip_html(content_html)

            if not text or len(text.strip()) < 50:
                continue

            yield self.normalize({
                "title": title,
                "text": text,
                "date": date_str if date_str else None,
                "url": link,
                "post_id": post_id,
                "court": court,
                "pdf_url": pdf_url or "",
            })


if __name__ == "__main__":
    scraper = BNCourtsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        import requests
        try:
            resp = requests.get(
                API_URL,
                params={"per_page": 1, "categories": 182},
                headers={"User-Agent": "LegalDataHunter/1.0"},
                timeout=30,
            )
            print(f"Connection OK: {resp.status_code}")
            data = resp.json()
            if data:
                print(f"Sample post: {data[0].get('title', {}).get('rendered', 'N/A')}")
        except Exception as e:
            print(f"Connection failed: {e}")
            sys.exit(1)
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
