#!/usr/bin/env python3
"""
TT/TATT-Decisions -- Trinidad & Tobago Telecommunications Authority

Fetches regulatory decisions, determinations, frameworks, spectrum plans,
and consultation documents from TATT's WordPress site.

Strategy:
  - Solve Sucuri WAF JS cookie challenge to bypass the WAF
  - Enumerate regulatory_framework and consultation posts via WP REST API
  - Search media API for matching PDFs by title keywords
  - Download PDFs and extract full text via common/pdf_extract

Endpoints:
  - Posts:  https://tatt.org.tt/wp-json/wp/v2/regulatory_framework?per_page=100
  - Media:  https://tatt.org.tt/wp-json/wp/v2/media?per_page=100&media_type=application

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import base64
import logging
import json
import time
import html as html_mod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Any
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TT.TATT-Decisions")

BASE_URL = "https://tatt.org.tt"
API_BASE = f"{BASE_URL}/wp-json/wp/v2"


def _strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_mod.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _solve_sucuri_challenge(html_body: str) -> Optional[str]:
    """
    Parse the Sucuri WAF JS challenge and compute the cookie value.

    The challenge page contains base64-encoded JS that builds a cookie string.
    We decode it and evaluate the simple string concatenation to get the cookie.
    """
    # Extract the base64 payload
    m = re.search(r"S='([A-Za-z0-9+/=]+)'", html_body)
    if not m:
        return None

    decoded = base64.b64decode(m.group(1)).decode("utf-8", errors="replace")

    # Extract cookie name — it's built char-by-char in JS:
    # document.cookie='s'+'u'+'c'+...+'4'+"="
    cookie_part = re.search(r"document\.cookie=(.*?)=", decoded, re.DOTALL)
    if not cookie_part:
        return None
    cookie_expr = cookie_part.group(1)
    cookie_name = ""
    for ch in re.findall(r"['\"](.)['\"]", cookie_expr):
        cookie_name += ch
    if not cookie_name.startswith("sucuri_cloudproxy_uuid_"):
        return None

    # Build the cookie value by evaluating the JS string concatenation
    # Variable name changes each request (d=, q=, b=, etc.)
    var_match = re.search(r'^([a-zA-Z]+)=(.*?);document\.cookie', decoded, re.DOTALL)
    if not var_match:
        return None

    expr = var_match.group(2)
    value = ""
    for part in re.split(r'\s*\+\s*', expr):
        part = part.strip()
        cc = re.match(r'String\.fromCharCode\((\d+)\)', part)
        if cc:
            value += chr(int(cc.group(1)))
        elif len(part) >= 2 and part[0] in ('"', "'") and part[-1] in ('"', "'"):
            value += part[1:-1]

    if not value:
        return None

    return f"{cookie_name}={value}"


class TATTDecisionsScraper(BaseScraper):
    """Scraper for TT/TATT-Decisions."""

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
        self._sucuri_cookie = None

    def _ensure_sucuri_cookie(self):
        """Solve the Sucuri WAF challenge and set the cookie on the session."""
        if self._sucuri_cookie:
            return
        logger.info("Solving Sucuri WAF challenge...")
        try:
            resp = self.http.session.get(
                f"{BASE_URL}/wp-json/", allow_redirects=False, timeout=30
            )
            if resp.status_code == 307 or "sucuri_cloudproxy_js" in resp.text:
                cookie_str = _solve_sucuri_challenge(resp.text)
                if cookie_str:
                    name, value = cookie_str.split("=", 1)
                    self.http.session.cookies.set(name, value, domain="tatt.org.tt")
                    self._sucuri_cookie = cookie_str
                    logger.info(f"Sucuri cookie set: {name}")
                else:
                    logger.warning("Could not solve Sucuri challenge")
            else:
                logger.info("No Sucuri challenge detected (direct access)")
        except Exception as e:
            logger.warning(f"Sucuri challenge error: {e}")

    def _get_json(self, url: str, params: dict = None) -> Optional[Any]:
        """GET JSON from WP API with retry and Sucuri bypass."""
        self._ensure_sucuri_cookie()
        for attempt in range(3):
            try:
                resp = self.http.session.get(url, params=params, timeout=60)
                if resp.status_code == 400:
                    return None  # past last page
                if resp.status_code == 307 and "sucuri" in resp.text.lower():
                    # Cookie expired, re-solve
                    self._sucuri_cookie = None
                    self._ensure_sucuri_cookie()
                    continue
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    def _get_total_pages(self, url: str, params: dict = None) -> int:
        """Get total number of pages from WP API headers."""
        self._ensure_sucuri_cookie()
        try:
            resp = self.http.session.head(url, params=params, timeout=30)
            return int(resp.headers.get("X-WP-TotalPages", 1))
        except Exception:
            return 1

    def _fetch_all_posts(self, post_type: str) -> list[dict]:
        """Fetch all posts of a given custom post type."""
        url = f"{API_BASE}/{post_type}"
        posts = []
        page = 1
        while True:
            data = self._get_json(url, params={
                "per_page": 100,
                "page": page,
                "_fields": "id,title,date,link,slug,content",
            })
            if not data:
                break
            posts.extend(data)
            if len(data) < 100:
                break
            page += 1
            time.sleep(1)
        logger.info(f"Fetched {len(posts)} {post_type} posts")
        return posts

    def _search_media_for_post(self, post: dict) -> Optional[dict]:
        """Search the media API for a PDF matching this post's title."""
        title = _strip_html(post.get("title", {}).get("rendered", ""))
        if not title:
            return None

        # Build search terms from the title — use first few significant words
        words = re.findall(r'\b[A-Za-z]{4,}\b', title)
        if len(words) < 2:
            search_term = title[:60]
        else:
            search_term = " ".join(words[:4])

        data = self._get_json(f"{API_BASE}/media", params={
            "per_page": 5,
            "media_type": "application",
            "search": search_term,
        })
        if not data:
            return None

        # Find the best matching PDF
        for item in data:
            mime = item.get("mime_type", "")
            src = item.get("source_url", "")
            if mime == "application/pdf" and "ninja-forms" not in src:
                return item
        return None

    def _extract_pdf_text(self, url: str, doc_id: str) -> Optional[str]:
        """Download a PDF and extract text."""
        try:
            text = extract_pdf_markdown(
                "TT/TATT-Decisions",
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

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all regulatory documents with full text from PDFs."""
        seen_ids = set()
        total = 0

        # Fetch regulatory framework posts
        rf_posts = self._fetch_all_posts("regulatory_framework")
        # Fetch consultation posts
        consult_posts = self._fetch_all_posts("consultation")

        all_posts = rf_posts + consult_posts
        logger.info(f"Processing {len(all_posts)} total posts...")

        for post in all_posts:
            post_id = str(post.get("id", ""))
            if post_id in seen_ids:
                continue
            seen_ids.add(post_id)

            title = _strip_html(post.get("title", {}).get("rendered", ""))
            if not title:
                continue

            date_str = post.get("date", "")
            link = post.get("link", "")

            # Search for matching PDF in media library
            media_item = self._search_media_for_post(post)
            if not media_item:
                logger.debug(f"No PDF found for: {title}")
                continue

            pdf_url = media_item.get("source_url", "")
            if not pdf_url:
                continue

            # Extract text from PDF
            logger.info(f"Extracting PDF for: {title[:80]}")
            text = self._extract_pdf_text(pdf_url, post_id)
            if not text:
                logger.debug(f"No text extracted for: {title}")
                continue

            total += 1
            doc_type = "regulatory_framework"
            if post in consult_posts:
                doc_type = "consultation"

            yield {
                "id": post_id,
                "title": title,
                "text": text,
                "date": date_str,
                "url": pdf_url,
                "link": link,
                "doc_type": doc_type,
                "media_id": str(media_item.get("id", "")),
            }
            time.sleep(1.5)

        logger.info(f"Total: {total} documents with full text")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents modified since the given date."""
        since_iso = since.strftime("%Y-%m-%dT%H:%M:%S")
        for post_type in ("regulatory_framework", "consultation"):
            url = f"{API_BASE}/{post_type}"
            page = 1
            while True:
                data = self._get_json(url, params={
                    "per_page": 100,
                    "after": since_iso,
                    "orderby": "date",
                    "order": "desc",
                    "page": page,
                    "_fields": "id,title,date,link,slug,content",
                })
                if not data:
                    break
                for post in data:
                    post_id = str(post.get("id", ""))
                    title = _strip_html(post.get("title", {}).get("rendered", ""))
                    media_item = self._search_media_for_post(post)
                    if not media_item:
                        continue
                    pdf_url = media_item.get("source_url", "")
                    text = self._extract_pdf_text(pdf_url, post_id)
                    if not text:
                        continue
                    yield {
                        "id": post_id,
                        "title": title,
                        "text": text,
                        "date": post.get("date", ""),
                        "url": pdf_url,
                        "link": post.get("link", ""),
                        "doc_type": post_type,
                        "media_id": str(media_item.get("id", "")),
                    }
                    time.sleep(1.5)
                if len(data) < 100:
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
            "_source": "TT/TATT-Decisions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date_str,
            "url": raw.get("url", ""),
            "link": raw.get("link", ""),
            "doc_type": raw.get("doc_type", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = TATTDecisionsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        logger.info("Testing connectivity to TATT WP API...")
        scraper._ensure_sucuri_cookie()
        data = scraper._get_json(f"{API_BASE}/regulatory_framework", params={"per_page": 1})
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
