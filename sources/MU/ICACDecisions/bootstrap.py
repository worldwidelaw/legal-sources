#!/usr/bin/env python3
"""
MU/ICACDecisions -- Mauritius FCC (ex-ICAC) Court Decisions

Fetches court judgments, rulings, and sentences from anti-corruption cases
prosecuted by the Financial Crimes Commission (formerly ICAC/IRSA/ARID).

Strategy:
  - Enumerate PDFs via WP REST API media endpoint (no auth needed)
  - Filter by title for court decision patterns (ICAC v, FCC v, SCJ, etc.)
  - Download PDFs and extract full text via common/pdf_extract
  - Also fetch WP posts from legal categories for HTML-based content

Endpoints:
  - Media:  https://fcc.mu/wp-json/wp/v2/media?per_page=100&mime_type=application/pdf
  - Posts:  https://fcc.mu/wp-json/wp/v2/posts?categories=27,22,53

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
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MU.ICACDecisions")

BASE_URL = "https://fcc.mu"

# WP REST API endpoints
MEDIA_URL = f"{BASE_URL}/wp-json/wp/v2/media"
POSTS_URL = f"{BASE_URL}/wp-json/wp/v2/posts"

# Categories with legal content (judgments=27, rulings=22, former ICAC=53)
LEGAL_CATEGORIES = [27, 22, 53]

# Patterns identifying court decision PDFs
DECISION_PATTERNS = re.compile(
    r"(?:ICAC|FCC|IRSA|ARID)\s+v\s+"
    r"|v\s+(?:ICAC|FCC|The\s+State)"
    r"|\bSCJ\b"
    r"|\bINT\b\s+\d+"
    r"|Ruling"
    r"|Judgment"
    r"|Sentence"
    r"|Appeal"
    r"|Bail"
    r"|Annual.Report",
    re.IGNORECASE,
)


def _strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_mod.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


class ICACDecisionsScraper(BaseScraper):
    """Scraper for MU/ICACDecisions -- Mauritius FCC court decisions."""

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

    def _enumerate_pdf_media(self) -> Generator[Dict, None, None]:
        """Enumerate all PDF media items from WP REST API."""
        page = 1
        while True:
            data = self._get_json(MEDIA_URL, params={
                "per_page": 100,
                "mime_type": "application/pdf",
                "page": page,
            })
            if not data:
                break
            for item in data:
                title = item.get("title", {}).get("rendered", "")
                if DECISION_PATTERNS.search(title):
                    yield item
            if len(data) < 100:
                break
            page += 1
            time.sleep(1)

    def _enumerate_posts(self) -> Generator[Dict, None, None]:
        """Enumerate posts from legal categories via WP REST API."""
        cats = ",".join(str(c) for c in LEGAL_CATEGORIES)
        page = 1
        while True:
            data = self._get_json(POSTS_URL, params={
                "per_page": 100,
                "categories": cats,
                "page": page,
            })
            if not data:
                break
            for post in data:
                yield post
            if len(data) < 100:
                break
            page += 1
            time.sleep(1)

    def _extract_pdf_text(self, url: str, doc_id: str) -> Optional[str]:
        """Download a PDF and extract text."""
        try:
            text = extract_pdf_markdown(
                "MU/ICACDecisions",
                doc_id,
                pdf_url=url,
                table="case_law",
                force=True,
            )
            if text and len(text.strip()) > 200:
                # Filter out scanned PDFs with only OCR artifacts
                cleaned = re.sub(r'(CamScanner|Scanned|Page \d+)\s*', '', text.strip())
                if len(cleaned.strip()) > 200:
                    return text.strip()
            return None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {url}: {e}")
            return None

    def _extract_pdf_url_from_content(self, html_content: str) -> Optional[str]:
        """Extract embedded PDF URL from post HTML content."""
        # Look for pdfjs viewer or direct PDF links
        patterns = [
            r'file=([^"&]+\.pdf)',
            r'href="([^"]+\.pdf)"',
            r'src="([^"]+\.pdf)"',
        ]
        for pat in patterns:
            m = re.search(pat, html_content)
            if m:
                url = m.group(1)
                if not url.startswith("http"):
                    url = BASE_URL + url
                return url
        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all court decision documents."""
        seen_urls = set()

        # 1) PDF media items (primary source of decisions)
        logger.info("Enumerating PDF media items...")
        pdf_count = 0
        for item in self._enumerate_pdf_media():
            pdf_url = item.get("source_url", "")
            if not pdf_url or pdf_url in seen_urls:
                continue
            seen_urls.add(pdf_url)

            title = _strip_html(item.get("title", {}).get("rendered", ""))
            date_str = item.get("date", "")

            doc_id = str(item.get("id", ""))
            logger.info(f"Downloading PDF: {title}")
            text = self._extract_pdf_text(pdf_url, doc_id)
            if not text:
                logger.warning(f"No text extracted from PDF: {title}")
                continue

            pdf_count += 1
            yield {
                "source": "media",
                "id": doc_id,
                "title": title,
                "text": text,
                "date": date_str,
                "url": pdf_url,
                "wp_link": item.get("link", ""),
            }
            time.sleep(1)

        logger.info(f"PDF media: {pdf_count} decisions extracted")

        # 2) Posts from legal categories (some have HTML content or embedded PDFs)
        logger.info("Fetching posts from legal categories...")
        post_count = 0
        for post in self._enumerate_posts():
            content_html = post.get("content", {}).get("rendered", "")
            title = _strip_html(post.get("title", {}).get("rendered", ""))
            date_str = post.get("date", "")
            link = post.get("link", "")

            # Try to extract text from HTML content first
            text = _strip_html(content_html)

            # If HTML content is too short, try to find an embedded PDF
            if len(text) < 200:
                pdf_url = self._extract_pdf_url_from_content(content_html)
                if pdf_url and pdf_url not in seen_urls:
                    seen_urls.add(pdf_url)
                    logger.info(f"Downloading embedded PDF for post: {title}")
                    pdf_text = self._extract_pdf_text(pdf_url, f"post-{post.get('id', '')}")
                    if pdf_text:
                        text = pdf_text
                    else:
                        continue
                elif not text or len(text) < 100:
                    continue  # skip posts with no substantive content

            post_count += 1
            yield {
                "source": "post",
                "id": f"post-{post.get('id', '')}",
                "title": title,
                "text": text,
                "date": date_str,
                "url": link,
                "wp_link": link,
                "categories": post.get("categories", []),
            }

        logger.info(f"Posts: {post_count} records with content")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents modified since the given date."""
        since_iso = since.strftime("%Y-%m-%dT%H:%M:%S")
        # Check recent media
        data = self._get_json(MEDIA_URL, params={
            "per_page": 100,
            "mime_type": "application/pdf",
            "after": since_iso,
            "orderby": "date",
            "order": "desc",
        })
        if data:
            for item in data:
                title = item.get("title", {}).get("rendered", "")
                if DECISION_PATTERNS.search(title):
                    pdf_url = item.get("source_url", "")
                    if not pdf_url:
                        continue
                    doc_id = str(item.get("id", ""))
                    text = self._extract_pdf_text(pdf_url, doc_id)
                    if not text:
                        continue
                    yield {
                        "source": "media",
                        "id": doc_id,
                        "title": _strip_html(title),
                        "text": text,
                        "date": item.get("date", ""),
                        "url": pdf_url,
                        "wp_link": item.get("link", ""),
                    }
                    time.sleep(1)

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        date_str = raw.get("date", "")
        if date_str:
            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                date_str = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                pass

        doc_type = "case_law"
        title = raw.get("title", "")
        if re.search(r"Annual.Report|Guidelines|Code.of.Conduct|Bulletin", title, re.IGNORECASE):
            doc_type = "doctrine"

        return {
            "_id": raw.get("id", ""),
            "_source": "MU/ICACDecisions",
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": date_str,
            "url": raw.get("url", ""),
            "wp_link": raw.get("wp_link", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = ICACDecisionsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        logger.info("Testing connectivity to FCC WP API...")
        data = scraper._get_json(MEDIA_URL, params={"per_page": 1})
        if data:
            logger.info(f"OK — got {len(data)} media item(s)")
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
