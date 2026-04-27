#!/usr/bin/env python3
"""
VC/FSA -- St Vincent & Grenadines Financial Services Authority

Fetches legislation, regulatory guidelines, warning notices, and publications
from the SVG Financial Services Authority.

Strategy:
  1. WP REST API posts: warning notices, circulars, news (HTML content)
  2. Forms-and-applications page: scrape links to legislation, guidelines,
     publications, and reports (PDFs served directly at page URLs)
  3. Download PDFs and extract text via common/pdf_extract

Endpoints:
  - Posts: https://fsasvg.com/wp-json/wp/v2/posts?per_page=100
  - Content page: https://fsasvg.com/forms-and-applications/

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
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.VC.FSA")

BASE_URL = "https://fsasvg.com"
POSTS_URL = f"{BASE_URL}/wp-json/wp/v2/posts"
FORMS_PAGE_URL = f"{BASE_URL}/forms-and-applications/"

# Sections on the forms-and-applications page to scrape (by heading text)
# We skip "Forms" sections as those aren't regulatory content
CONTENT_SECTIONS = {
    "Guidelines": "doctrine",
    "Legislation": "legislation",
    "Publications": "doctrine",
    "Reports": "doctrine",
}

# WP post category IDs
INVESTOR_ALERTS_CAT = 9
NEWS_CAT = 10
PRESS_RELEASE_CAT = 12


def _strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_mod.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


class VCFSAScraper(BaseScraper):
    """Scraper for VC/FSA -- SVG Financial Services Authority."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
                "Accept": "application/json, text/html",
            },
            timeout=60,
        )

    def _get_json(self, url: str, params: dict = None) -> Optional[Any]:
        """GET JSON from WP API with retry."""
        for attempt in range(3):
            try:
                resp = self.http.session.get(url, params=params, timeout=60)
                if resp.status_code == 400:
                    return None
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    def _get_html(self, url: str) -> Optional[str]:
        """GET HTML page with retry."""
        for attempt in range(3):
            try:
                resp = self.http.session.get(url, timeout=60)
                resp.raise_for_status()
                return resp.text
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    def _extract_pdf_text(self, url: str, doc_id: str) -> Optional[str]:
        """Download a PDF and extract text."""
        try:
            text = extract_pdf_markdown(
                "VC/FSA",
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

    def _classify_item(self, title: str, url: str) -> Optional[str]:
        """Classify a forms-page item by title/URL. Returns doc_type or None to skip."""
        t = title.lower()
        u = url.lower()

        # Skip forms, applications, navigation, and non-regulatory content
        skip_keywords = [
            "application", "form ", "form-", "renewal", "personal questionnaire",
            "checklist", "certificate of ", "notice of change", "self-certification",
            "reporting form", "fit proper", "fourth schedule", "share purchase",
            "articles of ", "election to register", "notice of cessation",
            "about us", "learning portal", "news and updates",
            "terms and condition", "faqs", "contact us", "entity name search",
            "file complaint", "entity self", "work hours",
        ]
        if any(kw in t for kw in skip_keywords):
            return None

        # Skip navigation/info pages (exact page slugs, not legislation with similar names)
        nav_pages = [
            "/about-us/", "/contact-us/", "/faqs/", "/news/", "/privacy-policy/",
            "/learning-portal/", "/international-financial-services/",
            "/file-complaint/", "/entity-name-search/", "/investor-alerts/",
            "/licensed-insurance-and-pension-plans", "/building-societies/",
            "/money-services-businesses/", "/credit-union/", "/friendly-societies/",
            "/mutual-funds/", "/trusts/", "/international-insurance/",
            "/registered-agents-and-trustees", "/virtual-asset-businesses/",
            "/limited-liability-companies-llc/", "/international-bank-list/",
            "/business-companies/", "/statistics/", "/intl-cooperation/",
            "/what-we-do/", "/useful-links/", "/fees/", "/team/",
        ]
        # Only skip if the URL ends with a nav page path (exact match, not substring)
        url_path = "/" + u.split("fsasvg.com/", 1)[-1] if "fsasvg.com/" in u else u
        if any(url_path.rstrip("/") + "/" == nav.rstrip("/") + "/" for nav in nav_pages):
            return None

        # Classify guidance/guidelines FIRST (before legislation check)
        guideline_keywords = ["guidance", "guideline", "code of", "competency",
                              "simplified due diligence", "ongoing monitoring",
                              "requirements for", "crs ", "share capital",
                              "disclosures for"]
        if any(kw in t for kw in guideline_keywords):
            return "doctrine"

        publication_keywords = ["newsletter", "enforcement policy", "strategic plan",
                                "emagazine", "risk-based", "statement of guidance",
                                "abc of insurance", "built to last", "disconnecting",
                                "demonstrable compliance", "jurisdiction information",
                                "ifs e-guide", "complaints procedure"]
        if any(kw in t for kw in publication_keywords):
            return "doctrine"

        report_keywords = ["annual report", "annual insurance report",
                           "quarterly insurance statistic"]
        if any(kw in t for kw in report_keywords):
            return "doctrine"

        # Classify legislation using word-boundary regex to avoid substring matches
        if re.search(r'\bact\b|\bregulations?\b|\bsro\b|\bcap\s*\d', t):
            return "legislation"

        return None

    def _scrape_forms_page(self) -> Generator[dict, None, None]:
        """Parse the forms-and-applications page for legislation, guidelines, etc.

        The page uses Elementor icon-list widgets. Each item is an <li> with
        class 'elementor-icon-list-item' containing an <a> with href and a
        <span class='elementor-icon-list-text'> with the title.
        """
        html = self._get_html(FORMS_PAGE_URL)
        if not html:
            logger.error("Failed to fetch forms-and-applications page")
            return

        # Extract all items from Elementor icon-list elements
        items = re.findall(
            r'<li\s+class="elementor-icon-list-item">\s*'
            r'<a\s+href="([^"]+)"[^>]*>.*?'
            r'<span class="elementor-icon-list-text">(.*?)</span>',
            html, re.DOTALL,
        )

        seen = set()
        for url, text_html in items:
            title = _strip_html(text_html).strip()
            if not title or len(title) < 5:
                continue

            if url in seen:
                continue
            seen.add(url)

            full_url = urljoin(BASE_URL + "/", url) if not url.startswith("http") else url
            doc_type = self._classify_item(title, full_url)
            if doc_type is None:
                continue

            slug = url.strip("/").split("/")[-1]

            yield {
                "id": f"page-{slug}",
                "title": title,
                "url": full_url,
                "doc_type": doc_type,
                "date": "",
            }

    def _fetch_wp_posts(self) -> Generator[dict, None, None]:
        """Fetch all WP REST API posts."""
        page = 1
        while True:
            data = self._get_json(POSTS_URL, params={
                "per_page": 100,
                "page": page,
                "_fields": "id,title,date,link,content,categories,slug",
            })
            if not data:
                break
            for post in data:
                yield post
            if len(data) < 100:
                break
            page += 1
            time.sleep(1)

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents with full text."""
        seen_ids = set()
        total = 0

        # --- Part 1: WP REST API posts (warning notices, circulars, news) ---
        logger.info("Fetching WP REST API posts...")
        for post in self._fetch_wp_posts():
            post_id = str(post.get("id", ""))
            if post_id in seen_ids:
                continue
            seen_ids.add(post_id)

            title = _strip_html(post.get("title", {}).get("rendered", ""))
            content_html = post.get("content", {}).get("rendered", "")
            date_str = post.get("date", "")
            link = post.get("link", "")

            # Skip job postings and non-regulatory content
            if any(kw in title.lower() for kw in ["career opportunity", "hurricane ready"]):
                continue

            # Try PDF extraction from embedded links
            text = None
            pdf_urls = re.findall(r'href="([^"]+\.pdf[^"]*)"', content_html)
            for purl in pdf_urls:
                if not purl.startswith("http"):
                    purl = urljoin(BASE_URL, purl)
                text = self._extract_pdf_text(purl, post_id)
                if text:
                    break
                time.sleep(1)

            # Fall back to HTML content
            if not text:
                html_text = _strip_html(content_html)
                if len(html_text) >= 100:
                    text = html_text

            if not text:
                logger.debug(f"Skipping post with no substantial content: {title}")
                continue

            total += 1
            yield {
                "id": f"post-{post_id}",
                "title": title,
                "text": text,
                "date": date_str,
                "url": link,
                "doc_type": "doctrine",
            }
            time.sleep(1)

        logger.info(f"WP posts: {total} documents")

        # --- Part 2: Legislation, guidelines, publications from forms page ---
        logger.info("Scraping forms-and-applications page for legislation & guidelines...")
        page_total = 0

        for item in self._scrape_forms_page():
            doc_id = item["id"]
            if doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)

            url = item["url"]
            title = item["title"]

            # Try to download as PDF and extract text
            logger.info(f"Downloading: {title}")
            text = self._extract_pdf_text(url, doc_id)

            if not text:
                # Some pages might have HTML content rather than serving PDFs
                html = self._get_html(url)
                if html:
                    # Check if it's actually a PDF (starts with %PDF)
                    if not html.startswith("%PDF"):
                        html_text = _strip_html(html)
                        if len(html_text) >= 200:
                            text = html_text

            if not text:
                logger.warning(f"No text extracted for: {title}")
                continue

            page_total += 1
            total += 1
            yield {
                "id": doc_id,
                "title": title,
                "text": text,
                "date": item.get("date", ""),
                "url": url,
                "doc_type": item.get("doc_type", "doctrine"),
            }
            time.sleep(2)

        logger.info(f"Forms page: {page_total} documents")
        logger.info(f"Total: {total} documents with full text")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents modified since the given date."""
        since_iso = since.strftime("%Y-%m-%dT%H:%M:%S")
        page = 1
        while True:
            data = self._get_json(POSTS_URL, params={
                "per_page": 100,
                "after": since_iso,
                "orderby": "date",
                "order": "desc",
                "page": page,
                "_fields": "id,title,date,link,content,categories,slug",
            })
            if not data:
                break
            for post in data:
                title = _strip_html(post.get("title", {}).get("rendered", ""))
                content_html = post.get("content", {}).get("rendered", "")
                text = None
                pdf_urls = re.findall(r'href="([^"]+\.pdf[^"]*)"', content_html)
                for purl in pdf_urls:
                    if not purl.startswith("http"):
                        purl = urljoin(BASE_URL, purl)
                    text = self._extract_pdf_text(purl, str(post["id"]))
                    if text:
                        break
                    time.sleep(1)
                if not text:
                    html_text = _strip_html(content_html)
                    if len(html_text) >= 100:
                        text = html_text
                if not text:
                    continue
                yield {
                    "id": f"post-{post['id']}",
                    "title": title,
                    "text": text,
                    "date": post.get("date", ""),
                    "url": post.get("link", ""),
                    "doc_type": "doctrine",
                }
                time.sleep(1)
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

        doc_type = raw.get("doc_type", "doctrine")
        # Map to our three allowed types
        if doc_type == "legislation":
            _type = "legislation"
        else:
            _type = "doctrine"

        return {
            "_id": raw.get("id", ""),
            "_source": "VC/FSA",
            "_type": _type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date_str,
            "url": raw.get("url", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = VCFSAScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        logger.info("Testing connectivity to FSA SVG WP API...")
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
