#!/usr/bin/env python3
"""
INTL/ERCA-Competition -- ECOWAS Regional Competition Authority

Scrapes decisions (HTML posts) and legal instruments (PDF downloads) from
erca-arcc.org. Uses sitemap XML to discover content, extracts full text
from HTML article bodies and PDF documents.

Content types:
  - Decisions: merger control decisions (case_law) published as HTML posts
  - Legal instruments: regulations, supplementary acts, enabling rules (legislation)
  - Official journals, guidelines, market studies (doctrine)

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch ~15 sample records
  python bootstrap.py test               # Connectivity test
"""

import sys
import io
import re
import hashlib
import html as html_lib
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

from pdfminer.high_level import extract_text as pdfminer_extract
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.ERCA-Competition")

BASE_URL = "https://erca-arcc.org"
SITEMAP_POSTS = f"{BASE_URL}/wp-sitemap-posts-post-1.xml"
SITEMAP_FILES = f"{BASE_URL}/wp-sitemap-posts-wpfd_file-1.xml"
DELAY = 2.0

# Post URL patterns for classification
DECISION_PATTERN = re.compile(
    r"decision[-/]|/decision-", re.I
)
RELEASE_PATTERN = re.compile(
    r"release[-/]|notification[-/]|communique[-/]", re.I
)
LEGISLATION_KEYWORDS = re.compile(
    r"\b(?:Regulation|Supplementary\s+Act|Enabling\s+Rule|Directive|"
    r"SA[-/]\d|REG[-/]|PC[-/]REX|A[-/]DIR)\b", re.I
)


def _slug(url: str) -> str:
    """Derive a unique ID slug from a URL."""
    slug = url.rstrip("/").rsplit("/", 1)[-1]
    slug = re.sub(r"[^a-zA-Z0-9_.-]", "_", slug)
    slug = re.sub(r"_+", "_", slug).strip("_")
    if len(slug) > 100:
        # Append hash suffix to avoid collisions on truncated slugs
        h = hashlib.md5(url.encode()).hexdigest()[:8]
        slug = slug[:100] + "_" + h
    return slug


def _extract_date_from_str(s: str) -> Optional[str]:
    """Try to parse ISO date from various formats."""
    s = s.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d", "%B %d, %Y", "%b %d, %Y",
                "%d %B, %Y", "%d %b, %Y", "%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(s[:25], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    m = re.search(r"\b(20\d{2})\b", s)
    if m:
        return f"{m.group(1)}-01-01"
    return None


def _classify_post(url: str, title: str) -> str:
    """Classify a post as case_law, legislation, or doctrine."""
    if DECISION_PATTERN.search(url) or DECISION_PATTERN.search(title):
        combined = f"{url} {title}".lower()
        if "compendium" in combined or "benefits" in combined or "session" in combined:
            return "doctrine"
        return "case_law"
    return "doctrine"


def _classify_file(title: str, url: str) -> str:
    """Classify a PDF file entry."""
    if LEGISLATION_KEYWORDS.search(title) or LEGISLATION_KEYWORDS.search(url):
        return "legislation"
    return "doctrine"


def _clean_html(html_text: str) -> str:
    """Strip HTML tags and decode entities."""
    text = re.sub(r"<br\s*/?>", "\n", html_text)
    text = re.sub(r"</?p[^>]*>", "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_lib.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class ERCACompetitionScraper(BaseScraper):

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,*/*",
        })

    # ---- Sitemap parsing ----

    def _get_sitemap_urls(self, sitemap_url: str) -> list[str]:
        """Parse a WP sitemap XML and return all <loc> URLs."""
        try:
            resp = self.session.get(sitemap_url, timeout=30)
            resp.raise_for_status()
            return re.findall(r"<loc>([^<]+)</loc>", resp.text)
        except Exception as e:
            logger.error(f"Failed to fetch sitemap {sitemap_url}: {e}")
            return []

    # ---- HTML post scraping ----

    def _extract_post_content(self, url: str) -> Optional[dict]:
        """Fetch a post page and extract title, date, and body text."""
        try:
            time.sleep(DELAY)
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch post {url}: {e}")
            return None

        html = resp.text

        # Title from <title> tag
        title_m = re.search(r"<title[^>]*>(.*?)</title>", html, re.DOTALL | re.I)
        title = ""
        if title_m:
            title = _clean_html(title_m.group(1))
            title = re.sub(r"\s*[-–|]\s*ERCA.*$", "", title).strip()

        # Date: try multiple sources
        date = None
        # 1) class="date" elements (e.g., "24 May, 2026")
        date_m = re.search(r'class="date[^"]*"[^>]*>([^<]+)<', html, re.I)
        if date_m:
            date = _extract_date_from_str(date_m.group(1))
        # 2) article:published_time meta
        if not date:
            date_m = re.search(
                r'<meta\s+property="article:published_time"\s+content="([^"]+)"',
                html, re.I
            )
            if date_m:
                date = _extract_date_from_str(date_m.group(1))
        # 3) <time> tag
        if not date:
            time_m = re.search(r'<time[^>]+datetime="([^"]+)"', html, re.I)
            if time_m:
                date = _extract_date_from_str(time_m.group(1))
        # 4) Decision number pattern (EC/D.XX/MM/YY)
        if not date:
            dec_m = re.search(r'EC/D\.\d+/(\d{2})/(\d{2})', html)
            if dec_m:
                month, year = dec_m.group(1), dec_m.group(2)
                full_year = f"20{year}" if int(year) < 50 else f"19{year}"
                date = f"{full_year}-{month}-01"

        # Extract body text from <p> tags within content area
        paragraphs = re.findall(r"<p[^>]*>(.*?)</p>", html, re.DOTALL | re.I)
        body_parts = []
        for p in paragraphs:
            text = _clean_html(p)
            if len(text) > 30:
                body_parts.append(text)

        body = "\n\n".join(body_parts)
        if not body or len(body) < 100:
            return None

        return {
            "title": title or _slug(url).replace("_", " ").title(),
            "date": date,
            "text": body,
            "url": url,
        }

    # ---- PDF file scraping ----

    def _get_pdf_download_url(self, page_url: str) -> Optional[str]:
        """Fetch a wpfd_file page and extract the PDF download link."""
        try:
            time.sleep(DELAY)
            resp = self.session.get(page_url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch file page {page_url}: {e}")
            return None

        m = re.search(
            r'href="(https://erca-arcc\.org/download/[^"]*\.pdf)"',
            resp.text, re.I
        )
        if m:
            return m.group(1)
        return None

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download a PDF and extract text."""
        try:
            time.sleep(DELAY)
            resp = self.session.get(pdf_url, timeout=120)
            resp.raise_for_status()
            if len(resp.content) < 200:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {pdf_url}")
                return None
            text = pdfminer_extract(io.BytesIO(resp.content))
            return text if text and len(text.strip()) >= 50 else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return None

    def _title_from_file_slug(self, slug: str) -> str:
        """Derive a human-readable title from a wpfd_file slug."""
        title = slug.replace("-", " ").replace("_", " ")
        title = re.sub(r"\s+", " ", title).strip()
        return title.title() if title else slug

    # ---- Main fetch methods ----

    def _get_decision_posts(self) -> list[str]:
        """Get URLs of posts that are decisions or significant releases."""
        all_urls = self._get_sitemap_urls(SITEMAP_POSTS)
        decision_urls = []
        for url in all_urls:
            slug_lower = url.lower()
            if any(kw in slug_lower for kw in [
                "decision", "release-decision", "release-notification",
                "communique", "release-proposed", "notification-"
            ]):
                # Skip non-substantive pages
                if any(skip in slug_lower for skip in [
                    "compendium", "benefits-of", "questionnaire",
                    "recruitment", "consultancy", "save-the-date"
                ]):
                    continue
                decision_urls.append(url)
        return decision_urls

    def _get_all_post_urls(self) -> list[str]:
        """Get all post URLs that contain substantive content."""
        all_urls = self._get_sitemap_urls(SITEMAP_POSTS)
        filtered = []
        for url in all_urls:
            slug_lower = url.lower()
            # Skip questionnaires, test pages, recruitment, consultancy
            if any(skip in slug_lower for skip in [
                "questionnaire", "test", "/test-", "recruitment",
                "consultancy", "eysdc", "bonjour"
            ]):
                continue
            filtered.append(url)
        return filtered

    def _get_file_urls(self) -> list[str]:
        """Get all wpfd_file page URLs."""
        return self._get_sitemap_urls(SITEMAP_FILES)

    def fetch_all(self) -> Generator[dict, None, None]:
        post_urls = self._get_all_post_urls()
        file_urls = self._get_file_urls()
        logger.info(f"Found {len(post_urls)} substantive post URLs, {len(file_urls)} file URLs")

        # Yield decisions first, then PDF files, then remaining posts
        decision_urls = []
        other_urls = []
        for url in post_urls:
            if any(kw in url.lower() for kw in ["decision", "release-notification",
                                                  "release-proposed", "communique",
                                                  "notification-"]):
                decision_urls.append(url)
            else:
                other_urls.append(url)

        for url in decision_urls:
            yield {"type": "post", "url": url}
        for url in file_urls:
            yield {"type": "file", "url": url}
        for url in other_urls:
            yield {"type": "post", "url": url}

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        url = raw["url"]
        entry_type = raw["type"]
        slug = _slug(url)

        if entry_type == "post":
            content = self._extract_post_content(url)
            if not content:
                return None
            title = content["title"]
            text = content["text"]
            date = content["date"]
            doc_type = _classify_post(url, title)
            doc_url = url

        elif entry_type == "file":
            pdf_url = self._get_pdf_download_url(url)
            if not pdf_url:
                logger.warning(f"No PDF download found for {url}")
                return None
            text = self._extract_pdf_text(pdf_url)
            if not text or len(text.strip()) < 50:
                logger.warning(f"Insufficient PDF text for {slug}")
                return None
            title = self._title_from_file_slug(slug)
            doc_type = _classify_file(title, url)
            date = None
            # Try to extract date from title or slug
            date_m = re.search(r"\b(20\d{2})\b", slug)
            if date_m:
                date = f"{date_m.group(1)}-01-01"
            doc_url = pdf_url
        else:
            return None

        if len(text.strip()) < 100:
            return None

        doc_id = f"INTL-ERCA-{slug}"

        return {
            "_id": doc_id,
            "_source": "INTL/ERCA-Competition",
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text.strip(),
            "date": date,
            "url": doc_url,
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = ERCACompetitionScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        posts = scraper._get_all_post_urls()
        files = scraper._get_file_urls()
        if not posts and not files:
            print("FAILED: no content found")
            sys.exit(1)
        print(f"OK: {len(posts)} posts, {len(files)} file entries")
        # Test one decision
        decisions = [u for u in posts if "decision" in u.lower()]
        if decisions:
            content = scraper._extract_post_content(decisions[0])
            if content:
                print(f"  Decision sample: {content['title'][:80]}")
                print(f"  Text length: {len(content['text'])} chars")
        # Test one PDF
        if files:
            pdf_url = scraper._get_pdf_download_url(files[0])
            if pdf_url:
                print(f"  PDF sample: {pdf_url}")
                text = scraper._extract_pdf_text(pdf_url)
                if text:
                    print(f"  PDF text length: {len(text)} chars")
    elif command in ("bootstrap", "update"):
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
