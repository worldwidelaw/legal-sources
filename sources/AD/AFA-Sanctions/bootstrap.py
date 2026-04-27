#!/usr/bin/env python3
"""
AD/AFA-Sanctions -- Andorra Financial Authority Sanctions

Fetches sanctions on supervised entities from the AFA website (Plone CMS).
Scrapes listing pages and individual detail pages for full text.

Endpoints:
  - Listing: /en/entitats-supervisades/sancions-a-entitats-supervisades?b_start:int={offset}
  - Detail: /en/entitats-supervisades/sancions-a-entitats-supervisades/{slug}/view

Data:
  - ~15 sanctions (serious/very serious infringements)
  - Full text in HTML detail pages
  - Language: English (also Catalan, Spanish, French)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import html as html_mod
import hashlib
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AD.AFA-Sanctions")

BASE_URL = "https://www.afa.ad"
LISTING_PATH = "/en/entitats-supervisades/sancions-a-entitats-supervisades"
PER_PAGE = 10

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")

# Link extraction: match both absolute and relative URLs to detail pages
DETAIL_LINK_RE = re.compile(
    r'href="((?:https?://www\.afa\.ad)?/en/entitats-supervisades/'
    r'sancions-a-entitats-supervisades/[^"]+/view)"',
    re.IGNORECASE,
)

# Title: try documentFirstHeading class first, then any h1 inside #content
TITLE_RE_PRIMARY = re.compile(
    r'<h1[^>]*class="[^"]*documentFirstHeading[^"]*"[^>]*>(.*?)</h1>', re.DOTALL
)
TITLE_RE_FALLBACK = re.compile(
    r'<h1[^>]*>\s*((?:Legal entity|Individual|Personne|Entitat)[^<]*)</h1>', re.DOTALL
)

# Description / status
DESC_RE_PRIMARY = re.compile(
    r'<div[^>]*class="[^"]*documentDescription[^"]*"[^>]*>(.*?)</div>', re.DOTALL
)
DESC_RE_FALLBACK = re.compile(
    r'(?:Status|Estat|Statut)\s*:\s*([^<]{5,100})', re.IGNORECASE
)

# Full text: multiple strategies
TEXT_RE_PRIMARY = re.compile(
    r'<div[^>]*id="parent-fieldname-text"[^>]*>(.*?)</div>\s*</div>', re.DOTALL
)
TEXT_RE_CONTENT_CORE = re.compile(
    r'<div[^>]*id="content-core"[^>]*>(.*?)</div>\s*(?:</div>\s*)*</article>', re.DOTALL
)
TEXT_RE_CONTENT_BODY = re.compile(
    r'<div[^>]*id="content"[^>]*>(.*?)</footer>', re.DOTALL
)

# Date extraction from listing page entries
DATE_RE = re.compile(r'(\d{2}/\d{2}/\d{4})')


def strip_html(html_str: str) -> str:
    """Remove HTML tags and normalize whitespace."""
    text = TAG_RE.sub(" ", html_str)
    text = html_mod.unescape(text)
    text = WS_RE.sub(" ", text).strip()
    return text


def _normalize_link(href: str) -> str:
    """Ensure link is a relative path (strip domain if present)."""
    href = href.strip()
    if href.startswith("https://www.afa.ad"):
        href = href[len("https://www.afa.ad"):]
    elif href.startswith("http://www.afa.ad"):
        href = href[len("http://www.afa.ad"):]
    return href


class AFASanctionsScraper(BaseScraper):
    """Scraper for AD/AFA-Sanctions -- Andorra Financial Authority Sanctions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9,ca;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            },
            timeout=90,
            max_retries=5,
            backoff_factor=2.0,
        )

    def _fetch_page(self, url: str, label: str = "page") -> Optional[str]:
        """Fetch a page with retries and detailed error logging."""
        for attempt in range(3):
            try:
                self.rate_limiter.wait()
                resp = self.client.get(url)
                resp.raise_for_status()
                html_text = resp.text
                if len(html_text) < 500:
                    logger.warning(
                        f"  {label}: suspiciously short response ({len(html_text)} chars), "
                        f"attempt {attempt + 1}/3"
                    )
                    if attempt < 2:
                        time.sleep(5 * (attempt + 1))
                        continue
                return html_text
            except Exception as e:
                logger.warning(f"  {label}: fetch failed (attempt {attempt + 1}/3): {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        return None

    def _get_detail_links(self) -> list[dict]:
        """Fetch listing pages and extract detail page URLs with metadata."""
        entries = []
        seen_paths = set()
        offset = 0

        while True:
            url = f"{LISTING_PATH}?b_start:int={offset}"
            logger.info(f"Fetching listing page (offset={offset})...")

            html_text = self._fetch_page(url, label=f"listing(offset={offset})")
            if not html_text:
                logger.error(f"Failed to fetch listing page at offset={offset}")
                break

            found = DETAIL_LINK_RE.findall(html_text)
            if not found:
                # If this is the first page and we found nothing, log the issue
                if offset == 0:
                    logger.error(
                        f"No detail links found on first listing page! "
                        f"HTML length={len(html_text)}, "
                        f"contains 'sancions'={'sancions' in html_text.lower()}, "
                        f"contains '/view'={'/view' in html_text}"
                    )
                    # Try a broader link search as fallback
                    broad_links = re.findall(
                        r'href="([^"]*sancions-a-entitats-supervisades/[^"]+)"',
                        html_text,
                        re.IGNORECASE,
                    )
                    if broad_links:
                        logger.info(f"Broad search found {len(broad_links)} potential links")
                        for bl in broad_links:
                            if "/view" not in bl:
                                # Append /view if not present
                                bl = bl.rstrip("/") + "/view"
                            path = _normalize_link(bl)
                            if path not in seen_paths:
                                seen_paths.add(path)
                                entries.append({"path": path, "date_str": None})
                break

            for href in found:
                path = _normalize_link(href)
                if path not in seen_paths:
                    seen_paths.add(path)
                    # Try to extract date near this link
                    idx = html_text.find(href)
                    context = html_text[max(0, idx - 200):idx + 500] if idx >= 0 else ""
                    date_match = DATE_RE.search(context)
                    date_str = date_match.group(1) if date_match else None
                    entries.append({"path": path, "date_str": date_str})

            logger.info(f"  Found {len(found)} links on this page (total unique: {len(entries)})")

            if len(found) < PER_PAGE:
                break
            offset += PER_PAGE

        return entries

    def _extract_text_from_html(self, html: str) -> dict:
        """Extract title, status, and full text from a detail page HTML.

        Uses multiple fallback strategies to handle different Plone template
        variations and ensure text extraction even when the page structure
        changes slightly.
        """
        # --- Title ---
        title = ""
        m = TITLE_RE_PRIMARY.search(html)
        if m:
            title = strip_html(m.group(1))
        else:
            m = TITLE_RE_FALLBACK.search(html)
            if m:
                title = strip_html(m.group(1))

        # --- Status / description ---
        status = ""
        m = DESC_RE_PRIMARY.search(html)
        if m:
            status = strip_html(m.group(1))
        else:
            m = DESC_RE_FALLBACK.search(html)
            if m:
                status = strip_html(m.group(1)).strip()

        # --- Full text body ---
        text = ""

        # Strategy 1: parent-fieldname-text (standard Plone)
        m = TEXT_RE_PRIMARY.search(html)
        if m:
            text = strip_html(m.group(1))

        # Strategy 2: content-core div
        if not text or len(text) < 30:
            m = TEXT_RE_CONTENT_CORE.search(html)
            if m:
                candidate = strip_html(m.group(1))
                if len(candidate) > len(text):
                    text = candidate

        # Strategy 3: broader content div (between #content and footer)
        if not text or len(text) < 30:
            m = TEXT_RE_CONTENT_BODY.search(html)
            if m:
                candidate = strip_html(m.group(1))
                # Filter out navigation noise: only keep if it has sanction keywords
                if any(kw in candidate.lower() for kw in
                       ["penalty", "sanction", "infringement", "fine", "euros",
                        "sanció", "infracció", "multa"]):
                    # Try to isolate the relevant portion
                    # Look for the title in the text and take everything after it
                    if title and title in candidate:
                        idx = candidate.index(title) + len(title)
                        candidate = candidate[idx:].strip()
                    if len(candidate) > len(text):
                        text = candidate

        # Strategy 4: extract all paragraphs with sanction-related content
        if not text or len(text) < 30:
            paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', html, re.DOTALL)
            sanction_paras = []
            for p in paragraphs:
                p_text = strip_html(p)
                if len(p_text) > 20 and any(
                    kw in p_text.lower() for kw in
                    ["penalty", "sanction", "infringement", "euros", "amount",
                     "suspension", "article", "law", "regulation",
                     "sanció", "infracció", "llei", "import"]
                ):
                    sanction_paras.append(p_text)
            if sanction_paras:
                text = " ".join(sanction_paras)

        # --- Date from detail page (fallback for when listing doesn't provide it) ---
        date_str = None
        # Look for date near the title or in the main content area
        # Common formats on AFA: DD/MM/YYYY
        date_candidates = DATE_RE.findall(html)
        for dc in date_candidates:
            # Validate it's a plausible date (not a random number sequence)
            try:
                dt = datetime.strptime(dc, "%d/%m/%Y")
                if 2015 <= dt.year <= 2030:
                    date_str = dc
                    break
            except ValueError:
                continue

        return {
            "title": title,
            "status": status,
            "text": text,
            "detail_date_str": date_str,
        }

    def _fetch_detail(self, entry: dict) -> Optional[Dict[str, Any]]:
        """Fetch a detail page and extract title, status, and full text."""
        path = entry["path"]
        html = self._fetch_page(path, label=path.split("/")[-2])
        if not html:
            return None

        extracted = self._extract_text_from_html(html)

        slug = path.split("/")[-2] if "/view" in path else path.split("/")[-1]

        # Use listing date if available, otherwise fall back to detail page date
        date_str = entry.get("date_str") or extracted.get("detail_date_str")

        return {
            "slug": slug,
            "path": path,
            "title": extracted["title"],
            "status": extracted["status"],
            "text": extracted["text"],
            "date_str": date_str,
        }

    def _parse_date(self, date_str: Optional[str]) -> Optional[str]:
        """Parse DD/MM/YYYY to ISO format."""
        if not date_str:
            return None
        try:
            dt = datetime.strptime(date_str, "%d/%m/%Y")
            return dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        slug = raw.get("slug", "")
        doc_id = hashlib.sha256(slug.encode()).hexdigest()[:16]
        path = raw.get("path", "")
        full_url = f"{BASE_URL}{path}" if path else ""

        # Combine status and text for full content
        status = raw.get("status", "")
        text = raw.get("text", "")
        # Avoid "Status: Status: ..." duplication -- the HTML description
        # may already start with "Status:"
        status_line = status
        if status_line and not status_line.lower().startswith("status"):
            status_line = f"Status: {status_line}"
        if status_line and text:
            full_text = f"{status_line}\n\n{text}"
        elif text:
            full_text = text
        elif status_line:
            full_text = status_line
        else:
            full_text = ""

        return {
            "_id": f"AD/AFA-Sanctions/{doc_id}",
            "_source": "AD/AFA-Sanctions",
            "_type": "doctrine",
            "_fetched_at": now,
            "title": raw.get("title", ""),
            "text": full_text,
            "date": self._parse_date(raw.get("date_str")),
            "url": full_url,
            "doc_id": doc_id,
            "status": raw.get("status", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        entries = self._get_detail_links()
        logger.info(f"Found {len(entries)} sanctions to fetch")

        if not entries:
            logger.error(
                "No sanction entries found. The website may be blocking this IP, "
                "or the page structure may have changed."
            )
            return

        limit = 15 if sample else None
        count = 0

        for entry in entries:
            if limit and count >= limit:
                break

            slug = entry["path"].split("/")[-2]
            logger.info(f"  [{count + 1}/{len(entries)}] Fetching {slug}")
            detail = self._fetch_detail(entry)
            if not detail:
                logger.warning(f"    Failed to fetch detail page for {slug}")
                continue

            text = detail.get("text", "")
            if len(text) < 20:
                logger.warning(f"    Short text ({len(text)} chars) for {slug}")
                # Still yield it if we have a title -- some sanctions may have
                # minimal text but we should not silently drop them
                if not detail.get("title"):
                    logger.warning(f"    Skipping - no title and text too short")
                    continue

            yield detail
            count += 1

        logger.info(f"Fetched {count} sanctions total")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def test_connection(self):
        """Quick connectivity test against the AFA website."""
        print(f"Testing connection to {BASE_URL}...")
        try:
            html = self._fetch_page(LISTING_PATH, label="test")
            if html is None:
                print("FAIL: Could not fetch listing page")
                sys.exit(1)

            print(f"  HTML length: {len(html)}")
            links = DETAIL_LINK_RE.findall(html)
            print(f"  Detail links found: {len(links)}")

            if links:
                # Test first detail page
                path = _normalize_link(links[0])
                print(f"  Testing detail page: {path}")
                detail_html = self._fetch_page(path, label="test-detail")
                if detail_html:
                    extracted = self._extract_text_from_html(detail_html)
                    print(f"  Title: {extracted['title'][:60]}")
                    print(f"  Status: {extracted['status'][:60]}")
                    print(f"  Text length: {len(extracted['text'])}")
                    print(f"  Text preview: {extracted['text'][:120]}...")
                    print("OK: Connection successful, text extraction working")
                else:
                    print("FAIL: Could not fetch detail page")
                    sys.exit(1)
            else:
                print("WARN: No detail links found on listing page")
                print(f"  Page contains 'sancions': {'sancions' in html.lower()}")
                print(f"  Page contains '/view': {'/view' in html}")
                sys.exit(1)
        except Exception as e:
            print(f"FAIL: {e}")
            sys.exit(1)


if __name__ == "__main__":
    scraper = AFASanctionsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
