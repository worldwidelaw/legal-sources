#!/usr/bin/env python3
"""
AE/CBUAE -- Central Bank of the UAE Rulebook

Scrapes the CBUAE Rulebook (Drupal 10) for regulations, standards, and guidelines.
Each regulation page contains full text with structured articles.

Usage:
  python bootstrap.py bootstrap --sample    # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Quick connectivity test
"""

import sys
import re
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict, Set

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AE.CBUAE")

BASE_URL = "https://rulebook.centralbank.ae"
CATEGORIES = [
    "/en/rulebook/all-licensed-financial-institutions",
    "/en/rulebook/banking",
    "/en/rulebook/insurance",
    "/en/rulebook/other-regulated-entities",
]
DELAY = 2.0
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _clean_html(html: str) -> str:
    """Strip HTML tags, decode entities, clean whitespace."""
    text = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.DOTALL)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</?(p|div|li|h[1-6]|tr|td|th|ul|ol|table|thead|tbody)[^>]*>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    # Decode common HTML entities
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&quot;", '"').replace("&#039;", "'").replace("&nbsp;", " ")
    text = text.replace("\u00a0", " ")
    # Clean whitespace
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _extract_title(html: str) -> str:
    """Extract page title from HTML."""
    m = re.search(r"<title>(.*?)\s*\|", html)
    if m:
        title = m.group(1).strip()
    else:
        m = re.search(r"<title>(.*?)</title>", html)
        title = m.group(1).strip() if m else ""
    # Decode HTML entities in title
    title = title.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    title = title.replace("&quot;", '"').replace("&#039;", "'")
    return title


def _extract_body_text(html: str) -> str:
    """Extract the main regulation text from the page body."""
    # Try to find the main content area (Drupal field--name-body or article region)
    body_match = re.search(
        r'<article[^>]*>(.*?)</article>',
        html, re.DOTALL
    )
    if body_match:
        text = _clean_html(body_match.group(1))
    else:
        # Fallback: look for field--name-body divs
        bodies = re.findall(
            r"class=['\"]field--name-body[^'\"]*['\"][^>]*>(.*?)</div>",
            html, re.DOTALL
        )
        if bodies:
            text = _clean_html("\n\n".join(bodies))
        else:
            # Fallback: look for page-title and content region
            content_match = re.search(
                r'class="[^"]*region-content[^"]*"[^>]*>(.*?)</(?:div|main|section)',
                html, re.DOTALL
            )
            if content_match:
                text = _clean_html(content_match.group(1))
            else:
                return ""

    # Remove Drupal navigation boilerplate
    text = re.sub(r"Book traversal links for.*?\n", "", text)
    text = re.sub(r"^[‹›].*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^Up\s*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"Main navigation\n?", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _extract_category_from_breadcrumb(html: str) -> str:
    """Extract category from breadcrumb navigation."""
    bc = re.search(r'class="[^"]*breadcrumb[^"]*"[^>]*>(.*?)</(?:nav|ol|ul|div)', html, re.DOTALL)
    if bc:
        links = re.findall(r'<a[^>]*>(.*?)</a>', bc.group(1))
        for link in links:
            text = _clean_html(link).lower()
            if "banking" in text:
                return "banking"
            if "insurance" in text:
                return "insurance"
            if "other" in text:
                return "other_regulated_entities"
            if "all licensed" in text or "all-licensed" in text:
                return "all_licensed_financial_institutions"
    return "general"


class CBUAEScraper(BaseScraper):
    """Scraper for CBUAE Rulebook."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(headers={"User-Agent": UA})

    def _fetch_page(self, path: str) -> Optional[str]:
        """Fetch a page from the rulebook site."""
        url = f"{BASE_URL}{path}" if path.startswith("/") else path
        try:
            resp = self.http.get(url, timeout=30)
            if resp.status_code == 200:
                return resp.text
            logger.warning("HTTP %d for %s", resp.status_code, url)
            return None
        except Exception as e:
            logger.warning("Error fetching %s: %s", url, e)
            return None

    def _discover_regulation_slugs(self) -> List[Dict[str, str]]:
        """Discover all regulation page slugs from category pages."""
        all_slugs: Dict[str, str] = {}  # slug -> category

        for cat_path in CATEGORIES:
            cat_name = cat_path.split("/")[-1]
            logger.info("Discovering regulations from category: %s", cat_name)

            html = self._fetch_page(cat_path)
            if not html:
                logger.warning("Failed to fetch category: %s", cat_path)
                continue

            links = re.findall(r'href="(/en/rulebook/[^"]+)"', html)
            for link in links:
                slug = link.replace("/en/rulebook/", "")
                # Skip the 4 top-level category pages
                if slug in (
                    "all-licensed-financial-institutions",
                    "banking",
                    "insurance",
                    "other-regulated-entities",
                ):
                    continue
                if slug not in all_slugs:
                    all_slugs[slug] = cat_name

            logger.info("Category %s: found %d regulation links", cat_name, len(links))
            time.sleep(DELAY)

        result = [{"slug": s, "category": c} for s, c in all_slugs.items()]
        logger.info("Total unique regulations discovered: %d", len(result))
        return result

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all regulation documents with full text."""
        regulations = self._discover_regulation_slugs()
        logger.info("Total regulations to process: %d", len(regulations))

        for i, reg in enumerate(regulations):
            slug = reg["slug"]
            category = reg["category"]
            path = f"/en/rulebook/{slug}"

            logger.info("[%d/%d] Fetching: %s", i + 1, len(regulations), slug)
            html = self._fetch_page(path)
            if not html:
                continue

            title = _extract_title(html)
            text = _extract_body_text(html)

            if not text or len(text.strip()) < 100:
                logger.warning("Insufficient text for %s (%d chars), skipping",
                               slug, len(text) if text else 0)
                continue

            yield {
                "slug": slug,
                "title": title,
                "text": text,
                "category": category,
                "url": f"{BASE_URL}{path}",
            }

            time.sleep(DELAY)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch updates since a given date (re-fetches all for this source)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Normalize a regulation record into the standard schema."""
        slug = raw["slug"]
        return {
            "_id": f"AE_CBUAE_{slug}",
            "_source": "AE/CBUAE",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": None,
            "url": raw["url"],
            "category": raw.get("category", ""),
            "slug": slug,
        }


# ── CLI ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    scraper = CBUAEScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "test":
        logger.info("Testing connectivity to CBUAE Rulebook...")
        html = scraper._fetch_page("/en/rulebook/consumer-protection-regulation")
        if html:
            title = _extract_title(html)
            text = _extract_body_text(html)
            logger.info("OK — Title: %s | Text length: %d chars", title, len(text))
        else:
            logger.error("FAILED — could not fetch test page")
            sys.exit(1)

    elif cmd == "bootstrap":
        sample = "--sample" in sys.argv
        stats = scraper.bootstrap(sample_mode=sample, sample_size=15)
        logger.info("Bootstrap complete: %s", stats)

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
