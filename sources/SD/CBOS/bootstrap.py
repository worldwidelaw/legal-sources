#!/usr/bin/env python3
"""
SD/CBOS -- Central Bank of Sudan

Scrapes the CBOS website (Drupal 7) for laws, regulations, circulars, and
policy documents. Content is extracted from Drupal field-name-body divs.
Mix of English (laws/regulations) and Arabic (FX circulars, policies).

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
logger = logging.getLogger("legal-data-hunter.SD.CBOS")

BASE_URL = "https://cbos.gov.sd"
DELAY = 2.0
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Index pages to crawl for discovering content links
INDEX_PAGES = [
    ("/en/content/laws-and-regulations", "law"),
    ("/en/content/financial-institution-and-system-wing-circulars", "circular"),
    ("/en/content/anti-money-laundering-and-financing-terrorism", "circular"),
    ("/en/content/regulations-and-orders-foreign-exchange-operations", "circular"),
]

# Known law/regulation content pages (discovered from the laws-and-regulations index)
LAW_SLUGS = [
    "banking-business-act-2003",
    "anti-money-laundering-financing-terrorism-act",
    "deposit-guarantee-fund-act",
    "electronic-transactions-act-2007",
    "foreign-exchange-dealing-act",
    "informatic-offences-combating-act-2007",
    "property-mortgaged-banks-sale-act-1990",
    "foreign-exchange-dealing-regulation",
    "regulation-governing-business-financial-investment-institutions",
    "regulation-governing-business-financial-leasing-institutions-%E2%80%9Clil-ijara%E2%80%9D-2004",
    "regulation-governing-business-foreign-exchange-bureaus-2002",
    "regulation-governing-licensing-conducting-banking-business-2004",
    "rules-conducting-business-and-licensing-representative-offices-foreign-banks-2003",
]

# Pages to skip (navigation/non-content pages)
SKIP_SLUGS = {
    "laws-and-regulations", "circulars", "copy-right-disclaimer",
    "5-year-strategic-plan", "authorized-exchange-bureaus",
    "banking-spread", "bids-tenders-and-development-projects",
    "correspondents-bank", "financial-institutions",
    "governors-cbos", "important-sites", "international-relations",
    "microfinance-guarantee-agency", "nature-banking-system",
    "operating-banks-sudan", "organizational-administrative-structure",
    "vision-mission-and-core-value", "financial-institution-and-system-wing-circulars",
    "anti-money-laundering-and-financing-terrorism",
    "regulations-and-orders-foreign-exchange-operations",
    "charter-organizing-work-internal-audit-central-bank-sudan",
}


def _clean_html(html: str) -> str:
    """Strip HTML tags, decode entities, clean whitespace."""
    text = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.DOTALL)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(
        r"</?(p|div|li|h[1-6]|tr|td|th|ul|ol|table|thead|tbody)[^>]*>",
        "\n", text, flags=re.I
    )
    text = re.sub(r"<[^>]+>", "", text)
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&quot;", '"').replace("&#039;", "'").replace("&nbsp;", " ")
    text = text.replace("\u00a0", " ")
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
    title = title.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    title = title.replace("&quot;", '"').replace("&#039;", "'")
    return title


def _extract_body_text(html: str) -> str:
    """Extract main body text from Drupal 7 field-name-body."""
    idx = html.find("field-name-body")
    if idx < 0:
        return ""

    segment = html[idx:]
    # Try to extract content from the property="content:encoded" div
    content_match = re.search(
        r'property="content:encoded">(.*?)</div>\s*</div>\s*</div>',
        segment, re.DOTALL
    )
    if content_match:
        return _clean_html(content_match.group(1))

    # Fallback: extract everything from field-name-body to the next major section
    end_match = re.search(
        r'</div>\s*</div>\s*</div>\s*(?:<div class="(?:region|block|footer))',
        segment, re.DOTALL
    )
    if end_match:
        raw = segment[: end_match.start()]
    else:
        raw = segment[:50000]

    return _clean_html(raw)


def _extract_date(text: str, title: str) -> Optional[str]:
    """Try to extract a year from the title or text."""
    # Check title for year pattern
    m = re.search(r"\b(19\d{2}|20[0-2]\d)\b", title)
    if m:
        return f"{m.group(1)}-01-01"
    m = re.search(r"\b(19\d{2}|20[0-2]\d)\b", text[:500])
    if m:
        return f"{m.group(1)}-01-01"
    return None


def _slug_from_path(path: str) -> str:
    """Extract a slug identifier from a URL path."""
    if "/node/" in path:
        m = re.search(r"/node/(\d+)", path)
        return f"node-{m.group(1)}" if m else path
    if "/content/" in path:
        return path.split("/content/")[-1].rstrip("/")
    return path.strip("/").replace("/", "-")


class CBOSScraper(BaseScraper):
    """Scraper for Central Bank of Sudan."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(headers={"User-Agent": UA})

    def _fetch_page(self, path: str) -> Optional[str]:
        """Fetch a page from cbos.gov.sd."""
        if path.startswith("http"):
            url = path
        else:
            url = f"{BASE_URL}{path}"
        try:
            resp = self.http.get(url, timeout=30)
            if resp.status_code == 200:
                return resp.text
            logger.warning("HTTP %d for %s", resp.status_code, url)
            return None
        except Exception as e:
            logger.warning("Error fetching %s: %s", url, e)
            return None

    def _discover_pages(self) -> List[Dict[str, str]]:
        """Discover all content pages from index pages."""
        seen: Set[str] = set()
        pages: List[Dict[str, str]] = []

        for index_path, category in INDEX_PAGES:
            logger.info("Crawling index: %s", index_path)
            html = self._fetch_page(index_path)
            if not html:
                continue

            # Find /en/content/ links
            content_links = re.findall(r'href="(/en/content/[^"]+)"', html)
            for link in content_links:
                slug = link.split("/content/")[-1].rstrip("/")
                if slug in SKIP_SLUGS or slug in seen:
                    continue
                seen.add(slug)
                # Determine category from slug
                cat = category
                if any(kw in slug for kw in ("act-", "act", "-act")):
                    cat = "law"
                elif "regulation" in slug or "rules-" in slug:
                    cat = "regulation"
                elif "policies" in slug:
                    cat = "policy"
                pages.append({"path": link, "slug": slug, "category": cat})

            # Find /node/ and /en/node/ links (circulars, FX operations)
            node_links = re.findall(r'href="(/(?:en/)?node/(\d+))"', html)
            for full_path, node_id in node_links:
                key = f"node-{node_id}"
                if key in seen:
                    continue
                seen.add(key)
                # Normalize to /en/node/ path
                path = f"/en/node/{node_id}"
                pages.append({"path": path, "slug": key, "category": category})

            time.sleep(DELAY)

        # Ensure all known law slugs are included
        for slug in LAW_SLUGS:
            if slug not in seen:
                seen.add(slug)
                pages.append({
                    "path": f"/en/content/{slug}",
                    "slug": slug,
                    "category": "law" if "act" in slug else "regulation",
                })

        # Also discover from the Central Bank Act page (not linked from laws page)
        for extra_slug in ["central-bank-sudan-act-2002"]:
            if extra_slug not in seen:
                seen.add(extra_slug)
                pages.append({
                    "path": f"/en/content/{extra_slug}",
                    "slug": extra_slug,
                    "category": "law",
                })

        logger.info("Total unique pages discovered: %d", len(pages))
        return pages

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents with full text."""
        pages = self._discover_pages()
        logger.info("Processing %d pages", len(pages))

        for i, page in enumerate(pages):
            path = page["path"]
            slug = page["slug"]
            category = page["category"]

            logger.info("[%d/%d] Fetching: %s", i + 1, len(pages), slug)
            html = self._fetch_page(path)
            if not html:
                continue

            title = _extract_title(html)
            text = _extract_body_text(html)

            if not text or len(text.strip()) < 1500:
                logger.warning(
                    "Insufficient text for %s (%d chars), skipping",
                    slug, len(text) if text else 0,
                )
                continue

            url = f"{BASE_URL}{path}"
            yield {
                "slug": slug,
                "title": title,
                "text": text,
                "category": category,
                "url": url,
                "date": _extract_date(text, title),
            }

            time.sleep(DELAY)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Re-fetch all (site has no change tracking)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Normalize a record into the standard schema."""
        return {
            "_id": f"SD_CBOS_{raw['slug']}",
            "_source": "SD/CBOS",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "category": raw.get("category", ""),
            "slug": raw["slug"],
        }


# ── CLI ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = CBOSScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "test":
        logger.info("Testing connectivity to CBOS...")
        html = scraper._fetch_page("/en/content/banking-business-act-2003")
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
