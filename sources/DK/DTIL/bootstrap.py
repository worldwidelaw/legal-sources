#!/usr/bin/env python3
"""
DK/DTIL -- Danish Data Protection Authority (Datatilsynet) Data Fetcher

Fetches GDPR enforcement decisions from the Danish Data Protection Authority.

Strategy:
  - Discovery: Parse sitemap at /sitemap.xml for decision URLs
  - Filter: Match URLs with pattern /afgoerelser/{section}/YYYY/mon/slug where
    section is afgoerelser (current decisions), historiske-afgoerelser
    (pre-GDPR decisions back to 2000) or tilladelser (permits under § 10)
  - Full text: Fetch each HTML page and extract the `news-page` article body

URL patterns:
  - Sitemap: https://www.datatilsynet.dk/sitemap.xml
  - Decision: https://www.datatilsynet.dk/afgoerelser/afgoerelser/2024/jan/decision-slug

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap-fast     # Full pull, concurrent (used by the fleet)
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update             # Incremental update (new decisions)
  python bootstrap.py test-api           # Quick API connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from xml.etree import ElementTree as ET

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DK.DTIL")

# Constants
BASE_URL = "https://www.datatilsynet.dk"
SITEMAP_URL = "/sitemap.xml"

# URL pattern for individual decisions. The site groups them into three
# sections, all sharing the /YYYY/{danish-month}/{slug} layout.
DECISION_URL_PATTERN = re.compile(
    r"/afgoerelser/(afgoerelser|historiske-afgoerelser|tilladelser)"
    r"/(\d{4})/([a-z]+)/([^/]+)$"
)

# Short code per section, used to keep _id values unique across sections
SECTION_CODE = {
    "afgoerelser": "AFG",
    "historiske-afgoerelser": "HIST",
    "tilladelser": "TILL",
}

# Danish month abbreviations
MONTH_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "maj": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "okt": "10", "nov": "11", "dec": "12",
}


class DatatilsynetScraper(BaseScraper):
    """
    Scraper for DK/DTIL -- Danish Data Protection Authority (Datatilsynet).
    Country: DK
    URL: https://www.datatilsynet.dk

    Data types: regulatory_decisions
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.http = HttpClient(
            base_url=BASE_URL,
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=60,
        )

    def _fetch_sitemap(self) -> list[str]:
        """
        Fetch and parse the sitemap to extract decision URLs.

        Returns:
            List of decision page URLs (full URLs).
        """
        logger.info("Fetching sitemap...")
        self.rate_limiter.wait()
        resp = self.http.get(SITEMAP_URL)
        resp.raise_for_status()

        # Parse XML sitemap
        root = ET.fromstring(resp.content)
        namespace = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        decision_urls = []
        for url_elem in root.findall(".//ns:url/ns:loc", namespace):
            url = url_elem.text
            if url and DECISION_URL_PATTERN.search(url):
                decision_urls.append(url)

        if not decision_urls:
            raise RuntimeError(
                f"Sitemap {BASE_URL}{SITEMAP_URL} returned no decision URLs "
                f"(HTTP {resp.status_code}, {len(resp.content)} bytes). The "
                "sitemap location or URL layout changed, or the request was "
                "blocked — refusing to report an empty crawl as success."
            )

        logger.info(f"Found {len(decision_urls)} decision URLs in sitemap")
        return decision_urls

    def _fetch_decision_page(self, url: str) -> Optional[dict]:
        """
        Fetch a single decision page and extract raw content.

        Returns:
            Dict with raw HTML content and metadata, or None on error.
        """
        try:
            self.rate_limiter.wait()
            resp = self.http.get(url)

            if resp.status_code == 404:
                logger.warning(f"Decision not found: {url}")
                return None

            resp.raise_for_status()

            # Extract URL parts for metadata
            match = DECISION_URL_PATTERN.search(url)
            if not match:
                return None

            section, year, month_abbr, slug = match.groups()

            return {
                "_raw_html": resp.text,
                "_url": url,
                "_section": section,
                "_year": year,
                "_month_abbr": month_abbr,
                "_slug": slug,
            }

        except Exception as e:
            logger.warning(f"Error fetching {url}: {e}")
            return None

    @staticmethod
    def _balanced_div(html_content: str, start: int) -> str:
        """
        Return the inner HTML of the <div> whose opening tag begins at `start`,
        matching nested <div>s so the block is not truncated at the first
        </div>. Falls back to the remainder of the document if unbalanced.
        """
        open_end = html_content.find(">", start)
        if open_end == -1:
            return ""
        depth = 1
        pos = open_end + 1
        tag_re = re.compile(r"<(/?)div\b", re.IGNORECASE)
        while depth:
            m = tag_re.search(html_content, pos)
            if not m:
                return html_content[open_end + 1:]
            depth += -1 if m.group(1) else 1
            pos = m.end()
        return html_content[open_end + 1:html_content.rfind("</div", open_end, pos)]

    def _extract_text_from_html(self, html_content: str) -> str:
        """
        Extract the main decision text from the HTML page.

        The 2025 site rebuild dropped <article>; a decision now lives in
        <div class="news-page"> with the summary in <p class="lead"> and the
        body in one or more <div class="rich-text"> blocks.
        """
        # Remove script and style elements
        html_content = re.sub(r"<script[^>]*>.*?</script>", "", html_content, flags=re.DOTALL | re.IGNORECASE)
        html_content = re.sub(r"<style[^>]*>.*?</style>", "", html_content, flags=re.DOTALL | re.IGNORECASE)

        # Scope to the decision article first so sidebar/footer rich-text
        # blocks elsewhere on the page cannot leak in.
        page = re.search(r'<div[^>]*class="[^"]*\bnews-page\b[^"]*"', html_content, re.IGNORECASE)
        if page:
            html_content = self._balanced_div(html_content, page.start())

        parts = []
        lead = re.search(
            r'<p[^>]*class="[^"]*\blead\b[^"]*"[^>]*>(.*?)</p>',
            html_content, re.DOTALL | re.IGNORECASE,
        )
        if lead:
            parts.append(lead.group(1))

        for m in re.finditer(r'<div[^>]*class="[^"]*\brich-text\b[^"]*"', html_content, re.IGNORECASE):
            parts.append(self._balanced_div(html_content, m.start()))

        if parts:
            html_content = "\n".join(parts)

        # Remove navigation and footer elements
        html_content = re.sub(r"<nav[^>]*>.*?</nav>", "", html_content, flags=re.DOTALL | re.IGNORECASE)
        html_content = re.sub(r"<footer[^>]*>.*?</footer>", "", html_content, flags=re.DOTALL | re.IGNORECASE)
        html_content = re.sub(r"<header[^>]*>.*?</header>", "", html_content, flags=re.DOTALL | re.IGNORECASE)
        html_content = re.sub(r"<aside[^>]*>.*?</aside>", "", html_content, flags=re.DOTALL | re.IGNORECASE)

        # Convert block-level tags to newlines
        block_tags = ["p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "li", "br", "tr"]
        for tag in block_tags:
            html_content = re.sub(f"</{tag}>", "\n", html_content, flags=re.IGNORECASE)
            html_content = re.sub(f"<{tag}[^>]*>", "", html_content, flags=re.IGNORECASE)

        # Remove all remaining HTML tags
        html_content = re.sub(r"<[^>]+>", " ", html_content)

        # Decode HTML entities
        text = html.unescape(html_content)

        # Clean up whitespace
        text = re.sub(r"[ \t]+", " ", text)  # Multiple spaces to single space
        text = re.sub(r"\n[ \t]+", "\n", text)  # Leading whitespace on lines
        text = re.sub(r"[ \t]+\n", "\n", text)  # Trailing whitespace on lines
        text = re.sub(r"\n{3,}", "\n\n", text)  # Multiple newlines to double

        return text.strip()

    def _extract_title_from_html(self, html_content: str) -> str:
        """Extract the decision title from HTML."""
        # Try <h1> first
        match = re.search(r"<h1[^>]*>(.*?)</h1>", html_content, re.DOTALL | re.IGNORECASE)
        if match:
            title = re.sub(r"<[^>]+>", "", match.group(1))
            return html.unescape(title).strip()

        # Try <title> tag
        match = re.search(r"<title[^>]*>(.*?)</title>", html_content, re.DOTALL | re.IGNORECASE)
        if match:
            title = match.group(1).split("|")[0].strip()
            return html.unescape(title).strip()

        return ""

    def _extract_date_from_html(self, html_content: str) -> str:
        """
        Extract the decision date from HTML.

        Datatilsynet uses various date formats in the page metadata.
        """
        # Look for date in meta tags. The rebuilt site renders the publication
        # date as <span class="datetime" data-date="2026-05-05T10:39:00Z">.
        date_patterns = [
            r'<span[^>]*class="[^"]*\bdatetime\b[^"]*"[^>]*data-date="([^"]+)"',
            r'<meta[^>]*property="article:published_time"[^>]*content="([^"]+)"',
            r'<meta[^>]*name="date"[^>]*content="([^"]+)"',
            r'<time[^>]*datetime="([^"]+)"',
        ]

        for pattern in date_patterns:
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match:
                date_str = match.group(1)
                # Try to parse and normalize to ISO format
                try:
                    if "T" in date_str:
                        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                        return dt.strftime("%Y-%m-%d")
                    return date_str[:10]  # Take first 10 chars (YYYY-MM-DD)
                except ValueError:
                    pass

        # Look for date in content (Danish format: DD-MM-YYYY or DD.MM.YYYY)
        date_match = re.search(r"(\d{2})[-.\/](\d{2})[-.\/](\d{4})", html_content)
        if date_match:
            day, month, year = date_match.groups()
            return f"{year}-{month}-{day}"

        return ""

    def _extract_case_number(self, html_content: str) -> str:
        """
        Extract the case number (journalnummer) from HTML.

        Format is typically: YYYY-XXX-NNNN (e.g., 2023-431-0001)
        """
        patterns = [
            r"[Jj]ournalnummer:?\s*(\d{4}-\d+-\d+)",
            r"[Jj]\.?\s*nr\.?:?\s*(\d{4}-\d+-\d+)",
            r"[Ss]agsnummer:?\s*(\d{4}-\d+-\d+)",
            r"\b(\d{4}-\d{2,3}-\d{4})\b",  # Generic case number pattern
        ]

        for pattern in patterns:
            match = re.search(pattern, html_content)
            if match:
                return match.group(1)

        return ""

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all decision documents from Datatilsynet.

        Iterates through all decision URLs from the sitemap.
        """
        for url in self._interleave_sections(self._fetch_sitemap()):
            raw = self._fetch_decision_page(url)
            if raw:
                yield raw

    @staticmethod
    def _interleave_sections(urls: list[str]) -> list[str]:
        """
        Round-robin the three sections so a truncated run (e.g. --sample)
        still covers decisions, historical decisions and permits.
        """
        buckets: dict[str, list[str]] = {}
        for url in urls:
            match = DECISION_URL_PATTERN.search(url)
            buckets.setdefault(match.group(1), []).append(url)

        ordered = []
        for i in range(max((len(b) for b in buckets.values()), default=0)):
            for section in SECTION_CODE:
                bucket = buckets.get(section, [])
                if i < len(bucket):
                    ordered.append(bucket[i])
        return ordered

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield decisions modified since the given date.

        Since we don't have a change API, we re-fetch all and rely on dedup.
        In practice, new decisions are added to the sitemap.
        """
        # Get current year and previous year to limit scope
        current_year = datetime.now().year
        target_years = [str(current_year), str(current_year - 1)]

        decision_urls = self._fetch_sitemap()

        # Filter to recent years only for updates
        recent_urls = [
            url for url in decision_urls
            if any(f"/{year}/" in url for year in target_years)
        ]

        logger.info(f"Checking {len(recent_urls)} recent decisions for updates")

        for url in recent_urls:
            raw = self._fetch_decision_page(url)
            if raw:
                yield raw

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw HTML document into standard schema.

        CRITICAL: Extracts and includes FULL TEXT from HTML content.
        """
        html_content = raw["_raw_html"]
        url = raw["_url"]
        section = raw["_section"]
        year = raw["_year"]
        month_abbr = raw["_month_abbr"]
        slug = raw["_slug"]
        code = SECTION_CODE[section]

        # Extract full text
        full_text = self._extract_text_from_html(html_content)

        # Skip if no meaningful text
        if len(full_text) < 100:
            logger.warning(f"Skipping {url}: text too short ({len(full_text)} chars)")
            return None

        # Extract metadata
        title = self._extract_title_from_html(html_content)
        date = self._extract_date_from_html(html_content)
        case_number = self._extract_case_number(html_content)

        # Build ID from case number or URL components. The section code keeps
        # ids unique when the same journalnummer surfaces in two sections.
        if case_number:
            doc_id = f"DK-DTIL-{code}-{case_number}"
        else:
            doc_id = f"DK-DTIL-{code}-{year}-{month_abbr}-{slug}"

        # Convert month abbr to number for date fallback
        if not date and month_abbr in MONTH_MAP:
            # Use first of month as fallback
            date = f"{year}-{MONTH_MAP[month_abbr]}-01"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "DK/DTIL",
            "_type": "doctrine",  # GDPR regulatory decisions
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # Source-specific fields
            "case_number": case_number,
            "section": section,
            "year": int(year),
            "month": MONTH_MAP.get(month_abbr, ""),
            "slug": slug,
            "authority": "Datatilsynet",
            "country": "DK",
            "language": "da",
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing Datatilsynet endpoints...")

        # Test sitemap
        print("\n1. Testing sitemap endpoint...")
        try:
            urls = self._fetch_sitemap()
            print(f"   Found {len(urls)} decision URLs")
            if urls:
                print(f"   First URL: {urls[0]}")
                print(f"   Last URL: {urls[-1]}")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        # Test fetching a decision
        print("\n2. Testing decision fetch...")
        if urls:
            raw = self._fetch_decision_page(urls[0])
            if raw:
                print(f"   Fetched: {raw['_url']}")
                text = self._extract_text_from_html(raw["_raw_html"])
                print(f"   Text length: {len(text)} characters")
                print(f"   First 200 chars: {text[:200]}...")

                # Test normalization
                record = self.normalize(raw)
                if record:
                    print(f"\n3. Normalized record:")
                    print(f"   ID: {record['_id']}")
                    print(f"   Title: {record['title'][:80]}...")
                    print(f"   Date: {record['date']}")
                    print(f"   Case #: {record['case_number']}")
                    print(f"   Text length: {len(record['text'])} chars")
            else:
                print("   ERROR: Could not fetch decision")

        print("\nAPI test complete!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = DatatilsynetScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "bootstrap-fast":
        # The fleet wrapper invokes this; without it argparse-less dispatch used
        # to exit 1 and the pipeline fell back to re-ingesting sample/.
        stats = scraper.bootstrap_fast()
        print(
            f"\nBootstrap-fast complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated, "
            f"{stats['records_skipped']} skipped"
        )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
