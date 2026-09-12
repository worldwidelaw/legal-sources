#!/usr/bin/env python3
"""
INTL/ColumbiaGFE -- Columbia University Global Freedom of Expression Case Law

Fetches case analyses from the Columbia Global Freedom of Expression database
at globalfreedomofexpression.columbia.edu.

Strategy:
  - Parse WordPress sitemap XMLs (case-sitemap.xml through case-sitemap4.xml)
    to collect all case URLs (~3,483 cases)
  - Scrape each case page for metadata and full analysis text
  - No authentication required

Data: case analyses with facts, decision overview, outcome, significance.
Covers 130+ countries, 1918–present, updated weekly.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import html as html_mod
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, List

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.ColumbiaGFE")

BASE_URL = "https://globalfreedomofexpression.columbia.edu"
SITEMAP_INDEX = f"{BASE_URL}/sitemap_index.xml"

# WordPress sitemap namespace
NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

# Date parsing patterns for "Month Day, Year" format
MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    if not text:
        return ""
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</(?:p|div|h[1-6]|li|tr|blockquote)>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = html_mod.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def parse_date(date_str: str) -> Optional[str]:
    """Parse 'Month Day, Year' to ISO 8601 date."""
    if not date_str:
        return None
    date_str = date_str.strip()
    m = re.match(r'(\w+)\s+(\d{1,2}),?\s+(\d{4})', date_str)
    if m:
        month_name, day, year = m.group(1).lower(), int(m.group(2)), int(m.group(3))
        month = MONTH_MAP.get(month_name)
        if month:
            return f"{year:04d}-{month:02d}-{day:02d}"
    # Try year-only
    m = re.match(r'(\d{4})$', date_str)
    if m:
        return f"{m.group(1)}-01-01"
    return None


def extract_slug(url: str) -> str:
    """Extract the case slug from a URL like /cases/some-case-name/."""
    parts = url.rstrip("/").split("/")
    return parts[-1] if parts else url


class ColumbiaGFEScraper(BaseScraper):
    """
    Scraper for INTL/ColumbiaGFE -- Columbia GFE Case Law Database.
    Country: INTL
    URL: https://globalfreedomofexpression.columbia.edu/

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    def _fetch_case_urls_from_sitemaps(self) -> List[str]:
        """Fetch all case URLs from WordPress sitemap XMLs."""
        urls = []
        # First get the sitemap index to find case sitemaps
        self.rate_limiter.wait()
        resp = self.session.get(SITEMAP_INDEX, timeout=30)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)
        sitemap_urls = []
        for sitemap in root.findall("sm:sitemap", NS):
            loc = sitemap.find("sm:loc", NS)
            if loc is not None and "case-sitemap" in loc.text:
                sitemap_urls.append(loc.text)

        logger.info("Found %d case sitemaps", len(sitemap_urls))

        for sitemap_url in sorted(sitemap_urls):
            self.rate_limiter.wait()
            resp = self.session.get(sitemap_url, timeout=30)
            resp.raise_for_status()

            root = ET.fromstring(resp.content)
            for url_elem in root.findall("sm:url", NS):
                loc = url_elem.find("sm:loc", NS)
                if loc is not None:
                    urls.append(loc.text)

            logger.info("  %s: %d URLs (total: %d)", sitemap_url.split("/")[-1], len(urls), len(urls))

        return urls

    def _scrape_case_page(self, url: str) -> Optional[dict]:
        """Scrape a single case page for metadata and full analysis text."""
        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code != 200:
                logger.warning("HTTP %d for %s", resp.status_code, url)
                return None
            html = resp.text
        except requests.RequestException as e:
            logger.warning("Failed to fetch %s: %s", url, e)
            return None

        slug = extract_slug(url)

        # Extract title from <h1> or <title>
        title = ""
        title_match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
        if title_match:
            title = strip_html(title_match.group(1))
        if not title:
            title_match = re.search(r'<title>(.*?)</title>', html)
            if title_match:
                title = strip_html(title_match.group(1)).split(" - ")[0].strip()
        # Remove site prefix
        for prefix in ["Global Freedom of Expression | ", "Global Freedom of Expression |"]:
            if title.startswith(prefix):
                title = title[len(prefix):].strip()

        # Extract metadata from <li><strong>Label</strong> <br/>Value</li> pattern
        metadata = {}
        meta_pattern = re.compile(
            r'<li>\s*<strong>(.*?)</strong>\s*<br\s*/?>\s*(.*?)</li>',
            re.DOTALL | re.IGNORECASE
        )
        for match in meta_pattern.finditer(html):
            label = strip_html(match.group(1)).strip()
            value = strip_html(match.group(2)).strip()
            if label and value:
                metadata[label] = value

        # Also try the pattern with <br /> followed by link tags for Region/Country
        region_pattern = re.compile(
            r'<li>\s*<strong>(Region.*?)</strong>\s*<br\s*/?>\s*(.*?)</li>',
            re.DOTALL | re.IGNORECASE
        )
        for match in region_pattern.finditer(html):
            label = strip_html(match.group(1)).strip()
            value = strip_html(match.group(2)).strip()
            if label and value:
                metadata[label] = value

        # Extract themes (may contain links)
        themes_pattern = re.compile(
            r'<li>\s*<strong>Themes</strong>\s*<br\s*/?>\s*(.*?)</li>',
            re.DOTALL | re.IGNORECASE
        )
        themes_match = themes_pattern.search(html)
        if themes_match:
            metadata["Themes"] = strip_html(themes_match.group(1)).strip()

        # Extract tags
        tags_pattern = re.compile(
            r'<li>\s*<strong>Tags</strong>\s*<br\s*/?>\s*(.*?)</li>',
            re.DOTALL | re.IGNORECASE
        )
        tags_match = tags_pattern.search(html)
        if tags_match:
            metadata["Tags"] = strip_html(tags_match.group(1)).strip()

        # Extract case analysis full text
        # Between "Case Analysis" heading and "Official Case Documents" heading
        text = ""
        analysis_match = re.search(
            r'<h2[^>]*>\s*Case Analysis\s*</h2>(.*?)<h2[^>]*>\s*Official Case Documents\s*</h2>',
            html, re.DOTALL | re.IGNORECASE
        )
        if analysis_match:
            text = strip_html(analysis_match.group(1))
        else:
            # Fallback: try broader pattern
            analysis_match = re.search(
                r'Case Analysis(.*?)Official Case Documents',
                html, re.DOTALL
            )
            if analysis_match:
                text = strip_html(analysis_match.group(1))

        # If still no text, try extracting all content sections
        if not text:
            sections = []
            for heading in ["Case Summary and Outcome", "Facts", "Decision Overview",
                            "Decision Direction", "Global Perspective", "Case Significance"]:
                pattern = rf'<h[23][^>]*>\s*{re.escape(heading)}\s*</h[23]>\s*(.*?)(?=<h[23]|<div[^>]*class="(?:tab|related|official))'
                sec_match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
                if sec_match:
                    sec_text = strip_html(sec_match.group(1))
                    if sec_text:
                        sections.append(f"{heading}\n\n{sec_text}")
            if sections:
                text = "\n\n".join(sections)

        # Extract case status
        status = ""
        status_match = re.search(r'class="case-status\s+([^"]*)"', html)
        if status_match:
            status = status_match.group(1).strip()

        # Extract decision direction
        direction = ""
        dir_match = re.search(r'class="case-direction[^"]*"[^>]*>(.*?)</div>', html, re.DOTALL)
        if dir_match:
            direction = strip_html(dir_match.group(1))

        return {
            "slug": slug,
            "title": title,
            "text": text,
            "url": url,
            "date_of_decision": metadata.get("Date of Decision", ""),
            "outcome": metadata.get("Outcome", ""),
            "case_number": metadata.get("Case Number", ""),
            "region_country": metadata.get("Region & Country", metadata.get("Region &amp; Country", "")),
            "judicial_body": metadata.get("Judicial Body", ""),
            "law_type": metadata.get("Type of Law", ""),
            "expression_type": metadata.get("Mode of Expression", ""),
            "themes": metadata.get("Themes", ""),
            "tags": metadata.get("Tags", ""),
            "status": status,
            "direction": direction,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all case analyses from the GFE database."""
        urls = self._fetch_case_urls_from_sitemaps()
        logger.info("Total case URLs to scrape: %d", len(urls))

        for i, url in enumerate(urls, 1):
            raw = self._scrape_case_page(url)
            if raw and raw.get("text"):
                yield raw
            elif raw:
                logger.warning("No text content for %s", url)
            if i % 50 == 0:
                logger.info("Progress: %d/%d cases scraped", i, len(urls))

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch cases updated since a given date (uses sitemap lastmod)."""
        since_str = since.strftime("%Y-%m-%d")
        # Re-fetch sitemaps and check lastmod dates
        self.rate_limiter.wait()
        resp = self.session.get(SITEMAP_INDEX, timeout=30)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)

        sitemap_urls = []
        for sitemap in root.findall("sm:sitemap", NS):
            loc = sitemap.find("sm:loc", NS)
            if loc is not None and "case-sitemap" in loc.text:
                sitemap_urls.append(loc.text)

        for sitemap_url in sorted(sitemap_urls):
            self.rate_limiter.wait()
            resp = self.session.get(sitemap_url, timeout=30)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)

            for url_elem in root.findall("sm:url", NS):
                loc = url_elem.find("sm:loc", NS)
                lastmod = url_elem.find("sm:lastmod", NS)
                if loc is None:
                    continue
                # If lastmod available and after since, include it
                if lastmod is not None and lastmod.text:
                    mod_date = lastmod.text[:10]
                    if mod_date < since_str:
                        continue
                raw = self._scrape_case_page(loc.text)
                if raw and raw.get("text"):
                    yield raw

    def normalize(self, raw: dict) -> dict:
        """Transform raw case data into standard schema."""
        slug = raw["slug"]
        date = parse_date(raw.get("date_of_decision", ""))

        # Parse themes and tags into lists
        themes_str = raw.get("themes", "")
        themes = [t.strip() for t in re.split(r',\s*', themes_str) if t.strip()] if themes_str else []

        tags_str = raw.get("tags", "")
        tags = [t.strip() for t in re.split(r',\s*', tags_str) if t.strip()] if tags_str else []

        record_id = f"ColumbiaGFE-{slug}"
        return {
            "id": record_id,
            "_id": record_id,
            "_source": "INTL/ColumbiaGFE",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date,
            "url": raw.get("url", ""),
            "outcome": raw.get("outcome", ""),
            "case_number": raw.get("case_number", ""),
            "country": raw.get("region_country", ""),
            "judicial_body": raw.get("judicial_body", ""),
            "law_type": raw.get("law_type", ""),
            "expression_type": raw.get("expression_type", ""),
            "themes": themes,
            "tags": tags,
            "status": raw.get("status", ""),
            "direction": raw.get("direction", ""),
        }


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = ColumbiaGFEScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        print("Testing Columbia GFE connectivity...")
        # Test sitemap access
        resp = requests.get(SITEMAP_INDEX, timeout=30, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh) Chrome/120.0.0.0"
        })
        print(f"  Sitemap index: HTTP {resp.status_code}")

        # Count case sitemaps
        root = ET.fromstring(resp.content)
        case_maps = [s for s in root.findall("sm:sitemap", NS)
                     if "case-sitemap" in s.find("sm:loc", NS).text]
        print(f"  Case sitemaps: {len(case_maps)}")

        # Fetch first case URL
        first_map = case_maps[0].find("sm:loc", NS).text
        resp2 = requests.get(first_map, timeout=30, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh) Chrome/120.0.0.0"
        })
        root2 = ET.fromstring(resp2.content)
        first_url = root2.findall("sm:url", NS)[0].find("sm:loc", NS).text
        print(f"  First case URL: {first_url}")

        # Test scraping a case page
        raw = scraper._scrape_case_page(first_url)
        if raw:
            print(f"  Title: {raw['title'][:80]}")
            print(f"  Text length: {len(raw.get('text', ''))} chars")
            print(f"  Date: {raw.get('date_of_decision', 'N/A')}")
            print(f"  Country: {raw.get('region_country', 'N/A')}")
            print(f"  Outcome: {raw.get('outcome', 'N/A')}")
        print("OK")

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(json.dumps(result, indent=2, default=str))

    elif command == "update":
        since = datetime.now(timezone.utc) - timedelta(days=90)
        result = scraper.update(since=since)
        print(json.dumps(result, indent=2, default=str))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
