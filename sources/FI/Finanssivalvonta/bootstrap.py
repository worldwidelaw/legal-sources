#!/usr/bin/env python3
"""
FI/Finanssivalvonta — Finnish Financial Supervisory Authority (FIN-FSA)

Fetches supervision releases and administrative sanctions from FIN-FSA.

Strategy:
  1. Crawl yearly supervision-release index pages (2013-2026)
  2. Crawl the supervisory measures page for sanctions decision links
  3. Fetch each article page and extract full text from <article> tag

URL patterns:
  - Supervision releases: /en/publications-and-press-releases/supervision-releases/{YYYY}/
  - Press releases:       /en/publications-and-press-releases/Press-release/{YYYY}/{slug}/
  - Supervisory measures: /en/about-the-fin-fsa/powers-and-funding/powers-and-authority/supervisory-measures/

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
  python bootstrap.py test
"""

import sys
import re
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FI.Finanssivalvonta")

BASE_URL = "https://www.finanssivalvonta.fi"

SUPERVISION_YEARS = list(range(2013, 2027))

SUPERVISION_INDEX = "/en/publications-and-press-releases/supervision-releases/{year}/"
MEASURES_PAGE = "/en/about-the-fin-fsa/powers-and-funding/powers-and-authority/supervisory-measures/"

# Pattern for supervision release article URLs
SUPERVISION_RE = re.compile(
    r"/en/publications-and-press-releases/supervision-releases/(\d{4})/([^\"]+?)/?$"
)

# Pattern for press release article URLs (sanctions decisions)
PRESS_RELEASE_RE = re.compile(
    r"/en/publications-and-press-releases/Press-release/(\d{4})/([^\"]+?)/?$"
)


class FinanssivalvontaScraper(BaseScraper):
    """Scraper for FI/Finanssivalvonta — Finnish Financial Supervisory Authority."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=60,
        )

    def _discover_supervision_urls(self) -> list:
        """Crawl yearly index pages for supervision release URLs."""
        urls = []
        for year in SUPERVISION_YEARS:
            path = SUPERVISION_INDEX.format(year=year)
            logger.info(f"Fetching supervision index for {year}...")
            try:
                self.rate_limiter.wait()
                resp = self.http.get(path)
                if resp.status_code != 200:
                    logger.warning(f"  {year} index returned {resp.status_code}")
                    continue
            except Exception as e:
                logger.warning(f"  Error fetching {year} index: {e}")
                continue

            # Extract article links from page HTML
            for m in re.finditer(
                r'href="(/en/publications-and-press-releases/supervision-releases/\d{4}/[^"]+)"',
                resp.text,
            ):
                link = m.group(1).rstrip("/")
                full_url = BASE_URL + link
                if full_url not in urls:
                    urls.append(full_url)

            logger.info(f"  {year}: running total {len(urls)} URLs")

        logger.info(f"Total supervision release URLs: {len(urls)}")
        return urls

    def _discover_sanctions_urls(self) -> list:
        """Crawl supervisory measures page for sanctions decision URLs."""
        logger.info("Fetching supervisory measures page...")
        urls = []
        try:
            self.rate_limiter.wait()
            resp = self.http.get(MEASURES_PAGE)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Error fetching measures page: {e}")
            return urls

        # Extract press release links from the sanctions table
        for m in re.finditer(
            r'href="(/en/publications-and-press-releases/Press-release/\d{4}/[^"]+)"',
            resp.text,
        ):
            link = m.group(1).rstrip("/")
            full_url = BASE_URL + link
            if full_url not in urls:
                urls.append(full_url)

        logger.info(f"Found {len(urls)} sanctions decision URLs")
        return urls

    def _extract_text_from_html(self, html_content: str) -> dict:
        """Extract title, date, and text from article page HTML."""
        import html as html_mod

        title = None
        date_str = None
        text = ""

        # Title: from og:title or <title>
        og_m = re.search(r'property="og:title"\s+content="([^"]*)"', html_content)
        if not og_m:
            og_m = re.search(r'content="([^"]*)"\s+property="og:title"', html_content)
        if og_m:
            title = html_mod.unescape(og_m.group(1).strip())
            # Remove trailing " - YYYY - www.finanssivalvonta.fi" etc.
            title = re.sub(r"\s*-\s*\d{4}\s*-\s*www\.finanssivalvonta\.fi$", "", title)
            title = re.sub(r"\s*-\s*www\.finanssivalvonta\.fi$", "", title)
        if not title:
            tm = re.search(r"<title>([^<]+)</title>", html_content)
            if tm:
                title = html_mod.unescape(tm.group(1).strip())
                title = re.sub(r"\s*-\s*www\.finanssivalvonta\.fi$", "", title)

        # Date: from <time datetime="..."> or article:published_time or text pattern
        time_m = re.search(r'<time[^>]*datetime="([^"]*)"', html_content)
        if time_m:
            date_str = time_m.group(1)[:10]
        if not date_str:
            pub_m = re.search(r'article:published_time"\s+content="([^"]*)"', html_content)
            if pub_m:
                date_str = pub_m.group(1)[:10]
        if not date_str:
            # Try "DD Month YYYY" or "D.M.YYYY" patterns in article content
            article_m = re.search(r"<article[^>]*>(.*?)</article>", html_content, re.DOTALL)
            if article_m:
                article_html = article_m.group(1)
                # Finnish date: D.M.YYYY
                dm = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", article_html)
                if dm:
                    day, month, year = dm.groups()
                    date_str = f"{year}-{int(month):02d}-{int(day):02d}"
                else:
                    # English date: "DD Month YYYY"
                    months_en = {
                        "january": 1, "february": 2, "march": 3, "april": 4,
                        "may": 5, "june": 6, "july": 7, "august": 8,
                        "september": 9, "october": 10, "november": 11, "december": 12,
                    }
                    em = re.search(
                        r"(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})",
                        article_html, re.IGNORECASE,
                    )
                    if em:
                        day, month_name, year = em.groups()
                        m_num = months_en.get(month_name.lower())
                        if m_num:
                            date_str = f"{year}-{m_num:02d}-{int(day):02d}"

        # Extract text from <article> tag
        article_m = re.search(r"<article[^>]*>(.*?)</article>", html_content, re.DOTALL)
        if article_m:
            raw_html = article_m.group(1)
        else:
            # Fallback: look for main content div
            main_m = re.search(r'<main[^>]*>(.*?)</main>', html_content, re.DOTALL)
            raw_html = main_m.group(1) if main_m else html_content

        # Strip HTML tags
        text = re.sub(r"<script[^>]*>.*?</script>", "", raw_html, flags=re.DOTALL)
        text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL)
        text = re.sub(r"<[^>]+>", "\n", text)
        # Decode HTML entities
        text = html_mod.unescape(text)
        # Clean whitespace
        lines = [l.strip() for l in text.split("\n") if l.strip()]
        text = "\n".join(lines)

        return {
            "title": title or "",
            "date": date_str,
            "text": text,
        }

    def _classify_url(self, url: str) -> str:
        """Return 'supervision' or 'sanctions' based on URL pattern."""
        if "/supervision-releases/" in url:
            return "supervision_release"
        return "sanctions_decision"

    def _slug_from_url(self, url: str) -> str:
        """Extract slug from URL."""
        parts = url.rstrip("/").split("/")
        return parts[-1] if parts else url

    def _year_from_url(self, url: str) -> Optional[str]:
        """Extract year from URL."""
        m = re.search(r"/(\d{4})/", url)
        return m.group(1) if m else None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Discover all article URLs from index pages."""
        sup_urls = self._discover_supervision_urls()
        san_urls = self._discover_sanctions_urls()

        # Merge, dedup
        all_urls = []
        seen = set()
        for url in sup_urls + san_urls:
            normalized = url.rstrip("/")
            if normalized not in seen:
                seen.add(normalized)
                all_urls.append(normalized)

        logger.info(f"Total unique URLs to process: {len(all_urls)}")

        for i, url in enumerate(all_urls):
            yield {
                "_url": url,
                "_slug": self._slug_from_url(url),
                "_year": self._year_from_url(url),
                "_category": self._classify_url(url),
                "_index": i,
                "_total": len(all_urls),
            }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield all records (no incremental support)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Fetch article page and normalize."""
        url = raw["_url"]
        slug = raw["_slug"]
        year = raw.get("_year", "")
        category = raw.get("_category", "")
        idx = raw.get("_index", 0)
        total = raw.get("_total", 0)

        logger.info(f"  [{idx+1}/{total}] Fetching {slug[:50]}...")

        try:
            self.rate_limiter.wait()
            resp = self.http.get(url)
            if resp.status_code == 404:
                logger.warning(f"  404: {url}")
                return None
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"  Error fetching {url}: {e}")
            return None

        extracted = self._extract_text_from_html(resp.text)
        text = extracted["text"]

        if not text or len(text) < 50:
            logger.warning(f"  Text too short ({len(text)} chars) for {slug}")
            return None

        date = extracted["date"]
        if not date and year:
            date = f"{year}-01-01"

        title = extracted["title"] or slug.replace("-", " ").title()

        doc_id = f"finfsa_{year}_{slug}"

        logger.info(f"  Extracted {len(text)} chars: {title[:60]}")

        return {
            "_id": doc_id,
            "_source": "FI/Finanssivalvonta",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "slug": slug,
            "year": year,
            "category": category,
            "issuing_body": "Finanssivalvonta (Finnish Financial Supervisory Authority / FIN-FSA)",
            "language": "en",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing FI/Finanssivalvonta endpoints...")

        print("\n1. Fetching 2025 supervision releases index...")
        self.rate_limiter.wait()
        resp = self.http.get(SUPERVISION_INDEX.format(year=2025))
        print(f"   Status: {resp.status_code}")
        links = re.findall(
            r'href="(/en/publications-and-press-releases/supervision-releases/2025/[^"]+)"',
            resp.text,
        )
        print(f"   Found {len(set(links))} release links")

        print("\n2. Fetching supervisory measures page...")
        self.rate_limiter.wait()
        resp = self.http.get(MEASURES_PAGE)
        print(f"   Status: {resp.status_code}")
        san_links = re.findall(
            r'href="(/en/publications-and-press-releases/Press-release/\d{4}/[^"]+)"',
            resp.text,
        )
        print(f"   Found {len(set(san_links))} sanctions links")

        if links:
            sample_url = BASE_URL + list(set(links))[0]
            print(f"\n3. Fetching sample article: {sample_url}")
            self.rate_limiter.wait()
            resp = self.http.get(sample_url)
            print(f"   Status: {resp.status_code}")

            extracted = self._extract_text_from_html(resp.text)
            print(f"   Title: {extracted['title'][:80]}")
            print(f"   Date: {extracted['date']}")
            print(f"   Text: {len(extracted['text'])} chars")
            print(f"   Preview: {extracted['text'][:300]}...")

        print("\nTest complete!")


def main():
    scraper = FinanssivalvontaScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, "
                  f"{stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
