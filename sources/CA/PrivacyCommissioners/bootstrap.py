#!/usr/bin/env python3
"""
CA/PrivacyCommissioners -- Office of the Privacy Commissioner of Canada

Fetches investigation findings from the federal OPC (priv.gc.ca).
Covers PIPEDA findings (businesses) and Privacy Act findings (federal institutions).

Strategy:
  - Bootstrap: Paginate through both PIPEDA and Privacy Act listing pages,
    collect all finding URLs, then fetch full text from each finding page
  - Update: Re-check recent pages for new findings
  - Sample: Fetch 12 recent findings for validation

Source: https://www.priv.gc.ca/en/opc-actions-and-decisions/investigations/

Data notes:
  - ~164 PIPEDA findings (2001-present)
  - ~132 Privacy Act findings (2002-present)
  - Full HTML investigation reports with numbered paragraphs
  - Crown copyright / open access

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test-api           # Quick API connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Tuple
from html.parser import HTMLParser

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CA.PrivacyCommissioners")

BASE_URL = "https://www.priv.gc.ca"

PIPEDA_LIST_PATH = "/en/opc-actions-and-decisions/investigations/investigations-into-businesses/"
PRIVACY_ACT_LIST_PATH = "/en/opc-actions-and-decisions/investigations/investigations-into-federal-institutions/"


class MainContentExtractor(HTMLParser):
    """Extract text from the <main> content area, skipping nav/script/style."""

    def __init__(self):
        super().__init__()
        self.text_parts: List[str] = []
        self.in_main = False
        self.skip_depth = 0
        self.skip_tags = {"script", "style", "nav", "noscript"}

    def handle_starttag(self, tag, attrs):
        if tag == "main":
            self.in_main = True
        if tag in self.skip_tags:
            self.skip_depth += 1

    def handle_endtag(self, tag):
        if tag == "main":
            self.in_main = False
        if tag in self.skip_tags:
            self.skip_depth = max(0, self.skip_depth - 1)
        if self.in_main and self.skip_depth == 0 and tag in (
            "p", "li", "h1", "h2", "h3", "h4", "h5", "h6", "div", "tr", "br",
        ):
            self.text_parts.append("\n")

    def handle_data(self, data):
        if self.in_main and self.skip_depth == 0:
            self.text_parts.append(data)

    def get_text(self) -> str:
        raw = "".join(self.text_parts)
        # Collapse whitespace within lines, preserve paragraph breaks
        lines = raw.split("\n")
        cleaned = []
        for line in lines:
            line = " ".join(line.split())
            if line:
                cleaned.append(line)
        text = "\n".join(cleaned)
        # Remove repeated table-of-contents at end
        toc_marker = "Date modified:"
        idx = text.rfind(toc_marker)
        if idx > 0:
            text = text[:idx].rstrip()
        return text


class CAPrivacyCommissionersScraper(BaseScraper):
    """
    Scraper for CA/PrivacyCommissioners -- OPC investigation findings.
    Country: CA
    URL: https://www.priv.gc.ca/en/opc-actions-and-decisions/investigations/

    Data types: doctrine
    Auth: none (Open Access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html, application/xhtml+xml",
            },
            timeout=60,
        )

    def _get_finding_links(self, list_path: str, category: str) -> List[Tuple[str, str]]:
        """
        Paginate through a listing page and collect all finding links.
        Returns list of (url_path, category) tuples.
        """
        links = []
        seen_urls = set()
        page = 1

        while True:
            url = f"{list_path}?o=d&Page={page}&Filter=True"
            logger.info(f"Fetching {category} list page {page}: {url}")

            try:
                resp = self.client.get(url)
                html = resp.text
            except Exception as e:
                logger.error(f"Failed to fetch list page {page}: {e}")
                break

            self.rate_limiter.wait()

            # Extract finding links (must contain year/slug, not pagination params)
            pattern = re.compile(
                rf'<a[^>]+href="({re.escape(list_path)}\d{{4}}(?:-\d{{2}})?/[^"]+/)"[^>]*>(.*?)</a>',
                re.DOTALL,
            )
            page_links = pattern.findall(html)

            if not page_links:
                logger.info(f"No more links found on page {page}")
                break

            for href, title in page_links:
                if href not in seen_urls:
                    seen_urls.add(href)
                    links.append((href, category))

            logger.info(f"Found {len(page_links)} links on page {page} (total: {len(links)})")

            # Check if there's a next page
            total_match = re.search(r"Showing items \d+ through (\d+) of (\d+)", html)
            if total_match:
                shown = int(total_match.group(1))
                total = int(total_match.group(2))
                if shown >= total:
                    break
            else:
                break

            page += 1
            time.sleep(1)

        return links

    def _fetch_finding(self, url_path: str, category: str) -> Optional[dict]:
        """Fetch and parse a single investigation finding page."""
        full_url = f"{BASE_URL}{url_path}"
        logger.info(f"Fetching finding: {full_url}")

        try:
            resp = self.client.get(url_path)
            html = resp.text
        except Exception as e:
            logger.error(f"Failed to fetch {full_url}: {e}")
            return None

        self.rate_limiter.wait()

        # Extract title
        title_match = re.search(r'<h1 id="wb-cont">(.*?)</h1>', html, re.DOTALL)
        title = title_match.group(1).strip() if title_match else ""
        # Clean any HTML from title
        title = re.sub(r"<[^>]+>", "", title).strip()

        # Extract date from dcterms.issued or dcterms.modified meta tag
        date_str = None
        for meta_pattern in [
            r'dcterms\.issued["\s][^>]*content="(\d{4}-\d{2}-\d{2})"',
            r'dcterms\.modified["\s][^>]*content="(\d{4}-\d{2}-\d{2})"',
        ]:
            m = re.search(meta_pattern, html)
            if m:
                date_str = m.group(1)
                break

        # Extract finding ID from URL or content
        finding_id = url_path.rstrip("/").split("/")[-1]

        # Extract full text
        parser = MainContentExtractor()
        parser.feed(html)
        text = parser.get_text()

        if not text or len(text) < 200:
            logger.warning(f"Short/empty text for {url_path}: {len(text)} chars")
            return None

        return {
            "finding_id": finding_id,
            "title": title,
            "text": text,
            "date": date_str,
            "url": full_url,
            "category": category,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all investigation findings from both PIPEDA and Privacy Act."""
        # Collect all finding links
        all_links = []

        logger.info("Collecting PIPEDA finding links...")
        all_links.extend(self._get_finding_links(PIPEDA_LIST_PATH, "PIPEDA"))

        logger.info("Collecting Privacy Act finding links...")
        all_links.extend(self._get_finding_links(PRIVACY_ACT_LIST_PATH, "Privacy Act"))

        logger.info(f"Total findings to fetch: {len(all_links)}")

        for url_path, category in all_links:
            finding = self._fetch_finding(url_path, category)
            if finding:
                yield finding
            time.sleep(1)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recently published findings."""
        # Check first 2 pages of each category for recent items
        for list_path, category in [
            (PIPEDA_LIST_PATH, "PIPEDA"),
            (PRIVACY_ACT_LIST_PATH, "Privacy Act"),
        ]:
            for page in range(1, 3):
                url = f"{list_path}?o=d&Page={page}&Filter=True"
                try:
                    resp = self.client.get(url)
                    html = resp.text
                except Exception:
                    continue

                self.rate_limiter.wait()

                pattern = re.compile(
                    rf'<a[^>]+href="({re.escape(list_path)}[^"]+)"[^>]*>',
                    re.DOTALL,
                )
                for match in pattern.finditer(html):
                    url_path = match.group(1)
                    finding = self._fetch_finding(url_path, category)
                    if finding and finding.get("date"):
                        try:
                            finding_date = datetime.strptime(finding["date"], "%Y-%m-%d").replace(
                                tzinfo=timezone.utc
                            )
                            if finding_date >= since:
                                yield finding
                        except ValueError:
                            yield finding
                    time.sleep(1)

    def normalize(self, raw: dict) -> dict:
        """Transform raw finding data into standard schema."""
        finding_id = raw.get("finding_id", "unknown")
        return {
            "_id": f"ca_opc_{finding_id}",
            "_source": "CA/PrivacyCommissioners",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "finding_id": finding_id,
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "category": raw.get("category", ""),
        }

    def test_api(self):
        """Quick connectivity test."""
        logger.info("Testing OPC website connectivity...")

        # Test PIPEDA list page
        try:
            resp = self.client.get(f"{PIPEDA_LIST_PATH}?o=d&Page=1&Filter=True")
            total_match = re.search(r"Showing items \d+ through \d+ of (\d+)", resp.text)
            pipeda_count = int(total_match.group(1)) if total_match else "unknown"
            logger.info(f"PIPEDA findings: {pipeda_count}")
        except Exception as e:
            logger.error(f"PIPEDA list failed: {e}")
            return False

        # Test Privacy Act list page
        try:
            resp = self.client.get(f"{PRIVACY_ACT_LIST_PATH}?o=d&Page=1&Filter=True")
            total_match = re.search(r"Showing items \d+ through \d+ of (\d+)", resp.text)
            pa_count = int(total_match.group(1)) if total_match else "unknown"
            logger.info(f"Privacy Act findings: {pa_count}")
        except Exception as e:
            logger.error(f"Privacy Act list failed: {e}")
            return False

        # Test fetching a single finding
        try:
            finding = self._fetch_finding(
                f"{PIPEDA_LIST_PATH}2025/pipeda-2025-004/", "PIPEDA"
            )
            if finding and len(finding.get("text", "")) > 1000:
                logger.info(f"Sample finding: {finding['title'][:60]}... ({len(finding['text'])} chars)")
                return True
            else:
                logger.error("Sample finding text too short or missing")
                return False
        except Exception as e:
            logger.error(f"Sample finding fetch failed: {e}")
            return False


# ── CLI entry point ───────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="CA/PrivacyCommissioners bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (12 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap")

    args = parser.parse_args()
    scraper = CAPrivacyCommissionersScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        result = scraper.bootstrap(sample_mode=not args.full, sample_size=12)
        logger.info(f"Bootstrap complete: {json.dumps(result, indent=2, default=str)}")
    elif args.command == "update":
        from datetime import timedelta

        since = datetime.now(timezone.utc) - timedelta(days=30)
        result = scraper.update(since)
        logger.info(f"Update complete: {json.dumps(result, indent=2, default=str)}")


if __name__ == "__main__":
    main()
