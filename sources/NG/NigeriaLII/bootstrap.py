#!/usr/bin/env python3
"""
NG/NigeriaLII -- Nigeria Legal Information Institute (Peachjam platform)

Fetches court judgments from NigeriaLII via HTML scraping of public pages.
Full text extracted from AKN (Akoma Ntoso) document pages.

Strategy:
  - List courts and years from /judgments/{COURT}/{YEAR}/ pages
  - Extract document URLs from listing pages
  - Fetch full text from individual AKN pages (<div id="document_content">)
  - Respect 5-second crawl delay per robots.txt

API:
  - Base: https://nigerialii.org
  - Court listing: /judgments/{COURT}/
  - Year listing: /judgments/{COURT}/{YEAR}/
  - Document: /akn/ng/judgment/{court}/{year}/{number}/...
  - No auth required for HTML pages

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch ~15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as htmlmod
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NG.NigeriaLII")

BASE_URL = "https://nigerialii.org"

# Courts to harvest (excluding state courts blocked in robots.txt)
COURTS = [
    {"code": "NGSC", "name": "Supreme Court of Nigeria"},
    {"code": "NGCA", "name": "Court of Appeal of Nigeria"},
    {"code": "NGFCHC", "name": "Federal High Court of Nigeria"},
    {"code": "NGHC", "name": "High Court of Nigeria"},
    {"code": "NGCCA", "name": "Court of Criminal Appeal"},
]


def clean_html_text(html_str: str) -> str:
    """Strip HTML tags and clean text."""
    if not html_str:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_str, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<(?:p|div|br|h[1-6]|li|tr|blockquote)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = htmlmod.unescape(text)
    lines = [line.strip() for line in text.split('\n')]
    lines = [line for line in lines if line]
    return '\n'.join(lines).strip()


class NigeriaLIIScraper(BaseScraper):
    """Scraper for NG/NigeriaLII -- Nigerian court judgments."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/worldwidelaw/legal-sources)",
                "Accept": "text/html,application/xhtml+xml,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=60,
        )

    def _get_years_for_court(self, court_code: str) -> List[int]:
        """Get available years for a court from its index page."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(f"/judgments/{court_code}/")
            if not resp or resp.status_code != 200:
                return []
            # Extract year links from the page
            years = re.findall(r'/judgments/' + court_code + r'/(\d{4})/', resp.text)
            return sorted(set(int(y) for y in years), reverse=True)
        except Exception as e:
            logger.warning(f"Error getting years for {court_code}: {e}")
            return []

    def _get_documents_for_year(self, court_code: str, year: int) -> List[Dict]:
        """Get document URLs and metadata from a court/year listing page."""
        docs = []
        page = 1
        while True:
            self.rate_limiter.wait()
            try:
                url = f"/judgments/{court_code}/{year}/"
                if page > 1:
                    url += f"?page={page}"
                resp = self.client.get(url)
                if not resp or resp.status_code != 200:
                    break

                html = resp.text

                # Find document links (AKN URIs)
                # Pattern: <a href="/akn/ng/judgment/...">Title</a>
                pattern = r'<a[^>]*href="(/akn/ng/judgment/[^"]+)"[^>]*>([^<]+)</a>'
                matches = re.findall(pattern, html)

                if not matches:
                    break

                for href, title in matches:
                    title = htmlmod.unescape(title.strip())
                    if not title or title in ("", "Read more"):
                        continue
                    docs.append({
                        "url": href,
                        "title": title,
                        "court_code": court_code,
                        "year": year,
                    })

                # Check for next page
                if f'page={page + 1}' in html or f'?page={page + 1}' in html:
                    page += 1
                else:
                    break

            except Exception as e:
                logger.warning(f"Error listing {court_code}/{year} page {page}: {e}")
                break

        return docs

    def _fetch_document_text(self, doc_url: str) -> Optional[str]:
        """Fetch full text from an AKN document page."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(doc_url)
            if not resp or resp.status_code != 200:
                return None

            html = resp.text

            # Extract content from <div id="document_content">
            # Try multiple patterns
            content = None

            # Pattern 1: document_content div
            match = re.search(
                r'<div[^>]*id="document[_-]content"[^>]*>(.*?)</div>\s*(?:</div>|\s*<div[^>]*class="[^"]*document-)',
                html, re.DOTALL
            )
            if match:
                content = match.group(1)

            # Pattern 2: la-akoma-ntoso content
            if not content:
                match = re.search(
                    r'<la-akoma-ntoso[^>]*>(.*?)</la-akoma-ntoso>',
                    html, re.DOTALL
                )
                if match:
                    content = match.group(1)

            # Pattern 3: between database header/footer comments
            if not content:
                match = re.search(
                    r'<!--make_database header end-->(.*?)<!--(?:sino noindex|make_database footer)-->',
                    html, re.DOTALL
                )
                if match:
                    content = match.group(1)

            # Pattern 4: main content area
            if not content:
                match = re.search(
                    r'<article[^>]*>(.*?)</article>',
                    html, re.DOTALL
                )
                if match:
                    content = match.group(1)

            if not content:
                return None

            text = clean_html_text(content)
            return text if len(text) >= 100 else None

        except Exception as e:
            logger.warning(f"Error fetching document {doc_url}: {e}")
            return None

    def _extract_date_from_url(self, url: str) -> Optional[str]:
        """Extract date from AKN URL pattern like /eng@2017-06-22."""
        match = re.search(r'@(\d{4}-\d{2}-\d{2})', url)
        if match:
            return match.group(1)
        match = re.search(r'/(\d{4})/', url)
        if match:
            return f"{match.group(1)}-01-01"
        return None

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all judgment documents from all courts."""
        for court in COURTS:
            code = court["code"]
            name = court["name"]

            logger.info(f"Processing court: {name} ({code})")
            years = self._get_years_for_court(code)
            if not years:
                logger.warning(f"No years found for {code}")
                continue

            logger.info(f"  {code}: {len(years)} years ({min(years)}-{max(years)})")

            for year in years:
                docs = self._get_documents_for_year(code, year)
                logger.info(f"  {code}/{year}: {len(docs)} documents")

                for doc in docs:
                    yield {
                        "_court_code": code,
                        "_court_name": name,
                        "_year": year,
                        "_doc_url": doc["url"],
                        "_title": doc["title"],
                    }

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent judgments (last 2 years)."""
        current_year = datetime.now().year
        for court in COURTS:
            code = court["code"]
            name = court["name"]
            for year in [current_year, current_year - 1]:
                docs = self._get_documents_for_year(code, year)
                for doc in docs:
                    yield {
                        "_court_code": code,
                        "_court_name": name,
                        "_year": year,
                        "_doc_url": doc["url"],
                        "_title": doc["title"],
                    }

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw data into standard schema, fetching full text."""
        doc_url = raw.get("_doc_url", "")
        title = raw.get("_title", "")
        court_code = raw.get("_court_code", "")
        court_name = raw.get("_court_name", "")

        if not doc_url or not title:
            return None

        text = self._fetch_document_text(doc_url)
        if not text:
            return None

        date_str = self._extract_date_from_url(doc_url)
        doc_id = re.sub(r'[^a-zA-Z0-9._-]', '_', doc_url.strip("/").split("/")[-1] or doc_url)
        unique_id = f"NG-{court_code}-{doc_id}"

        return {
            "_id": unique_id,
            "_source": "NG/NigeriaLII",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": f"{BASE_URL}{doc_url}",
            "court": court_name,
            "court_code": court_code,
            "jurisdiction": "NG",
            "language": "en",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing NigeriaLII...")

        for court in COURTS[:3]:
            code = court["code"]
            name = court["name"]

            print(f"\n--- {name} ({code}) ---")
            years = self._get_years_for_court(code)
            if not years:
                print("  No years found")
                continue

            print(f"  Years: {min(years)}-{max(years)} ({len(years)} years)")

            # Get docs from most recent year
            year = years[0]
            docs = self._get_documents_for_year(code, year)
            print(f"  {year}: {len(docs)} documents")

            if docs:
                doc = docs[0]
                print(f"  First: {doc['title'][:80]}")
                print(f"  URL: {doc['url']}")

                text = self._fetch_document_text(doc["url"])
                if text:
                    print(f"  Full text: {len(text)} chars")
                    print(f"  Sample: {text[:200]}...")
                else:
                    print("  FAILED: No text extracted")

        print("\nTest complete!")


def main():
    scraper = NigeriaLIIScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, {stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
