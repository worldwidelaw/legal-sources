#!/usr/bin/env python3
"""
NG/PLAC -- Nigeria Laws of the Federation (Policy and Legal Advocacy Centre)

Fetches federal legislation from PLAC's online compendium of the 2004 Laws of Nigeria.
Full text extracted from HTML view pages (view2.php?sn=N).

Strategy:
  - Paginate through index pages (?page=1..69) to collect all law sn values and titles
  - Fetch full text from view2.php?sn=N pages
  - Extract text from <div class="field-item even"> container
  - Skip PDF-only entries (no HTML full text available)

API:
  - Base: https://placng.org/lawsofnigeria/
  - Index: ?page=N (N=1..69, ~7 entries per page)
  - View: view2.php?sn=N (full text HTML)
  - No auth required

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
logger = logging.getLogger("legal-data-hunter.NG.PLAC")

BASE_URL = "https://placng.org/lawsofnigeria"
TOTAL_PAGES = 69


def clean_html_text(html_str: str) -> str:
    """Strip HTML tags and clean text."""
    if not html_str:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_str, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<(?:p|div|br|h[1-6]|li|tr|blockquote)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</(?:p|div|li|tr|blockquote|ol|ul|table)>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = htmlmod.unescape(text)
    lines = [line.strip() for line in text.split('\n')]
    lines = [line for line in lines if line]
    return '\n'.join(lines).strip()


class PLACScraper(BaseScraper):
    """Scraper for NG/PLAC -- Nigerian federal legislation."""

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

    def _get_all_law_entries(self, max_pages: int = TOTAL_PAGES) -> List[Dict]:
        """Collect all law entries (sn + title) from paginated index."""
        entries = []
        seen_sns = set()

        for page in range(1, max_pages + 1):
            self.rate_limiter.wait()
            try:
                resp = self.client.get(f"/?page={page}")
                if not resp or resp.status_code != 200:
                    logger.warning(f"Failed to fetch page {page}")
                    continue

                html = resp.text

                # Extract view2.php links with titles (href uses single quotes)
                pattern = r"<a[^>]*href=['\"]view2\.php\?sn=(\d+)['\"][^>]*>\s*([^<]+?)\s*</a>"
                matches = re.findall(pattern, html, re.IGNORECASE)

                for sn, title in matches:
                    title = htmlmod.unescape(title.strip())
                    if sn not in seen_sns and title:
                        seen_sns.add(sn)
                        entries.append({"sn": sn, "title": title})

            except Exception as e:
                logger.warning(f"Error fetching page {page}: {e}")

        logger.info(f"Collected {len(entries)} law entries from {max_pages} pages")
        return entries

    def _fetch_law_text(self, sn: str) -> Optional[str]:
        """Fetch full text from a view2.php page."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(f"/view2.php?sn={sn}")
            if not resp or resp.status_code != 200:
                return None

            html = resp.text

            # Pattern 1: field-item even div (Drupal node content)
            match = re.search(
                r'<div\s+class="field-item\s+even"[^>]*>(.*?)</div>\s*</div>\s*</div>\s*</div>\s*</div>',
                html, re.DOTALL
            )
            if match:
                text = clean_html_text(match.group(1))
                if len(text) >= 100:
                    return text

            # Pattern 2: broader content div
            match = re.search(
                r'<div\s+class="field-item\s+even"[^>]*>(.*?)</div>',
                html, re.DOTALL
            )
            if match:
                text = clean_html_text(match.group(1))
                if len(text) >= 100:
                    return text

            # Pattern 3: content between h3 title and footer
            match = re.search(
                r'<h3>([^<]+)</h3>\s*(?:<p>)?\s*<div\s+class="region[^"]*">(.*?)</div>\s*</div>\s*</td>',
                html, re.DOTALL
            )
            if match:
                text = clean_html_text(match.group(2))
                if len(text) >= 100:
                    return text

            # Pattern 4: everything inside the inner table cell after the h3
            match = re.search(
                r'<h3>[^<]+</h3>(.*?)</td>\s*</tr>\s*</table>',
                html, re.DOTALL
            )
            if match:
                text = clean_html_text(match.group(1))
                if len(text) >= 100:
                    return text

            return None

        except Exception as e:
            logger.warning(f"Error fetching law sn={sn}: {e}")
            return None

    def _extract_commencement_date(self, text: str) -> Optional[str]:
        """Try to extract commencement date from text."""
        match = re.search(r'\[(?:Commencement|Date of commencement)[.\s]*\]\s*\[?\s*(\d{1,2})\s*(?:st|nd|rd|th)?\s+(\w+),?\s+(\d{4})', text)
        if match:
            day, month_str, year = match.groups()
            months = {
                'january': '01', 'february': '02', 'march': '03', 'april': '04',
                'may': '05', 'june': '06', 'july': '07', 'august': '08',
                'september': '09', 'october': '10', 'november': '11', 'december': '12'
            }
            month = months.get(month_str.lower(), '01')
            return f"{year}-{month}-{day.zfill(2)}"
        return None

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all law entries from the index."""
        entries = self._get_all_law_entries()
        for entry in entries:
            yield entry

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Re-fetch all (static compendium, no date-based filtering)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema, fetching full text."""
        sn = raw.get("sn", "")
        title = raw.get("title", "")

        if not sn or not title:
            return None

        text = self._fetch_law_text(sn)
        if not text:
            return None

        date_str = self._extract_commencement_date(text)

        return {
            "_id": f"NG-PLAC-{sn}",
            "_source": "NG/PLAC",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": f"{BASE_URL}/view2.php?sn={sn}",
            "jurisdiction": "NG",
            "language": "en",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing PLAC Laws of Nigeria...")

        # Fetch first page
        resp = self.client.get("/?page=1")
        print(f"Index page: {resp.status_code if resp else 'FAILED'}")

        links = re.findall(r'view2\.php\?sn=(\d+)', resp.text) if resp else []
        print(f"Laws on page 1: {len(links)}")

        if links:
            sn = links[0]
            print(f"\nFetching sn={sn}...")
            text = self._fetch_law_text(sn)
            if text:
                print(f"Full text: {len(text)} chars")
                print(f"Sample: {text[:200]}...")
            else:
                print("FAILED: No text extracted")

        print("\nTest complete!")


def main():
    scraper = PLACScraper()

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
