#!/usr/bin/env python3
"""
World Wide Law - UK Advertising Standards Authority (ASA) Scraper

Fetches rulings from the ASA using:
  - POST /_filteredRuling/ (AJAX listing endpoint, returns HTML fragments)
  - GET /rulings/{slug}.html (individual ruling pages with full text)

Coverage: ~5 years of rulings. Approximately 864 rulings per year.
Published weekly (Wednesdays).

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
"""

import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional
from html import unescape

from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("UK/ASA")


class UKASAScraper(BaseScraper):
    """
    Scraper for: UK Advertising Standards Authority (ASA)
    Country: UK
    URL: https://www.asa.org.uk

    Data types: doctrine
    Auth: none

    Strategy:
    - POST to /_filteredRuling/ with date ranges and pagination
    - Extract ruling URLs from HTML fragments
    - Fetch each ruling page for full text
    """

    BASE_URL = "https://www.asa.org.uk"
    LISTING_URL = "/page-types/ruling_page/_filteredRuling/"
    RESULTS_PER_PAGE = 20

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=self.BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
            },
        )
        self.ajax_headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "*/*",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all rulings by paginating through the listing endpoint."""
        # Use quarterly date ranges to keep result sets manageable
        end_date = datetime.now()
        # Go back 5 years
        start_year = end_date.year - 5

        current_end = end_date
        while current_end.year >= start_year:
            quarter_start = current_end - timedelta(days=90)
            yield from self._fetch_date_range(quarter_start, current_end)
            current_end = quarter_start - timedelta(days=1)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield rulings published since the given datetime."""
        yield from self._fetch_date_range(since, datetime.now())

    def _fetch_date_range(self, start: datetime, end: datetime) -> Generator[dict, None, None]:
        """Fetch rulings within a date range."""
        from_date = start.strftime("%d/%m/%Y")
        to_date = end.strftime("%d/%m/%Y")
        logger.info(f"Fetching rulings: {from_date} to {to_date}")

        page = 1
        max_pages = 200  # Safety limit

        while page <= max_pages:
            self.rate_limiter.wait()

            data = {
                "page": str(page),
                "from_date": from_date,
                "to_date": to_date,
            }

            try:
                resp = self.client.post(
                    self.LISTING_URL,
                    data=data,
                    headers=self.ajax_headers,
                )
                if resp.status_code != 200:
                    logger.warning(f"Listing page {page} returned {resp.status_code}")
                    break
            except Exception as e:
                logger.error(f"Failed to fetch listing page {page}: {e}")
                break

            html = resp.text.strip()
            if not html or len(html) < 50:
                break

            # Parse listing HTML to extract ruling URLs and metadata
            rulings = self._parse_listing(html)
            if not rulings:
                break

            for ruling_meta in rulings:
                doc = self._fetch_ruling(ruling_meta)
                if doc:
                    yield doc

            logger.info(f"  Page {page}: {len(rulings)} rulings")
            page += 1

    def _parse_listing(self, html: str) -> list:
        """Parse the AJAX listing response to extract ruling URLs and metadata."""
        soup = BeautifulSoup(html, "html.parser")
        rulings = []

        # Each ruling is in an <li> element
        items = soup.find_all("li")
        for item in items:
            # Find the main link
            link = item.find("a", href=True)
            if not link:
                continue

            href = link.get("href", "")
            if not href or "/rulings/" not in href:
                continue

            # Normalize URL
            if href.startswith("/"):
                url = f"{self.BASE_URL}{href}"
            elif not href.startswith("http"):
                url = f"{self.BASE_URL}/{href}"
            else:
                url = href

            # Extract metadata from listing
            title = ""
            heading = item.find(["h4", "h3", "h2"])
            if heading:
                title = heading.get_text(strip=True)

            # Extract date
            date_str = ""
            date_elem = item.find("span", class_=re.compile(r"date|time", re.I))
            if not date_elem:
                date_elem = item.find("time")
            if date_elem:
                date_str = date_elem.get_text(strip=True)
                if not date_str:
                    date_str = date_elem.get("datetime", "")

            # Extract decision status
            decision = ""
            decision_elem = item.find(string=re.compile(r"Upheld|Not upheld|Informally", re.I))
            if decision_elem:
                decision = decision_elem.strip()

            # Extract medium
            medium = ""
            medium_elem = item.find("span", class_=re.compile(r"medium|type", re.I))
            if medium_elem:
                medium = medium_elem.get_text(strip=True)

            rulings.append({
                "url": url,
                "title": title,
                "date_str": date_str,
                "decision": decision,
                "medium": medium,
            })

        return rulings

    def _fetch_ruling(self, meta: dict) -> Optional[dict]:
        """Fetch a single ruling page and extract full text."""
        url = meta.get("url", "")
        if not url:
            return None

        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
            if resp.status_code != 200:
                logger.warning(f"Ruling page returned {resp.status_code}: {url}")
                return None
        except Exception as e:
            logger.warning(f"Failed to fetch ruling {url}: {e}")
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract full text from main content
        main_content = soup.find("div", class_="main-content")
        if not main_content:
            main_content = soup.find("main")
        if not main_content:
            main_content = soup.find("article")

        full_text = ""
        if main_content:
            # Remove scripts, styles, nav
            for tag in main_content.find_all(["script", "style", "nav"]):
                tag.decompose()
            full_text = self._clean_text(main_content.get_text(separator="\n", strip=True))

        if not full_text or len(full_text) < 100:
            logger.debug(f"No full text for: {url}")
            return None

        # Extract title from page
        title = meta.get("title", "")
        if not title:
            title_elem = soup.find("h1")
            if title_elem:
                title = title_elem.get_text(strip=True)

        # Extract metadata from title-section div
        title_section = soup.find("div", class_="title-section")
        advertiser = ""
        complaint_ref = ""
        medium = meta.get("medium", "")
        date_iso = self._parse_date(meta.get("date_str", ""))

        if title_section:
            ts_text = title_section.get_text(separator="|", strip=True)
            # Format: "ASA Ruling on\nAdvertiser|Decision|Medium|Date|..."
            parts = [p.strip() for p in ts_text.split("|") if p.strip()]
            # Find date in title section parts
            for part in parts:
                parsed = self._parse_date(part)
                if parsed:
                    date_iso = parsed
                    break
            # Medium is usually third element (after title and decision)
            if not medium:
                for part in parts:
                    if part in ("Website (own site)", "Internet", "TV", "Radio",
                                "Press", "Outdoor", "Social media", "Email",
                                "Direct mail", "Cinema", "Video on demand"):
                        medium = part
                        break

        # Extract sidebar metadata - advertiser name is the first line
        sidebar = soup.find("aside", class_="sidebar")
        if sidebar:
            sidebar_text = sidebar.get_text(separator="\n", strip=True)
            sidebar_lines = [l.strip() for l in sidebar_text.split("\n") if l.strip()]
            if sidebar_lines:
                advertiser = sidebar_lines[0]
            ref_match = re.search(r'Complaint Ref[:\s]*([^\n]+)', sidebar_text, re.I)
            if ref_match:
                complaint_ref = ref_match.group(1).strip()

        # Fallback date from page text
        if not date_iso:
            date_match = re.search(r'(\d{1,2}\s+\w+\s+\d{4})', soup.get_text()[:3000])
            if date_match:
                date_iso = self._parse_date(date_match.group(1))

        # Extract decision from page content if not from listing
        decision = meta.get("decision", "")
        if not decision:
            action_h2 = soup.find("h2", string=re.compile(r"Action|Decision", re.I))
            if action_h2:
                action_text = ""
                for sibling in action_h2.find_next_siblings():
                    if sibling.name == "h2":
                        break
                    action_text += sibling.get_text(strip=True) + " "
                if "upheld" in action_text.lower():
                    decision = "Upheld"
                elif "not upheld" in action_text.lower():
                    decision = "Not upheld"

        # Extract slug for ID
        slug = url.rstrip("/").split("/")[-1].replace(".html", "")

        return {
            "ruling_id": slug,
            "title": title,
            "full_text": full_text,
            "advertiser": advertiser,
            "decision": decision,
            "medium": meta.get("medium", ""),
            "complaint_ref": complaint_ref,
            "date_iso": date_iso,
            "url": url,
        }

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse various date formats to ISO 8601."""
        if not date_str:
            return None

        date_str = date_str.strip()

        # Try ISO format first
        if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
            return date_str[:10]

        # Try DD/MM/YYYY
        match = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', date_str)
        if match:
            d, m, y = match.groups()
            try:
                return datetime(int(y), int(m), int(d)).strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Try "DD Month YYYY"
        try:
            dt = datetime.strptime(date_str, "%d %B %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

        # Try "DD Mon YYYY"
        try:
            dt = datetime.strptime(date_str, "%d %b %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

        return None

    def _clean_text(self, text: str) -> str:
        """Clean up extracted text."""
        if not text:
            return ""
        text = unescape(text)
        text = text.replace("\xa0", " ")
        text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
        text = re.sub(r" +", " ", text)
        text = re.sub(r"^\s+", "", text, flags=re.MULTILINE)
        return text.strip()

    def normalize(self, raw: dict) -> dict:
        """Transform a raw document into the standard schema."""
        ruling_id = raw.get("ruling_id", "")
        title = raw.get("title", "")
        full_text = raw.get("full_text", "")
        date_iso = raw.get("date_iso")

        if not title:
            title = f"ASA Ruling {ruling_id}"

        return {
            "_id": f"UK/ASA/{ruling_id}",
            "_source": "UK/ASA",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),

            "title": title,
            "text": full_text,
            "date": date_iso,
            "url": raw.get("url"),

            "ruling_id": ruling_id,
            "advertiser": raw.get("advertiser"),
            "decision": raw.get("decision"),
            "medium": raw.get("medium"),
            "complaint_ref": raw.get("complaint_ref"),
        }


# ── CLI Entry Point ───────────────────────────────────────────────

def main():
    scraper = UKASAScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, {stats['records_updated']} updated")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
