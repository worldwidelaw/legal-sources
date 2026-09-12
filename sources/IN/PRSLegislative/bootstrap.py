#!/usr/bin/env python3
"""
IN/PRSLegislative -- PRS Legislative Research Bill Tracking

Fetches bill summaries and analysis from PRS India with full text.

Strategy:
  - GET the bill listing page (all years, all statuses)
  - Parse ~960 bill entries for title, status, and URL slug
  - GET each bill's detail page
  - Extract: title, ministry, body text, status timeline, PDF links
  - Body text is the full PRS analysis (10K+ chars typical)

Data:
  - ~960 bills tracked (1993-present)
  - Full summary/analysis text in HTML
  - CC BY 4.0 license
  - No authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch bills from last 90 days
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List

import requests

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: BeautifulSoup4 is required. Install with: pip install beautifulsoup4")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IN.PRSLegislative")

BASE_URL = "https://prsindia.org"
LIST_URL = f"{BASE_URL}/billtrack?field_bill_category=All&field_bill_status=All&field_bill_year=All"


def parse_date(date_str: str) -> Optional[str]:
    """Parse PRS date format (e.g., 'Apr 16, 2026') to ISO 8601."""
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%d %b %Y", "%d %B %Y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


class PRSLegislativeScraper(BaseScraper):
    """Scraper for IN/PRSLegislative — PRS Legislative Research Bill Tracking."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        })

    def _fetch_bill_list(self) -> List[Dict[str, str]]:
        """Fetch the master bill listing page and extract all bill entries."""
        self.rate_limiter.wait()
        resp = self.session.get(LIST_URL, timeout=60)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        rows = soup.find_all("div", class_="views-row")
        logger.info("Found %d bill entries on listing page", len(rows))

        bills = []
        for row in rows:
            title_div = row.find("div", class_="views-field-title-field")
            status_div = row.find("div", class_="views-field-field-bill-status")

            if not title_div:
                continue

            link = title_div.find("a", href=True)
            if not link:
                continue

            href = link["href"]
            if not href.startswith("/billtrack/") or href == "/billtrack":
                continue

            slug = href.replace("/billtrack/", "").strip("/")
            title = link.get_text(strip=True)
            status = ""
            if status_div:
                status = status_div.get_text(strip=True)

            bills.append({
                "slug": slug,
                "title": title,
                "list_status": status,
                "url": f"{BASE_URL}{href}",
            })

        return bills

    def _fetch_bill_detail(self, bill_url: str) -> Optional[Dict[str, Any]]:
        """Fetch a bill's detail page and extract all content."""
        self.rate_limiter.wait()
        try:
            resp = self.session.get(bill_url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning("Failed to fetch %s: %s", bill_url, e)
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        detail = {}

        # Title
        title_div = soup.find("div", class_="field-name-title-field")
        if title_div:
            detail["title"] = title_div.get_text(strip=True)
        else:
            h1 = soup.find("h1")
            detail["title"] = h1.get_text(strip=True) if h1 else ""

        # Ministry
        ministry_div = soup.find("div", class_="field-name-field-ministry")
        if ministry_div:
            ministry_link = ministry_div.find("a")
            detail["ministry"] = ministry_link.get_text(strip=True) if ministry_link else ministry_div.get_text(strip=True).replace("Ministry:", "").strip()
        else:
            detail["ministry"] = ""

        # Status timeline
        status_items = soup.find_all("div", class_="field-collection-item-field-own-status-details")
        timeline = []
        for si in status_items:
            title_el = si.find("div", class_="field-name-field-own-status-title")
            date_el = si.find("div", class_="field-name-field-own-status-date")
            status_el = si.find("div", class_="field-name-field-own-status")

            entry = {
                "house": title_el.get_text(strip=True) if title_el else "",
                "date": date_el.get_text(strip=True) if date_el else "",
                "action": status_el.get_text(strip=True) if status_el else "",
            }
            timeline.append(entry)
        detail["status_timeline"] = timeline

        # Body text (full analysis)
        body_div = soup.find("div", class_="field-name-body")
        if body_div:
            # Remove disclaimer if present
            text = body_div.get_text(separator="\n", strip=True)
            # Strip PRS disclaimer from the end
            disclaimer_markers = [
                "DISCLAIMER:",
                "Disclaimer:",
                "PRS Legislative Research",
                "This document has been prepared",
            ]
            for marker in disclaimer_markers:
                idx = text.rfind(marker)
                if idx > 0 and idx > len(text) * 0.8:
                    text = text[:idx].rstrip()
                    break
            detail["body_text"] = text
        else:
            detail["body_text"] = ""

        # PDF links
        pdf_links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".pdf" in href.lower():
                label = a.get_text(strip=True)
                if href.startswith("../"):
                    href = f"{BASE_URL}/{href.lstrip('../')}"
                elif href.startswith("/"):
                    href = f"{BASE_URL}{href}"
                pdf_links.append({"url": href, "label": label})
        detail["pdf_urls"] = pdf_links

        return detail

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all bills with full text from PRS India."""
        bills = self._fetch_bill_list()
        logger.info("Fetching details for %d bills", len(bills))

        for i, bill in enumerate(bills):
            detail = self._fetch_bill_detail(bill["url"])
            if not detail:
                continue

            if not detail.get("body_text"):
                logger.debug("No body text for %s, skipping", bill["slug"])
                continue

            yield {
                "slug": bill["slug"],
                "list_title": bill["title"],
                "list_status": bill["list_status"],
                "url": bill["url"],
                **detail,
            }

            if (i + 1) % 50 == 0:
                logger.info("Progress: %d/%d bills fetched", i + 1, len(bills))

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch bills updated since a given date."""
        bills = self._fetch_bill_list()
        logger.info("Checking %d bills for updates since %s", len(bills), since.isoformat())

        for bill in bills:
            detail = self._fetch_bill_detail(bill["url"])
            if not detail or not detail.get("body_text"):
                continue

            # Check if any timeline date is after since
            dominated_by_recent = False
            for entry in detail.get("status_timeline", []):
                d = parse_date(entry.get("date", ""))
                if d:
                    try:
                        entry_date = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                        if entry_date >= since:
                            dominated_by_recent = True
                            break
                    except ValueError:
                        continue

            if dominated_by_recent:
                yield {
                    "slug": bill["slug"],
                    "list_title": bill["title"],
                    "list_status": bill["list_status"],
                    "url": bill["url"],
                    **detail,
                }

    def normalize(self, raw: dict) -> dict:
        """Transform raw bill data into standardized schema."""
        slug = raw.get("slug", "")
        title = raw.get("title") or raw.get("list_title", "")
        body_text = raw.get("body_text", "")

        if not body_text or len(body_text) < 100:
            return None

        # Get earliest date from timeline
        date = None
        house = ""
        timeline = raw.get("status_timeline", [])
        if timeline:
            first_entry = timeline[0]
            date = parse_date(first_entry.get("date", ""))
            house = first_entry.get("house", "")

        # Build current status string
        status_parts = []
        for entry in timeline:
            action = entry.get("action", "")
            h = entry.get("house", "")
            d = entry.get("date", "")
            if action:
                status_parts.append(f"{action} ({h}, {d})" if h and d else action)
        current_status = raw.get("list_status", "") or ("; ".join(status_parts) if status_parts else "Unknown")

        return {
            "_id": f"IN/PRSLegislative/{slug}",
            "_source": "IN/PRSLegislative",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "bill_slug": slug,
            "title": title,
            "text": body_text,
            "date": date,
            "url": raw.get("url", f"{BASE_URL}/billtrack/{slug}"),
            "ministry": raw.get("ministry", ""),
            "house": house,
            "status": current_status,
            "status_timeline": timeline,
            "pdf_urls": raw.get("pdf_urls", []),
        }

    def test_connection(self) -> bool:
        """Test connectivity to PRS India."""
        try:
            resp = self.session.get(f"{BASE_URL}/billtrack", timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.find_all("div", class_="views-row")
            logger.info("Connection OK — found %d bills on listing page", len(rows))
            return len(rows) > 0
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description="IN/PRSLegislative Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = PRSLegislativeScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info("Bootstrap complete: %d records — %s", fetched, stats)
        if fetched == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info("Update complete: %s", stats)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
