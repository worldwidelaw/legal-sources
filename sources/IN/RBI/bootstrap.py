#!/usr/bin/env python3
"""
IN/RBI -- Reserve Bank of India Circulars and Master Directions

Fetches RBI regulatory publications (circulars, notifications, master directions)
with full text extracted from HTML pages.

Strategy:
  - Iterate notification IDs from recent (high IDs) to old (low IDs).
  - Each ID maps to: rbi.org.in/Scripts/NotificationUser.aspx?Id={ID}&Mode=0
  - Full text is embedded inline in HTML within <table class="tablebg"> elements.
  - Master directions are listed at BS_ViewMasDirections.aspx and fetched individually.

Data:
  - ~13,000+ circulars from 1998 to present
  - ~343 master directions
  - Full text as HTML (stripped to plain text)
  - License: Open Government Data (India)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records for validation
  python bootstrap.py update             # Incremental update (recent IDs only)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import time
import requests
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: BeautifulSoup4 is required. Install with: pip install beautifulsoup4")
    sys.exit(1)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IN.RBI")

BASE_URL = "https://www.rbi.org.in"

# Start from most recent ID and go down. As of April 2026, max ID is ~13400.
MAX_NOTIFICATION_ID = 13400
MIN_NOTIFICATION_ID = 1

# Maximum consecutive 404s before we assume we've passed all valid IDs
MAX_CONSECUTIVE_MISSES = 50


class RBIScraper(BaseScraper):
    """
    Scraper for IN/RBI -- Reserve Bank of India Circulars & Master Directions.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-IN,en;q=0.9",
        })

    def _fetch_notification(self, nid: int) -> Optional[Dict[str, Any]]:
        """
        Fetch a single notification page by ID and extract full text.

        Returns a raw dict with title, text, date, etc., or None if not found.
        """
        url = f"{BASE_URL}/Scripts/NotificationUser.aspx?Id={nid}&Mode=0"

        try:
            resp = self.session.get(url, timeout=30)

            if resp.status_code == 404:
                return None
            if resp.status_code != 200:
                logger.warning(f"ID {nid}: HTTP {resp.status_code}")
                return None

            html_content = resp.text

            # Check for error/redirect pages
            if "Page Not Found" in html_content or len(html_content) < 500:
                return None

            soup = BeautifulSoup(html_content, "html.parser")

            # Extract title from bold elements in the content area
            title = ""
            # Look for bold/strong text near the top of the content
            for b_tag in soup.find_all(["b", "strong"]):
                b_text = b_tag.get_text(strip=True)
                # Skip very short, PDF-size lines, and generic headers
                if (len(b_text) > 15
                    and "kb" not in b_text.lower()
                    and "mb" not in b_text.lower()
                    and b_text.lower() not in ("notifications", "reserve bank of india")
                    and not re.match(r"^RBI/\d", b_text)):
                    title = b_text
                    break

            # Fallback: try tableheader class
            if not title or title.lower() == "notifications":
                for header_class in ["tableheader", "head1"]:
                    header_el = soup.find(class_=header_class)
                    if header_el:
                        header_text = header_el.get_text(strip=True)
                        if len(header_text) > 15 and header_text.lower() != "notifications":
                            title = header_text
                            break

            # Extract date from page content
            date_str = ""
            # Look for date patterns in common locations
            date_patterns = [
                r"(\w+\s+\d{1,2},?\s+\d{4})",       # "January 15, 2024"
                r"(\d{1,2}[/-]\d{1,2}[/-]\d{4})",     # "15/01/2024"
                r"(\d{4}-\d{2}-\d{2})",                # "2024-01-15"
                r"(\d{1,2}\s+\w+\s+\d{4})",           # "15 January 2024"
            ]

            # Check tableheader or nearby elements for dates
            page_text = soup.get_text()
            for pattern in date_patterns:
                match = re.search(pattern, page_text[:2000])
                if match:
                    date_str = match.group(1)
                    break

            # Extract full text from the main content area
            full_text = ""

            # Primary: look for tablebg class (main content table)
            content_table = soup.find("table", class_="tablebg")
            if content_table:
                # Remove navigation and script elements
                for elem in content_table.find_all(["script", "style", "nav", "button", "select"]):
                    elem.decompose()
                full_text = content_table.get_text(separator="\n", strip=True)

            # Fallback: look for td class content area
            if not full_text or len(full_text) < 100:
                content_td = soup.find("table", class_="td")
                if content_td:
                    for elem in content_td.find_all(["script", "style", "nav", "button", "select"]):
                        elem.decompose()
                    full_text = content_td.get_text(separator="\n", strip=True)

            # Fallback: try main content div
            if not full_text or len(full_text) < 100:
                main_div = soup.find("div", id="example") or soup.find("div", class_="ftmlivediv")
                if main_div:
                    for elem in main_div.find_all(["script", "style", "nav", "button", "select"]):
                        elem.decompose()
                    full_text = main_div.get_text(separator="\n", strip=True)

            if not full_text or len(full_text) < 50:
                return None

            # Clean up the text
            # Remove PDF size lines like "(317 kb)" or "(1.2 mb)"
            full_text = re.sub(r"^\(\s*\d+[\d.]*\s*[km]b\s*\)\s*\n?", "", full_text, flags=re.IGNORECASE)
            full_text = re.sub(r"\n{3,}", "\n\n", full_text)
            full_text = re.sub(r"[ \t]+", " ", full_text)
            full_text = full_text.strip()

            # If title is still generic, extract from first line of text
            if not title or title.lower() == "notifications":
                lines = [l.strip() for l in full_text.split("\n") if l.strip()]
                for line in lines[:5]:
                    # Skip RBI reference numbers and date lines
                    if (len(line) > 20
                        and not re.match(r"^RBI/\d", line)
                        and not re.match(r"^[A-Z.]+\s*\(", line)  # dept codes
                        and not re.match(r"^\w+\s+\d{1,2},?\s+\d{4}", line)  # dates
                        and not re.match(r"^\d{1,2}\s+\w+\s+\d{4}", line)):  # dates
                        title = line[:200]
                        break

            # Extract circular number if present
            circular_no = ""
            circ_match = re.search(
                r"((?:RBI|DBOD|DNBS|DOR|FMRD|DPSS|DoR|DoS|FIDD|CO)[/.\-]\S+)",
                page_text[:3000]
            )
            if circ_match:
                circular_no = circ_match.group(1)

            # Extract PDF link if present
            pdf_url = ""
            pdf_link = soup.find("a", href=re.compile(r"rbidocs\.rbi\.org\.in.*\.pdf", re.IGNORECASE))
            if pdf_link:
                pdf_url = pdf_link.get("href", "")
                if pdf_url and not pdf_url.startswith("http"):
                    pdf_url = f"https:{pdf_url}" if pdf_url.startswith("//") else f"{BASE_URL}{pdf_url}"

            return {
                "notification_id": str(nid),
                "title": title,
                "full_text": full_text,
                "date_raw": date_str,
                "circular_number": circular_no,
                "pdf_url": pdf_url,
                "source_url": url,
            }

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout fetching ID {nid}")
            return None
        except Exception as e:
            logger.error(f"Error fetching ID {nid}: {e}")
            return None

    def _parse_date(self, date_str: str) -> str:
        """Try to parse various date formats into ISO 8601."""
        if not date_str:
            return ""

        formats = [
            "%B %d, %Y",       # "January 15, 2024"
            "%B %d %Y",        # "January 15 2024"
            "%d %B %Y",        # "15 January 2024"
            "%d/%m/%Y",        # "15/01/2024"
            "%d-%m-%Y",        # "15-01-2024"
            "%Y-%m-%d",        # "2024-01-15"
            "%b %d, %Y",       # "Jan 15, 2024"
            "%d %b %Y",        # "15 Jan 2024"
        ]

        # Clean up the date string
        date_str = date_str.strip().replace(",", ", ").replace("  ", " ")
        date_str = re.sub(r",\s*,", ",", date_str)

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        return date_str

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all RBI notifications by iterating IDs from recent to old.
        """
        consecutive_misses = 0

        for nid in range(MAX_NOTIFICATION_ID, MIN_NOTIFICATION_ID - 1, -1):
            self.rate_limiter.wait()

            raw = self._fetch_notification(nid)

            if raw is None:
                consecutive_misses += 1
                if consecutive_misses >= MAX_CONSECUTIVE_MISSES:
                    logger.info(f"Hit {MAX_CONSECUTIVE_MISSES} consecutive misses at ID {nid}, stopping")
                    break
                continue

            consecutive_misses = 0

            # Skip if text is too short
            if len(raw.get("full_text", "")) < 100:
                continue

            yield raw

            if nid % 100 == 0:
                logger.info(f"Progress: processed down to ID {nid}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Fetch recent notifications (high IDs only).
        """
        consecutive_misses = 0

        for nid in range(MAX_NOTIFICATION_ID, MAX_NOTIFICATION_ID - 500, -1):
            self.rate_limiter.wait()

            raw = self._fetch_notification(nid)

            if raw is None:
                consecutive_misses += 1
                if consecutive_misses >= MAX_CONSECUTIVE_MISSES:
                    break
                continue

            consecutive_misses = 0

            if len(raw.get("full_text", "")) < 100:
                continue

            # Try to filter by date
            parsed_date = self._parse_date(raw.get("date_raw", ""))
            if parsed_date:
                try:
                    doc_date = datetime.strptime(parsed_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    if doc_date < since:
                        continue
                except ValueError:
                    pass

            yield raw

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw notification data into standard schema.
        """
        nid = raw.get("notification_id", "")
        title = raw.get("title", "")
        full_text = raw.get("full_text", "")
        date_raw = raw.get("date_raw", "")
        parsed_date = self._parse_date(date_raw)
        circular_no = raw.get("circular_number", "")
        pdf_url = raw.get("pdf_url", "")
        source_url = raw.get("source_url", "")

        return {
            "_id": f"RBI-{nid}",
            "_source": "IN/RBI",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": parsed_date if parsed_date else date_raw,
            "url": source_url,
            "notification_id": nid,
            "circular_number": circular_no,
            "pdf_url": pdf_url,
            "language": "en",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing RBI endpoints...")

        # Test a known recent notification
        print("\n1. Testing notification page (ID 13300)...")
        raw = self._fetch_notification(13300)
        if raw:
            print(f"   Title: {raw['title'][:80]}")
            print(f"   Text length: {len(raw['full_text'])} chars")
            print(f"   Date: {raw['date_raw']}")
            print(f"   Text preview: {raw['full_text'][:200]}...")
        else:
            print("   ID 13300 not found, trying 13200...")
            raw = self._fetch_notification(13200)
            if raw:
                print(f"   Title: {raw['title'][:80]}")
                print(f"   Text length: {len(raw['full_text'])} chars")
            else:
                print("   ERROR: Could not fetch any notification")

        # Test an older notification
        print("\n2. Testing older notification (ID 5000)...")
        raw = self._fetch_notification(5000)
        if raw:
            print(f"   Title: {raw['title'][:80]}")
            print(f"   Text length: {len(raw['full_text'])} chars")
        else:
            print("   ID 5000 not found")

        print("\nTest complete!")


def main():
    scraper = RBIScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
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
