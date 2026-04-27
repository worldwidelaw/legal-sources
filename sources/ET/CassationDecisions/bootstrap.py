#!/usr/bin/env python3
"""
ET/CassationDecisions -- Ethiopia Federal Supreme Court Cassation Decisions

Fetches cassation decisions from lawethiopia.com.

Strategy:
  - Scrape the separated-cassation-decisions volume pages for individual decision URLs
  - For each decision, download the individual PDF and extract full text
  - Fall back to volume-level PDFs for volumes without separated pages

Usage:
  python bootstrap.py bootstrap          # Fetch all decisions
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin, quote

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ET.CassationDecisions")

BASE_URL = "https://www.lawethiopia.com"

# Volume pages with individual decision listings
SEPARATED_VOLUME_PAGES = {
    16: "/index.php/separated-cassation-decisions/6421-volume-16",
    17: "/index.php/separated-cassation-decisions/6422-volume-17",
    18: "/index.php/separated-cassation-decisions/6423-volume-18",
    19: "/index.php/separated-cassation-decisions/6424-volume-19",
    20: "/index.php/separated-cassation-decisions/6425-volume-20",
    21: "/index.php/separated-cassation-decisions/6426-volume-21",
    22: "/index.php/separated-cassation-decisions/6427-volume-22",
    23: "/index.php/separated-cassation-decisions/6428-volume-23",
    24: "/index.php/separated-cassation-decisions/6429-volume-24",
    25: "/index.php/separated-cassation-decisions/6430-volume-25",
    26: "/index.php/separated-cassation-decisions/6431-volume-26",
}

# Volumes available as bulk PDFs only
BULK_VOLUME_PDFS = {
    "1-3": "/images/cassation/cassation%20decisions%20by%20volumes/volume%201-3.pdf",
    "4": "/images/cassation/cassation%20decisions%20by%20volumes/volume%204.pdf",
    "5": "/images/cassation/cassation%20decisions%20by%20volumes/volume%205.pdf",
    "6": "/images/cassation/cassation%20decisions%20by%20volumes/volume%206.pdf",
    "7": "/images/cassation/cassation%20decisions%20by%20volumes/volume%207.pdf",
    "8": "/images/cassation/cassation%20decisions%20by%20volumes/volume%208.pdf",
    "9": "/images/cassation/cassation%20decisions%20by%20volumes/volume%209.pdf",
    "10": "/images/cassation/cassation%20decisions%20by%20volumes/volume%2010.pdf",
    "11": "/images/cassation/cassation%20decisions%20by%20volumes/volume%2011.pdf",
    "12": "/images/cassation/cassation%20decisions%20by%20volumes/volume%2012.pdf",
    "13": "/images/cassation/cassation%20decisions%20by%20volumes/volume%2013.pdf",
    "14": "/images/cassation/cassation%20decisions%20by%20volumes/volume%2014.pdf",
    "15": "/images/cassation/cassation%20decisions%20by%20volumes/volume%2015.pdf",
}


class CassationDecisionsScraper(BaseScraper):
    """Scraper for ET/CassationDecisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })
        # Visit homepage to establish session cookie
        try:
            self.session.get(f"{BASE_URL}/", timeout=30)
        except requests.exceptions.RequestException:
            pass

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        """HTTP GET with rate limiting and retry."""
        for attempt in range(3):
            try:
                time.sleep(1.5)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    def _parse_volume_page(self, html: str) -> List[Dict[str, str]]:
        """Parse a separated-cassation-decisions volume page for individual decision links."""
        soup = BeautifulSoup(html, "html.parser")
        decisions = []

        # Find all links that look like cassation decision pages
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            text = a.get_text(strip=True)

            # Match links like /index.php/100395-property-law-...
            m = re.match(r"/index\.php/(\d{4,7})-(.+)", href)
            if not m:
                continue

            case_num = m.group(1)
            slug = m.group(2)

            # Extract subjects from the slug
            subjects = slug.replace("-", " ").strip()

            decisions.append({
                "case_number": case_num,
                "page_url": urljoin(BASE_URL, href),
                "subjects": subjects,
                "title": text,
            })

        return decisions

    def _extract_decision_pdf_url(self, html: str, case_number: str) -> Optional[str]:
        """Find the PDF download link on a decision page."""
        soup = BeautifulSoup(html, "html.parser")

        # Look for PDF links in the content
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if href.lower().endswith(".pdf") and case_number in href:
                return urljoin(BASE_URL, href)

        # Try the standard pattern
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if href.lower().endswith(".pdf") and "cassation" in href.lower():
                return urljoin(BASE_URL, href)

        return None

    def _extract_date_from_page(self, html: str) -> str:
        """Extract the decision date from the HTML page."""
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text()

        # Look for date patterns
        patterns = [
            r"(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})",
        ]
        for pat in patterns:
            m = re.search(pat, text)
            if m:
                try:
                    dt = datetime.strptime(m.group(1), "%d %B %Y")
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass
        return ""

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        case_num = raw.get("case_number", "")
        volume = raw.get("volume", "")

        if case_num:
            doc_id = f"ET-CASS-{case_num}"
        else:
            doc_id = f"ET-CASS-VOL{volume}"

        title = raw.get("title", "")
        if not title and case_num:
            title = f"Cassation Decision No. {case_num}"
        if not title:
            title = f"Cassation Decisions Volume {volume}"

        return {
            "_id": doc_id,
            "_source": "ET/CassationDecisions",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
            "volume": volume,
            "case_number": case_num,
            "subjects": raw.get("subjects", ""),
        }

    def _fetch_individual_decisions(self, volume_num: int, page_url: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch individual decisions from a separated volume page."""
        resp = self._request(urljoin(BASE_URL, page_url))
        if resp is None:
            logger.warning(f"Failed to fetch volume {volume_num} listing page")
            return

        decisions = self._parse_volume_page(resp.text)
        logger.info(f"Volume {volume_num}: {len(decisions)} individual decisions found")

        for dec in decisions:
            case_num = dec["case_number"]

            # Try to construct PDF URL directly
            pdf_url = f"{BASE_URL}/images/cassation/cassation%20decisions%20by%20number/volume%20{volume_num}/{case_num}.pdf"

            logger.info(f"Extracting PDF for case {case_num}")
            try:
                text = extract_pdf_markdown(
                    source="ET/CassationDecisions",
                    source_id=f"ET-CASS-{case_num}",
                    pdf_url=pdf_url,
                    table="case_law",
                )
            except Exception as e:
                logger.warning(f"Direct PDF failed for case {case_num}: {e}")
                # Try fetching the page to find the actual PDF link
                page_resp = self._request(dec["page_url"])
                if page_resp:
                    alt_url = self._extract_decision_pdf_url(page_resp.text, case_num)
                    if alt_url:
                        try:
                            text = extract_pdf_markdown(
                                source="ET/CassationDecisions",
                                source_id=f"ET-CASS-{case_num}",
                                pdf_url=alt_url,
                                table="case_law",
                            )
                        except Exception as e2:
                            logger.warning(f"Alt PDF also failed for case {case_num}: {e2}")
                            text = None
                    else:
                        text = None
                else:
                    text = None

            if not text or len(text) < 50:
                logger.warning(f"Insufficient text for case {case_num}: {len(text) if text else 0} chars")
                continue

            # Try to get date from the individual page
            date = ""
            page_resp = self._request(dec["page_url"])
            if page_resp:
                date = self._extract_date_from_page(page_resp.text)

            yield {
                "case_number": case_num,
                "volume": str(volume_num),
                "title": dec.get("title", f"Cassation Decision No. {case_num}"),
                "subjects": dec.get("subjects", ""),
                "text": text,
                "date": date,
                "url": dec["page_url"],
            }

    def _fetch_bulk_volume(self, volume_label: str, pdf_path: str) -> Optional[Dict[str, Any]]:
        """Fetch a bulk volume PDF."""
        pdf_url = urljoin(BASE_URL, pdf_path)
        logger.info(f"Extracting bulk volume {volume_label}")

        try:
            text = extract_pdf_markdown(
                source="ET/CassationDecisions",
                source_id=f"ET-CASS-VOL{volume_label}",
                pdf_url=pdf_url,
                table="case_law",
            )
        except Exception as e:
            logger.warning(f"Failed to extract volume {volume_label}: {e}")
            return None

        if not text or len(text) < 100:
            logger.warning(f"Insufficient text for volume {volume_label}: {len(text) if text else 0}")
            return None

        return {
            "case_number": "",
            "volume": volume_label,
            "title": f"Federal Supreme Court Cassation Decisions Volume {volume_label}",
            "subjects": "",
            "text": text,
            "date": "",
            "url": pdf_url,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all cassation decisions."""
        count = 0

        # First: individual decisions from separated volume pages
        for vol_num, page_url in sorted(SEPARATED_VOLUME_PAGES.items()):
            for raw in self._fetch_individual_decisions(vol_num, page_url):
                count += 1
                yield raw

        # Then: bulk volume PDFs for volumes without individual pages
        for vol_label, pdf_path in sorted(BULK_VOLUME_PDFS.items(), key=lambda x: x[0]):
            raw = self._fetch_bulk_volume(vol_label, pdf_path)
            if raw:
                count += 1
                yield raw

        logger.info(f"Completed: {count} records fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent decisions from the latest volumes."""
        count = 0
        # Only fetch latest 2 volumes for updates
        latest_vols = sorted(SEPARATED_VOLUME_PAGES.keys())[-2:]
        for vol_num in latest_vols:
            page_url = SEPARATED_VOLUME_PAGES[vol_num]
            for raw in self._fetch_individual_decisions(vol_num, page_url):
                count += 1
                yield raw
        logger.info(f"Updates: {count} records fetched")

    def test(self) -> bool:
        """Quick connectivity test."""
        # Test the main page
        resp = self._request(f"{BASE_URL}/index.php/separated-cassation-decisions")
        if resp is None:
            logger.error("Cannot reach lawethiopia.com")
            return False

        logger.info("Site reachable")

        # Test a known decision PDF
        text = extract_pdf_markdown(
            source="ET/CassationDecisions",
            source_id="ET-CASS-100395-test",
            pdf_url=f"{BASE_URL}/images/cassation/cassation%20decisions%20by%20number/volume%2020/100395.pdf",
            table="case_law",
            force=True,
        )
        if text and len(text) >= 100:
            logger.info(f"PDF extraction OK: {len(text)} chars")
            return True

        logger.error("PDF extraction failed")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="ET/CassationDecisions data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CassationDecisionsScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
