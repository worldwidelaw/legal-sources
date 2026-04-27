#!/usr/bin/env python3
"""
NR/PacLII -- Nauru Court Decisions (PacLII) Fetcher

Fetches case law from the Pacific Islands Legal Information Institute (PacLII)
for Nauru. Covers Supreme Court, District Court, Court of Appeal, Public
Service Appeal Board, and High Court of Australia - Nauru.

Strategy:
  - Iterate court databases by year, collect document links
  - Fetch each HTML document for full text

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import hashlib
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NR.PacLII")

BASE_URL = "https://www.paclii.org"

COURTS = [
    {"code": "NRSC", "name": "Supreme Court", "path": "/nr/cases/NRSC/"},
    {"code": "NRDC", "name": "District Court", "path": "/nr/cases/NRDC/"},
    {"code": "NRCA", "name": "Court of Appeal", "path": "/nr/cases/NRCA/"},
    {"code": "NRPSAB", "name": "Public Service Appeal Board", "path": "/nr/cases/NRPSAB/"},
    {"code": "NRHCA", "name": "High Court of Australia - Nauru", "path": "/nr/cases/NRHCA/"},
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


class NauruPacLIIScraper(BaseScraper):
    """Scraper for NR/PacLII -- Nauru court decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _request(self, url: str, timeout: int = 30) -> Optional[requests.Response]:
        """HTTP GET with retry and rate limiting."""
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout, verify=False)
                if resp.status_code in (404, 410):
                    return None
                if resp.status_code == 503:
                    logger.warning(f"503 Service Unavailable: {url[:80]}")
                    if attempt < 2:
                        time.sleep(10)
                        continue
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url[:80]}: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        return None

    def _extract_document(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch a document page and extract full text."""
        resp = self._request(url)
        if resp is None:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        title = ""
        if soup.title and soup.title.string:
            title = soup.title.string.strip()
            title = re.sub(r"\s+", " ", title)

        if not title or "not found" in title.lower():
            return None

        body = soup.find("body")
        if not body:
            return None

        for tag in body.find_all(["script", "style"]):
            tag.decompose()

        text = body.get_text(separator="\n", strip=True)

        # Remove PacLII navigation boilerplate
        nav_end_markers = [
            "Download original PDF",
            "Download RTF",
            "Download original",
        ]
        for marker in nav_end_markers:
            idx = text.find(marker)
            if idx > 0:
                text = text[idx + len(marker):]
                break

        # Remove trailing boilerplate
        trail_markers = [
            "PacLII: Copyright Policy",
            "Disclaimers",
            "Privacy Policy",
        ]
        for marker in trail_markers:
            idx = text.find(marker)
            if idx > 0:
                text = text[:idx]

        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        text = text.strip()

        if len(text) < 50:
            return None

        path = url.replace(BASE_URL, "").strip("/")
        doc_id = hashlib.md5(path.encode("utf-8")).hexdigest()[:12]

        return {
            "document_id": f"NR-PAC-{doc_id}",
            "title": title,
            "text": text,
            "url": url,
        }

    def _parse_case_date(self, title: str) -> Optional[str]:
        """Extract date from case title like '[2024] NRSC 3 (29 January 2024)'."""
        m = re.search(r"\((\d{1,2})\s+(\w+)\s+(\d{4})\)\s*$", title)
        if m:
            day = int(m.group(1))
            month_name = m.group(2).lower()
            year = m.group(3)
            months = {
                "january": "01", "february": "02", "march": "03", "april": "04",
                "may": "05", "june": "06", "july": "07", "august": "08",
                "september": "09", "october": "10", "november": "11", "december": "12",
            }
            month = months.get(month_name, "01")
            return f"{year}-{month}-{day:02d}"

        m = re.search(r"\[(\d{4})\]", title)
        if m:
            return f"{m.group(1)}-01-01"

        return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        title = raw.get("title", "")
        date = self._parse_case_date(title)

        return {
            "_id": raw.get("document_id", ""),
            "_source": "NR/PacLII",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": date,
            "url": raw.get("url", ""),
            "court": raw.get("court", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Nauru court decisions from PacLII."""
        seen = set()
        current_year = datetime.now().year
        count = 0

        for court in COURTS:
            court_url = BASE_URL + court["path"]
            resp = self._request(court_url)
            if resp is None:
                logger.warning(f"Cannot access court {court['code']}")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            years = set()
            for a in soup.find_all("a", href=True):
                href = a["href"].rstrip("/")
                m = re.search(r"/(\d{4})/?$", href)
                if m:
                    years.add(int(m.group(1)))

            if not years:
                years = set(range(max(1970, current_year - 10), current_year + 1))

            logger.info(f"Court {court['code']}: {len(years)} years")

            for year in sorted(years, reverse=True):
                year_url = f"{court_url}{year}/"
                resp = self._request(year_url)
                if resp is None:
                    continue

                soup = BeautifulSoup(resp.text, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if not href.endswith(".html"):
                        continue
                    filename = href.rstrip("/").split("/")[-1].replace(".html", "")
                    if not filename.isdigit():
                        continue

                    full_url = urljoin(year_url, href)
                    if full_url in seen:
                        continue
                    seen.add(full_url)

                    doc = self._extract_document(full_url)
                    if doc is None:
                        continue
                    doc["doc_type"] = "case_law"
                    doc["court"] = court["code"]
                    if doc.get("text"):
                        count += 1
                        yield doc

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent documents."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._request(BASE_URL + "/countries/nr.html")
        if resp is None:
            logger.error("Cannot reach PacLII Nauru page")
            return False

        logger.info("PacLII Nauru page OK")

        doc = self._extract_document(BASE_URL + "/nr/cases/NRSC/2026/1.html")
        if doc:
            logger.info(f"Case OK: {doc['title'][:60]} ({len(doc['text'])} chars)")
        else:
            doc = self._extract_document(BASE_URL + "/nr/cases/NRSC/2025/1.html")
            if doc:
                logger.info(f"Case OK: {doc['title'][:60]} ({len(doc['text'])} chars)")
            else:
                logger.error("Failed to fetch case document")
                return False

        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="NR/PacLII data fetcher")
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

    scraper = NauruPacLIIScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records -- {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
