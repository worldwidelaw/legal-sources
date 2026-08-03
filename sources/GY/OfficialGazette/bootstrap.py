#!/usr/bin/env python3
"""
GY/OfficialGazette -- Official Gazette of Guyana

Fetches gazette issues from officialgazette.gov.gy.

Strategy:
  - Paginate through /index.php/publications?start=N (20 items per page)
  - Parse HTML to extract gazette titles, dates, and PDF links
  - Download PDFs and extract text with pdfplumber
  - Each gazette issue = one record

Data:
  - ~2900 gazette issues (official, extraordinary, legal supplements)
  - Full text in English
  - License: Public Domain (Government Works)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import io
import json
import logging
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

import pdfplumber
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GY.OfficialGazette")

# The site was redesigned in 2026 into a Next.js "e-Gazette" app backed by a
# clean JSON API. The old Joomla /index.php/publications HTML layout is gone
# (it now 301-redirects to the SPA homepage). See GH issue #1129.
BASE_URL = "https://egazette.officialgazette.gov.gy"
API_PATH = "/api/publications"          # ?page=N -> {"publications":[...], "totalItems":N}
PDF_PATH = "/api/pdf/{id}"              # born-digital gazette PDF by document id
ITEMS_PER_PAGE = 10                     # API returns 10 per page
MAX_PAGES = 500  # Safety cap

# Month name mapping for date parsing
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


class OfficialGazetteScraper(BaseScraper):
    """
    Scraper for GY/OfficialGazette -- Official Gazette of Guyana.
    Country: GY
    URL: https://officialgazette.gov.gy

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml",
        })
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml",
            },
            timeout=60,
        )

    def _parse_date(self, title: str) -> Optional[str]:
        """Extract date from gazette title like 'Official Gazettes - 2nd May, 2026'."""
        # Pattern: day month, year
        m = re.search(
            r'(\d{1,2})(?:st|nd|rd|th)?\s+(\w+),?\s+(\d{4})',
            title, re.IGNORECASE
        )
        if m:
            day = int(m.group(1))
            month_name = m.group(2).lower()
            year = int(m.group(3))
            month = MONTHS.get(month_name)
            if month:
                return f"{year}-{month:02d}-{day:02d}"
        return None

    def _classify_gazette(self, title: str) -> str:
        """Classify gazette type from title."""
        lower = title.lower()
        if "extraordinary" in lower or "extra" in lower:
            return "extraordinary"
        elif "legal supplement" in lower:
            return "legal_supplement"
        else:
            return "official"

    def _fetch_publications_page(self, page: int = 1) -> dict:
        """Fetch one page of publications from the JSON API.

        Returns the parsed JSON: {"publications": [...], "totalItems": N}.
        The API is 1-indexed by page and returns 10 items per page.
        """
        self.rate_limiter.wait()
        url = f"{API_PATH}?page={page}"
        resp = self.client.get(url)
        resp.raise_for_status()
        return resp.json()

    def _parse_publications(self, data) -> List[Dict[str, str]]:
        """Normalize the API's publication objects into flat entry dicts.

        Accepts either the raw API dict or its "publications" list (the `test`
        command passes the dict through). Each publication looks like:
          {"id": "<uuid>", "name": "...pdf", "title": "...",
           "publishedAt": "2026-07-12T...Z", "downloadUrl": "/api/pdf/<id>",
           "year": 2026, ...}
        """
        if isinstance(data, dict):
            pubs = data.get("publications", [])
        else:
            pubs = data or []

        entries = []
        for pub in pubs:
            pub_id = pub.get("id")
            if not pub_id:
                continue
            title = (pub.get("title") or pub.get("name") or "").strip()
            download = pub.get("downloadUrl") or PDF_PATH.format(id=pub_id)
            pdf_url = download if download.startswith("http") else f"{BASE_URL}{download}"

            # Prefer the explicit publish/gazette date; fall back to parsing
            # the human date out of the title, then the year.
            date = None
            published = pub.get("publishedAt")
            if published:
                # publishedAt is the upload timestamp; the gazette's own date is
                # in the title (e.g. "11th July, 2026"). Prefer the title date.
                date = self._parse_date(title)
                if not date:
                    date = published[:10]
            else:
                date = self._parse_date(title)
            if not date and pub.get("year"):
                date = f"{pub['year']}-01-01"

            entries.append({
                "pub_id": pub_id,
                "title": title,
                "pdf_url": pdf_url,
                "date": date,
                "gazette_type": self._classify_gazette(title),
            })

        return entries

    def _extract_text_from_pdf(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text using pdfplumber."""
        try:
            self.rate_limiter.wait()
            logger.info(f"Downloading PDF: {pdf_url[-60:]}...")

            resp = self.session.get(pdf_url, timeout=180)
            resp.raise_for_status()

            content = resp.content
            size_mb = len(content) / (1024 * 1024)
            if size_mb > 50:
                logger.warning(f"PDF too large ({size_mb:.1f} MB), skipping")
                return None

            text_parts = []
            with pdfplumber.open(io.BytesIO(content)) as pdf:
                for page in pdf.pages:
                    try:
                        page_text = page.extract_text()
                        if page_text:
                            text_parts.append(page_text)
                    except Exception:
                        continue
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass

            full_text = "\n\n".join(text_parts)
            logger.info(f"Extracted {len(full_text)} chars from PDF ({size_mb:.1f} MB)")
            return full_text if full_text.strip() else None

        except Exception as e:
            logger.error(f"PDF extraction failed: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all gazette documents with full text.

        NOTE: the API's ``totalItems`` field is unreliable — it is a running
        count that grows with the page number (page 1 → 92, page 50 → 530,
        page 150 → 1529) rather than the true corpus size, so it must NOT be
        used as a stop condition (doing so capped the sweep at page 10 / 100
        records, GH #1157). The real end of the corpus (~2,065 items over ~207
        pages of 10) is detected by the API returning an empty ``publications``
        list, which is the sole termination signal below.
        """
        page = 1
        seen = 0

        while page <= MAX_PAGES:
            logger.info(f"Fetching publications page {page}...")
            try:
                data = self._fetch_publications_page(page)
            except Exception as e:
                logger.warning(f"Failed to fetch page {page}: {e}")
                break

            entries = self._parse_publications(data)
            if not entries:
                logger.info(f"No entries on page {page}, stopping (fetched {seen})")
                break

            for entry in entries:
                text = self._extract_text_from_pdf(entry["pdf_url"])
                if text:
                    entry["text"] = text
                    yield entry
                else:
                    logger.warning(f"No text for: {entry['title']}")
                seen += 1

            page += 1

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent gazette issues (newest-first pages until older than `since`)."""
        page = 1
        for _ in range(5):
            logger.info(f"Checking updates page {page}...")
            try:
                data = self._fetch_publications_page(page)
            except Exception:
                break

            entries = self._parse_publications(data)
            if not entries:
                break

            for entry in entries:
                if entry.get("date"):
                    try:
                        pub_date = datetime.strptime(entry["date"], "%Y-%m-%d")
                        if pub_date.date() < since.date():
                            return
                    except ValueError:
                        pass

                text = self._extract_text_from_pdf(entry["pdf_url"])
                if text:
                    entry["text"] = text
                    yield entry

            page += 1

    def normalize(self, raw: dict) -> dict:
        """Transform raw gazette entry into standard schema."""
        text = raw.get("text", "")
        if not text or len(text.strip()) < 50:
            return None

        pub_id = raw.get("pub_id", "")
        title = raw.get("title", "")
        gazette_id = f"GY-OG-{pub_id}" if pub_id else hashlib.md5(title.encode()).hexdigest()[:12]

        return {
            "_id": f"GY/OfficialGazette/{gazette_id}",
            "_source": "GY/OfficialGazette",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "id": gazette_id,
            "gazette_id": gazette_id,
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "gazette_type": raw.get("gazette_type", "official"),
            "url": raw.get("pdf_url", ""),
            "country": "GY",
            "language": "en",
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="GY/OfficialGazette scraper")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = OfficialGazetteScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            data = scraper._fetch_publications_page(1)
            entries = scraper._parse_publications(data)
            logger.info(f"Connection OK. Found {len(entries)} entries on page 1 "
                        f"(totalItems={data.get('totalItems')}).")
            for e in entries[:3]:
                logger.info(f"  [{e['gazette_type']}] {e['title']} -> {e.get('date', 'no date')}")
            print("TEST PASSED")
        except Exception as e:
            logger.error(f"Test failed: {e}")
            print("TEST FAILED")
            sys.exit(1)

    elif args.command in ("bootstrap", "bootstrap-fast"):
        # bootstrap-fast is the VPS fleet entrypoint; it must run the FULL
        # corpus (streamed to data/records.jsonl by BaseScraper), never fall
        # back to sample mode. Plain `bootstrap` still defaults to sample.
        if args.command == "bootstrap-fast":
            sample_mode = False
        else:
            sample_mode = args.sample or not args.full
        sample_size = 15 if sample_mode else 99999
        logger.info(f"Bootstrap (sample={sample_mode}, size={sample_size})")
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=sample_size)
        logger.info(f"Done: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Update done: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
