#!/usr/bin/env python3
"""
SN/JournalOfficiel -- Senegal Official Gazette via primature.sn

Fetches Senegalese legislation (codes, laws, decrees) with full text from
the Primature website (government secretary general).

Strategy:
  - Scrape listing pages for codes and lois-et-decrets sections
  - Fetch each document page for full text (div.field--name-body)
  - ~60 documents total (18 codes + 40 decrees/laws)

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from html.parser import HTMLParser

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SN.JournalOfficiel")

BASE_URL = "https://primature.sn"
CODES_URL = f"{BASE_URL}/publications/lois-et-reglements/codes"
LOIS_DECRETS_URL = f"{BASE_URL}/publications/lois-et-reglements/lois-et-decrets"


class _BodyExtractor(HTMLParser):
    """Extract text from div.field--name-body on primature.sn pages."""

    def __init__(self):
        super().__init__()
        self.in_body = False
        self.depth = 0
        self.parts: List[str] = []

    def handle_starttag(self, tag, attrs):
        attrs_d = dict(attrs)
        cls = attrs_d.get("class", "")
        if "field--name-body" in cls:
            self.in_body = True
            self.depth = 0
        if self.in_body:
            self.depth += 1
            if tag in ("br",):
                self.parts.append("\n")
            elif tag in ("p", "div", "h1", "h2", "h3", "h4", "li"):
                self.parts.append("\n")

    def handle_endtag(self, tag):
        if self.in_body:
            self.depth -= 1
            if self.depth <= 0:
                self.in_body = False
            if tag in ("p", "li"):
                self.parts.append("\n")

    def handle_data(self, data):
        if self.in_body:
            self.parts.append(data)

    def get_text(self) -> str:
        text = "".join(self.parts).strip()
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        # Strip footer text that leaks into body div
        text = re.sub(r"\s*©\s*Primature\s*\d{4}\s*\|\s*www\.primature\.sn\s*", "", text)
        return text.strip()


class _ListingExtractor(HTMLParser):
    """Extract document links from listing pages."""

    def __init__(self):
        super().__init__()
        self.in_link = False
        self.items: List[Dict[str, str]] = []
        self.current_href = ""
        self.current_title = ""

    def handle_starttag(self, tag, attrs):
        if tag != "a":
            return
        attrs_d = dict(attrs)
        href = attrs_d.get("href", "")
        if "/publications/lois-et-reglements/" in href:
            # Skip category links
            slug = href.rstrip("/").split("/")[-1]
            if slug in ("codes", "lois-et-decrets", "arretes", "lois-et-reglements"):
                return
            self.in_link = True
            self.current_href = href
            self.current_title = ""

    def handle_endtag(self, tag):
        if tag == "a" and self.in_link:
            self.in_link = False
            if self.current_href and self.current_title.strip():
                self.items.append({
                    "url": self.current_href,
                    "title": self.current_title.strip(),
                })

    def handle_data(self, data):
        if self.in_link:
            self.current_title += data


class PrimatureSNScraper(BaseScraper):
    """Scraper for SN/JournalOfficiel -- Senegalese legislation via primature.sn."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.5,en;q=0.3",
        })

    def _request(self, url: str, timeout: int = 30) -> Optional[requests.Response]:
        """HTTP GET with 2-second delay and retry."""
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if resp.status_code == 404:
                    logger.warning(f"404 Not Found: {url}")
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    def _fetch_listing(self, base_url: str, max_pages: int = 10) -> List[Dict[str, str]]:
        """Fetch all document links from a paginated listing."""
        all_items = []
        seen_urls = set()

        for page in range(max_pages):
            url = f"{base_url}?page={page}" if page > 0 else base_url
            logger.info(f"Fetching listing page: {url}")
            resp = self._request(url)
            if resp is None:
                break

            parser = _ListingExtractor()
            parser.feed(resp.text)

            if not parser.items:
                break

            new_count = 0
            for item in parser.items:
                if item["url"] not in seen_urls:
                    seen_urls.add(item["url"])
                    all_items.append(item)
                    new_count += 1

            if new_count == 0:
                break

            logger.info(f"  Found {new_count} new items (total: {len(all_items)})")

        return all_items

    def _extract_body(self, html: str) -> str:
        """Extract body text from a document page."""
        parser = _BodyExtractor()
        parser.feed(html)
        return parser.get_text()

    def _extract_date(self, title: str) -> Optional[str]:
        """Try to extract a date from the document title."""
        # Pattern: "du DD mois YYYY" or "du DD/MM/YYYY"
        months_fr = {
            "janvier": "01", "février": "02", "mars": "03", "avril": "04",
            "mai": "05", "juin": "06", "juillet": "07", "août": "08",
            "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12",
        }

        # Try "du DD month YYYY"
        m = re.search(
            r"du\s+(\d{1,2})\s+(janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+(\d{4})",
            title, re.IGNORECASE
        )
        if m:
            day = int(m.group(1))
            month = months_fr.get(m.group(2).lower(), "01")
            year = m.group(3)
            return f"{year}-{month}-{day:02d}"

        # Try year from decree number: "n° YYYY-NNN"
        m = re.search(r"n°\s*(\d{4})-", title)
        if m:
            return f"{m.group(1)}-01-01"

        return None

    def _classify_document(self, url: str, title: str) -> str:
        """Classify document type from URL/title."""
        lower_title = title.lower()
        if "/code" in url or lower_title.startswith("code"):
            return "code"
        if "loi" in lower_title and "décret" not in lower_title:
            return "loi"
        if "décret" in lower_title or "decret" in url:
            return "decret"
        if "arrêté" in lower_title or "arrete" in url:
            return "arrete"
        return "legislation"

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents from primature.sn legislation sections."""
        # Fetch codes
        logger.info("=== Fetching Codes section ===")
        codes = self._fetch_listing(CODES_URL, max_pages=5)
        logger.info(f"Found {len(codes)} codes")

        # Fetch lois et décrets
        logger.info("=== Fetching Lois et Décrets section ===")
        lois_decrets = self._fetch_listing(LOIS_DECRETS_URL, max_pages=10)
        logger.info(f"Found {len(lois_decrets)} lois/décrets")

        all_items = codes + lois_decrets
        logger.info(f"Total documents to fetch: {len(all_items)}")

        for item in all_items:
            full_url = f"{BASE_URL}{item['url']}" if item["url"].startswith("/") else item["url"]
            logger.info(f"Fetching: {item['title'][:60]}...")

            resp = self._request(full_url)
            if resp is None:
                logger.warning(f"  Skipping (fetch failed): {item['title'][:60]}")
                continue

            body_text = self._extract_body(resp.text)
            if not body_text or len(body_text) < 50:
                logger.warning(f"  Skipping (no body text): {item['title'][:60]}")
                continue

            yield {
                "url": full_url,
                "path": item["url"],
                "title": item["title"],
                "body": body_text,
                "html": resp.text,
            }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents added since the given date (re-fetches all, filters by date)."""
        for raw in self.fetch_all():
            date_str = self._extract_date(raw["title"])
            if date_str:
                try:
                    doc_date = datetime.fromisoformat(date_str)
                    if doc_date >= since.replace(tzinfo=None):
                        yield raw
                except ValueError:
                    yield raw
            else:
                yield raw

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        title = raw["title"]
        slug = raw["path"].rstrip("/").split("/")[-1]
        doc_id = f"SN-JO-{slug}"
        date_str = self._extract_date(title)
        doc_type = self._classify_document(raw["path"], title)

        return {
            "_id": doc_id,
            "_source": "SN/JournalOfficiel",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "document_id": doc_id,
            "title": title,
            "text": raw["body"],
            "date": date_str,
            "document_type": doc_type,
            "url": raw["url"],
            "language": "fr",
            "country": "SN",
        }


# ── CLI entry point ──────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="SN/JournalOfficiel bootstrapper")
    parser.add_argument("command", choices=["bootstrap", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records (default)")
    args = parser.parse_args()

    scraper = PrimatureSNScraper()

    if args.command == "test":
        logger.info("Testing connectivity to primature.sn...")
        resp = scraper._request(f"{BASE_URL}/publications/lois-et-reglements")
        if resp and resp.status_code == 200:
            logger.info("SUCCESS: primature.sn is reachable")
            sys.exit(0)
        else:
            logger.error("FAILED: Cannot reach primature.sn")
            sys.exit(1)

    elif args.command == "bootstrap":
        sample_mode = args.sample
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
