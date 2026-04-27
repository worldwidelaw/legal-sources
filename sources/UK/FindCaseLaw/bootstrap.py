#!/usr/bin/env python3
"""
UK/FindCaseLaw -- UK Find Case Law (The National Archives)

Fetches UK court judgments from the National Archives Find Case Law service.

Strategy:
  - Paginate Atom feed at /atom.xml to discover judgments
  - Fetch full text Akoma Ntoso XML at /{uri}/data.xml
  - Extract clean text from XML

Data: ~365,000 judgments (UKSC, EWCA, EWHC, tribunals).
License: Open Justice Licence.
Rate limit: 1,000 requests per 5-minute window.

Usage:
  python bootstrap.py bootstrap            # Full pull (365K+ docs, ~30 hours)
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import time
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from xml.etree import ElementTree as ET

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UK.FindCaseLaw")

BASE_URL = "https://caselaw.nationalarchives.gov.uk"

# Atom/XML namespaces
ATOM_NS = "http://www.w3.org/2005/Atom"
TNA_NS = "https://caselaw.nationalarchives.gov.uk"
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"
UK_NS = "https://caselaw.nationalarchives.gov.uk/akn"


class FindCaseLawScraper(BaseScraper):
    """
    Scraper for UK/FindCaseLaw -- UK court judgments from The National Archives.
    Country: UK
    URL: https://caselaw.nationalarchives.gov.uk

    Data types: case_law
    Auth: none (Open Justice Licence)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/atom+xml, application/xml, text/xml",
            },
            timeout=30,
        )
        self._request_count = 0
        self._window_start = time.time()

    def _rate_limit(self):
        """Enforce 1,000 requests per 5-minute window."""
        self._request_count += 1
        if self._request_count >= 950:  # Leave some margin
            elapsed = time.time() - self._window_start
            if elapsed < 300:
                wait = 300 - elapsed + 5
                logger.info(f"Rate limit approaching, waiting {wait:.0f}s")
                time.sleep(wait)
            self._request_count = 0
            self._window_start = time.time()

    # -- Atom feed parsing -------------------------------------------------

    def _fetch_atom_page(self, page: int = 1, per_page: int = 50) -> Optional[list]:
        """Fetch one page of the Atom feed and return entry metadata."""
        url = f"{BASE_URL}/atom.xml"
        params = {"page": page, "per_page": per_page, "order": "-date"}
        self._rate_limit()

        try:
            resp = self.client.get(url, params=params, timeout=30)
            if resp is None or resp.status_code != 200:
                return None

            root = ET.fromstring(resp.content)
            entries = []

            for entry in root.findall(f"{{{ATOM_NS}}}entry"):
                title_el = entry.find(f"{{{ATOM_NS}}}title")
                published_el = entry.find(f"{{{ATOM_NS}}}published")
                updated_el = entry.find(f"{{{ATOM_NS}}}updated")
                author_el = entry.find(f"{{{ATOM_NS}}}author/{{{ATOM_NS}}}name")

                # Get data.xml link
                xml_link = None
                for link in entry.findall(f"{{{ATOM_NS}}}link"):
                    if "akn+xml" in (link.get("type") or ""):
                        xml_link = link.get("href")
                        break

                # Get document URI
                uri_el = entry.find(f"{{{TNA_NS}}}uri")
                # Get citation
                citation_el = entry.find(f"{{{TNA_NS}}}identifier[@type='ukncn']")
                if citation_el is None:
                    citation_el = entry.find(f"{{{TNA_NS}}}identifier")
                # Get content hash
                hash_el = entry.find(f"{{{TNA_NS}}}contenthash")

                uri = uri_el.text.strip() if uri_el is not None and uri_el.text else ""
                if not uri and xml_link:
                    # Extract URI from XML link
                    uri = xml_link.replace(f"{BASE_URL}/", "").replace("/data.xml", "")

                entries.append({
                    "title": title_el.text.strip() if title_el is not None and title_el.text else "",
                    "date": published_el.text.strip()[:10] if published_el is not None and published_el.text else "",
                    "updated": updated_el.text.strip() if updated_el is not None and updated_el.text else "",
                    "court": author_el.text.strip() if author_el is not None and author_el.text else "",
                    "xml_url": xml_link or "",
                    "uri": uri,
                    "citation": citation_el.text.strip() if citation_el is not None and citation_el.text else "",
                    "content_hash": hash_el.text.strip() if hash_el is not None and hash_el.text else "",
                })

            return entries
        except Exception as e:
            logger.warning(f"Failed to fetch Atom page {page}: {e}")
            return None

    # -- Full text extraction ----------------------------------------------

    def _fetch_full_text(self, xml_url: str) -> Optional[str]:
        """Fetch and extract clean text from Akoma Ntoso XML."""
        if not xml_url:
            return None

        self._rate_limit()
        try:
            resp = self.client.get(xml_url, timeout=30)
            if resp is None or resp.status_code != 200:
                return None

            root = ET.fromstring(resp.content)

            # Extract all text from the judgment body
            texts = []
            for elem in root.iter():
                if elem.text and elem.text.strip():
                    texts.append(elem.text.strip())
                if elem.tail and elem.tail.strip():
                    texts.append(elem.tail.strip())

            text = "\n".join(texts)
            # Clean up
            text = re.sub(r"\n{3,}", "\n\n", text)
            text = re.sub(r" {2,}", " ", text)
            return text.strip() if len(text) > 50 else None
        except Exception as e:
            logger.debug(f"Failed to fetch XML: {e}")
            return None

    # -- Normalize ---------------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry data into standard schema."""
        uri = raw.get("uri", "")
        text = raw.get("_full_text", "")
        if not uri or not text:
            return None

        doc_id = f"UK-FCL-{uri.replace('/', '-')}"

        return {
            "_id": doc_id,
            "_source": "UK/FindCaseLaw",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "uri": uri,
            "title": raw.get("title", ""),
            "text": text,
            "date": raw.get("date", ""),
            "url": f"{BASE_URL}/{uri}",
            "court": raw.get("court", "") or None,
            "citation": raw.get("citation", "") or None,
            "content_hash": raw.get("content_hash", "") or None,
        }

    # -- Fetch methods -----------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all UK court judgments with full text."""
        page = 1
        total = 0

        while True:
            entries = self._fetch_atom_page(page=page)
            if not entries:
                logger.info(f"No more entries at page {page}, stopping")
                break

            logger.info(f"Page {page}: {len(entries)} entries")

            for entry in entries:
                xml_url = entry.get("xml_url", "")
                if not xml_url:
                    # Construct from URI
                    uri = entry.get("uri", "")
                    if uri:
                        xml_url = f"{BASE_URL}/{uri}/data.xml"

                text = self._fetch_full_text(xml_url)
                if not text:
                    continue

                entry["_full_text"] = text
                total += 1
                if total % 100 == 0:
                    logger.info(f"Fetched {total} judgments")
                yield entry

            page += 1

        logger.info(f"Total fetched: {total}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield judgments updated since the given date."""
        page = 1
        since_str = since.strftime("%Y-%m-%d")
        total = 0

        while True:
            entries = self._fetch_atom_page(page=page)
            if not entries:
                break

            all_old = True
            for entry in entries:
                if entry.get("date", "") < since_str:
                    continue
                all_old = False

                xml_url = entry.get("xml_url", "")
                if not xml_url:
                    uri = entry.get("uri", "")
                    if uri:
                        xml_url = f"{BASE_URL}/{uri}/data.xml"

                text = self._fetch_full_text(xml_url)
                if not text:
                    continue

                entry["_full_text"] = text
                total += 1
                yield entry

            if all_old:
                break
            page += 1

        logger.info(f"Updates: {total} judgments since {since_str}")

    # -- CLI ---------------------------------------------------------------


def main():
    import argparse

    parser = argparse.ArgumentParser(description="UK/FindCaseLaw Data Fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = FindCaseLawScraper()

    if args.command == "test-api":
        logger.info("Testing Atom feed...")
        entries = scraper._fetch_atom_page(page=1, per_page=5)
        if entries:
            logger.info(f"Atom feed OK: {len(entries)} entries")
            for e in entries[:3]:
                logger.info(f"  {e['title'][:60]} | {e['date']} | {e['court']}")
            if entries[0].get("xml_url"):
                text = scraper._fetch_full_text(entries[0]["xml_url"])
                if text:
                    logger.info(f"Full text OK: {len(text)} chars")
        else:
            logger.error("Atom feed failed")
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=30)
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
