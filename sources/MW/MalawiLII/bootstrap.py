#!/usr/bin/env python3
"""
MW/MalawiLII -- Malawi Legal Information Institute (legislation)

Fetches Malawian legislation with full text from Akoma Ntoso HTML via MalawiLII.

Strategy:
  - Paginate legislation listing pages (~7 pages, ~335 acts)
  - Fetch each act page, extract text from la-akoma-ntoso element
  - Respect 5-second crawl delay per robots.txt
  - Judgments blocked by robots.txt; legislation only

Usage:
  python bootstrap.py bootstrap          # Fetch all legislation
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

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MW.MalawiLII")

BASE_URL = "https://malawilii.org"
LISTING_URL = f"{BASE_URL}/legislation/"
MAX_PAGES = 10


class MalawiLIIScraper(BaseScraper):
    """Scraper for MW/MalawiLII -- Malawian legislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        """HTTP GET with 5-second crawl delay and retry."""
        for attempt in range(3):
            try:
                time.sleep(5)
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
                logger.warning(f"Attempt {attempt+1} failed: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _parse_listing_page(self, html: str) -> List[Dict[str, str]]:
        """Parse a legislation listing page for document links."""
        soup = BeautifulSoup(html, "html.parser")
        documents = []
        seen = set()

        links = soup.find_all("a", href=lambda h: h and "/akn/mw/" in str(h))
        for link in links:
            href = link.get("href", "")
            if href in seen:
                continue
            seen.add(href)

            title = link.get_text(strip=True)
            if not title:
                continue

            full_url = href if href.startswith("http") else BASE_URL + href
            documents.append({
                "title": title,
                "url": full_url,
                "href": href,
            })

        return documents

    def _extract_full_text(self, html: str) -> Dict[str, str]:
        """Extract full text and metadata from a legislation page."""
        soup = BeautifulSoup(html, "html.parser")
        result = {"text": "", "date": "", "title": ""}

        h1 = soup.find("h1")
        if h1:
            result["title"] = h1.get_text(strip=True)

        akn = soup.find("la-akoma-ntoso")
        if akn:
            text = akn.get_text(separator="\n", strip=True)
            text = re.sub(r"\n{3,}", "\n\n", text)
            text = re.sub(r" {2,}", " ", text)
            result["text"] = text.strip()

        for attr in ["commencement-date", "assent-date", "publication-date"]:
            el = soup.find(attrs={"class": attr})
            if el:
                date_text = el.get_text(strip=True)
                date_m = re.search(r"(\d{1,2}\s+\w+\s+\d{4})", date_text)
                if date_m:
                    try:
                        parsed = datetime.strptime(date_m.group(1), "%d %B %Y")
                        result["date"] = parsed.strftime("%Y-%m-%d")
                        break
                    except ValueError:
                        pass

        if not result["date"]:
            date_m = re.search(r"eng@(\d{4}-\d{2}-\d{2})", html)
            if date_m:
                result["date"] = date_m.group(1)

        return result

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        href = raw.get("href", "")
        doc_id = re.sub(r"^/akn/mw/", "MW-", href)
        doc_id = re.sub(r"/eng@.*$", "", doc_id)
        doc_id = doc_id.replace("/", "-")

        return {
            "_id": doc_id,
            "_source": "MW/MalawiLII",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all legislation from paginated listing."""
        count = 0
        seen_urls = set()

        for page_num in range(1, MAX_PAGES + 1):
            url = f"{LISTING_URL}?page={page_num}"
            resp = self._request(url)
            if resp is None:
                break

            docs = self._parse_listing_page(resp.text)
            if not docs:
                logger.info(f"No documents on page {page_num}, stopping")
                break

            logger.info(f"Page {page_num}: {len(docs)} acts")

            for doc in docs:
                doc_url = doc["url"]
                if doc_url in seen_urls:
                    continue
                seen_urls.add(doc_url)

                doc_resp = self._request(doc_url)
                if doc_resp is None:
                    logger.warning(f"Failed to fetch: {doc['title'][:60]}")
                    continue

                extracted = self._extract_full_text(doc_resp.text)
                if not extracted["text"] or len(extracted["text"]) < 100:
                    logger.warning(f"Insufficient text: {doc['title'][:60]}")
                    continue

                raw = {
                    "href": doc["href"],
                    "title": extracted["title"] or doc["title"],
                    "text": extracted["text"],
                    "date": extracted["date"],
                    "url": doc_url,
                }
                count += 1
                yield self.normalize(raw)

        logger.info(f"Completed: {count} acts fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent legislation (first 2 pages)."""
        count = 0
        for page_num in range(1, 3):
            url = f"{LISTING_URL}?page={page_num}"
            resp = self._request(url)
            if resp is None:
                continue

            docs = self._parse_listing_page(resp.text)
            for doc in docs:
                doc_resp = self._request(doc["url"])
                if doc_resp is None:
                    continue

                extracted = self._extract_full_text(doc_resp.text)
                if not extracted["text"] or len(extracted["text"]) < 100:
                    continue

                raw = {
                    "href": doc["href"],
                    "title": extracted["title"] or doc["title"],
                    "text": extracted["text"],
                    "date": extracted["date"],
                    "url": doc["url"],
                }
                count += 1
                yield self.normalize(raw)

        logger.info(f"Updates: {count} acts fetched")

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._request(f"{LISTING_URL}?page=1")
        if resp is None:
            logger.error("Cannot reach MalawiLII listing page")
            return False

        docs = self._parse_listing_page(resp.text)
        if not docs:
            logger.error("No legislation found on listing page")
            return False

        logger.info(f"Listing OK: {len(docs)} acts on page 1")

        doc_resp = self._request(docs[0]["url"])
        if doc_resp:
            extracted = self._extract_full_text(doc_resp.text)
            logger.info(f"Doc OK: {docs[0]['title'][:60]} ({len(extracted['text'])} chars)")
            return True

        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MW/MalawiLII data fetcher")
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
    args = parser.parse_args()

    scraper = MalawiLIIScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        max_records = 15 if args.sample else None
        count = 0

        for record in scraper.fetch_all():
            out_path = sample_dir / f"record_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            text_len = len(record.get("text", ""))
            logger.info(
                f"[{count + 1}] {record.get('title', '?')[:80]} "
                f"({text_len:,} chars)"
            )

            count += 1
            if max_records and count >= max_records:
                break

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_updates():
            out_path = sample_dir / f"update_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    main()
