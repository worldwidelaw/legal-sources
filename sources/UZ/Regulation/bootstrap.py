#!/usr/bin/env python3
"""
UZ/Regulation — Uzbekistan Draft Laws Public Discussion (regulation.gov.uz)

Scrapes the public discussion portal for draft normative-legal acts.
Iterates listing pages to collect document IDs, then fetches each document
page for full text, metadata (author, dates, comments count).

~9,000 documents across ~450 listing pages. Uzbek/Russian languages.
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Dict, Iterator, List, Optional

from bs4 import BeautifulSoup
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://regulation.gov.uz"
SOURCE_ID = "UZ/Regulation"
SAMPLE_DIR = Path(__file__).parent / "sample"
MIN_TEXT_LENGTH = 200
MAX_LISTING_PAGES = 500  # safety cap; real count is ~450


class UzRegulationFetcher:
    """Fetcher for Uzbekistan draft legislation from regulation.gov.uz."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,*/*",
            "Accept-Language": "uz,ru;q=0.9,en;q=0.8",
        })

    def _get_listing_page(self, page: int) -> List[Dict]:
        """Fetch one listing page, return list of {id, title, author, doc_type, date, end_date, comments}."""
        url = f"{BASE_URL}/oz/document/index?page={page}"
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()

        docs = []
        # Extract document IDs and metadata from table rows
        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table")
        if not table:
            return docs

        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 6:
                continue
            # Skip filter row (has input elements)
            if cells[0].find("input"):
                continue

            link = cells[1].find("a")
            if not link:
                continue

            href = link.get("href", "")
            id_match = re.search(r"/oz/d/(\d+)", href)
            if not id_match:
                continue

            doc_id = id_match.group(1)
            title = link.get_text(strip=True)
            author = cells[3].get_text(strip=True) if len(cells) > 3 else ""
            doc_type = cells[4].get_text(strip=True) if len(cells) > 4 else ""
            date = cells[5].get_text(strip=True) if len(cells) > 5 else ""
            end_date = cells[6].get_text(strip=True) if len(cells) > 6 else ""
            comments = cells[7].get_text(strip=True) if len(cells) > 7 else "0"

            docs.append({
                "id": doc_id,
                "title": title,
                "author": author,
                "document_type": doc_type,
                "date": date,
                "end_date": end_date,
                "comments_count": comments,
            })

        return docs

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date from DD/MM/YYYY format to ISO 8601."""
        date_str = date_str.strip()
        if not date_str:
            return None
        # Try DD/MM/YYYY
        m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", date_str)
        if m:
            day, month, year = m.groups()
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        # Try YYYY-MM-DD
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})", date_str)
        if m:
            return date_str
        return None

    def _fetch_document_text(self, doc_id: str) -> str:
        """Fetch the full text of a document by its ID."""
        url = f"{BASE_URL}/oz/d/{doc_id}"
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        # Collect text from all tab_content_in divs
        text_parts = []
        for div in soup.find_all("div", class_="tab_content_in"):
            text = div.get_text(separator="\n", strip=True)
            if text:
                text_parts.append(text)

        # Also check page_content div for appendix materials
        page_content = soup.find("div", class_="page_content")
        if page_content:
            pc_text = page_content.get_text(separator="\n", strip=True)
            if pc_text and pc_text not in "\n".join(text_parts):
                text_parts.append(pc_text)

        full_text = "\n\n".join(text_parts)
        # Clean up excessive whitespace
        full_text = re.sub(r"\n{3,}", "\n\n", full_text)
        full_text = re.sub(r"[ \t]+", " ", full_text)
        # Decode HTML entities
        full_text = unescape(full_text)
        return full_text.strip()

    def normalize(self, listing_meta: Dict, full_text: str) -> Optional[Dict]:
        """Normalize a document into a standard record."""
        if len(full_text) < MIN_TEXT_LENGTH:
            return None

        doc_id = listing_meta["id"]
        title = unescape(listing_meta.get("title", "")).strip()
        if not title:
            # Fall back to first line of text
            title = full_text[:300].split("\n")[0].strip()
        title = re.sub(r"\s+", " ", title)[:500]

        date = self._parse_date(listing_meta.get("date", ""))
        end_date = self._parse_date(listing_meta.get("end_date", ""))

        comments_str = listing_meta.get("comments_count", "0")
        try:
            comments_count = int(re.sub(r"[^\d]", "", comments_str)) if comments_str else 0
        except ValueError:
            comments_count = 0

        return {
            "_id": f"reg-{doc_id}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": date,
            "end_date": end_date,
            "url": f"{BASE_URL}/oz/d/{doc_id}",
            "author": listing_meta.get("author", ""),
            "document_type": listing_meta.get("document_type", ""),
            "comments_count": comments_count,
            "language": "uz",
        }

    def fetch_all(self) -> Iterator[Dict]:
        """Yield all normalized documents by iterating listing pages."""
        total = 0
        for page in range(1, MAX_LISTING_PAGES + 1):
            logger.info(f"Fetching listing page {page}...")
            try:
                docs = self._get_listing_page(page)
            except Exception as e:
                logger.error(f"Failed to fetch listing page {page}: {e}")
                break

            if not docs:
                logger.info(f"No documents on page {page}, stopping.")
                break

            for meta in docs:
                try:
                    full_text = self._fetch_document_text(meta["id"])
                except Exception as e:
                    logger.warning(f"Failed to fetch document {meta['id']}: {e}")
                    continue

                record = self.normalize(meta, full_text)
                if record:
                    total += 1
                    yield record
                else:
                    logger.warning(
                        f"Skipping doc {meta['id']}: insufficient text ({len(full_text)} chars)"
                    )
                time.sleep(1)

            time.sleep(1)

        logger.info(f"Total records fetched: {total}")

    def fetch_updates(self, since: str) -> Iterator[Dict]:
        """Yield documents published since a given date."""
        try:
            since_dt = datetime.fromisoformat(since)
        except ValueError:
            logger.error(f"Invalid date format: {since}")
            return

        for page in range(1, MAX_LISTING_PAGES + 1):
            logger.info(f"Fetching listing page {page} (updates since {since})...")
            try:
                docs = self._get_listing_page(page)
            except Exception as e:
                logger.error(f"Failed page {page}: {e}")
                break

            if not docs:
                break

            all_before = True
            for meta in docs:
                pub_date = self._parse_date(meta.get("date", ""))
                if pub_date and pub_date < since:
                    continue
                all_before = False

                try:
                    full_text = self._fetch_document_text(meta["id"])
                except Exception as e:
                    logger.warning(f"Failed doc {meta['id']}: {e}")
                    continue

                record = self.normalize(meta, full_text)
                if record:
                    yield record
                time.sleep(1)

            if all_before:
                logger.info("All documents on this page are before the cutoff, stopping.")
                break
            time.sleep(1)


def bootstrap_sample(max_records: int = 20):
    """Fetch sample records and save to sample/."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = UzRegulationFetcher()
    count = 0
    for record in fetcher.fetch_all():
        if count >= max_records:
            break
        out_path = SAMPLE_DIR / f"{record['_id']}.json"
        out_path.write_text(
            json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        logger.info(f"Saved {out_path.name}: {record['title'][:80]}")
        count += 1
    logger.info(f"Sample complete: {count} records saved to {SAMPLE_DIR}")
    return count


def bootstrap_full():
    """Fetch all records."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = UzRegulationFetcher()
    count = 0
    for record in fetcher.fetch_all():
        out_path = SAMPLE_DIR / f"{record['_id']}.json"
        out_path.write_text(
            json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        count += 1
        if count % 100 == 0:
            logger.info(f"Progress: {count} records saved")
    logger.info(f"Full bootstrap complete: {count} records saved to {SAMPLE_DIR}")
    return count


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    parser = argparse.ArgumentParser(description="UZ/Regulation bootstrap")
    parser.add_argument(
        "action",
        choices=["bootstrap", "bootstrap-full"],
        help="bootstrap = sample, bootstrap-full = all",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample data only")
    parser.add_argument("--max", type=int, default=20, help="Max sample records")
    args = parser.parse_args()

    if args.action == "bootstrap" or args.sample:
        bootstrap_sample(args.max)
    else:
        bootstrap_full()
