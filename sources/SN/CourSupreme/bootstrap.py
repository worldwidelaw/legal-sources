#!/usr/bin/env python3
"""
SN/CourSupreme -- Senegal Supreme Court Decisions via Juricaf

Fetches Senegalese Supreme Court decisions with full text from juricaf.org
(AHJUCAF francophone court decisions database).

Strategy:
  - JSON API for paginated search (500 results/page, ~15 pages)
  - Fetch each decision page HTML for full text (div#textArret)
  - ~7,459 decisions from the Cour suprême

Usage:
  python bootstrap.py bootstrap          # Fetch all decisions
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
logger = logging.getLogger("legal-data-hunter.SN.CourSupreme")

BASE_URL = "https://juricaf.org"
SEARCH_URL = (
    f"{BASE_URL}/recherche/+/"
    "facet_pays:S%C3%A9n%C3%A9gal,"
    "facet_pays_juridiction:S%C3%A9n%C3%A9gal_%7C_Cour_supr%C3%AAme"
)
MAX_PAGES = 20  # 7,459 / 500 = ~15 pages, with margin


class _TextArretExtractor(HTMLParser):
    """Extract text from div#textArret in Juricaf decision pages."""

    def __init__(self):
        super().__init__()
        self.in_target = False
        self.depth = 0
        self.parts: List[str] = []

    def handle_starttag(self, tag, attrs):
        attrs_d = dict(attrs)
        if attrs_d.get("id") == "textArret":
            self.in_target = True
            self.depth = 0
        if self.in_target:
            self.depth += 1
            if tag in ("br",):
                self.parts.append("\n")
            elif tag in ("p", "div"):
                self.parts.append("\n")

    def handle_endtag(self, tag):
        if self.in_target:
            self.depth -= 1
            if self.depth <= 0:
                self.in_target = False
            if tag in ("p",):
                self.parts.append("\n")

    def handle_data(self, data):
        if self.in_target:
            self.parts.append(data)

    def get_text(self) -> str:
        text = "".join(self.parts).strip()
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        return text


class JuricafSNScraper(BaseScraper):
    """Scraper for SN/CourSupreme -- Senegalese Supreme Court via Juricaf."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.5,en;q=0.3",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
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
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _fetch_decision_list(self, page: int) -> List[Dict[str, Any]]:
        """Fetch a page of decision metadata via JSON API."""
        url = f"{SEARCH_URL}?format=json&page={page}"
        resp = self._request(url)
        if resp is None:
            return []
        try:
            data = resp.json()
        except (ValueError, json.JSONDecodeError):
            logger.warning(f"Invalid JSON on page {page}")
            return []
        return data.get("docs", [])

    def _extract_decision_text(self, html: str) -> Dict[str, str]:
        """Extract full text and metadata from a Juricaf decision page."""
        result = {"text": "", "date": "", "title": "", "court": "", "docket_number": ""}

        # Extract text from div#textArret
        parser = _TextArretExtractor()
        parser.feed(html)
        result["text"] = parser.get_text()

        # Dublin Core metadata from meta tags
        for pattern, field in [
            (r'<meta\s+name="dc\.creator"\s+content="([^"]*)"', "court"),
            (r'<meta\s+name="dc\.title"\s+content="([^"]*)"', "title"),
            (r'<meta\s+name="dc\.date"\s+content="([^"]*)"', "date"),
            (r'<meta\s+name="docketnumber"\s+content="([^"]*)"', "docket_number"),
        ]:
            m = re.search(pattern, html, re.IGNORECASE)
            if m:
                result[field] = m.group(1).strip()

        # Normalize date
        if result["date"]:
            raw = result["date"]
            if re.match(r"\d{4}-\d{2}-\d{2}", raw):
                pass
            elif m := re.match(r"(\d{2})/(\d{2})/(\d{4})", raw):
                result["date"] = f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
            elif re.match(r"\d{8}$", raw):
                result["date"] = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"

        return result

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("decision_id", ""),
            "_source": "SN/CourSupreme",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "court": raw.get("court", ""),
            "docket_number": raw.get("docket_number", ""),
            "url": raw.get("url", ""),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Senegalese Supreme Court decisions from Juricaf."""
        count = 0
        seen_ids = set()

        for page_num in range(1, MAX_PAGES + 1):
            docs = self._fetch_decision_list(page_num)
            if not docs:
                logger.info(f"No docs on page {page_num}, stopping pagination")
                break

            logger.info(f"Page {page_num}: {len(docs)} decisions listed")

            for doc in docs:
                if max_records and count >= max_records:
                    return

                doc_id = doc.get("id", "")
                if not doc_id or doc_id in seen_ids:
                    continue
                seen_ids.add(doc_id)

                # Fetch individual decision HTML for full text
                decision_url = f"{BASE_URL}/arret/{doc_id}"
                resp = self._request(decision_url)
                if resp is None:
                    logger.warning(f"Failed to fetch: {doc_id}")
                    continue

                extracted = self._extract_decision_text(resp.text)
                if not extracted["text"] or len(extracted["text"]) < 100:
                    logger.warning(
                        f"Insufficient text ({len(extracted.get('text', ''))} chars): {doc_id}"
                    )
                    continue

                # Use JSON date if HTML extraction didn't find one
                date = extracted["date"]
                if not date and doc.get("date_arret"):
                    date = doc["date_arret"][:10]  # YYYY-MM-DD from ISO

                raw = {
                    "decision_id": doc_id,
                    "title": extracted["title"] or doc.get("titre", ""),
                    "text": extracted["text"],
                    "date": date,
                    "court": extracted["court"] or doc.get("juridiction", ""),
                    "docket_number": extracted["docket_number"],
                    "url": decision_url,
                }
                count += 1
                yield self.normalize(raw)

        logger.info(f"Completed: {count} decisions fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent decisions (first page only)."""
        yield from self.fetch_all(max_records=20)

    def test(self) -> bool:
        """Quick connectivity test."""
        docs = self._fetch_decision_list(1)
        if not docs:
            logger.error("Cannot fetch decision list from Juricaf JSON API")
            return False

        logger.info(f"JSON API OK: {len(docs)} decisions on page 1")

        # Test fetching a single decision's full text
        doc_id = docs[0].get("id", "")
        resp = self._request(f"{BASE_URL}/arret/{doc_id}")
        if resp:
            extracted = self._extract_decision_text(resp.text)
            logger.info(
                f"Decision OK: {doc_id} "
                f"({len(extracted['text'])} chars, court={extracted['court']})"
            )
        else:
            logger.warning("Could not fetch sample decision")

        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="SN/CourSupreme data fetcher (Juricaf)")
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

    scraper = JuricafSNScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        max_records = 15 if args.sample else None

        for record in scraper.fetch_all(max_records=max_records):
            out_path = sample_dir / f"record_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            text_len = len(record.get("text", ""))
            logger.info(
                f"[{count + 1}] {record.get('title', '?')[:80]} "
                f"({text_len:,} chars)"
            )
            count += 1

        logger.info(f"Bootstrap complete: {count} records saved to sample/")

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
