#!/usr/bin/env python3
"""
GN/CourSupremeJuricaf -- Guinea Courts via Juricaf

Fetches court decisions from Guinea via juricaf.org (AHJUCAF).

Courts covered:
  - Cour suprême (Supreme Court) — 126 decisions
  - Cour d'appel (Court of Appeal) — 2 decisions

Strategy:
  - JSON API for paginated search (500 results/page)
  - Fetch each decision page HTML for full text (div#textArret)
  - ~128 decisions total

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
logger = logging.getLogger("legal-data-hunter.GN.CourSupremeJuricaf")

BASE_URL = "https://juricaf.org"
SEARCH_URL = f"{BASE_URL}/recherche/+/facet_pays:Guin%C3%A9e"
MAX_PAGES = 5


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


class JuricafGNScraper(BaseScraper):
    """Scraper for GN/CourSupremeJuricaf -- Guinea Courts via Juricaf."""

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
        result = {"text": "", "date": "", "title": "", "court": "", "docket_number": ""}

        parser = _TextArretExtractor()
        parser.feed(html)
        result["text"] = parser.get_text()

        for pattern, field in [
            (r'<meta\s+name="dc\.creator"\s+content="([^"]*)"', "court"),
            (r'<meta\s+name="dc\.title"\s+content="([^"]*)"', "title"),
            (r'<meta\s+name="dc\.date"\s+content="([^"]*)"', "date"),
            (r'<meta\s+name="docketnumber"\s+content="([^"]*)"', "docket_number"),
        ]:
            m = re.search(pattern, html, re.IGNORECASE)
            if m:
                result[field] = m.group(1).strip()

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
        decision_id = raw.get("decision_id", "")
        return {
            "_id": decision_id,
            "_source": "GN/CourSupremeJuricaf",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "decision_id": decision_id,
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "court": raw.get("court", ""),
            "docket_number": raw.get("docket_number", ""),
            "url": raw.get("url", ""),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
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

                date = extracted["date"]
                if not date and doc.get("date_arret"):
                    date = doc["date_arret"][:10]

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
                yield raw

        logger.info(f"Completed: {count} decisions fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all(max_records=20)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = JuricafGNScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
