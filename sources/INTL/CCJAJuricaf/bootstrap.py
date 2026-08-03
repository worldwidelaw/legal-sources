#!/usr/bin/env python3
"""
INTL/CCJAJuricaf -- OHADA Common Court of Justice and Arbitration (CCJA)
                    decisions with FULL TEXT via Juricaf.

The CCJA (Cour Commune de Justice et d'Arbitrage) is the supranational
supreme court for OHADA business law, binding across the 17 OHADA member
states. juricaf.org (AHJUCAF) publishes the CCJA jurisprudence with the
full judgment text — distinct from INTL/OHADA-CCJA (ohada.com), which
carries only short abstracts/summaries.

Strategy:
  - JSON API for paginated search (facet_pays:OHADA, ~1,325 decisions)
  - Fetch each decision page HTML for full text (div#textArret)

Contract (BaseScraper):
  - fetch_all() yields RAW dicts (full text already downloaded)
  - normalize(raw) maps RAW -> standard schema
  - bootstrap()/bootstrap_fast() stream to data/records.jsonl

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap-fast
  python bootstrap.py test
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.CCJAJuricaf")

SOURCE_ID = "INTL/CCJAJuricaf"
BASE_URL = "https://juricaf.org"
SEARCH_URL = f"{BASE_URL}/recherche/+/facet_pays:OHADA"
MAX_PAGES = 50  # Juricaf returns up to 500 docs/page; ~1,325 total -> 3 pages


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


class CCJAJuricafScraper(BaseScraper):
    """Scraper for INTL/CCJAJuricaf via Juricaf."""

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

    def _extract_decision(self, html: str) -> Dict[str, str]:
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

        raw = result["date"]
        if raw:
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
            "_source": SOURCE_ID,
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

                extracted = self._extract_decision(resp.text)
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
                    # dc.title meta is absent on OHADA pages -> fall back to JSON titre
                    "title": extracted["title"] or doc.get("titre", doc.get("title", "")),
                    "text": extracted["text"],
                    "date": date,
                    "court": extracted["court"] or doc.get("juridiction", "")
                    or "Cour commune de justice et d'arbitrage (CCJA)",
                    "docket_number": extracted["docket_number"],
                    "url": decision_url,
                }
                count += 1
                yield raw

        logger.info(f"Completed: {count} decisions fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all(max_records=20)

    def test(self) -> bool:
        docs = self._fetch_decision_list(1)
        if not docs:
            logger.error("Cannot fetch decision list from Juricaf JSON API")
            return False
        logger.info(f"JSON API OK: {len(docs)} decisions on page 1")
        doc_id = docs[0].get("id", "")
        resp = self._request(f"{BASE_URL}/arret/{doc_id}")
        if resp:
            extracted = self._extract_decision(resp.text)
            logger.info(f"Decision OK: {doc_id} ({len(extracted['text'])} chars)")
            return len(extracted["text"]) >= 100
        return False


def main():
    parser = argparse.ArgumentParser(description="INTL/CCJAJuricaf data fetcher")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = CCJAJuricafScraper()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
    elif args.command == "bootstrap-fast":
        # Full high-throughput run -> streams to data/records.jsonl
        stats = scraper.bootstrap_fast()
        logger.info(f"Bootstrap-fast complete: {stats}")
    elif args.command == "bootstrap":
        if args.sample:
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
            logger.info(f"Sample bootstrap complete: {stats}")
        else:
            # Full run -> streams to data/records.jsonl
            stats = scraper.bootstrap(sample_mode=False)
            logger.info(f"Bootstrap complete: {stats}")


if __name__ == "__main__":
    main()
