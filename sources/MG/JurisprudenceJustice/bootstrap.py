#!/usr/bin/env python3
"""
MG/JurisprudenceJustice -- Madagascar Court of Cassation Decisions

Fetches ~1,003 Court of Cassation decisions (civil, commercial, social)
from the Ministry of Justice jurisprudence portal.

Strategy:
  - Paginated listing pages for each category (civile, commerciale, sociale)
  - Extract decision IDs from /decision/details/{ID} links
  - Fetch each detail page for full text and metadata

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py test
"""

import argparse
import html
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
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
logger = logging.getLogger("legal-data-hunter.MG.JurisprudenceJustice")

BASE_URL = "https://jurisprudence.justice.gov.mg"
CATEGORIES = ["civile", "commerciale", "sociale"]


class MGJurisprudenceScraper(BaseScraper):
    """Scraper for MG/JurisprudenceJustice."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.3",
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

    def _list_decisions(self, category: str, page: int) -> List[str]:
        """Get decision IDs from a listing page."""
        url = f"{BASE_URL}/decision/{category}?page={page}"
        resp = self._request(url)
        if resp is None:
            return []
        ids = re.findall(r'/decision/details/(DCS[A-F0-9]+)', resp.text)
        return list(dict.fromkeys(ids))  # deduplicate preserving order

    def _extract_decision(self, page_html: str, decision_id: str) -> Dict[str, str]:
        """Extract decision metadata and full text from detail page."""
        result = {"text": "", "title": "", "date": "", "docket_number": "", "category": ""}

        # Extract title from h1
        m = re.search(r'<h1[^>]*>(.*?)</h1>', page_html, re.DOTALL)
        if m:
            result["title"] = html.unescape(re.sub(r'<[^>]+>', '', m.group(1)).strip())

        # Extract subtitle with docket number and date from h3
        for m in re.finditer(r'<h3[^>]*>(.*?)</h3>', page_html, re.DOTALL):
            h3_text = html.unescape(re.sub(r'<[^>]+>', '', m.group(1)).strip())
            h3_text = re.sub(r'\s+', ' ', h3_text)
            # Pattern: "Title - dossier XX/XX-YY - N° NNN du DD/MM/YYYY"
            date_m = re.search(r'du\s+(\d{2}/\d{2}/\d{4})', h3_text)
            if date_m:
                try:
                    dt = datetime.strptime(date_m.group(1), "%d/%m/%Y")
                    result["date"] = dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass
            docket_m = re.search(r'dossier\s+(\S+)', h3_text)
            if docket_m:
                result["docket_number"] = docket_m.group(1)
            num_m = re.search(r'N°\s*(\d+)', h3_text)
            if num_m and not result["title"]:
                result["title"] = h3_text

        # Extract full text from "Contenu de la décision" section
        content_idx = page_html.find("Contenu de la")
        if content_idx >= 0:
            section = page_html[content_idx:]
            # Find the end of the content section (next major div or card)
            end_m = re.search(r'</div>\s*</div>\s*</div>\s*</div>', section)
            if end_m:
                section = section[:end_m.start()]

            # Remove CSS style blocks
            section = re.sub(r'\.cs[A-F0-9]+\{[^}]+\}', '', section)
            # Remove HTML tags
            text = re.sub(r'<[^>]+>', '\n', section)
            # Clean up
            text = re.sub(r'Contenu de la d.cision', '', text)
            text = re.sub(r'T.l.charger PDF', '', text)
            text = text.replace('\xa0', ' ')
            text = re.sub(r'\n{3,}', '\n\n', text)
            text = re.sub(r' {2,}', ' ', text)
            text = html.unescape(text.strip())
            result["text"] = text

        return result

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("decision_id", ""),
            "_source": "MG/JurisprudenceJustice",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "docket_number": raw.get("docket_number", ""),
            "category": raw.get("category", ""),
            "url": raw.get("url", ""),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        count = 0
        seen_ids = set()

        for category in CATEGORIES:
            page = 1
            while True:
                if max_records and count >= max_records:
                    return

                ids = self._list_decisions(category, page)
                if not ids:
                    logger.info(f"No more decisions in {category} page {page}")
                    break

                logger.info(f"{category} page {page}: {len(ids)} decisions")

                for dec_id in ids:
                    if max_records and count >= max_records:
                        return
                    if dec_id in seen_ids:
                        continue
                    seen_ids.add(dec_id)

                    url = f"{BASE_URL}/decision/details/{dec_id}"
                    resp = self._request(url)
                    if resp is None:
                        logger.warning(f"Failed to fetch: {dec_id}")
                        continue

                    extracted = self._extract_decision(resp.text, dec_id)
                    if not extracted["text"] or len(extracted["text"]) < 100:
                        logger.warning(f"Insufficient text ({len(extracted.get('text', ''))} chars): {dec_id}")
                        continue

                    raw = {
                        "decision_id": dec_id,
                        "title": extracted["title"],
                        "text": extracted["text"],
                        "date": extracted["date"],
                        "docket_number": extracted["docket_number"],
                        "category": category,
                        "url": url,
                    }
                    count += 1
                    yield raw

                page += 1

        logger.info(f"Completed: {count} decisions fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all(max_records=20)

    def test(self) -> bool:
        ids = self._list_decisions("civile", 1)
        if not ids:
            logger.error("Cannot fetch decision list")
            return False
        logger.info(f"Listing OK: {len(ids)} decisions on civile page 1")

        url = f"{BASE_URL}/decision/details/{ids[0]}"
        resp = self._request(url)
        if resp:
            extracted = self._extract_decision(resp.text, ids[0])
            logger.info(f"Decision OK: {ids[0]} ({len(extracted['text'])} chars, date={extracted['date']})")
        return True


def main():
    parser = argparse.ArgumentParser(description="MG/JurisprudenceJustice data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = MGJurisprudenceScraper()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        max_records = 15 if args.sample else None
        for record in scraper.fetch_all(max_records=max_records):
            out_path = sample_dir / f"record_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"[{count+1}] {record.get('title','?')[:60]} ({len(record.get('text','')):#,} chars)")
            count += 1
        logger.info(f"Bootstrap complete: {count} records saved to sample/")

    elif args.command == "update":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_updates():
            with open(sample_dir / f"update_{count:04d}.json", "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
