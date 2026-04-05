#!/usr/bin/env python3
"""
HR/PoreznaUprava-Misljenja -- Croatian Tax Authority Opinions Fetcher

Fetches tax opinions (Mišljenja Središnjeg ureda) from the Croatian Tax
Administration (Porezna uprava) by scraping the public listing and detail pages.

Source: https://porezna-uprava.gov.hr/hr/misljenja-su/3951
Volume: 2,681+ opinions covering VAT, income tax, corporate tax, property tax,
        excise, local taxes, contributions, fiscalization, double taxation, etc.

Full text is available inline as HTML in detail pages at:
  /Misljenja/Detaljno/{id}

Listing pages at /hr/misljenja-su/3951?Page={n} show 10 items per page.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental (newest first)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.HR.PoreznaUprava-Misljenja")

BASE_URL = "https://porezna-uprava.gov.hr"
LISTING_URL = f"{BASE_URL}/hr/misljenja-su/3951"
DETAIL_URL = f"{BASE_URL}/Misljenja/Detaljno"
DELAY = 1.5  # seconds between requests

CATEGORIES = {
    "1": "Porez na dodanu vrijednost",
    "2": "Porez na dodanu vrijednost i trošarine",
    "3": "Posebni porezi i trošarine",
    "4": "Porez na dohodak",
    "5": "Porez na dobit",
    "6": "Porez na promet nekretnina",
    "7": "Igre na sreću i zabavne igre",
    "8": "Lokalni porezi",
    "9": "Županijski porezi",
    "10": "Gradski i općinski porezi",
    "11": "Opći porezni zakon",
    "12": "Doprinosi",
    "13": "Financijsko poslovanje i predstečajna nagodba",
    "14": "OIB",
    "15": "Fiskalizacija",
    "16": "Dvostruko oporezivanje",
    "17": "ePorezna",
    "18": "Ostalo",
}


def strip_html(raw_html: str) -> str:
    """Strip HTML tags and decode entities to plain text."""
    text = re.sub(r'<(style|script)[^>]*>.*?</\1>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<(br|p|div|h[1-6]|li|tr)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class PoreznaUpravaMisljenja(BaseScraper):
    SOURCE_ID = "HR/PoreznaUprava-Misljenja"

    def __init__(self):
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "hr,en;q=0.5",
                "User-Agent": "LegalDataHunter/1.0 (academic research; open legal data)",
            },
        )

    def fetch_listing_page(self, page: int) -> List[int]:
        """Fetch a listing page and extract opinion IDs."""
        resp = self.http.get(f"{LISTING_URL}?Page={page}")
        time.sleep(DELAY)
        if resp is None or resp.status_code != 200:
            logger.warning("Failed to fetch listing page %d", page)
            return []

        html = resp.text
        ids = re.findall(r'Misljenja/Detaljno/(\d+)', html)
        # Deduplicate while preserving order
        seen = set()
        unique_ids = []
        for id_str in ids:
            if id_str not in seen:
                seen.add(id_str)
                unique_ids.append(int(id_str))
        return unique_ids

    def fetch_opinion(self, opinion_id: int) -> Optional[Dict[str, Any]]:
        """Fetch and parse a single opinion detail page."""
        resp = self.http.get(f"{DETAIL_URL}/{opinion_id}")
        time.sleep(DELAY)
        if resp is None or resp.status_code != 200:
            return None

        html = resp.text

        # Extract content from vijesti_paragrafi div
        content_match = re.search(
            r'<div\s+class="vijesti_paragrafi[^"]*">(.*?)<div\s+class="article_prev_next',
            html, re.DOTALL
        )
        if not content_match:
            logger.warning("Could not find content div for opinion %d", opinion_id)
            return None

        content_html = content_match.group(1)

        # Extract title
        title_match = re.search(r'<h1\s+class="vijesti_h1">(.*?)</h1>', content_html, re.DOTALL)
        title = strip_html(title_match.group(1)).strip() if title_match else ""

        # Extract metadata from <p> tags
        category = ""
        year = ""
        klasa = ""
        urbroj = ""
        city = ""
        date_str = ""

        meta_patterns = {
            "category": r'<p>\s*Kategorija:\s*(.*?)</p>',
            "year": r'<p>\s*Godina:\s*(.*?)</p>',
            "klasa": r'<p>\s*Broj klase:\s*(.*?)</p>',
            "urbroj": r'<p>\s*Urudžbeni broj:\s*(.*?)</p>',
            "city": r'<p>\s*Mjesto:\s*(.*?)</p>',
            "date": r'<p>\s*Datum:\s*(.*?)</p>',
        }

        for key, pattern in meta_patterns.items():
            match = re.search(pattern, content_html, re.DOTALL | re.IGNORECASE)
            if match:
                val = strip_html(match.group(1)).strip()
                if key == "category":
                    category = val
                elif key == "year":
                    year = val
                elif key == "klasa":
                    klasa = val
                elif key == "urbroj":
                    urbroj = val
                elif key == "city":
                    city = val
                elif key == "date":
                    date_str = val

        # Extract body text: everything after the last metadata <p> tag
        # Remove title and metadata <p> tags, keep the rest
        body_html = content_html
        # Remove the h1 title
        body_html = re.sub(r'<h1\s+class="vijesti_h1">.*?</h1>', '', body_html, flags=re.DOTALL)
        # Remove metadata paragraphs
        for pattern in meta_patterns.values():
            body_html = re.sub(pattern, '', body_html, flags=re.DOTALL | re.IGNORECASE)
        # Remove the row/col div at the end
        body_html = re.sub(r'<div\s+class="row">.*', '', body_html, flags=re.DOTALL)

        text = strip_html(body_html).strip()

        # Parse date to ISO format
        iso_date = None
        if date_str:
            # Format: DD.MM.YYYY. (with trailing dot)
            date_clean = date_str.rstrip('.')
            try:
                dt = datetime.strptime(date_clean, "%d.%m.%Y")
                iso_date = dt.strftime("%Y-%m-%d")
            except ValueError:
                iso_date = date_str

        # Look up category ID
        category_id = ""
        for cid, cname in CATEGORIES.items():
            if cname == category:
                category_id = cid
                break

        return {
            "opinion_id": opinion_id,
            "title": title,
            "text": text,
            "category": category,
            "category_id": category_id,
            "year": year,
            "klasa": klasa,
            "urbroj": urbroj,
            "city": city,
            "date": iso_date or date_str,
            "date_raw": date_str,
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw opinion into the standard schema."""
        return {
            "_id": f"HR-PU-{raw['opinion_id']}",
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw["date"],
            "url": f"{DETAIL_URL}/{raw['opinion_id']}",
            "language": "hr",
            "category": raw["category"],
            "category_id": raw["category_id"],
            "year": raw["year"],
            "klasa": raw["klasa"],
            "urbroj": raw["urbroj"],
            "city": raw["city"],
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all opinions by paginating through listing pages."""
        total_yielded = 0
        sample_limit = 15 if sample else None
        page = 1
        max_pages = 3 if sample else 300  # ~269 expected pages
        consecutive_empty = 0

        while page <= max_pages:
            if sample_limit and total_yielded >= sample_limit:
                break

            logger.info("Fetching listing page %d...", page)
            ids = self.fetch_listing_page(page)

            if not ids:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    logger.info("3 consecutive empty pages, stopping.")
                    break
                page += 1
                continue
            consecutive_empty = 0

            for opinion_id in ids:
                if sample_limit and total_yielded >= sample_limit:
                    break

                raw = self.fetch_opinion(opinion_id)
                if not raw:
                    logger.warning("Failed to fetch opinion %d", opinion_id)
                    continue

                record = self.normalize(raw)
                if not record["text"]:
                    logger.warning("Empty text for opinion %d: %s", opinion_id, record["title"][:60])
                    continue

                yield record
                total_yielded += 1

                if total_yielded % 50 == 0:
                    logger.info("  Progress: %d opinions fetched", total_yielded)

            page += 1

        logger.info("Fetch complete. Total opinions: %d", total_yielded)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch opinions published since a given date (YYYY-MM-DD)."""
        page = 1
        found_older = False

        while not found_older:
            logger.info("Checking page %d for updates since %s...", page, since)
            ids = self.fetch_listing_page(page)
            if not ids:
                break

            for opinion_id in ids:
                raw = self.fetch_opinion(opinion_id)
                if not raw:
                    continue

                if raw["date"] and raw["date"] < since:
                    found_older = True
                    break

                record = self.normalize(raw)
                if record["text"]:
                    yield record

            page += 1
            if page > 300:
                break

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            ids = self.fetch_listing_page(1)
            logger.info("Test: found %d opinion IDs on page 1", len(ids))
            if not ids:
                return False
            raw = self.fetch_opinion(ids[0])
            if raw and raw["text"]:
                logger.info("Test passed: opinion %d has %d chars of text", ids[0], len(raw["text"]))
                return True
            return False
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


# === CLI entry point ===

def main():
    import argparse

    parser = argparse.ArgumentParser(description="HR/PoreznaUprava-Misljenja bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10-15 sample records")
    parser.add_argument("--since", type=str, help="Date for incremental update (YYYY-MM-DD)")
    args = parser.parse_args()

    scraper = PoreznaUpravaMisljenja()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            safe_name = re.sub(r'[^\w\-.]', '_', record['_id'])
            out_file = sample_dir / f"{safe_name}.json"
            out_file.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            count += 1
            text_len = len(record.get("text", ""))
            logger.info(
                "  [%d] %s | %s | text=%d chars",
                count, record["date"], record["title"][:60], text_len
            )

        logger.info("Bootstrap complete: %d records saved to sample/", count)
        sys.exit(0 if count >= 10 else 1)

    if args.command == "update":
        since = args.since or "2026-01-01"
        count = 0
        for record in scraper.fetch_updates(since):
            count += 1
            logger.info("  [%d] %s: %s", count, record["date"], record["title"][:60])
        logger.info("Update complete: %d new records since %s", count, since)


if __name__ == "__main__":
    main()
