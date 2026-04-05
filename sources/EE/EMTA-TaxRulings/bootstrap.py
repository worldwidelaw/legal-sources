#!/usr/bin/env python3
"""
EE/EMTA-TaxRulings -- Estonian Tax and Customs Board Binding Rulings Fetcher

Fetches binding advance tax rulings (siduvad eelotsused) from the Estonian Tax
and Customs Board (EMTA/MTA) by scraping their Drupal-based website.

Source: https://www.emta.ee/eraklient/maksud-ja-tasumine/tasumine-volad/siduvad-eelotsused
Volume: 109+ topic/year pages with ~200-350 total rulings (2013-2025)

The rulings are organized by topic (dividends, VAT, fringe benefits, etc.) and year.
Each subpage contains one or more ruling summaries as HTML text sections.

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
logger = logging.getLogger("legal-data-hunter.EE.EMTA-TaxRulings")

BASE_URL = "https://www.emta.ee"
INDEX_PATH = "/eraklient/maksud-ja-tasumine/tasumine-volad/siduvad-eelotsused"
DELAY = 1.5  # seconds between requests

# Common intro text sections that appear on every subpage (skip these)
INTRO_MARKERS = [
    "Siduvaid eelotsuseid taotletakse",
    "Õiguslik alus",
    "Siduva eelotsuse taotluse tingimused",
    "Maksu- ja Tolliamet (MTA) lähtub",
    "Riigilõivuseaduse",
    "Kui Maksu- ja Tolliamet (MTA) leiab",
    "Taotluses puuduste esinemise korral",
    "Maksukorralduse seaduse (MKS)",
    "Eelotsused ei ole maksukohustuslasele",
    "Väljastatud eelotsused",
    "Käsiraamat",
]

# Topic translations for metadata
TOPIC_TRANSLATIONS = {
    "dividendid": "Dividends",
    "kasumieraldised": "Profit distributions",
    "erisoodustus": "Fringe benefits",
    "optsioonid": "Options",
    "ettevotlusega-mitteseotud-kulu": "Non-business expenses",
    "fuusilise-isiku-tulud": "Individual income",
    "maksuvabastused": "Tax exemptions",
    "mitteresidendid": "Non-residents",
    "omakapital": "Equity",
    "pusiv-tegevuskoht": "Permanent establishment",
    "tehingud-vaartpaberitega": "Securities transactions",
    "vara-voorandamine": "Asset disposal",
    "uhineminejagunemine": "Mergers/demergers",
    "kauba-uhendusesisene": "Intra-EU goods",
    "kinnisvara": "Real estate",
    "finantsteenused": "Financial services",
    "poordmaksustamine": "Reverse charge",
    "teenuste-osutamine": "Service provision",
    "teenused-valismaal": "Foreign services",
    "maksuvaba-kaive": "Tax-exempt turnover",
    "sisendkaibemaks": "Input VAT",
    "eksport-import": "Export/import",
}


def strip_html(raw_html: str) -> str:
    """Strip HTML tags and decode entities to plain text."""
    text = re.sub(r'<(style|script)[^>]*>.*?</\1>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<(br|p|div|h[1-6]|li|tr)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class EMTATaxRulings(BaseScraper):
    SOURCE_ID = "EE/EMTA-TaxRulings"

    def __init__(self):
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "et,en;q=0.5",
                "User-Agent": "LegalDataHunter/1.0 (academic research; open legal data)",
            },
        )

    def fetch_subpage_links(self) -> List[str]:
        """Fetch the index page and extract all subpage URLs."""
        resp = self.http.get(INDEX_PATH)
        time.sleep(DELAY)
        if resp is None or resp.status_code != 200:
            logger.error("Failed to fetch index page")
            return []

        html = resp.text
        links = re.findall(
            r'href="(' + re.escape(INDEX_PATH) + r'/[^"]+)"',
            html
        )
        unique = list(dict.fromkeys(links))

        # Filter to year-specific pages (ending with 4-digit year)
        year_pages = [l for l in unique if re.search(r'\d{4}(-\d)?$', l)]
        # Also include pages that don't end with year but aren't pure category pages
        category_slugs = {"tulumaks", "kaibemaks"}
        other_pages = [l for l in unique
                       if l.split("/")[-1] not in category_slugs
                       and l not in year_pages]
        all_pages = year_pages + other_pages

        logger.info("Found %d subpage links (%d year-specific, %d other)",
                     len(all_pages), len(year_pages), len(other_pages))
        return all_pages

    def is_intro_text(self, text: str) -> bool:
        """Check if a text section is common intro/boilerplate."""
        for marker in INTRO_MARKERS:
            if marker in text[:200]:
                return True
        return False

    def fetch_ruling_page(self, path: str) -> Optional[Dict[str, Any]]:
        """Fetch a ruling subpage and extract the ruling text."""
        resp = self.http.get(path)
        time.sleep(DELAY)
        if resp is None or resp.status_code != 200:
            logger.warning("Failed to fetch %s (status=%s)",
                           path, resp.status_code if resp else "None")
            return None

        html = resp.text
        slug = path.split("/")[-1]

        # Extract page title - try heading in the main content first
        heading_match = re.search(
            r'field--name-field-text-section-title[^>]*>.*?<h[234][^>]*>(.*?)</h[234]>',
            html, re.DOTALL
        )
        if not heading_match:
            heading_match = re.search(r'<title[^>]*>(.*?)</title>', html, re.DOTALL)
        page_title = strip_html(heading_match.group(1)).strip() if heading_match else slug
        # Clean up title (remove site name suffix)
        page_title = re.sub(r'\s*\|\s*Maksu.*$', '', page_title).strip()
        # If still generic, construct from topic + year
        if page_title in ("Siduvad eelotsused", ""):
            page_title = f"Siduvad eelotsused: {slug}"

        # Extract year from slug
        year_match = re.search(r'(\d{4})(?:-\d)?$', slug)
        year = year_match.group(1) if year_match else ""

        # Extract topic from slug (remove year suffix)
        topic_slug = re.sub(r'-?\d{4}(?:-\d)?$', '', slug)

        # Translate topic
        topic_name = topic_slug
        for key, translation in TOPIC_TRANSLATIONS.items():
            if key in topic_slug:
                topic_name = translation
                break

        # Determine tax category from path context
        # Income tax topics appear before VAT topics in the index
        tax_category = "tulumaks"  # default
        vat_keywords = ["kauba", "kinnisvara", "finants", "poordmaksust",
                        "teenuste-osutamine", "teenused-valismaal", "maksuvaba",
                        "sisendkaibemaks", "eksport"]
        if any(kw in slug for kw in vat_keywords):
            tax_category = "kaibemaks"

        # Extract all text-section-content fields
        sections = re.findall(
            r'field--name-field-text-section-content[^>]*>(.*?)</div>',
            html, re.DOTALL
        )

        # Filter out intro/boilerplate sections and collect ruling text
        ruling_texts = []
        for section_html in sections:
            text = strip_html(section_html)
            if len(text) < 30:
                continue
            if self.is_intro_text(text):
                continue
            ruling_texts.append(text)

        if not ruling_texts:
            logger.warning("No ruling text found on %s", path)
            return None

        full_text = "\n\n".join(ruling_texts)

        return {
            "slug": slug,
            "path": path,
            "title": page_title,
            "text": full_text,
            "year": year,
            "topic": topic_name,
            "topic_slug": topic_slug,
            "tax_category": "Income Tax" if tax_category == "tulumaks" else "VAT",
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw ruling into the standard schema."""
        return {
            "_id": f"EE-EMTA-{raw['slug']}",
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": f"{raw['year']}-01-01" if raw["year"] else None,
            "url": f"{BASE_URL}{raw['path']}",
            "language": "et",
            "topic": raw["topic"],
            "tax_category": raw["tax_category"],
            "year": raw["year"],
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all rulings from all subpages."""
        links = self.fetch_subpage_links()
        if not links:
            return

        total_yielded = 0
        sample_limit = 15 if sample else None

        for i, path in enumerate(links):
            if sample_limit and total_yielded >= sample_limit:
                break

            logger.info("[%d/%d] Fetching %s...", i + 1, len(links), path.split("/")[-1])
            raw = self.fetch_ruling_page(path)
            if not raw:
                continue

            record = self.normalize(raw)
            if not record["text"]:
                continue

            yield record
            total_yielded += 1

            if total_yielded % 20 == 0:
                logger.info("  Progress: %d rulings fetched", total_yielded)

        logger.info("Fetch complete. Total rulings: %d", total_yielded)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch rulings from the current year's pages."""
        current_year = datetime.now().year
        links = self.fetch_subpage_links()
        for path in links:
            slug = path.split("/")[-1]
            year_match = re.search(r'(\d{4})', slug)
            if year_match and int(year_match.group(1)) >= int(since[:4]):
                raw = self.fetch_ruling_page(path)
                if raw:
                    record = self.normalize(raw)
                    if record["text"]:
                        yield record

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            links = self.fetch_subpage_links()
            logger.info("Test: found %d subpage links", len(links))
            if not links:
                return False
            raw = self.fetch_ruling_page(links[0])
            if raw and raw["text"]:
                logger.info("Test passed: %s has %d chars of text",
                            raw["slug"], len(raw["text"]))
                return True
            logger.error("Test failed: no text extracted")
            return False
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


# === CLI entry point ===

def main():
    import argparse

    parser = argparse.ArgumentParser(description="EE/EMTA-TaxRulings bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10-15 sample records")
    parser.add_argument("--since", type=str, help="Date for incremental update (YYYY-MM-DD)")
    args = parser.parse_args()

    scraper = EMTATaxRulings()

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
                count, record["year"], record["title"][:60], text_len
            )

        logger.info("Bootstrap complete: %d records saved to sample/", count)
        sys.exit(0 if count >= 10 else 1)

    if args.command == "update":
        since = args.since or "2026-01-01"
        count = 0
        for record in scraper.fetch_updates(since):
            count += 1
            logger.info("  [%d] %s: %s", count, record["year"], record["title"][:60])
        logger.info("Update complete: %d records since %s", count, since)


if __name__ == "__main__":
    main()
