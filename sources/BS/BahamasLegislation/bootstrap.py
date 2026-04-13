#!/usr/bin/env python3
"""
BS/BahamasLegislation -- Bahamas Legislation Online (iLAWS)

Fetches full-text legislation from The Bahamas official legislation portal.
Scrapes the alphabetical index (A-Z) via POST requests, extracts PDF links,
and downloads full text from each act's PDF.

Data access:
  - HTML index at laws.bahamas.gov.bs/cms/legislation/acts_only/by-alphabetical-order.html
  - POST form with submit4=<LETTER> to navigate letters
  - PDF files at /cms/images/LEGISLATION/PRINCIPAL/<year>/<year-number>/<file>.pdf
  - Full text extracted from PDFs via common/pdf_extract

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10-15 sample records
  python bootstrap.py update             # Incremental (not supported, runs full)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BS.BahamasLegislation")

BASE_URL = "https://laws.bahamas.gov.bs"
ALPHA_URL = f"{BASE_URL}/cms/legislation/acts_only/by-alphabetical-order.html"
DELAY = 2.0
LETTERS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

# Regex to extract act title and PDF URL from the HTML
# Pattern: <a class="npWrap" href="/cms/images/LEGISLATION/PRINCIPAL/..." target="_blank">Title&nbsp;...
ACT_PATTERN = re.compile(
    r'<a\s+class="npWrap"\s+href="(/cms/images/LEGISLATION/PRINCIPAL/[^"]+\.pdf)"'
    r'\s+target="_blank"\s*>([^<]+?)(?:&nbsp;|\s*<)',
    re.IGNORECASE,
)

# Extract year and act number from the PDF path
# e.g., /cms/images/LEGISLATION/PRINCIPAL/2014/2014-0047/2014-0047_2.pdf
PATH_PATTERN = re.compile(r'/PRINCIPAL/(\d{4})/(\d{4}-\d{4})')

# Extract legislation number from notes popover
NOTES_PATTERN = re.compile(
    r'Legislation Number:\s*([\d-]+)',
    re.IGNORECASE,
)


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "LegalDataHunter/1.0 (legal-data-research)",
        "Accept": "text/html,application/xhtml+xml",
    })
    return session


class BahamasLegislationFetcher:
    SOURCE_ID = "BS/BahamasLegislation"

    def __init__(self):
        self.session = get_session()

    def _fetch_letter_page(self, letter: str) -> str:
        """Fetch the HTML for a given letter page via POST."""
        data = {
            "submit4": letter,
            "pointintime_post": datetime.now().strftime("%Y-%m-%d 00:00:00"),
            "pointintime_post_alpha": datetime.now().strftime("%Y-%m-%d 00:00:00"),
        }
        for attempt in range(3):
            try:
                resp = self.session.post(ALPHA_URL, data=data, timeout=60)
                if resp.status_code == 200:
                    return resp.text
                logger.warning("HTTP %d for letter %s (attempt %d)", resp.status_code, letter, attempt + 1)
            except requests.RequestException as e:
                logger.warning("Request error for letter %s (attempt %d): %s", letter, attempt + 1, e)
            time.sleep(5 * (attempt + 1))
        return ""

    def _parse_acts_from_html(self, html: str) -> List[Dict[str, str]]:
        """Extract act titles and PDF URLs from HTML."""
        acts = []
        seen_paths = set()
        for match in ACT_PATTERN.finditer(html):
            pdf_path = match.group(1)
            title = match.group(2).strip()
            # Deduplicate by PDF path
            if pdf_path in seen_paths:
                continue
            seen_paths.add(pdf_path)

            # Extract year and number from path
            path_match = PATH_PATTERN.search(pdf_path)
            year = path_match.group(1) if path_match else None
            act_number = path_match.group(2) if path_match else None

            acts.append({
                "title": title,
                "pdf_path": pdf_path,
                "pdf_url": f"{BASE_URL}{pdf_path}",
                "year": year,
                "act_number": act_number,
            })
        return acts

    def _extract_pdf_text(self, pdf_url: str, source_id: str) -> Optional[str]:
        """Extract text from a PDF using the centralized extractor."""
        return extract_pdf_markdown(
            source=self.SOURCE_ID,
            source_id=source_id,
            pdf_url=pdf_url,
            table="legislation",
        ) or ""

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw act data into the standard schema."""
        act_number = raw.get("act_number", "")
        return {
            "_id": f"BS-{act_number}" if act_number else f"BS-{raw['title'][:80]}",
            "_source": self.SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw.get("text", ""),
            "date": f"{raw['year']}-01-01" if raw.get("year") else None,
            "year": int(raw["year"]) if raw.get("year") else None,
            "act_number": act_number,
            "url": raw["pdf_url"],
            "country": "BS",
            "jurisdiction": "Bahamas",
            "language": "en",
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all acts from A-Z alphabetical index."""
        total_fetched = 0
        for letter in LETTERS:
            logger.info("Fetching acts for letter %s...", letter)
            html = self._fetch_letter_page(letter)
            if not html:
                logger.warning("No HTML returned for letter %s, skipping", letter)
                continue
            time.sleep(DELAY)

            acts = self._parse_acts_from_html(html)
            logger.info("  Found %d principal acts for letter %s", len(acts), letter)

            for act in acts:
                # Download and extract PDF text
                source_id = act.get("act_number", act["title"][:80])
                text = self._extract_pdf_text(act["pdf_url"], source_id)

                if not text or len(text) < 50:
                    logger.warning("  Skipping '%s' (no text extracted from PDF)", act["title"])
                    continue

                act["text"] = text
                record = self.normalize(act)
                yield record
                total_fetched += 1

                if sample and total_fetched >= 15:
                    logger.info("Sample mode: reached %d records, stopping", total_fetched)
                    return

                time.sleep(DELAY)

        logger.info("Total acts fetched: %d", total_fetched)

    def fetch_updates(self, since: Optional[datetime] = None) -> Generator[Dict[str, Any], None, None]:
        """No incremental endpoint available — runs full fetch."""
        logger.info("No incremental endpoint; running full fetch")
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            resp = self.session.get(ALPHA_URL, timeout=30)
            if resp.status_code != 200:
                logger.error("Test failed: HTTP %d", resp.status_code)
                return False
            acts = self._parse_acts_from_html(resp.text)
            logger.info("Test passed: found %d acts on default page", len(acts))

            # Test one PDF download
            if acts:
                test_act = acts[0]
                logger.info("Testing PDF download: %s", test_act["title"])
                text = self._extract_pdf_text(test_act["pdf_url"], test_act.get("act_number", "test"))
                if text and len(text) > 50:
                    logger.info("PDF extraction OK (%d chars)", len(text))
                else:
                    logger.warning("PDF extraction returned insufficient text")
            return True
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


def main():
    parser = argparse.ArgumentParser(description="BS/BahamasLegislation bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10-15 sample records")
    args = parser.parse_args()

    fetcher = BahamasLegislationFetcher()
    source_dir = Path(__file__).parent

    if args.command == "test":
        success = fetcher.test()
        sys.exit(0 if success else 1)

    if args.command == "bootstrap":
        sample_dir = source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in fetcher.fetch_all(sample=args.sample):
            fname = re.sub(r'[^\w\-]', '_', record["_id"])[:100] + ".json"
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info("[%d] Saved: %s", count, record["title"])

        logger.info("Bootstrap complete: %d records saved to %s", count, sample_dir)

    elif args.command == "update":
        for record in fetcher.fetch_updates():
            logger.info("Updated: %s", record["title"])


if __name__ == "__main__":
    main()
