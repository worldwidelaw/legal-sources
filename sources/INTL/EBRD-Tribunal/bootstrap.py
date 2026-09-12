#!/usr/bin/env python3
"""
INTL/EBRD-Tribunal -- EBRD Administrative Tribunal Decisions

Fetches decisions from the EBRD Administrative Tribunal website.

Strategy:
  - Parse tribunal listing page for decision cards
  - Download PDFs from ebrd.com/content/dam/ebrd_dxp/assets/pdfs/administrative-tribunal/
  - Extract full text from PDFs using pdfplumber
  - ~61 decisions since 2003

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin, unquote

import requests
import pdfplumber
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.EBRD-Tribunal")

LISTING_URL = (
    "https://www.ebrd.com/home/who-we-are/our-organisation/"
    "ebrd-governance-leadership/corporate-governance/administrative-tribunal.html"
)
BASE_URL = "https://www.ebrd.com"


def extract_case_ref(title: str, pdf_path: str) -> str:
    """Extract a normalized case reference from the title or PDF path.

    Examples:
      'EBRDAT 2023-AT-01' -> 'EBRDAT-2023-AT-01'
      'EBRD 2025/AT/01'   -> 'EBRDAT-2025-AT-01'
      'EBRDAT 2006/AT/04' -> 'EBRDAT-2006-AT-04'
      'EBRDAT 2003-01'    -> 'EBRDAT-2003-01'
    """
    # Try modern format: EBRDAT YYYY-AT-NN or EBRD YYYY/AT/NN
    m = re.search(r"EBRD(?:AT)?\s*(\d{4})[/-]AT[/-](\d+)", title, re.IGNORECASE)
    if m:
        return f"EBRDAT-{m.group(1)}-AT-{m.group(2).zfill(2)}"

    # Try old format: EBRDAT 2003-01
    m = re.search(r"EBRDAT\s*(\d{4})-(\d+)", title, re.IGNORECASE)
    if m:
        return f"EBRDAT-{m.group(1)}-{m.group(2).zfill(2)}"

    # Fallback: derive from PDF filename
    fn = unquote(pdf_path.split("/")[-1]).replace(".pdf", "")
    fn = re.sub(r"[^a-zA-Z0-9]", "-", fn).strip("-")
    return fn


def extract_year(title: str, pdf_path: str) -> Optional[str]:
    """Extract the decision year from title or PDF path."""
    m = re.search(r"(\d{4})", title)
    if m:
        year = int(m.group(1))
        if 2000 <= year <= 2030:
            return str(year)
    m = re.search(r"/(\d{4})/", pdf_path)
    if m:
        return m.group(1)
    return None


class EBRDTribunalScraper(BaseScraper):
    """
    Scraper for INTL/EBRD-Tribunal -- EBRD Administrative Tribunal.
    Country: INTL
    URL: https://www.ebrd.com/.../administrative-tribunal.html

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })

    def _parse_listing(self) -> list[dict]:
        """Parse the tribunal listing page for decision cards."""
        resp = self.session.get(LISTING_URL, timeout=30)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.find_all("div", class_="main-download-module__main-block")

        decisions = []
        seen_refs = set()

        for card in cards:
            link = card.find("a", href=True)
            if not link:
                continue

            href = link["href"]
            if not href.endswith(".pdf"):
                continue

            # Skip annual reports
            if "at-reports" in href or "annual-report" in href:
                continue

            text_parts = card.get_text("|", strip=True).split("|")
            title = text_parts[0].strip() if text_parts else ""

            # Skip if it looks like a plain year (annual report card)
            if re.match(r"^\d{4}(-\d+)?$", title):
                continue

            pdf_url = urljoin(BASE_URL, href)
            case_ref = extract_case_ref(title, href)
            year = extract_year(title, href)

            # Deduplicate by case_ref
            if case_ref in seen_refs:
                # Keep the entry but make it unique by appending a suffix
                suffix_match = re.search(
                    r"(jurisdiction|merits|costs|liability|remedy|"
                    r"compensation|preliminary|final|interlocutory|"
                    r"supplemental|joinder|interpretation|withdrawn)",
                    title.lower(),
                )
                if suffix_match:
                    case_ref = f"{case_ref}-{suffix_match.group(1)}"
                else:
                    case_ref = f"{case_ref}-{len(seen_refs)}"

            seen_refs.add(case_ref)

            decisions.append({
                "title": title,
                "case_ref": case_ref,
                "year": year,
                "pdf_url": pdf_url,
            })

        logger.info(f"Parsed {len(decisions)} decisions from listing page")
        return decisions

    def _download_and_extract_pdf(self, url: str) -> Optional[str]:
        """Download PDF and extract text using pdfplumber."""
        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()

            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                pages = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages.append(text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass

                full_text = "\n\n".join(pages)
                if len(full_text.strip()) < 100:
                    logger.warning(f"Very short text from {url}: {len(full_text)} chars")
                    return None
                return full_text

        except Exception as e:
            logger.error(f"PDF extraction failed for {url}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all tribunal decisions with full text."""
        decisions = self._parse_listing()

        for i, decision in enumerate(decisions):
            logger.info(
                f"Fetching {decision['case_ref']} ({i+1}/{len(decisions)})"
            )
            if i > 0:
                time.sleep(2)

            text = self._download_and_extract_pdf(decision["pdf_url"])
            if text:
                decision["text"] = text
                yield decision
            else:
                logger.warning(f"No text extracted for {decision['case_ref']}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch decisions added since a given date."""
        decisions = self._parse_listing()
        since_year = since.strftime("%Y")

        for decision in decisions:
            if decision["year"] and decision["year"] >= since_year:
                time.sleep(2)
                text = self._download_and_extract_pdf(decision["pdf_url"])
                if text:
                    decision["text"] = text
                    yield decision

    def normalize(self, raw: dict) -> dict:
        """Transform raw decision data into standard schema."""
        case_ref = raw["case_ref"]
        year = raw.get("year")
        date = f"{year}-01-01" if year else None

        return {
            "_id": case_ref,
            "_source": "INTL/EBRD-Tribunal",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", case_ref),
            "text": raw.get("text", ""),
            "date": date,
            "url": raw.get("pdf_url", ""),
            "case_ref": case_ref,
        }


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/EBRD-Tribunal data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = EBRDTribunalScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            decisions = scraper._parse_listing()
            logger.info(f"OK: {len(decisions)} decisions found")
            if decisions:
                d = decisions[0]
                logger.info(f"First: {d['case_ref']} - {d['title'][:80]}")
                text = scraper._download_and_extract_pdf(d["pdf_url"])
                if text:
                    logger.info(f"PDF text extracted: {len(text)} chars")
                    logger.info(f"Preview: {text[:200]}")
                else:
                    logger.error("Failed to extract PDF text")
                    sys.exit(1)
            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
