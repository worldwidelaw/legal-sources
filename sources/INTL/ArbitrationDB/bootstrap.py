#!/usr/bin/env python3
"""
INTL/ArbitrationDB -- International Arbitration Database (arbitration.org)

Fetches international arbitration awards from The International Arbitration Society.

Strategy:
  - Scrape paginated list at /award/recent?page=N (943+ pages, 9 per page)
  - Scrape individual award pages at /award/{ID} for metadata + PDF links
  - Download PDFs from /sites/default/files/awards/arb{N}.pdf
  - Extract full text via common/pdf_extract
  - ~8,500 arbitral awards total

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import json
import re
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.ArbitrationDB")

BASE_URL = "https://arbitration.org"
LIST_URL = f"{BASE_URL}/award/recent"

MONTH_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
    "january": "01", "february": "02", "march": "03", "april": "04",
    "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


class ArbitrationDBScraper(BaseScraper):
    """
    Scraper for INTL/ArbitrationDB -- International Arbitration Database.
    Country: INTL
    URL: https://arbitration.org/

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _scrape_list_page(self, page: int) -> list[dict]:
        """Scrape one page of the award listing. Returns list of {url, title}."""
        url = f"{LIST_URL}?page={page}"
        try:
            r = self.session.get(url, timeout=60)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch list page {page}: {e}")
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        awards = []

        for link in soup.select("a[href^='/award/']"):
            href = link.get("href", "")
            # Skip pagination and non-award links
            if href in ("/award/recent",) or "?page=" in href:
                continue
            # Only match /award/{number}
            m = re.match(r"^/award/(\d+)$", href)
            if not m:
                continue
            title = link.get_text(strip=True)
            if not title:
                continue
            award_id = m.group(1)
            # Avoid duplicates on the same page
            if not any(a["award_id"] == award_id for a in awards):
                awards.append({
                    "award_id": award_id,
                    "title": title,
                    "page_url": f"{BASE_URL}{href}",
                })

        return awards

    def _scrape_award_page(self, award_url: str) -> dict:
        """Scrape an individual award page for metadata and PDF links."""
        try:
            r = self.session.get(award_url, timeout=60)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch award page {award_url}: {e}")
            return {}

        soup = BeautifulSoup(r.text, "html.parser")
        meta = {}

        # Extract metadata from field items
        for field_div in soup.select(".field"):
            label_el = field_div.select_one(".field-label")
            if not label_el:
                continue
            label = label_el.get_text(strip=True).rstrip(":").lower()
            items_el = field_div.select_one(".field-items") or field_div.select_one(".field-item")
            if not items_el:
                continue
            value = items_el.get_text(strip=True)

            if "case" in label and ("id" in label or "number" in label or "no" in label):
                meta["case_id"] = value
            elif "country" in label:
                meta["country"] = value
            elif "sector" in label:
                meta["sector"] = value
            elif "treaty" in label:
                meta["treaty"] = value
            elif "rule" in label:
                meta["rules"] = value
            elif "section" in label or "tag" in label:
                meta["section"] = value
            elif "source" in label:
                meta["data_source"] = value

        # Extract PDF links and dates — only awards/decisions, not expert reports etc.
        SKIP_KEYWORDS = {"expert report", "declaration of", "witness statement",
                         "letter by", "presentation", "defense on appeal",
                         "reply to", "motion to", "response to"}
        pdf_docs = []
        for link in soup.select("a[href$='.pdf']"):
            href = link.get("href", "")
            if not href:
                continue
            if not href.startswith("http"):
                href = f"{BASE_URL}{href}"

            # Get the label
            label = link.get_text(strip=True)
            label_lower = label.lower()

            # Skip non-substantive documents
            if any(skip in label_lower for skip in SKIP_KEYWORDS):
                continue

            # Walk up to the div.content container to find date
            doc_date = None
            el = link
            for _ in range(8):
                el = el.parent
                if el is None:
                    break
                classes = el.get("class", [])
                if "content" in classes:
                    # Look for Drupal date field with machine-readable content attr
                    date_span = el.select_one("span.date-display-single[content]")
                    if date_span:
                        # content="2017-10-13T00:00:00-04:00"
                        doc_date = date_span["content"][:10]
                    else:
                        # Fallback: parse from text
                        doc_date = self._parse_date_from_text(el.get_text(strip=True))
                    break

            pdf_docs.append({
                "url": href,
                "label": label,
                "date": doc_date,
            })

        meta["pdf_docs"] = pdf_docs
        return meta

    def _parse_date_from_text(self, text: str) -> Optional[str]:
        """Try to extract an ISO date from text near a document link."""
        # Pattern: DD Mon YYYY or Mon DD, YYYY or YYYY-MM-DD
        # Try YYYY-MM-DD first
        m = re.search(r"(\d{4}-\d{2}-\d{2})", text)
        if m:
            return m.group(1)

        # Try "Month DD, YYYY" or "DD Month YYYY"
        m = re.search(r"(\w+)\s+(\d{1,2}),?\s+(\d{4})", text)
        if m:
            month_str = m.group(1).lower()
            month = MONTH_MAP.get(month_str)
            if month:
                day = m.group(2).zfill(2)
                return f"{m.group(3)}-{month}-{day}"

        m = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})", text)
        if m:
            month_str = m.group(2).lower()
            month = MONTH_MAP.get(month_str)
            if month:
                day = m.group(1).zfill(2)
                return f"{m.group(3)}-{month}-{day}"

        return None

    def _get_last_page(self) -> int:
        """Determine the last page number from pagination."""
        try:
            r = self.session.get(LIST_URL, timeout=60)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch first list page: {e}")
            return 0

        soup = BeautifulSoup(r.text, "html.parser")
        # Look for "last" pagination link
        last_link = soup.select_one("a[title='Go to last page'], li.pager-last a")
        if last_link:
            href = last_link.get("href", "")
            m = re.search(r"page=(\d+)", href)
            if m:
                return int(m.group(1))

        # Fallback: look for highest page number in pagination
        max_page = 0
        for link in soup.select("a[href*='page=']"):
            href = link.get("href", "")
            m = re.search(r"page=(\d+)", href)
            if m:
                max_page = max(max_page, int(m.group(1)))
        return max_page

    def _download_pdf_text(self, pdf_url: str, award_id: str) -> Optional[str]:
        """Download PDF and extract text."""
        return extract_pdf_markdown(
            source="INTL/ArbitrationDB",
            source_id=f"ARB-{award_id}",
            pdf_url=pdf_url,
            table="case_law",
        ) or ""

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw award record into standard schema."""
        text = raw.get("text", "").strip()
        if not text:
            return None

        award_id = raw.get("award_id", "")
        title = raw.get("title", "").strip()
        date = raw.get("date")
        page_url = raw.get("page_url", f"{BASE_URL}/award/{award_id}")

        return {
            "_id": f"ARB-{award_id}",
            "_source": "INTL/ArbitrationDB",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": page_url,
            "case_id": raw.get("case_id", ""),
            "country": raw.get("country", ""),
            "sector": raw.get("sector", ""),
            "treaty": raw.get("treaty", ""),
            "rules": raw.get("rules", ""),
            "document_label": raw.get("document_label", ""),
            "court": "International Arbitration (arbitration.org)",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all awards with full text from PDFs."""
        last_page = self._get_last_page()
        logger.info(f"Total pages to scrape: {last_page + 1} (pages 0-{last_page})")

        yielded = 0
        for page in range(0, last_page + 1):
            logger.info(f"[Page {page}/{last_page}] Scraping list page...")
            awards = self._scrape_list_page(page)
            if not awards:
                logger.info(f"  No awards found on page {page}")
                time.sleep(1)
                continue

            logger.info(f"  Found {len(awards)} awards on page {page}")

            for award in awards:
                award_id = award["award_id"]
                title_short = award["title"][:60]
                logger.info(f"  Processing award {award_id}: {title_short}")

                # Scrape award detail page
                time.sleep(1)
                meta = self._scrape_award_page(award["page_url"])
                if not meta:
                    continue

                pdf_docs = meta.pop("pdf_docs", [])
                if not pdf_docs:
                    logger.info(f"    No PDFs found for award {award_id}")
                    continue

                # Download and extract text from PDFs (max 5 per award)
                texts = []
                labels = []
                best_date = None

                for doc in pdf_docs[:5]:
                    logger.info(f"    Downloading PDF: {doc['label'][:50]}")
                    text = self._download_pdf_text(doc["url"], award_id)
                    if text:
                        texts.append(f"--- {doc['label']} ---\n\n{text}")
                        labels.append(doc["label"])
                        if doc.get("date") and not best_date:
                            best_date = doc["date"]
                    time.sleep(1.5)

                if texts:
                    combined = "\n\n".join(texts)
                    record = {
                        **award,
                        **meta,
                        "text": combined,
                        "date": best_date,
                        "document_label": "; ".join(labels),
                    }
                    yield record
                    yielded += 1
                    logger.info(f"    Yielded award {award_id} with {len(combined)} chars")
                else:
                    logger.info(f"    No text extracted for award {award_id}")

            time.sleep(1)

        logger.info(f"Finished: {yielded} awards yielded with full text")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent awards (scrape first few pages only)."""
        since_str = since.strftime("%Y-%m-%d") if isinstance(since, datetime) else str(since)
        logger.info(f"Fetching awards since {since_str}")

        # Recent awards are on the first pages
        for page in range(0, 20):
            awards = self._scrape_list_page(page)
            if not awards:
                break

            found_old = False
            for award in awards:
                time.sleep(1)
                meta = self._scrape_award_page(award["page_url"])
                if not meta:
                    continue

                pdf_docs = meta.pop("pdf_docs", [])
                if not pdf_docs:
                    continue

                # Check date of first PDF
                first_date = pdf_docs[0].get("date") if pdf_docs else None
                if first_date and first_date < since_str:
                    found_old = True
                    break

                texts = []
                labels = []
                best_date = None
                for doc in pdf_docs[:5]:
                    text = self._download_pdf_text(doc["url"], award["award_id"])
                    if text:
                        texts.append(f"--- {doc['label']} ---\n\n{text}")
                        labels.append(doc["label"])
                        if doc.get("date") and not best_date:
                            best_date = doc["date"]
                    time.sleep(1.5)

                if texts:
                    record = {
                        **award,
                        **meta,
                        "text": "\n\n".join(texts),
                        "date": best_date,
                        "document_label": "; ".join(labels),
                    }
                    yield record

            if found_old:
                break
            time.sleep(1)


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/ArbitrationDB data fetcher")
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

    scraper = ArbitrationDBScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            awards = scraper._scrape_list_page(0)
            logger.info(f"OK: {len(awards)} awards on first page")
            if awards:
                a = awards[0]
                logger.info(f"First: {a['title'][:80]}")
                meta = scraper._scrape_award_page(a["page_url"])
                pdfs = meta.get("pdf_docs", [])
                logger.info(f"PDFs: {len(pdfs)}")
                if pdfs:
                    text = scraper._download_pdf_text(pdfs[0]["url"], a["award_id"])
                    if text:
                        logger.info(f"PDF text extracted: {len(text)} chars")
                        logger.info(f"Preview: {text[:200]}")
                    else:
                        logger.warning("No text from first PDF")
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
    main()
