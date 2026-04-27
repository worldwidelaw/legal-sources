#!/usr/bin/env python3
"""
INTL/PCA -- Permanent Court of Arbitration Case Repository

Fetches arbitration awards and decisions from the PCA case repository.

Strategy:
  - JSON API at pcacases.com/web/api/cases/search for case listing + metadata
  - Scrape individual case pages at pca-cpa.org/en/cases/{id}/ for document PDFs
  - Download award/decision PDFs from pcacases.com/web/sendAttach/{doc_id}
  - Extract full text from PDFs using PyMuPDF
  - ~288 cases covering inter-state, investor-state, and contract-based arbitrations

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
import fitz  # PyMuPDF
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.PCA")

API_URL = "https://pcacases.com/web/api/cases/search?1"
CASE_PAGE_URL = "https://pca-cpa.org/en/cases/{id}/"
PDF_BASE = "https://pcacases.com/web/sendAttach/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

API_HEADERS = {
    **HEADERS,
    "Accept": "application/json",
    "Referer": "https://pca-cpa.org/en/cases/",
    "Origin": "https://pca-cpa.org",
}


class PCAScraper(BaseScraper):
    """
    Scraper for INTL/PCA -- Permanent Court of Arbitration.
    Country: INTL
    URL: https://pca-cpa.org/en/cases/

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _fetch_case_list(self) -> list[dict]:
        """Fetch all cases from the PCA JSON API."""
        logger.info(f"Fetching case list from API: {API_URL}")
        r = self.session.get(API_URL, headers=API_HEADERS, timeout=60)
        r.raise_for_status()
        cases = r.json()
        logger.info(f"API returned {len(cases)} cases")
        return cases

    def _scrape_case_documents(self, case_id: int) -> list[dict]:
        """Scrape a case page for award/decision PDF download links.

        Returns list of dicts with keys: label, url, date, section.
        Only returns documents from the 'Award or other decision' section.
        """
        url = CASE_PAGE_URL.format(id=case_id)
        try:
            r = self.session.get(url, timeout=60)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch case page {url}: {e}")
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        documents = []

        foldouts = soup.select(".fold-out")
        for fo in foldouts:
            h6 = fo.select_one("h6")
            section_name = h6.get_text(strip=True).lower() if h6 else ""

            # Only get awards/decisions — these contain the substantive text
            if "award" not in section_name and "decision" not in section_name:
                continue

            downloads = fo.select(".download")
            for dl in downloads:
                label_el = dl.select_one(".download-label")
                link_el = dl.select_one('a[href*="sendAttach"]')
                links_div = dl.select_one(".download-links")

                if not link_el:
                    continue

                label = label_el.get_text(strip=True) if label_el else ""
                href = link_el.get("href", "")
                link_info = links_div.get_text(strip=True) if links_div else ""

                # Parse date
                date_match = re.search(r"(\d{2})\s+(\w+)\s+(\d{4})", link_info)
                doc_date = None
                if date_match:
                    months = {
                        "january": "01", "february": "02", "march": "03",
                        "april": "04", "may": "05", "june": "06",
                        "july": "07", "august": "08", "september": "09",
                        "october": "10", "november": "11", "december": "12",
                    }
                    day = date_match.group(1)
                    month = months.get(date_match.group(2).lower(), "01")
                    year = date_match.group(3)
                    doc_date = f"{year}-{month}-{day}"

                # Check language — prefer English
                lang = "English" if "english" in link_info.lower() else ""

                documents.append({
                    "label": label,
                    "url": href,
                    "date": doc_date,
                    "language": lang,
                    "section": section_name,
                })

        return documents

    def _download_and_extract_pdf(self, pdf_url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="INTL/PCA",
            source_id="",
            pdf_url=pdf_url,
            table="case_law",
        ) or ""

    def _clean_html(self, html: str) -> str:
        """Strip HTML tags and clean whitespace."""
        if not html:
            return ""
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        # Collapse whitespace
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw case record into standard schema."""
        text = raw.get("text", "").strip()
        if not text:
            return None

        case_name = raw.get("case_name", "").strip()
        case_number = raw.get("case_number", "")

        # Parse date from date_of_filling
        date = None
        date_raw = raw.get("date_of_filling") or raw.get("award_date")
        if date_raw:
            try:
                date_str = str(date_raw)
                if "T" in date_str:
                    dt = datetime.fromisoformat(date_str)
                    date = dt.strftime("%Y-%m-%d")
                elif re.match(r"^\d{2}-\d{2}-\d{4}$", date_str):
                    # DD-MM-YYYY format from API
                    parts = date_str.split("-")
                    date = f"{parts[2]}-{parts[1]}-{parts[0]}"
                elif re.match(r"^\d{4}-\d{2}-\d{2}", date_str):
                    date = date_str[:10]
                else:
                    date = date_str[:10]
            except (ValueError, TypeError):
                date = None

        case_id = raw.get("id", "")
        url = f"https://pca-cpa.org/en/cases/{case_id}/" if case_id else "https://pca-cpa.org/en/cases/"

        claimants = raw.get("claimants_names", [])
        respondents = raw.get("respondents_names", [])
        description = self._clean_html(raw.get("case_description", ""))

        return {
            "_id": f"PCA-{case_number}" if case_number else f"PCA-{case_id}",
            "_source": "INTL/PCA",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": case_name,
            "text": text,
            "date": date,
            "url": url,
            "case_number": case_number,
            "case_type": raw.get("case_type", ""),
            "case_status": raw.get("case_status", ""),
            "claimants": claimants,
            "respondents": respondents,
            "description": description,
            "document_label": raw.get("document_label", ""),
            "court": "Permanent Court of Arbitration",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all PCA cases with award/decision full text."""
        cases = self._fetch_case_list()
        total = len(cases)
        logger.info(f"Total cases to process: {total}")

        yielded = 0
        for i, case in enumerate(cases):
            case_id = case.get("id")
            case_name = case.get("case_name", "")[:80]
            logger.info(f"[{i+1}/{total}] Processing case {case_id}: {case_name}")

            # Scrape case page for award/decision documents
            documents = self._scrape_case_documents(case_id)
            if not documents:
                logger.info(f"  No award/decision documents found")
                time.sleep(1)
                continue

            logger.info(f"  Found {len(documents)} award/decision documents")

            # Download and extract text from each document
            texts = []
            doc_labels = []
            best_date = None

            for doc in documents:
                logger.info(f"  Downloading: {doc['label'][:60]}")
                text = self._download_and_extract_pdf(doc["url"])
                if text:
                    texts.append(f"--- {doc['label']} ---\n\n{text}")
                    doc_labels.append(doc["label"])
                    if doc.get("date") and not best_date:
                        best_date = doc["date"]
                time.sleep(1.5)

            if texts:
                combined_text = "\n\n".join(texts)
                record = {**case, "text": combined_text}
                if best_date:
                    record["award_date"] = best_date
                record["document_label"] = "; ".join(doc_labels)
                yield record
                yielded += 1
                logger.info(f"  Yielded case with {len(combined_text)} chars of text")
            else:
                logger.info(f"  No text extracted from any document")

            time.sleep(1)

        logger.info(f"Finished: {yielded}/{total} cases yielded with full text")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch cases filed after given date."""
        since_str = since.strftime("%Y-%m-%d") if isinstance(since, datetime) else str(since)
        logger.info(f"Fetching cases since {since_str}")

        cases = self._fetch_case_list()
        for case in cases:
            date_raw = case.get("date_of_filling", "")
            if date_raw and str(date_raw)[:10] >= since_str:
                case_id = case.get("id")
                documents = self._scrape_case_documents(case_id)
                texts = []
                doc_labels = []
                best_date = None

                for doc in documents:
                    text = self._download_and_extract_pdf(doc["url"])
                    if text:
                        texts.append(f"--- {doc['label']} ---\n\n{text}")
                        doc_labels.append(doc["label"])
                        if doc.get("date") and not best_date:
                            best_date = doc["date"]
                    time.sleep(1.5)

                if texts:
                    record = {**case, "text": "\n\n".join(texts)}
                    if best_date:
                        record["award_date"] = best_date
                    record["document_label"] = "; ".join(doc_labels)
                    yield record
                time.sleep(1)


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/PCA data fetcher")
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

    scraper = PCAScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            cases = scraper._fetch_case_list()
            logger.info(f"OK: {len(cases)} cases from API")
            if cases:
                c = cases[0]
                logger.info(f"First: {c['case_name'][:80]}")
                docs = scraper._scrape_case_documents(c["id"])
                logger.info(f"Documents: {len(docs)}")
                if docs:
                    text = scraper._download_and_extract_pdf(docs[0]["url"])
                    if text:
                        logger.info(f"PDF text extracted: {len(text)} chars")
                        logger.info(f"Preview: {text[:200]}")
                    else:
                        logger.warning("No text from first document PDF")
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
