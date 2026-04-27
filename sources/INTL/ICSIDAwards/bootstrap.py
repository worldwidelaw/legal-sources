#!/usr/bin/env python3
"""
INTL/ICSIDAwards -- ICSID Arbitration Awards (World Bank)

Fetches investment arbitration awards and decisions from ICSID.

Strategy:
  - JSON API at /api/all/cases for case metadata (1,130 cases)
  - Scrape case detail pages for PDF document links
  - Download PDFs from icsidfiles.worldbank.org (no Cloudflare)
  - Extract text via PyMuPDF
  - ~1,400 documents total

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import gc
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

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.ICSIDAwards")

CASES_API = "https://icsid.worldbank.org/api/all/cases"
CASE_DETAIL_URL = "https://icsid.worldbank.org/cases/case-database/case-detail?CaseNo={caseno}"
MONTH_MAP = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}


class ICSIDAwardsScraper(BaseScraper):
    """
    Scraper for INTL/ICSIDAwards -- ICSID Arbitration Awards.
    Country: INTL
    URL: https://icsid.worldbank.org/cases

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    def _get_all_cases(self) -> list[dict]:
        """Fetch all cases from the JSON API."""
        r = self.session.get(CASES_API, headers={"Accept": "application/json"}, timeout=60)
        r.raise_for_status()
        data = r.json()
        return data.get("data", {}).get("GetAllCasesResult", [])

    def _scrape_case_documents(self, case_no: str) -> list[dict]:
        """Scrape a case detail page for document links."""
        url = CASE_DETAIL_URL.format(caseno=case_no)
        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch case detail for {case_no}: {e}")
            return []

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(r.text, "html.parser")
        del r  # free response memory

        documents = []
        # Find all PDF links from icsidfiles
        for p_tag in soup.find_all("p"):
            links = p_tag.find_all("a", href=re.compile(r"icsidfiles.*\.pdf"))
            if not links:
                continue

            # Get the decision title from span or the link text
            span = p_tag.find("span")
            title_text = ""
            date_text = ""

            if span:
                title_text = span.get_text(strip=True)
            elif links:
                title_text = links[0].get_text(strip=True)

            # Extract date from title text (e.g., "Award (November 13, 2000)")
            date_match = re.search(
                r"\((\w+)\s+(\d{1,2}),?\s+(\d{4})\)", title_text
            )
            if date_match:
                month = date_match.group(1).lower()
                day = date_match.group(2).zfill(2)
                year = date_match.group(3)
                if month in MONTH_MAP:
                    date_text = f"{year}-{MONTH_MAP[month]}-{day}"
                # Clean date from title
                title_text = re.sub(r"\s*\(\w+\s+\d{1,2},?\s+\d{4}\)\s*$", "", title_text).strip()

            # Prefer English PDF
            en_link = None
            for link in links:
                href = link.get("href", "")
                link_text = link.get_text(strip=True).lower()
                if "_en" in href.lower() or "english" in link_text:
                    en_link = href
                    break

            # If no English found, take the first non-language-specific or first available
            if not en_link:
                for link in links:
                    href = link.get("href", "")
                    if not re.search(r"_(Sp|Fr)\.", href):
                        en_link = href
                        break
                if not en_link:
                    en_link = links[0].get("href", "")

            if en_link and title_text:
                documents.append({
                    "title": title_text,
                    "date": date_text or None,
                    "pdf_url": en_link,
                })

        return documents

    def _download_and_extract_pdf(self, pdf_url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor.

        Caps extracted text at 500K chars to prevent OOM on large awards.
        """
        text = extract_pdf_markdown(
            source="INTL/ICSIDAwards",
            source_id="",
            pdf_url=pdf_url,
            table="case_law",
        ) or ""
        # Cap text to prevent OOM on 4GB VPS — ICSID awards can be 500+ pages
        if text and len(text) > 500_000:
            text = text[:500_000]
        return text

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw record into standard schema."""
        text = raw.get("text", "").strip()
        if not text:
            return None

        title = raw.get("title", "").strip()
        case_title = raw.get("case_title", "")
        case_no = raw.get("case_no", "")

        full_title = title
        if case_title and title:
            full_title = f"{title} — {case_title}"

        pdf_url = raw.get("pdf_url", "")
        doc_id = pdf_url.split("/")[-1].replace(".pdf", "") if pdf_url else ""

        return {
            "_id": f"ICSID-{case_no}-{doc_id}" if case_no else f"ICSID-{hash(full_title)}",
            "_source": "INTL/ICSIDAwards",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": full_title,
            "text": text,
            "date": raw.get("date"),
            "url": f"https://icsid.worldbank.org/cases/case-database/case-detail?CaseNo={case_no}",
            "case_number": case_no,
            "case_title": case_title,
            "claimant": raw.get("claimant", ""),
            "respondent": raw.get("respondent", ""),
            "case_type": raw.get("case_type", ""),
            "pdf_url": pdf_url,
            "court": "International Centre for Settlement of Investment Disputes",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all ICSID documents."""
        cases = self._get_all_cases()
        logger.info(f"Total cases: {len(cases)}")

        # Only process concluded cases (they have published documents)
        concluded = [c for c in cases if c.get("status") == "Concluded"]
        logger.info(f"Concluded cases: {len(concluded)}")

        total_docs = 0
        for i, case in enumerate(concluded):
            case_no = case.get("caseno", "")
            case_title = case.get("casetitle", "")

            logger.info(f"[{i+1}/{len(concluded)}] Scraping: {case_no} - {case_title[:60]}")

            documents = self._scrape_case_documents(case_no)
            if not documents:
                continue

            for doc in documents:
                pdf_url = doc["pdf_url"]
                logger.info(f"  Downloading: {doc['title'][:60]}")

                text = self._download_and_extract_pdf(pdf_url)
                if text:
                    yield {
                        "title": doc["title"],
                        "date": doc.get("date"),
                        "text": text,
                        "pdf_url": pdf_url,
                        "case_no": case_no,
                        "case_title": case_title,
                        "claimant": case.get("claimant", ""),
                        "respondent": case.get("respondent", ""),
                        "case_type": case.get("casetype", ""),
                    }
                    total_docs += 1
                    del text  # free large string immediately
                else:
                    logger.warning(f"  No text extracted from: {doc['title'][:60]}")

                # Force GC every 10 docs to prevent OOM on 4GB VPS
                if total_docs % 10 == 0:
                    gc.collect()

                time.sleep(1)

            time.sleep(0.5)

        logger.info(f"Total documents fetched: {total_docs}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch documents from recently concluded cases."""
        since_str = since.strftime("%Y-%m-%d") if isinstance(since, datetime) else str(since)
        logger.info(f"Fetching updates since {since_str}")

        cases = self._get_all_cases()
        for case in cases:
            concluded = case.get("Date_Concluded", "")
            if concluded and concluded >= since_str:
                case_no = case.get("caseno", "")
                docs = self._scrape_case_documents(case_no)
                for doc in docs:
                    text = self._download_and_extract_pdf(doc["pdf_url"])
                    if text:
                        yield {
                            "title": doc["title"],
                            "date": doc.get("date"),
                            "text": text,
                            "pdf_url": doc["pdf_url"],
                            "case_no": case_no,
                            "case_title": case.get("casetitle", ""),
                            "claimant": case.get("claimant", ""),
                            "respondent": case.get("respondent", ""),
                            "case_type": case.get("casetype", ""),
                        }
                    time.sleep(1)


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/ICSIDAwards data fetcher")
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

    scraper = ICSIDAwardsScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            cases = scraper._get_all_cases()
            concluded = [c for c in cases if c.get("status") == "Concluded"]
            logger.info(f"OK: {len(cases)} total cases, {len(concluded)} concluded")

            # Test one case with docs
            test_case = "ARB/97/7"
            docs = scraper._scrape_case_documents(test_case)
            logger.info(f"Case {test_case}: {len(docs)} documents")

            if docs:
                text = scraper._download_and_extract_pdf(docs[0]["pdf_url"])
                if text:
                    logger.info(f"PDF text: {len(text)} chars")
                    logger.info(f"Preview: {text[:200]}")
                else:
                    logger.warning("PDF text extraction failed")

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
