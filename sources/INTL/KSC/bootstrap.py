#!/usr/bin/env python3
"""
INTL/KSC -- Kosovo Specialist Chambers

Fetches decisions, orders, judgments, and transcripts from the KSC
public court records repository at repository.scp-ks.org.

Strategy:
  - Paginate through the HTML search results (10 per page, ~8200 docs)
  - Parse each listing to extract metadata and detail page link
  - Fetch detail page to get the PDF download URL
  - Download PDF and extract text via common/pdf_extract

Data Coverage:
  - ~8200+ public English filings, transcripts, and exhibits
  - Cases: KSC-BC-2020-06 (Thaçi et al.), KSC-BC-2023-12, etc.
  - War crimes / crimes against humanity in Kosovo (1998-2000)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

import requests

from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.KSC")

REPO_BASE = "https://repository.scp-ks.org"
SEARCH_URL = REPO_BASE + "/"
DETAIL_URL = REPO_BASE + "/details.php"
MAX_PDF_BYTES = 80 * 1024 * 1024  # 80MB limit


class KSCScraper(BaseScraper):
    """Scraper for Kosovo Specialist Chambers public court records."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en",
        })

    def _search_page(self, page: int = 1) -> str:
        """Fetch a single page of search results."""
        params = {
            "icc_filters[text_search]": "",
            "icc_filters[case_number]": "_all",
            "icc_filters[language_short]": "eng",
            "icc_filters[record_type_short]": "_all",
            "icc_filters[sort_order]": "_sort_date_newest",
            "lang": "eng",
            "page": page,
        }
        resp = self.session.get(SEARCH_URL, params=params, timeout=30)
        resp.raise_for_status()
        return resp.text

    def _parse_search_results(self, html: str) -> list[dict]:
        """Parse search result HTML to extract document metadata."""
        results = []
        # Match each teaser-list-item
        items = re.findall(
            r"<li class='teaser-list-item'>(.*?)</li>",
            html, re.DOTALL
        )
        for item in items:
            doc = {}
            # Title and detail link
            title_match = re.search(
                r"<a href='([^']*details\.php[^']*)'>(.*?)</a>",
                item, re.DOTALL
            )
            if title_match:
                doc["detail_url"] = title_match.group(1)
                doc["title"] = unescape(re.sub(r"<[^>]+>", "", title_match.group(2))).strip()
                # Extract doc_id and doc_type from detail URL
                id_match = re.search(r"doc_id=([^&]+)", doc["detail_url"])
                type_match = re.search(r"doc_type=([^&]+)", doc["detail_url"])
                if id_match:
                    doc["doc_id"] = id_match.group(1)
                if type_match:
                    doc["doc_type"] = type_match.group(1)

            # Case number
            case_match = re.search(
                r"case-number-info[^>]*><span>(.*?)</span>",
                item
            )
            if case_match:
                doc["case_number"] = case_match.group(1).strip()

            # Date
            date_match = re.search(
                r"date-info[^>]*><span>(.*?)</span>",
                item
            )
            if date_match:
                doc["date_raw"] = date_match.group(1).strip()

            # Type
            type_info_match = re.search(
                r"type-info[^>]*><span>(.*?)</span>",
                item
            )
            if type_info_match:
                doc["record_type"] = type_info_match.group(1).strip()

            # Filing ID
            filing_match = re.search(
                r"filing-id-info[^>]*><span>(.*?)</span>",
                item
            )
            if filing_match:
                doc["filing_id"] = filing_match.group(1).strip()

            # Language
            lang_match = re.search(
                r"language-info[^>]*><span>(.*?)</span>",
                item
            )
            if lang_match:
                doc["language"] = lang_match.group(1).strip()

            if doc.get("doc_id"):
                results.append(doc)

        return results

    def _get_total_results(self, html: str) -> int:
        """Extract total result count from search page."""
        m = re.search(r"results-total-number[^>]*>(\d+)", html)
        return int(m.group(1)) if m else 0

    def _get_pdf_url(self, doc_id: str, doc_type: str) -> Optional[str]:
        """Fetch the detail page and extract the PDF download URL."""
        params = {"doc_id": doc_id, "doc_type": doc_type, "lang": "eng"}
        try:
            resp = self.session.get(DETAIL_URL, params=params, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch detail page for {doc_id}: {e}")
            return None

        # Look for PDF download link
        m = re.search(
            r'<a\s+href="(/LW/Published/[^"]+\.pdf)"',
            resp.text
        )
        if m:
            return REPO_BASE + m.group(1)

        # Alternative: look for any download URL
        m = re.search(
            r'class="download-url"[^>]*href="([^"]+)"',
            resp.text
        )
        if not m:
            m = re.search(
                r'href="([^"]*)"[^>]*class="download-url"',
                resp.text
            )
        if m:
            url = m.group(1)
            if url.startswith("/"):
                url = REPO_BASE + url
            return url

        return None

    def _parse_date(self, date_raw: str) -> Optional[str]:
        """Parse DD/MM/YYYY to ISO 8601."""
        if not date_raw:
            return None
        try:
            dt = datetime.strptime(date_raw, "%d/%m/%Y")
            return dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return None

    def _iterate_documents(self, max_pages: int = None) -> Generator[dict, None, None]:
        """Iterate through all search result pages."""
        page = 1
        total = None
        yielded = 0

        while True:
            if max_pages and page > max_pages:
                break

            logger.info(f"Fetching search page {page}...")
            try:
                html = self._search_page(page)
            except Exception as e:
                logger.error(f"Search page {page} failed: {e}")
                break

            if total is None:
                total = self._get_total_results(html)
                logger.info(f"Total results: {total}")

            docs = self._parse_search_results(html)
            if not docs:
                break

            for doc in docs:
                yield doc
                yielded += 1

            if total and yielded >= total:
                break

            page += 1
            time.sleep(1)

        logger.info(f"Listed {yielded} documents across {page} pages")

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all public court records with full text."""
        for doc in self._iterate_documents():
            doc_id = doc.get("doc_id", "")
            doc_type = doc.get("doc_type", "stl_filing")
            title = doc.get("title", "")

            logger.info(f"  Processing {doc.get('filing_id', doc_id)}: {title[:60]}...")
            time.sleep(1)

            # Get PDF URL from detail page
            pdf_url = self._get_pdf_url(doc_id, doc_type)
            if not pdf_url:
                logger.warning(f"  No PDF URL for {doc_id}")
                continue

            time.sleep(0.5)

            # Extract text from PDF
            text = extract_pdf_markdown(
                source="INTL/KSC",
                source_id=doc.get("filing_id", doc_id),
                pdf_url=pdf_url,
                table="case_law",
            )
            if not text or len(text.strip()) < 50:
                logger.warning(f"  Insufficient text for {doc_id} ({len(text or '')} chars)")
                continue

            doc["text"] = text
            doc["pdf_url"] = pdf_url
            yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch documents filed since a given date (newest first)."""
        for doc in self._iterate_documents():
            date_str = self._parse_date(doc.get("date_raw", ""))
            if date_str:
                try:
                    doc_date = datetime.strptime(date_str, "%Y-%m-%d")
                    if doc_date.replace(tzinfo=timezone.utc) < since:
                        logger.info(f"Reached documents older than {since}, stopping")
                        return
                except (ValueError, TypeError):
                    pass

            doc_id = doc.get("doc_id", "")
            doc_type = doc.get("doc_type", "stl_filing")

            time.sleep(1)
            pdf_url = self._get_pdf_url(doc_id, doc_type)
            if not pdf_url:
                continue

            time.sleep(0.5)
            text = extract_pdf_markdown(
                source="INTL/KSC",
                source_id=doc.get("filing_id", doc_id),
                pdf_url=pdf_url,
                table="case_law",
            )
            if not text or len(text.strip()) < 50:
                continue

            doc["text"] = text
            doc["pdf_url"] = pdf_url
            yield doc

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw KSC record into standard schema."""
        text = raw.get("text", "").strip()
        if not text or len(text) < 50:
            return None

        doc_id = raw.get("doc_id", "")
        filing_id = raw.get("filing_id", "")
        case_number = raw.get("case_number", "")
        title = raw.get("title", "") or filing_id
        date_str = self._parse_date(raw.get("date_raw", ""))

        # Build a unique ID from case number + filing ID
        unique_part = filing_id or doc_id
        _id = f"INTL_KSC_{case_number}_{unique_part}".replace(" ", "_")

        return {
            "_id": _id,
            "_source": "INTL/KSC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title.strip(),
            "text": text,
            "date": date_str,
            "url": raw.get("pdf_url", ""),
            "case_number": case_number,
            "filing_id": filing_id,
            "record_type": raw.get("record_type", ""),
            "language": raw.get("language", "English (eng)"),
            "court": "Kosovo Specialist Chambers (KSC)",
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="KSC bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = KSCScraper()

    if args.command == "test":
        print("Testing KSC repository...")
        try:
            html = scraper._search_page(1)
            total = scraper._get_total_results(html)
            docs = scraper._parse_search_results(html)
            print(f"OK: {total} total English documents")
            if docs:
                d = docs[0]
                print(f"  First: {d.get('filing_id', '?')} - {d.get('title', '')[:60]}")
                print(f"  Case: {d.get('case_number', '?')}, Date: {d.get('date_raw', '?')}")
                # Test PDF extraction
                pdf_url = scraper._get_pdf_url(d["doc_id"], d.get("doc_type", "stl_filing"))
                if pdf_url:
                    print(f"  PDF URL: {pdf_url[:80]}...")
                    text = extract_pdf_markdown(
                        source="INTL/KSC",
                        source_id=d.get("filing_id", d["doc_id"]),
                        pdf_url=pdf_url,
                        table="case_law",
                    )
                    if text:
                        print(f"  PDF text extraction: OK ({len(text)} chars)")
                    else:
                        print("  PDF text extraction: FAILED")
                else:
                    print("  No PDF URL found on detail page")
        except Exception as e:
            print(f"FAIL: {e}")
            import traceback
            traceback.print_exc()
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample)
        print(json.dumps(stats, indent=2))
    elif args.command == "update":
        stats = scraper.update()
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
