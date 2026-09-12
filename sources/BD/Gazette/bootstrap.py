#!/usr/bin/env python3
"""
BD/Gazette -- Bangladesh Government Press Gazette Archive

Fetches official gazettes (weekly and extraordinary) from the
Bangladesh Government Press (BG Press) archive.

Strategy:
  - Paginate the gazette listing at /document/gazettes/140
  - For each entry, determine type (extraordinary vs weekly)
  - Fetch the intermediate page to extract PDF URLs
  - Download PDF and extract full text via common/pdf_extract

Endpoints:
  - Listing:       /document/gazettes/140/publication_date/{page}
  - Extraordinary:  /document/get_extraordinary/{id}
  - Weekly:         /document/get_gazette_part/{volume}/{date}
  - PDFs:           http://www.dpp.gov.bd/upload_file/gazettes/{id}_{hash}.pdf

Data:
  - ~50,713 gazette records
  - Language: Bengali (bn)
  - Rate limit: 1 request/second

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
import html as html_mod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BD.Gazette")

BASE_URL = "https://www.dpp.gov.bd/bgpress/index.php"
PDF_BASE = "http://www.dpp.gov.bd"


class BangladeshGazetteScraper(BaseScraper):
    """
    Scraper for BD/Gazette -- Bangladesh Government Press Gazette Archive.
    Country: BD
    URL: https://www.dpp.gov.bd/bgpress/index.php/document/gazettes/140

    Data types: legislation
    Auth: none (Open public access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "bn,en;q=0.5",
            },
            timeout=60,
        )

    def _fetch_listing_page(self, page: int = 1) -> List[Dict[str, Any]]:
        """
        Fetch one page of the gazette listing archive.
        Returns list of dicts with: type, link_id, title, date_str, link_url.
        """
        if page == 1:
            url = "/document/gazettes/140"
        else:
            url = f"/document/gazettes/140/publication_date/{page}"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()
            content = resp.text
        except Exception as e:
            logger.error(f"Failed to fetch listing page {page}: {e}")
            return []

        entries = []
        seen_ids = set()

        # Parse extraordinary gazette links: /document/get_extraordinary/{id}
        for m in re.finditer(
            r'<a[^>]*href="[^"]*?/document/get_extraordinary/(\d+)"[^>]*>(.*?)</a>',
            content,
            re.DOTALL | re.IGNORECASE,
        ):
            gaz_id = m.group(1)
            if gaz_id in seen_ids:
                continue
            seen_ids.add(gaz_id)
            link_text = re.sub(r'<[^>]+>', '', m.group(2)).strip()
            entries.append({
                "type": "extraordinary",
                "link_id": gaz_id,
                "title": link_text,
                "link_url": f"/document/get_extraordinary/{gaz_id}",
            })

        # Parse weekly gazette links: /document/get_gazette_part/{vol}/{date}
        for m in re.finditer(
            r'<a[^>]*href="[^"]*?/document/get_gazette_part/(\d+)/(\d{4}-\d{2}-\d{2})"[^>]*>(.*?)</a>',
            content,
            re.DOTALL | re.IGNORECASE,
        ):
            vol = m.group(1)
            date_str = m.group(2)
            lid = f"vol{vol}_{date_str}"
            if lid in seen_ids:
                continue
            seen_ids.add(lid)
            link_text = re.sub(r'<[^>]+>', '', m.group(3)).strip()
            entries.append({
                "type": "weekly",
                "link_id": lid,
                "title": link_text,
                "date_str": date_str,
                "link_url": f"/document/get_gazette_part/{vol}/{date_str}",
            })

        logger.info(f"Page {page}: found {len(entries)} gazette entries")
        return entries

    def _extract_pdf_urls_from_page(self, page_url: str) -> List[Dict[str, str]]:
        """
        Fetch an intermediate gazette page and extract all PDF links.
        Returns list of dicts with: pdf_url, title, ministry, pages.
        """
        try:
            self.rate_limiter.wait()
            resp = self.client.get(page_url)
            resp.raise_for_status()
            content = resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch gazette page {page_url}: {e}")
            return []

        pdfs = []
        # Look for PDF links: href="http://www.dpp.gov.bd/upload_file/gazettes/XXXXX_YYYYY.pdf"
        for m in re.finditer(
            r'<a[^>]*href="((?:https?://www\.dpp\.gov\.bd)?/upload_file/gazettes/[^"]+\.pdf)"[^>]*>(.*?)</a>',
            content,
            re.DOTALL | re.IGNORECASE,
        ):
            pdf_url = m.group(1)
            if not pdf_url.startswith("http"):
                pdf_url = PDF_BASE + pdf_url
            link_text = re.sub(r'<[^>]+>', '', m.group(2)).strip()
            link_text = html_mod.unescape(link_text)
            pdfs.append({
                "pdf_url": pdf_url,
                "title": link_text,
            })

        # Also try to extract ministry info from the table
        # Look for table rows with ministry information
        ministry_matches = re.findall(
            r'<td[^>]*>(.*?)</td>',
            content,
            re.DOTALL | re.IGNORECASE,
        )

        # Extract page title/heading for context
        title_match = re.search(
            r'<h[1-4][^>]*>(.*?)</h[1-4]>',
            content,
            re.DOTALL | re.IGNORECASE,
        )
        page_title = ""
        if title_match:
            page_title = re.sub(r'<[^>]+>', '', title_match.group(1)).strip()

        # Add page context to PDFs
        for pdf in pdfs:
            if not pdf["title"]:
                pdf["title"] = page_title

        if not pdfs:
            # Try broader PDF link pattern
            for m in re.finditer(
                r'href="([^"]*\.pdf)"',
                content,
                re.IGNORECASE,
            ):
                pdf_url = m.group(1)
                if "upload_file" in pdf_url or "gazettes" in pdf_url:
                    if not pdf_url.startswith("http"):
                        pdf_url = PDF_BASE + pdf_url
                    pdfs.append({
                        "pdf_url": pdf_url,
                        "title": page_title or "Bangladesh Gazette",
                    })

        logger.info(f"Found {len(pdfs)} PDF(s) from {page_url}")
        return pdfs

    def _parse_date_from_text(self, text: str) -> str:
        """Try to parse date from gazette entry text. Returns ISO date or empty."""
        # Pattern: "Month DD, YYYY" or "DD-Mon-YYYY"
        months = {
            "january": "01", "february": "02", "march": "03", "april": "04",
            "may": "05", "june": "06", "july": "07", "august": "08",
            "september": "09", "october": "10", "november": "11", "december": "12",
            "jan": "01", "feb": "02", "mar": "03", "apr": "04",
            "jun": "06", "jul": "07", "aug": "08", "sep": "09",
            "oct": "10", "nov": "11", "dec": "12",
        }
        # "April 30, 2026" or "30-Apr-2026"
        m = re.search(r'(\w+)\s+(\d{1,2}),?\s+(\d{4})', text)
        if m:
            month_name = m.group(1).lower()
            day = m.group(2).zfill(2)
            year = m.group(3)
            month = months.get(month_name, "")
            if month:
                return f"{year}-{month}-{day}"

        m = re.search(r'(\d{1,2})-(\w+)-(\d{4})', text)
        if m:
            day = m.group(1).zfill(2)
            month_name = m.group(2).lower()
            year = m.group(3)
            month = months.get(month_name, "")
            if month:
                return f"{year}-{month}-{day}"

        return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all gazette documents with full text from PDFs."""
        page = 1
        max_pages = 3381
        empty_count = 0

        while page <= max_pages:
            entries = self._fetch_listing_page(page)
            if not entries:
                empty_count += 1
                if empty_count >= 3:
                    logger.info("3 consecutive empty pages, stopping pagination")
                    break
                page += 1
                continue
            empty_count = 0

            for entry in entries:
                pdfs = self._extract_pdf_urls_from_page(entry["link_url"])
                if not pdfs:
                    logger.warning(f"No PDFs found for {entry['link_id']}")
                    continue

                for i, pdf_info in enumerate(pdfs):
                    doc_id = f"{entry['link_id']}"
                    if len(pdfs) > 1:
                        doc_id = f"{entry['link_id']}_part{i+1}"

                    text = extract_pdf_markdown(
                        source="BD/Gazette",
                        source_id=doc_id,
                        pdf_url=pdf_info["pdf_url"],
                        table="legislation",
                    )
                    if not text:
                        logger.warning(f"No text from PDF: {pdf_info['pdf_url']}")
                        continue

                    date_str = entry.get("date_str", "")
                    if not date_str:
                        date_str = self._parse_date_from_text(entry.get("title", ""))

                    yield {
                        "gazette_id": doc_id,
                        "title": pdf_info.get("title") or entry.get("title", ""),
                        "text": text,
                        "date": date_str,
                        "gazette_type": entry["type"],
                        "pdf_url": pdf_info["pdf_url"],
                        "source_page": entry["link_url"],
                    }

            page += 1

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield gazette documents published after the given date."""
        since_str = since.strftime("%Y-%m-%d")
        page = 1

        while page <= 50:  # check recent pages only
            entries = self._fetch_listing_page(page)
            if not entries:
                break

            found_old = False
            for entry in entries:
                date_str = entry.get("date_str", "")
                if not date_str:
                    date_str = self._parse_date_from_text(entry.get("title", ""))

                if date_str and date_str < since_str:
                    found_old = True
                    continue

                pdfs = self._extract_pdf_urls_from_page(entry["link_url"])
                for i, pdf_info in enumerate(pdfs):
                    doc_id = f"{entry['link_id']}"
                    if len(pdfs) > 1:
                        doc_id = f"{entry['link_id']}_part{i+1}"

                    text = extract_pdf_markdown(
                        source="BD/Gazette",
                        source_id=doc_id,
                        pdf_url=pdf_info["pdf_url"],
                        table="legislation",
                    )
                    if not text:
                        continue

                    yield {
                        "gazette_id": doc_id,
                        "title": pdf_info.get("title") or entry.get("title", ""),
                        "text": text,
                        "date": date_str,
                        "gazette_type": entry["type"],
                        "pdf_url": pdf_info["pdf_url"],
                        "source_page": entry["link_url"],
                    }

            if found_old:
                break
            page += 1

    def normalize(self, raw: dict) -> dict:
        """Transform raw gazette data into standard schema."""
        gazette_id = raw.get("gazette_id", "")
        return {
            "id": f"BD/Gazette/{gazette_id}",
            "_id": f"BD/Gazette/{gazette_id}",
            "_source": "BD/Gazette",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": f"{BASE_URL}{raw.get('source_page', '')}",
            "gazette_type": raw.get("gazette_type", ""),
            "pdf_url": raw.get("pdf_url", ""),
            "language": "bn",
            "authority": "Bangladesh Government Press",
            "country": "BD",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing BD/Gazette endpoints...")

        print("\n1. Testing listing page...")
        try:
            entries = self._fetch_listing_page(1)
            print(f"   Found {len(entries)} entries on page 1")
            if entries:
                e = entries[0]
                print(f"   First: [{e['type']}] {e.get('title', '')[:60]}")
                print(f"   Link: {e['link_url']}")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        print("\n2. Testing gazette detail page...")
        try:
            if entries:
                pdfs = self._extract_pdf_urls_from_page(entries[0]["link_url"])
                print(f"   Found {len(pdfs)} PDF(s)")
                if pdfs:
                    print(f"   PDF URL: {pdfs[0]['pdf_url']}")
                    print(f"   Title: {pdfs[0]['title'][:60]}")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        print("\n3. Testing PDF text extraction...")
        try:
            if pdfs:
                text = extract_pdf_markdown(
                    source="BD/Gazette",
                    source_id="test",
                    pdf_url=pdfs[0]["pdf_url"],
                    table="legislation",
                )
                if text:
                    print(f"   Text length: {len(text)} chars")
                    print(f"   Sample: {text[:150]}...")
                else:
                    print("   WARNING: No text extracted from PDF")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = BangladeshGazetteScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
