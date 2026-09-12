#!/usr/bin/env python3
"""
LT/KT -- Lithuanian Competition Council Decisions

Fetches decisions (nutarimai) from the Lithuanian Competition Council
(Konkurencijos taryba) at https://kt.gov.lt/lt/dokumentai.

Strategy:
  1. Scrape paginated HTML listing of decisions
  2. Extract metadata (date, decision number, title, category, PDF URL)
  3. Download each PDF and extract full text via pdfplumber
  4. Normalize into standard schema

Data source:
  - Listing: https://kt.gov.lt/lt/dokumentai?page=N (173+ pages, 12 per page)
  - PDFs: https://kt.gov.lt/uploads/docs/docs/YYYY-MM/<hash>.pdf
  - Open data: https://kt.gov.lt/lt/atviri-duomenys

Coverage:
  - Decisions from 1999 to present
  - Antitrust, mergers, abuse of dominance, state aid, advertising, retail

License: Public Domain (Government decisions)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent pages)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

import requests

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.LT.KT")

BASE_URL = "https://kt.gov.lt"
LIST_URL = "https://kt.gov.lt/lt/dokumentai"
SOURCE_ID = "LT/KT"


class KTScraper(BaseScraper):
    """
    Scraper for LT/KT -- Lithuanian Competition Council.
    Country: LT
    URL: https://kt.gov.lt/lt/dokumentai

    Data types: case_law
    Auth: none
    """

    SOURCE_ID = SOURCE_ID

    def __init__(self):
        source_dir = str(Path(__file__).resolve().parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url=BASE_URL,
            verify=False,
        )

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            resp = self.http.get(LIST_URL, timeout=15)
            if resp.status_code == 200 and "docs_item" in resp.text:
                logger.info("Connectivity OK — listing page accessible")
                return True
            logger.error(f"Unexpected response: {resp.status_code}")
            return False
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False

    def _parse_listing_page(self, html: str) -> list:
        """Parse a single listing page, returning list of decision dicts."""
        results = []
        items = re.findall(
            r'<div class="docs_item">(.*?)<div class="clear">',
            html,
            re.DOTALL,
        )
        for item in items:
            try:
                record = self._parse_item(item)
                if record:
                    results.append(record)
            except Exception as e:
                logger.warning(f"Failed to parse item: {e}")
        return results

    def _parse_item(self, item_html: str) -> Optional[Dict[str, Any]]:
        """Parse a single docs_item block."""
        # Decision date
        date_match = re.search(
            r'<li class="float-start">(\d{4}\s+\d{2}\s+\d{2})</li>', item_html
        )
        if not date_match:
            return None
        raw_date = date_match.group(1).replace(" ", "-")

        # Decision number
        nr_match = re.search(r"Nutarimo Nr\.:\s*([^<]+)", item_html)
        decision_nr = nr_match.group(1).strip() if nr_match else None

        # Publication date
        pub_match = re.search(r"Paskelbimo data:\s*([^<]+)", item_html)
        pub_date = pub_match.group(1).strip().replace(" ", "-") if pub_match else None

        # PDF link and title
        pdf_match = re.search(
            r'class="docs_link">\s*<a[^>]+href="([^"]+\.pdf)"[^>]*><p>(.*?)</p></a>',
            item_html,
            re.DOTALL,
        )
        if not pdf_match:
            # Try alternate pattern — some items link directly
            pdf_match = re.search(
                r'href="(/uploads/docs/docs/[^"]+\.pdf)"[^>]*>.*?<', item_html, re.DOTALL
            )
            if not pdf_match:
                return None

        pdf_path = pdf_match.group(1)
        title = ""
        if pdf_match.lastindex and pdf_match.lastindex >= 2:
            title = re.sub(r"<[^>]+>", "", pdf_match.group(2)).strip()

        # Category tags
        cats = re.findall(
            r'href="[^"]*dokumentai/([^"?]+)"[^>]*>([^<]+)', item_html
        )
        categories = [c[1].strip() for c in cats]

        # Status tags
        status = re.findall(
            r'href="[^"]*sarasas/status\.\d+"[^>]*>([^<]+)', item_html
        )

        pdf_url = pdf_path if pdf_path.startswith("http") else BASE_URL + pdf_path

        return {
            "date": raw_date,
            "decision_number": decision_nr,
            "publication_date": pub_date,
            "title": title,
            "pdf_url": pdf_url,
            "categories": categories,
            "status_tags": [s.strip() for s in status],
        }

    def _get_max_page(self, html: str) -> int:
        """Extract the maximum page number from pagination."""
        pages = re.findall(r'dokumentai\?page=(\d+)', html)
        if pages:
            return max(int(p) for p in pages)
        return 1

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download a PDF and extract text via pdfplumber."""
        if pdfplumber is None:
            logger.error("pdfplumber not installed — cannot extract PDF text")
            return None
        try:
            resp = self.http.get(pdf_url, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"PDF download failed: {resp.status_code} for {pdf_url}")
                return None

            with tempfile.NamedTemporaryFile(suffix=".pdf") as tmp:
                tmp.write(resp.content)
                tmp.flush()
                with pdfplumber.open(tmp.name) as pdf:
                    pages_text = []
                    for page in pdf.pages:
                        t = page.extract_text()
                        if t:
                            pages_text.append(t)
                        try:
                            page.flush_cache(); page.get_textmap.cache_clear()
                        except Exception:
                            pass
                    return "\n\n".join(pages_text) if pages_text else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw decision record into standard schema."""
        date_str = raw.get("date", "")
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            date_iso = dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            date_iso = date_str

        decision_nr = raw.get("decision_number") or "unknown"
        doc_id = f"LT-KT-{decision_nr}".replace(" ", "").replace("/", "-")

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date_iso,
            "decision_number": decision_nr,
            "publication_date": raw.get("publication_date", ""),
            "categories": raw.get("categories", []),
            "url": raw.get("pdf_url", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all decisions from the KT listing pages."""
        logger.info("Fetching first page to determine total pages...")
        resp = self.http.get(LIST_URL, timeout=15)
        if resp.status_code != 200:
            logger.error(f"Failed to fetch listing: {resp.status_code}")
            return

        max_page = self._get_max_page(resp.text)
        logger.info(f"Total pages: {max_page}")

        if sample:
            max_page = min(max_page, 2)  # 2 pages = ~24 items, aim for 12+

        count = 0
        for page_num in range(1, max_page + 1):
            url = f"{LIST_URL}?page={page_num}" if page_num > 1 else LIST_URL
            logger.info(f"Fetching page {page_num}/{max_page}...")

            try:
                resp = self.http.get(url, timeout=15)
                if resp.status_code != 200:
                    logger.warning(f"Page {page_num} returned {resp.status_code}")
                    continue
            except Exception as e:
                logger.warning(f"Failed to fetch page {page_num}: {e}")
                continue

            items = self._parse_listing_page(resp.text)
            if not items:
                logger.warning(f"No items found on page {page_num}")
                continue

            for item in items:
                # Download and extract PDF text
                text = self._extract_pdf_text(item["pdf_url"])
                if not text:
                    logger.warning(
                        f"No text extracted for {item.get('decision_number', 'unknown')}"
                    )
                    continue

                item["text"] = text
                record = self.normalize(item)
                yield record
                count += 1

                if sample and count >= 12:
                    logger.info(f"Sample complete: {count} records")
                    return

        logger.info(f"Fetch complete: {count} records")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent decisions (first few pages only)."""
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        logger.info(f"Fetching updates since {since}...")
        try:
            since_date = datetime.strptime(since, "%Y-%m-%d")
        except ValueError:
            since_date = datetime(2020, 1, 1)

        for page_num in range(1, 20):  # Check first 20 pages for recent updates
            url = f"{LIST_URL}?page={page_num}" if page_num > 1 else LIST_URL

            try:
                resp = self.http.get(url, timeout=15)
                if resp.status_code != 200:
                    break
            except Exception:
                break

            items = self._parse_listing_page(resp.text)
            if not items:
                break

            page_has_old = False
            for item in items:
                try:
                    item_date = datetime.strptime(item["date"], "%Y-%m-%d")
                    if item_date < since_date:
                        page_has_old = True
                        continue
                except ValueError:
                    pass

                text = self._extract_pdf_text(item["pdf_url"])
                if not text:
                    continue
                item["text"] = text
                yield self.normalize(item)

            if page_has_old:
                break


def main():
    import argparse

    parser = argparse.ArgumentParser(description="LT/KT Bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--since", type=str, help="Date for incremental update")
    args = parser.parse_args()

    scraper = KTScraper()

    if args.command == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            if args.sample:
                out_file = sample_dir / f"{count:04d}.json"
                with open(out_file, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                logger.info(
                    f"[{count+1}] {record['decision_number']} — "
                    f"{len(record.get('text',''))} chars"
                )
            else:
                print(json.dumps(record, ensure_ascii=False))
            count += 1
        logger.info(f"Done: {count} records")

    elif args.command == "update":
        since = args.since or "2024-01-01"
        count = 0
        for record in scraper.fetch_updates(since):
            print(json.dumps(record, ensure_ascii=False))
            count += 1
        logger.info(f"Update done: {count} records since {since}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
