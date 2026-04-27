#!/usr/bin/env python3
"""
IE/TaxAppealsCommission -- Irish Tax Appeals Commission Determinations

Fetches anonymised tax appeal determinations from taxappeals.ie.
These are published under Section 949AO TCA 1997 and cover income tax,
corporation tax, CGT, VAT, stamp duty, customs, excise, PAYE, VRT, etc.

Strategy:
  - Paginate through the determinations listing (127+ pages, 10 per page)
  - Extract reference numbers and tax types from each entry
  - Construct PDF URLs from the pattern /_fileupload/Determinations/{year}/{ref}.pdf
  - Download PDFs and extract full text
  - ~1,270 determinations from 2016 to present

Usage:
  python bootstrap.py bootstrap          # Full fetch
  python bootstrap.py bootstrap --sample # Fetch 15 samples
  python bootstrap.py test               # Connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IE.TaxAppealsCommission")

BASE_URL = "https://www.taxappeals.ie"
LISTING_URL = "/en/determinations/"
DELAY = 2.0


def parse_reference(text: str) -> Optional[Tuple[str, str, int]]:
    """
    Parse a determination reference like '29TACD2026 - Income Tax'.
    Returns (reference, tax_type, year) or None.
    """
    match = re.match(r'(\d+TACD(\d{4}))\s*[-–]\s*(.+)', text.strip())
    if match:
        ref = match.group(1)
        year = int(match.group(2))
        tax_type = match.group(3).strip()
        return ref, tax_type, year
    return None


def pdf_url_from_ref(ref: str, year: int) -> str:
    """Construct PDF URL from reference and year."""
    return f"{BASE_URL}/_fileupload/Determinations/{year}/{ref}.pdf"


class TaxAppealsCommission:
    SOURCE_ID = "IE/TaxAppealsCommission"

    def __init__(self):
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            },
            respect_robots=False,
        )

    def list_determinations(self, max_pages: int = 200) -> List[Dict[str, Any]]:
        """Paginate through the listing and extract all determination entries."""
        entries = []
        page = 1

        while page <= max_pages:
            url = LISTING_URL if page == 1 else f"{LISTING_URL}{page}"
            try:
                resp = self.http.get(url)
                time.sleep(1.0)
                if not resp or resp.status_code != 200:
                    logger.warning("Failed to fetch page %d (status %s)",
                                   page, resp.status_code if resp else "None")
                    break
            except Exception as e:
                logger.warning("Error fetching page %d: %s", page, e)
                break

            html = resp.text

            # Extract determination links - pattern: href="/en/determinations/SLUG">REF - TYPE</a>
            links = re.findall(
                r'href="(/en/determinations/\d+tacd\d{4}[^"]*)"[^>]*>\s*(.+?)\s*</a>',
                html, re.IGNORECASE
            )

            if not links:
                logger.info("No more entries found on page %d, stopping", page)
                break

            for slug_url, link_text in links:
                # Clean HTML entities and tags
                clean_text = re.sub(r'<[^>]+>', '', link_text).strip()
                parsed = parse_reference(clean_text)
                if parsed:
                    ref, tax_type, year = parsed
                    entries.append({
                        "reference": ref,
                        "tax_type": tax_type,
                        "year": year,
                        "slug_url": slug_url,
                        "pdf_url": pdf_url_from_ref(ref, year),
                    })

            # Check if there's a next page
            if f'href="{LISTING_URL}{page + 1}"' not in html and \
               f"href=\"/en/determinations/{page + 1}\"" not in html:
                # Also check for » or next link
                if '»' not in html and 'next' not in html.lower():
                    logger.info("No next page link found after page %d", page)
                    break

            page += 1

        logger.info("Listed %d determinations across %d pages", len(entries), page - 1)
        return entries

    def fetch_pdf_text(self, pdf_url: str, ref: str) -> Optional[str]:
        """Download a determination PDF and extract text."""
        try:
            resp = self.http.get(pdf_url)
            time.sleep(DELAY)
            if not resp or resp.status_code != 200:
                # Try fallback: fetch the individual page to find the actual PDF link
                logger.debug("Direct PDF URL failed for %s, trying page", ref)
                return self._fetch_via_page(ref, pdf_url)
        except Exception as e:
            logger.warning("Error downloading PDF %s: %s", ref, e)
            return None

        text = extract_pdf_markdown(
            source=self.SOURCE_ID,
            source_id=ref,
            pdf_bytes=resp.content,
            table="case_law",
        )
        return text if text and len(text) >= 50 else None

    def _fetch_via_page(self, ref: str, original_url: str) -> Optional[str]:
        """Fallback: fetch the individual determination page to find PDF link."""
        # Construct the page URL from the slug
        slug = ref.lower()
        # Try fetching the individual page
        try:
            # We don't know the exact slug, try the reference page
            page_url = f"/en/determinations/{slug}"
            resp = self.http.get(page_url)
            time.sleep(1.0)
            if resp and resp.status_code == 200:
                # Find PDF link in the page
                pdf_match = re.search(r'href="([^"]+\.pdf)"', resp.text)
                if pdf_match:
                    pdf_href = pdf_match.group(1)
                    pdf_full = pdf_href if pdf_href.startswith("http") else BASE_URL + pdf_href
                    resp2 = self.http.get(pdf_full)
                    time.sleep(DELAY)
                    if resp2 and resp2.status_code == 200:
                        text = extract_pdf_markdown(
                            source=self.SOURCE_ID,
                            source_id=ref,
                            pdf_bytes=resp2.content,
                            table="case_law",
                        )
                        return text if text and len(text) >= 50 else None
        except Exception as e:
            logger.debug("Fallback page fetch failed for %s: %s", ref, e)
        return None

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all determination documents with full text."""
        entries = self.list_determinations()
        if not entries:
            logger.error("No determination entries found")
            return

        sample_limit = 15 if sample else len(entries)
        total_yielded = 0
        failed = 0

        for entry in entries:
            if total_yielded >= sample_limit:
                break

            ref = entry["reference"]
            text = self.fetch_pdf_text(entry["pdf_url"], ref)
            if not text:
                failed += 1
                logger.warning("No text for %s (failed: %d)", ref, failed)
                continue

            record = {
                "_id": f"tac-{ref}",
                "_source": self.SOURCE_ID,
                "_type": "case_law",
                "_fetched_at": datetime.now(timezone.utc).isoformat(),
                "title": f"{ref} - {entry['tax_type']}",
                "text": text,
                "date": None,
                "url": f"{BASE_URL}{entry['slug_url']}",
                "language": "en",
                "reference": ref,
                "tax_type": entry["tax_type"],
                "year": entry["year"],
            }

            yield record
            total_yielded += 1

            if total_yielded % 10 == 0:
                logger.info("  Progress: %d/%d documents (failed: %d)",
                            total_yielded, sample_limit, failed)

        logger.info("Fetch complete: %d documents yielded, %d failed",
                    total_yielded, failed)

    def _process_entry(self, entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Download and process a single determination entry. Thread-safe."""
        ref = entry["reference"]
        text = self.fetch_pdf_text(entry["pdf_url"], ref)
        if not text:
            return None
        return {
            "_id": f"tac-{ref}",
            "_source": self.SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"{ref} - {entry['tax_type']}",
            "text": text,
            "date": None,
            "url": f"{BASE_URL}{entry['slug_url']}",
            "language": "en",
            "reference": ref,
            "tax_type": entry["tax_type"],
            "year": entry["year"],
        }

    def fetch_all_fast(self, max_workers: int = 5) -> Generator[Dict[str, Any], None, None]:
        """Fetch all determinations using concurrent PDF downloads."""
        entries = self.list_determinations()
        if not entries:
            logger.error("No determination entries found")
            return

        total = len(entries)
        yielded = 0
        failed = 0

        logger.info("Fast mode: processing %d entries with %d workers", total, max_workers)

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(self._process_entry, e): e for e in entries}
            for future in as_completed(futures):
                entry = futures[future]
                try:
                    record = future.result()
                except Exception as exc:
                    failed += 1
                    logger.warning("Error processing %s: %s", entry["reference"], exc)
                    continue

                if record is None:
                    failed += 1
                    continue

                yield record
                yielded += 1
                if yielded % 50 == 0:
                    logger.info("  Progress: %d/%d done (%d failed)", yielded, total, failed)

        logger.info("Fast fetch complete: %d yielded, %d failed out of %d", yielded, failed, total)

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            entries = self.list_determinations(max_pages=1)
            if not entries:
                logger.error("Test failed: no entries found on first page")
                return False
            logger.info("Test: found %d entries on first page", len(entries))

            # Test one PDF download
            entry = entries[0]
            text = self.fetch_pdf_text(entry["pdf_url"], entry["reference"])
            if text and len(text) >= 50:
                logger.info("Test passed: extracted %d chars from %s",
                            len(text), entry["reference"])
                return True
            logger.error("Test failed: could not extract text from %s", entry["reference"])
            return False
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="IE/TaxAppealsCommission bootstrap")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--workers", type=int, default=5, help="Workers for bootstrap-fast")
    args = parser.parse_args()

    scraper = TaxAppealsCommission()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)

    if args.command in ("bootstrap", "bootstrap-fast"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        if args.command == "bootstrap-fast":
            gen = scraper.fetch_all_fast(max_workers=args.workers)
        else:
            gen = scraper.fetch_all(sample=args.sample)

        count = 0
        for record in gen:
            safe_name = re.sub(r'[^\w\-.]', '_', record['_id'])[:100]
            out_file = sample_dir / f"{safe_name}.json"
            out_file.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            count += 1
            text_len = len(record.get("text", ""))
            logger.info("  [%d] %s | text=%d chars",
                        count, record["title"][:60], text_len)

        logger.info("Bootstrap complete: %d records saved to sample/", count)
        sys.exit(0 if count >= 10 else 1)

    if args.command == "update":
        count = sum(1 for _ in scraper.fetch_all(sample=False))
        logger.info("Update complete: %d documents", count)


if __name__ == "__main__":
    main()
