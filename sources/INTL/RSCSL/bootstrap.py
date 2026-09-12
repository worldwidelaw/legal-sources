#!/usr/bin/env python3
"""
INTL/RSCSL -- Residual Special Court for Sierra Leone

Fetches decisions from the RSCSL/SCSL document archive at docs.rscsl.org.

Strategy:
  - Query the Laravel DataTables JSON API for all decisions (server-side paging)
  - For each decision, fetch the document detail page to locate the S3 PDF URL
  - Download the PDF and extract text via common/pdf_extract

Data Coverage:
  - ~879 decisions from SCSL and RSCSL
  - Cases: CDF, RUF, AFRC, Charles Taylor, Contempt
  - International criminal law (war crimes, crimes against humanity)

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

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.RSCSL")

API_URL = "https://docs.rscsl.org/embed/decisions"
DETAIL_URL = "https://docs.rscsl.org/document/{doc_no}"
PAGE_SIZE = 50


class RSCSLScraper(BaseScraper):
    """Scraper for Residual Special Court for Sierra Leone decisions."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
            "Accept": "application/json, text/html",
            "Accept-Language": "en",
            "X-Requested-With": "XMLHttpRequest",
        })

    def _fetch_decisions_page(self, start: int = 0, length: int = PAGE_SIZE) -> dict:
        """Fetch a page of decisions from the DataTables API."""
        params = {
            "draw": 1,
            "start": start,
            "length": length,
            "order[0][column]": 3,
            "order[0][dir]": "desc",
            "columns[0][data]": "id",
            "columns[1][data]": "scsl_docno",
            "columns[2][data]": "courtCase",
            "columns[3][data]": "doc_date",
            "columns[4][data]": "documentType",
            "columns[5][data]": "title",
            "columns[6][data]": "noofpages",
        }
        resp = self.session.get(API_URL, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _extract_doc_no(self, scsl_docno_html: str) -> Optional[str]:
        """Extract the clean document number from the HTML link."""
        match = re.search(r'document/([^"]+)', scsl_docno_html)
        if match:
            return match.group(1)
        clean = re.sub(r'<[^>]+>', '', scsl_docno_html).strip()
        return clean if clean else None

    def _get_pdf_url(self, doc_no: str) -> Optional[str]:
        """Fetch the document detail page and extract the S3 PDF URL."""
        url = DETAIL_URL.format(doc_no=doc_no)
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning("Failed to fetch detail page for %s: %s", doc_no, e)
            return None

        match = re.search(r'src="(https://scsl\.s3[^"]+\.pdf)"', resp.text)
        if match:
            return match.group(1)

        match = re.search(r'href="(https://scsl\.s3[^"]+\.pdf)"', resp.text)
        if match:
            return match.group(1)

        logger.warning("No PDF URL found for %s", doc_no)
        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions with full text from PDFs."""
        first_page = self._fetch_decisions_page(start=0, length=1)
        total = first_page.get("recordsTotal", 0)
        logger.info("Total decisions available: %d", total)

        start = 0
        yielded = 0
        while start < total:
            data = self._fetch_decisions_page(start=start, length=PAGE_SIZE)
            records = data.get("data", [])
            if not records:
                break

            for record in records:
                doc_no = self._extract_doc_no(record.get("scsl_docno", ""))
                if not doc_no:
                    logger.warning("Skipping record with no doc_no: id=%s", record.get("id"))
                    continue

                time.sleep(1)
                pdf_url = self._get_pdf_url(doc_no)
                if not pdf_url:
                    continue

                time.sleep(0.5)
                text = extract_pdf_markdown(
                    source="INTL/RSCSL",
                    source_id=doc_no,
                    pdf_url=pdf_url,
                    table="case_law",
                )
                if not text or len(text.strip()) < 50:
                    logger.warning("No text extracted for %s, skipping", doc_no)
                    continue

                record["_pdf_url"] = pdf_url
                record["_text"] = text
                record["_doc_no"] = doc_no
                yield record
                yielded += 1

            start += PAGE_SIZE
            logger.info("Progress: %d/%d records processed, %d yielded", start, total, yielded)

        logger.info("Completed: %d documents with full text out of %d total", yielded, total)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Fetch decisions ordered by most recent first, stop at `since` date."""
        if not since:
            yield from self.fetch_all()
            return

        try:
            since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            yield from self.fetch_all()
            return

        first_page = self._fetch_decisions_page(start=0, length=1)
        total = first_page.get("recordsTotal", 0)

        start = 0
        while start < total:
            data = self._fetch_decisions_page(start=start, length=PAGE_SIZE)
            records = data.get("data", [])
            if not records:
                break

            for record in records:
                submitted = record.get("submitted_date")
                if submitted:
                    try:
                        rec_dt = datetime.fromisoformat(submitted)
                        if rec_dt.tzinfo is None:
                            rec_dt = rec_dt.replace(tzinfo=timezone.utc)
                        if rec_dt < since_dt:
                            return
                    except (ValueError, TypeError):
                        pass

                doc_no = self._extract_doc_no(record.get("scsl_docno", ""))
                if not doc_no:
                    continue

                time.sleep(1)
                pdf_url = self._get_pdf_url(doc_no)
                if not pdf_url:
                    continue

                time.sleep(0.5)
                text = extract_pdf_markdown(
                    source="INTL/RSCSL",
                    source_id=doc_no,
                    pdf_url=pdf_url,
                    table="case_law",
                )
                if not text or len(text.strip()) < 50:
                    continue

                record["_pdf_url"] = pdf_url
                record["_text"] = text
                record["_doc_no"] = doc_no
                yield record

            start += PAGE_SIZE

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw decision record into standard schema."""
        doc_no = raw.get("_doc_no", "")
        case_obj = raw.get("court_case", {})
        doc_type_obj = raw.get("document_type", {})

        # Parse date
        date_str = raw.get("submitted_date") or raw.get("doc_date", "")
        iso_date = None
        if date_str:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d %b %Y"):
                try:
                    iso_date = datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
                    break
                except (ValueError, AttributeError):
                    continue

        return {
            "_id": doc_no,
            "_source": "INTL/RSCSL",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", "").strip(),
            "text": raw.get("_text", ""),
            "date": iso_date,
            "url": f"https://docs.rscsl.org/document/{doc_no}",
            "pdf_url": raw.get("_pdf_url", ""),
            "scsl_docno": doc_no,
            "case_name": case_obj.get("case_name", raw.get("courtCase", "")),
            "case_no": case_obj.get("case_no", ""),
            "document_type": doc_type_obj.get("type", raw.get("documentType", "")),
            "pages": raw.get("pages", ""),
            "noofpages": raw.get("noofpages"),
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="RSCSL bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = RSCSLScraper()

    if args.command == "test":
        print("Testing RSCSL document archive...")
        try:
            data = scraper._fetch_decisions_page(start=0, length=2)
            total = data.get("recordsTotal", 0)
            records = data.get("data", [])
            print(f"OK: {total} total decisions")
            if records:
                r = records[0]
                doc_no = scraper._extract_doc_no(r.get("scsl_docno", ""))
                print(f"  First: {doc_no} - {r.get('title', '')[:60]}")
                print(f"  Case: {r.get('courtCase', '?')}, Date: {r.get('doc_date', '?')}")
                if doc_no:
                    pdf_url = scraper._get_pdf_url(doc_no)
                    if pdf_url:
                        print(f"  PDF: {pdf_url[:80]}...")
                        text = extract_pdf_markdown(
                            source="INTL/RSCSL",
                            source_id=doc_no,
                            pdf_url=pdf_url,
                            table="case_law",
                            force=True,
                        )
                        if text:
                            print(f"  Text extraction: OK ({len(text)} chars)")
                        else:
                            print("  Text extraction: FAILED")
                    else:
                        print("  No PDF URL found")
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
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
