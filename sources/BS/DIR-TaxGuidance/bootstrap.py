#!/usr/bin/env python3
"""
BS/DIR-TaxGuidance -- Bahamas Department of Inland Revenue Tax Guidance

Fetches tax guidance documents from the Bahamas DIR via WordPress REST API.

Strategy:
  - Use WP REST API media endpoint to enumerate all PDFs
  - Filter for substantive guidance (exclude forms, registration lists)
  - Download PDFs and extract full text

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Re-fetch all
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BS.DIR-TaxGuidance")

BASE_URL = "https://inlandrevenue.finance.gov.bs"
API_URL = f"{BASE_URL}/wp-json/wp/v2/media"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)

# Filename patterns to EXCLUDE (forms, lists, templates, not substantive guidance)
EXCLUDE_PATTERNS = [
    r"^VAT-\d+",           # VAT numbered forms (VAT-101, VAT-201, etc.)
    r"registrant.*list",   # Monthly taxpayer registration lists
    r"registration.*list",
    r"reg.*list",          # "Vat Reg List", "Tin Reg" etc.
    r"list.*reg",
    r"^form",
    r"template",
    r"cash-basis.*template",
    r"pc.?list",           # Practitioners/certified list
    r"bica.*list",         # BICA licensee listings
    r"licen[cs]ee",        # Licensee lists
    r"notification.?form", # DMTT notification forms (not guidance)
    r"tin.?reg",           # TIN registration lists
    r"vat.?reg",           # VAT registration lists
    r"monthly.*return",    # Monthly return forms
]


def _is_excluded(filename: str) -> bool:
    """Check if a filename should be excluded (forms, lists, etc.)."""
    lower = filename.lower()
    for pat in EXCLUDE_PATTERNS:
        if re.search(pat, lower):
            return True
    return False


def _categorize(title: str, filename: str) -> str:
    """Categorize a document based on its title/filename."""
    lower = (title + " " + filename).lower()
    if "rule" in lower and "vat" in lower:
        return "VAT Rule"
    if "guidance" in lower or "guide" in lower:
        return "VAT Guide"
    if "act" in lower or "amendment" in lower:
        return "Legislation"
    if "regulation" in lower:
        return "Regulation"
    if "ruling" in lower:
        return "Advanced Ruling"
    if "notice" in lower:
        return "Notice"
    if "business licence" in lower or "business license" in lower:
        return "Business Licence"
    if "property tax" in lower or "real property" in lower:
        return "Real Property Tax"
    if "dmtt" in lower or "minimum top-up" in lower:
        return "DMTT"
    return "VAT Guide"


class BSDIRScraper(BaseScraper):
    SOURCE_ID = "BS/DIR-TaxGuidance"

    def __init__(self):
        source_dir = str(Path(__file__).resolve().parent)
        super().__init__(source_dir)

    def _api_get(self, url: str, timeout: int = 30) -> Optional[Any]:
        """Fetch JSON from the WP REST API."""
        req = Request(url, headers={"User-Agent": USER_AGENT})
        try:
            resp = urlopen(req, timeout=timeout)
            return json.loads(resp.read().decode("utf-8"))
        except (HTTPError, URLError) as e:
            logger.warning(f"API request failed: {url} - {e}")
            return None

    def _list_pdfs(self) -> List[Dict[str, Any]]:
        """Enumerate all PDFs via WP REST API media endpoint."""
        all_pdfs = []
        page = 1
        while True:
            url = f"{API_URL}?per_page=100&mime_type=application/pdf&page={page}"
            data = self._api_get(url)
            if not data:
                break
            if len(data) == 0:
                break

            for item in data:
                source_url = item.get("source_url", "")
                title = item.get("title", {}).get("rendered", "")
                filename = source_url.split("/")[-1] if source_url else ""
                media_id = item.get("id", "")
                date = item.get("date", "")

                if not source_url or not filename.endswith(".pdf"):
                    continue

                if _is_excluded(filename):
                    continue

                all_pdfs.append({
                    "id": str(media_id),
                    "title": title or filename.replace(".pdf", "").replace("-", " "),
                    "source_url": source_url,
                    "filename": filename,
                    "date": date[:10] if date else None,
                    "category": _categorize(title or filename, filename),
                })

            logger.info(f"API page {page}: {len(data)} items (total PDFs so far: {len(all_pdfs)})")
            if len(data) < 100:
                break
            page += 1
            time.sleep(1)

        return all_pdfs

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all guidance PDFs with full text."""
        pdfs = self._list_pdfs()
        logger.info(f"Total substantive PDFs to process: {len(pdfs)}")

        for pdf in pdfs:
            time.sleep(1.5)
            doc_id = f"media_{pdf['id']}"

            text = extract_pdf_markdown(
                source=self.SOURCE_ID,
                source_id=doc_id,
                pdf_url=pdf["source_url"],
                table="doctrine",
            )
            if not text or len(text.strip()) < 100:
                logger.warning(f"Skipping {pdf['filename']}: insufficient text ({len(text) if text else 0} chars)")
                continue

            yield self.normalize({
                "doc_id": doc_id,
                "title": pdf["title"],
                "text": text,
                "url": pdf["source_url"],
                "date": pdf["date"],
                "category": pdf["category"],
            })

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch documents uploaded after a given date."""
        pdfs = self._list_pdfs()
        for pdf in pdfs:
            if pdf.get("date") and pdf["date"] >= since:
                time.sleep(1.5)
                doc_id = f"media_{pdf['id']}"
                text = extract_pdf_markdown(
                    source=self.SOURCE_ID,
                    source_id=doc_id,
                    pdf_url=pdf["source_url"],
                    table="doctrine",
                )
                if not text or len(text.strip()) < 100:
                    continue
                yield self.normalize({
                    "doc_id": doc_id,
                    "title": pdf["title"],
                    "text": text,
                    "url": pdf["source_url"],
                    "date": pdf["date"],
                    "category": pdf["category"],
                })

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record into standard schema."""
        return {
            "_id": raw["doc_id"],
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "category": raw.get("category", ""),
        }


# ─── CLI Entry Point ─────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="BS/DIR-TaxGuidance bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    args = parser.parse_args()

    scraper = BSDIRScraper()

    if args.command == "test":
        pdfs = scraper._list_pdfs()
        print(f"OK: Found {len(pdfs)} substantive PDFs via WP REST API")
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    limit = 15 if args.sample else 9999

    for record in scraper.fetch_all():
        count += 1
        fname = re.sub(r'[^\w\-]', '_', record["_id"])[:80] + ".json"
        with open(sample_dir / fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        text_len = len(record.get("text", ""))
        logger.info(f"[{count}] {record['title'][:60]} ({text_len} chars)")

        if count >= limit:
            logger.info(f"Sample limit reached ({limit} records)")
            break

    print(f"\nDone: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
