#!/usr/bin/env python3
"""
US/FINRA-Awards -- FINRA Arbitration Awards

Fetches securities arbitration awards from FINRA's Arbitration Awards Online (AAO)
database. ~66,000+ awards since 1988, available as PDFs.

Data access:
  - HTML search results at /arbitration-mediation/arbitration-awards-online
  - 15 results per page, paginated with ?page=N
  - Each award links to a PDF at /sites/default/files/aao_documents/{case_id}.pdf
  - Text extracted via pdfplumber

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental (newest first)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import io
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.FINRA-Awards")

BASE_URL = "https://www.finra.org"
SEARCH_URL = (
    BASE_URL + "/arbitration-mediation/arbitration-awards-online"
    "?search=&field_case_id_text="
    "&field_core_official_dt%5Bmin%5D=&field_core_official_dt%5Bmax%5D="
    "&order=field_core_official_dt&sort=DESC&page={page}"
)
DELAY = 2.0


def parse_date(date_str: str) -> Optional[str]:
    """Parse MM/DD/YYYY to ISO 8601."""
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y"]:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def parse_search_results(html: str) -> List[Dict[str, Any]]:
    """Parse search result HTML to extract award metadata."""
    results = []

    # Find all PDF links with case IDs
    # Pattern: <a href="/sites/default/files/aao_documents/CASE-ID.pdf">
    pdf_pattern = re.compile(
        r'href="(/sites/default/files/aao_documents/([^"]+?)\.pdf)"',
        re.IGNORECASE,
    )

    # Split by result blocks - each result contains a PDF link
    # We'll find each PDF link and extract surrounding context
    blocks = re.split(r'(?=href="/sites/default/files/aao_documents/)', html)

    for block in blocks:
        pdf_match = pdf_pattern.search(block)
        if not pdf_match:
            continue

        pdf_path = pdf_match.group(1)
        case_id = pdf_match.group(2)

        # Extract metadata from the block text
        # Clean HTML tags for text extraction
        text_block = re.sub(r'<[^>]+>', ' ', block)
        text_block = re.sub(r'\s+', ' ', text_block).strip()

        # Extract date (MM/DD/YYYY pattern)
        date_match = re.search(r'(\d{2}/\d{2}/\d{4})', text_block)
        date_str = date_match.group(1) if date_match else ""

        # Extract claimant
        claimant_match = re.search(r'Claimant\(s\):\s*([^|]+?)(?:Claimant Rep|Respondent|Neutral|Hearing|Award|$)', text_block)
        claimant = claimant_match.group(1).strip().rstrip(',. ') if claimant_match else ""

        # Extract respondent
        respondent_match = re.search(r'Respondent\(s\):\s*([^|]+?)(?:Respondent Rep|Neutral|Hearing|Award|$)', text_block)
        respondent = respondent_match.group(1).strip().rstrip(',. ') if respondent_match else ""

        # Extract hearing site
        hearing_match = re.search(r'Hearing Site:\s*([^|]+?)(?:Award|FINRA|NASD|NYSE|$)', text_block)
        hearing_site = hearing_match.group(1).strip().rstrip(',. ') if hearing_match else ""

        # Extract forum
        forum = ""
        for f in ["FINRA", "NASD", "NYSE", "AMEX", "CBOE", "PHLX", "MSRB"]:
            if f in text_block:
                forum = f
                break

        # Extract document type
        doc_type = "Award"
        for dt in ["Motion to Confirm", "Motion to Vacate", "Order to Confirm", "Order to Vacate"]:
            if dt in text_block:
                doc_type = dt
                break

        results.append({
            "case_id": case_id,
            "pdf_path": pdf_path,
            "date": date_str,
            "claimant": claimant,
            "respondent": respondent,
            "hearing_site": hearing_site,
            "forum": forum,
            "doc_type": doc_type,
        })

    return results


class FINRAAwardsScraper(BaseScraper):
    SOURCE_ID = "US/FINRA-Awards"

    def __init__(self):
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Accept": "text/html,application/xhtml+xml",
            },
        )

    def fetch_search_page(self, page: int) -> List[Dict[str, Any]]:
        """Fetch one page of search results."""
        url = SEARCH_URL.format(page=page)
        resp = self.http.get(url)
        time.sleep(DELAY)
        if resp is None or resp.status_code != 200:
            logger.warning("Failed to fetch search page %d: status=%s",
                           page, resp.status_code if resp else "None")
            return []
        return parse_search_results(resp.text)

    def fetch_pdf_text(self, pdf_path: str) -> str:
        """Download a PDF and extract its text."""
        url = f"{BASE_URL}{pdf_path}" if pdf_path.startswith("/") else pdf_path
        resp = self.http.get(url)
        time.sleep(DELAY)
        if resp is None or resp.status_code != 200:
            return ""
        try:
            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                pages = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages.append(text)
                return "\n\n".join(pages)
        except Exception as e:
            logger.warning("Failed to extract PDF text from %s: %s", pdf_path, e)
            return ""

    def normalize(self, meta: Dict[str, Any], text: str) -> Dict[str, Any]:
        """Normalize an award into the standard schema."""
        case_id = meta.get("case_id", "")
        claimant = meta.get("claimant", "")
        respondent = meta.get("respondent", "")

        title_parts = [f"FINRA Case {case_id}"]
        if claimant and respondent:
            title_parts.append(f"{claimant} v. {respondent}")
        elif claimant:
            title_parts.append(claimant)

        return {
            "_id": case_id,
            "_source": self.SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": " — ".join(title_parts),
            "text": text,
            "date": parse_date(meta.get("date", "")),
            "url": f"{BASE_URL}{meta.get('pdf_path', '')}",
            "language": "en",
            "case_id": case_id,
            "claimant": claimant,
            "respondent": respondent,
            "hearing_site": meta.get("hearing_site", ""),
            "forum": meta.get("forum", ""),
            "doc_type": meta.get("doc_type", "Award"),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all arbitration awards."""
        total_yielded = 0
        sample_limit = 15 if sample else None
        page = 0
        max_pages = 4414  # ~66,186 / 15 per page
        consecutive_empty = 0

        while page <= max_pages:
            if sample_limit and total_yielded >= sample_limit:
                break

            logger.info("Fetching search page %d...", page)
            results = self.fetch_search_page(page)

            if not results:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    logger.info("3 consecutive empty pages, stopping.")
                    break
                page += 1
                continue
            consecutive_empty = 0

            for meta in results:
                if sample_limit and total_yielded >= sample_limit:
                    break

                pdf_path = meta.get("pdf_path", "")
                text = self.fetch_pdf_text(pdf_path)
                if not text:
                    logger.warning("Empty text for %s", meta.get("case_id", ""))
                    continue

                record = self.normalize(meta, text)
                yield record
                total_yielded += 1

                if total_yielded % 50 == 0:
                    logger.info("  Progress: %d documents fetched", total_yielded)

            page += 1

        logger.info("Fetch complete. Total documents: %d", total_yielded)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch awards published since a given date."""
        page = 0
        while page < 100:  # Reasonable limit for updates
            results = self.fetch_search_page(page)
            if not results:
                break

            found_older = False
            for meta in results:
                doc_date = parse_date(meta.get("date", ""))
                if doc_date and doc_date < since:
                    found_older = True
                    break
                text = self.fetch_pdf_text(meta.get("pdf_path", ""))
                if text:
                    yield self.normalize(meta, text)

            if found_older:
                break
            page += 1

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            results = self.fetch_search_page(0)
            logger.info("Test passed: %d results on first page", len(results))
            return len(results) > 0
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


# === CLI entry point ===

def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/FINRA-Awards bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10-15 sample records")
    parser.add_argument("--since", type=str, help="Date for incremental update (YYYY-MM-DD)")
    args = parser.parse_args()

    scraper = FINRAAwardsScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            safe_name = re.sub(r'[^\w\-.]', '_', str(record['_id']))
            out_file = sample_dir / f"{safe_name}.json"
            out_file.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            count += 1
            text_len = len(record.get("text", ""))
            logger.info(
                "  [%d] %s | %s | text=%d chars",
                count, record["date"], record["title"][:60], text_len
            )

        logger.info("Bootstrap complete: %d records saved to sample/", count)
        sys.exit(0 if count >= 10 else 1)

    if args.command == "update":
        since = args.since or "2026-01-01"
        count = 0
        for record in scraper.fetch_updates(since):
            count += 1
            logger.info("  [%d] %s: %s", count, record["date"], record["title"][:60])
        logger.info("Update complete: %d new records since %s", count, since)


if __name__ == "__main__":
    main()
