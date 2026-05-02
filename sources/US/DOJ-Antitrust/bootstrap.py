#!/usr/bin/env python3
"""
US/DOJ-Antitrust -- U.S. DOJ Antitrust Division Case Filings Data Fetcher

Fetches antitrust case filings from the U.S. Department of Justice.

Strategy:
  - Scrape the alphabetical case listing at /atr/antitrust-case-filings-alpha
  - For each case, fetch the detail page to extract metadata and document links
  - Download the primary document PDF (complaint or final judgment) and extract text
  - Use common/pdf_extract for PDF text extraction

Endpoints:
  - Case listing: https://www.justice.gov/atr/antitrust-case-filings-alpha
  - Case detail: https://www.justice.gov/atr/case/{slug}
  - Documents: PDFs at justice.gov/d9/... or /media/{id}/dl?inline

Data:
  - ~923 antitrust cases
  - Case types: Civil Merger, Civil Non Merger, Criminal
  - Violations: Price Fixing, Market Allocation, Monopolization, etc.
  - Language: English
  - License: U.S. Public Domain (17 U.S.C. § 105)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
import socket
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional, List
from urllib.parse import urljoin

# Safety net against silent socket hangs
socket.setdefaulttimeout(120)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

try:
    import requests
except ImportError:
    print("ERROR: requests is required. Install with: pip install requests")
    sys.exit(1)

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.DOJ-Antitrust")

BASE_URL = "https://www.justice.gov"
HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html, application/xhtml+xml",
    "Accept-Language": "en-US,en",
}


class DOJAntitrustScraper(BaseScraper):
    """
    Scraper for US/DOJ-Antitrust -- DOJ Antitrust Division Case Filings.
    Country: US
    URL: https://www.justice.gov/atr

    Data types: case_law
    Auth: none (Public Domain)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers=HEADERS,
            timeout=60,
        )

    def _get_case_slugs(self) -> List[str]:
        """Scrape the alphabetical listing to get all case URL slugs."""
        logger.info("Fetching case listing page...")
        self.rate_limiter.wait()
        resp = self.client.get("/atr/antitrust-case-filings-alpha")
        resp.raise_for_status()

        slugs = re.findall(r'href="/atr/case/([^"]+)"', resp.text)
        slugs = list(dict.fromkeys(slugs))  # deduplicate preserving order
        logger.info(f"Found {len(slugs)} case slugs")
        return slugs

    def _parse_case_detail(self, slug: str, page_html: str) -> Optional[Dict[str, Any]]:
        """Parse a case detail page for metadata and document links."""
        case = {"slug": slug}

        # Extract title from og:title meta tag or <h1>
        title_match = re.search(r'og:title"\s+content="([^"]+)"', page_html)
        if title_match:
            case["title"] = html_module.unescape(title_match.group(1).strip())
        else:
            title_match = re.search(r'<h1[^>]*>([^<]+)</h1>', page_html)
            if title_match:
                case["title"] = html_module.unescape(title_match.group(1).strip())

        # Extract case type
        type_match = re.search(r'field_case_type">\s*([^<]+)', page_html)
        if type_match:
            case["case_type"] = type_match.group(1).strip()

        # Extract violations
        violations = re.findall(r'field_violation">\s*<li>([^<]+)</li>', page_html)
        if violations:
            case["violations"] = [v.strip() for v in violations]

        # Extract open date from <time datetime="..."> element near "Case Open Date"
        date_match = re.search(
            r'datetime="(\d{4}-\d{2}-\d{2})',
            page_html[page_html.find("Case Open Date"):] if "Case Open Date" in page_html else "",
        )
        if date_match:
            case["date"] = date_match.group(1)
        else:
            # Fallback: article:published_time meta tag
            pub_match = re.search(r'article:published_time"\s+content="(\d{4}-\d{2}-\d{2})', page_html)
            if pub_match:
                case["date"] = pub_match.group(1)

        # Extract document links (PDFs and inline documents)
        doc_links = []

        # Pattern 1: <a href="URL">Document Title</a> (Month DD, YYYY)
        for match in re.finditer(
            r'<a\s+href="([^"]+)"[^>]*>([^<]+)</a>\s*\(([^)]+)\)',
            page_html
        ):
            url, title, date_text = match.groups()
            if '.pdf' in url.lower() or '/media/' in url.lower() or '/d9/' in url.lower():
                full_url = urljoin(BASE_URL, url)
                doc_links.append({
                    "url": full_url,
                    "title": html_module.unescape(title.strip()),
                    "date": date_text.strip(),
                })

        case["documents"] = doc_links

        # Select primary document: prefer Complaint, then Final Judgment
        primary_pdf = None
        for doc in doc_links:
            title_lower = doc["title"].lower()
            if "complaint" in title_lower and "competitive" not in title_lower:
                primary_pdf = doc["url"]
                break
        if not primary_pdf:
            for doc in doc_links:
                title_lower = doc["title"].lower()
                if "final judgment" in title_lower or "consent decree" in title_lower:
                    primary_pdf = doc["url"]
                    break
        if not primary_pdf and doc_links:
            primary_pdf = doc_links[0]["url"]

        case["primary_pdf"] = primary_pdf
        return case

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all antitrust cases with full text from PDFs."""
        existing = preload_existing_ids("US/DOJ-Antitrust", table="case_law")

        slugs = self._get_case_slugs()

        for i, slug in enumerate(slugs):
            if slug in existing:
                logger.debug(f"Skipping {slug} — already in Neon")
                continue

            logger.info(f"Processing case {i+1}/{len(slugs)}: {slug}")

            # Fetch case detail page
            self.rate_limiter.wait()
            try:
                resp = self.client.get(f"/atr/case/{slug}")
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"Failed to fetch case {slug}: {e}")
                continue

            case = self._parse_case_detail(slug, resp.text)
            if not case:
                continue

            if not case.get("primary_pdf"):
                logger.warning(f"No PDF for {slug}")
                continue

            # Extract text from PDF
            self.rate_limiter.wait()
            text = extract_pdf_markdown(
                source="US/DOJ-Antitrust",
                source_id=slug,
                pdf_url=case["primary_pdf"],
                table="case_law",
            )

            if not text or len(text) < 100:
                logger.warning(f"Insufficient text for {slug}")
                continue

            yield self.normalize({**case, "text": text})

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch all cases (same as fetch_all — no incremental endpoint)."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw case data into standard schema."""
        slug = raw.get("slug", "")
        title = raw.get("title", "")
        text = raw.get("text", "")
        date_str = raw.get("date")

        return {
            "_id": slug,
            "_source": "US/DOJ-Antitrust",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_slug": slug,
            "title": title,
            "text": text,
            "date": date_str,
            "case_type": raw.get("case_type"),
            "violations": raw.get("violations", []),
            "url": f"{BASE_URL}/atr/case/{slug}",
            "primary_pdf": raw.get("primary_pdf"),
            "documents": raw.get("documents", []),
        }

    def test_connection(self) -> bool:
        """Quick connectivity test."""
        try:
            resp = self.client.get("/atr/antitrust-case-filings-alpha")
            resp.raise_for_status()
            slugs = re.findall(r'href="/atr/case/([^"]+)"', resp.text)
            logger.info(f"Connection OK — found {len(slugs)} case links")
            return len(slugs) > 0
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/DOJ-Antitrust Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to execute")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only 10-15 sample records")
    parser.add_argument("--full", action="store_true",
                        help="Fetch all records (full bootstrap)")
    args = parser.parse_args()

    scraper = DOJAntitrustScraper()

    if args.command == "test":
        success = scraper.test_connection()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        max_records = 15 if args.sample else 999999

        for record in scraper.fetch_all():
            if count >= max_records:
                break

            filename = re.sub(r'[^\w\-.]', '_', record["_id"])[:80] + ".json"
            filepath = sample_dir / filename
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, indent=2, ensure_ascii=False)

            text_len = len(record.get("text", ""))
            logger.info(f"Saved {record['_id']}: {record['title'][:60]}... ({text_len} chars)")
            count += 1

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        count = 0
        for record in scraper.fetch_updates():
            count += 1
            logger.info(f"Updated: {record['_id']}")
        logger.info(f"Update complete: {count} new/updated records")


if __name__ == "__main__":
    main()
