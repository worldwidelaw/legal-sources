#!/usr/bin/env python3
"""
ZW/ConstitutionalCourt -- Zimbabwe Court Decisions via Veritas Zimbabwe

Fetches court judgments from veritaszim.net with full text from PDF attachments.

Strategy:
  - Scrape listing pages for each court (High Court, Supreme Court,
    Constitutional Court, Electoral Court, Labour Court)
  - Extract node URLs and titles from listing pages
  - For each node, fetch the page and extract the PDF attachment URL
  - Download PDF and extract text via PyMuPDF (fitz)
  - 2-second delay between requests

Usage:
  python bootstrap.py bootstrap          # Fetch all judgments
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from html import unescape

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ZW.ConstitutionalCourt")

BASE_URL = "https://www.veritaszim.net"

# Court listing endpoints: (name, path, max_pages)
COURTS = [
    ("High Court", "/high-court", 30),
    ("Supreme Court", "/taxonomy/term/166", 5),
    ("Constitutional Court", "/taxonomy/term/102", 3),
    ("Electoral Court", "/electoral-court", 3),
    ("Labour Court", "/labour-court", 3),
]


class VeritasScraper(BaseScraper):
    """Scraper for ZW/ConstitutionalCourt -- Zimbabwe court decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        """HTTP GET with 2-second delay and retry."""
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _scrape_listing_page(self, url: str) -> List[Dict[str, str]]:
        """Extract case node URLs and titles from a listing page."""
        resp = self._request(url)
        if not resp:
            return []

        html = resp.text
        entries = []
        seen_nodes = set()

        # Find case links - pattern: <a href="/node/XXXX">Title</a>
        # within the main content area (avoid nav/sidebar duplicates)
        for match in re.finditer(
            r'<h\d[^>]*>\s*<a\s+href="(/node/(\d+))"[^>]*>([^<]+)</a>',
            html
        ):
            path, node_id, title = match.group(1), match.group(2), match.group(3)
            if node_id not in seen_nodes:
                seen_nodes.add(node_id)
                entries.append({
                    "node_id": node_id,
                    "url": f"{BASE_URL}{path}",
                    "title": unescape(title).strip(),
                })

        return entries

    def _get_all_cases(self, court_name: str, base_path: str,
                       max_pages: int) -> List[Dict[str, str]]:
        """Paginate through a court's listing pages to get all cases."""
        all_cases = []
        seen = set()

        for page in range(max_pages):
            url = f"{BASE_URL}{base_path}" + (f"?page={page}" if page > 0 else "")
            entries = self._scrape_listing_page(url)

            if not entries:
                break

            new_count = 0
            for entry in entries:
                if entry["node_id"] not in seen:
                    seen.add(entry["node_id"])
                    entry["court"] = court_name
                    all_cases.append(entry)
                    new_count += 1

            logger.info(f"{court_name} page {page}: {new_count} new cases")
            if new_count == 0:
                break

        return all_cases

    def _extract_pdf_url(self, html: str) -> Optional[str]:
        """Extract PDF attachment URL from a node page."""
        # Look for PDF links in file attachment spans
        match = re.search(
            r'<a\s+href="([^"]*\.pdf)"[^>]*type="application/pdf"',
            html
        )
        if match:
            url = match.group(1)
            if not url.startswith("http"):
                url = BASE_URL + url
            return url

        # Fallback: any PDF link in the content area
        match = re.search(
            r'<a\s+href="(https?://[^"]*veritaszim\.net/sites/[^"]*\.pdf)"',
            html
        )
        if match:
            return match.group(1)

        # Last resort: any PDF link
        match = re.search(r'href="([^"]*\.pdf)"', html)
        if match:
            url = match.group(1)
            if not url.startswith("http"):
                url = BASE_URL + url
            return url

        return None

    def _extract_date(self, html: str) -> Optional[str]:
        """Extract publication date from node page."""
        match = re.search(
            r'<span class="date-display-single"[^>]*>([^<]+)</span>',
            html
        )
        if match:
            try:
                dt = datetime.strptime(match.group(1).strip(), "%d/%m/%Y")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Try submitted date
        match = re.search(
            r'Submitted on\s+\w+,\s+(\d{2}/\d{2}/\d{4})',
            html
        )
        if match:
            try:
                dt = datetime.strptime(match.group(1), "%d/%m/%Y")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        return None

    def _extract_text_from_pdf(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text using PyMuPDF."""
        try:
            import fitz
        except ImportError:
            logger.error("PyMuPDF (fitz) not installed")
            return None

        resp = self._request(pdf_url, timeout=120)
        if not resp:
            return None

        try:
            doc = fitz.open(stream=resp.content, filetype="pdf")
            text_parts = []
            for page in doc:
                page_text = page.get_text()
                if page_text:
                    text_parts.append(page_text)
            doc.close()

            text = "\n".join(text_parts).strip()
            if len(text) < 100:
                logger.warning(f"PDF text too short ({len(text)} chars): {pdf_url}")
                return None
            return text
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return None

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all court decisions with full text from PDFs."""
        all_cases = []
        for court_name, path, max_pages in COURTS:
            cases = self._get_all_cases(court_name, path, max_pages)
            all_cases.extend(cases)
            logger.info(f"Total {court_name}: {len(cases)} cases")

        # Deduplicate by node_id (some cases appear in multiple courts)
        seen = set()
        unique_cases = []
        for case in all_cases:
            if case["node_id"] not in seen:
                seen.add(case["node_id"])
                unique_cases.append(case)

        logger.info(f"Total unique cases: {len(unique_cases)}")

        for i, case in enumerate(unique_cases):
            logger.info(f"[{i+1}/{len(unique_cases)}] Fetching {case['title'][:80]}")

            # Fetch node page
            resp = self._request(case["url"])
            if not resp:
                logger.warning(f"Failed to fetch {case['url']}")
                continue

            # Extract PDF URL and date
            pdf_url = self._extract_pdf_url(resp.text)
            date = self._extract_date(resp.text)

            if not pdf_url:
                logger.warning(f"No PDF found on {case['url']}")
                continue

            # Extract text from PDF
            text = self._extract_text_from_pdf(pdf_url)
            if not text:
                continue

            yield {
                "node_id": case["node_id"],
                "title": case["title"],
                "text": text,
                "date": date,
                "court": case["court"],
                "url": case["url"],
                "pdf_url": pdf_url,
            }

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Yield decisions published after `since` date."""
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
        for record in self.fetch_all():
            if record.get("date") and record["date"] >= since:
                yield record

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw record into standard schema."""
        return {
            "_id": f"ZW/ConstitutionalCourt/{raw['node_id']}",
            "_source": "ZW/ConstitutionalCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "court": raw.get("court"),
            "url": raw["url"],
            "pdf_url": raw.get("pdf_url"),
        }


def main():
    scraper = VeritasScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        resp = scraper._request(f"{BASE_URL}/high-court")
        if resp:
            nodes = re.findall(r'href="(/node/\d+)"', resp.text)
            print(f"OK: High Court page returned {len(set(nodes))} unique node links")
        else:
            print("FAIL: Could not reach veritaszim.net")
            sys.exit(1)

    elif command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        max_records = 15 if sample_mode else 99999

        for raw in scraper.fetch_all():
            record = scraper.normalize(raw)
            out_file = sample_dir / f"{raw['node_id']}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            text_len = len(record.get("text", ""))
            logger.info(f"Saved {out_file.name}: {record['title'][:60]} ({text_len} chars)")

            if count >= max_records:
                logger.info(f"Sample mode: stopping at {count} records")
                break

        logger.info(f"Done: {count} records saved to {sample_dir}")

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
