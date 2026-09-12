#!/usr/bin/env python3
"""
Hungarian Energy and Public Utility Regulatory Authority (MEKH) Data Fetcher

Fetches regulatory decisions (határozatok) from mekh.hu.
Uses session-based HTML scraping: GET with doctype=2 to set filter, then paginate.
Documents are PDFs — text extracted via pdfminer.

Note: mekh.hu has TLS issues that prevent Python's requests/urllib3 from
connecting reliably. We use curl subprocess for HTTP requests.

Data source: https://mekh.hu/mekh-hatarozatok
License: Public (Government of Hungary)
"""

import io
import json
import re
import subprocess
import sys
import time
import hashlib
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import unquote

from bs4 import BeautifulSoup
from pdfminer.high_level import extract_text as pdf_extract_text

BASE_URL = "https://mekh.hu"
SEARCH_URL = f"{BASE_URL}/kereso"
RATE_LIMIT_DELAY = 1.5
ITEMS_PER_PAGE = 10

USER_AGENT = "LegalDataHunter/1.0 (legal research; open data collection)"

HU_MONTHS = {
    "január": 1, "február": 2, "március": 3, "április": 4,
    "május": 5, "június": 6, "július": 7, "augusztus": 8,
    "szeptember": 9, "október": 10, "november": 11, "december": 12,
}


def parse_hu_date(date_str: str) -> Optional[str]:
    """Parse Hungarian date like '2026. május 28.' to ISO format."""
    if not date_str:
        return None
    date_str = date_str.strip().rstrip(".")
    m = re.match(r"(\d{4})\.\s*(\w+)\s+(\d{1,2})", date_str)
    if m:
        year = int(m.group(1))
        month_name = m.group(2).lower()
        day = int(m.group(3))
        month = HU_MONTHS.get(month_name)
        if month:
            return f"{year}-{month:02d}-{day:02d}"
    return None


class CurlSession:
    """HTTP session using curl subprocess (bypasses Python SSL/TLS issues)."""

    def __init__(self):
        self.cookie_file = tempfile.NamedTemporaryFile(
            suffix=".txt", delete=False, mode="w"
        )
        self.cookie_file.close()
        self.cookie_path = self.cookie_file.name

    def get(self, url: str) -> str:
        cmd = [
            "curl", "-skL",
            "-b", self.cookie_path,
            "-c", self.cookie_path,
            "-A", USER_AGENT,
            "--max-time", "60",
            url,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
        if result.returncode != 0:
            raise RuntimeError(f"curl failed ({result.returncode}): {result.stderr}")
        return result.stdout

    def download(self, url: str) -> bytes:
        cmd = [
            "curl", "-skL",
            "-b", self.cookie_path,
            "-c", self.cookie_path,
            "-A", USER_AGENT,
            "--max-time", "120",
            url,
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=150)
        if result.returncode != 0:
            raise RuntimeError(f"curl failed ({result.returncode}): {result.stderr.decode()}")
        return result.stdout

    def cleanup(self):
        Path(self.cookie_path).unlink(missing_ok=True)


class MEKHFetcher:
    """Fetches MEKH regulatory decisions."""

    def __init__(self):
        self.http = CurlSession()

    def init_search(self) -> int:
        """Initialize session with doctype=2 filter and return total count."""
        html = self.http.get(f"{SEARCH_URL}?doctype=2")
        total_match = re.search(r"(\d+)\s*<span>találat", html)
        total = int(total_match.group(1)) if total_match else 0
        print(f"Search initialized: {total} határozatok found", file=sys.stderr)
        return total

    def list_page(self, page: int) -> list[dict]:
        """Fetch a results page and return list of items with PDF URLs."""
        url = f"{SEARCH_URL}?page={page}" if page > 1 else SEARCH_URL
        html = self.http.get(url)

        soup = BeautifulSoup(html, "html.parser")
        items = []

        for div in soup.select("div.post-list-item"):
            title_el = div.select_one("h3.title")
            title = title_el.get_text(strip=True) if title_el else ""

            date_el = div.select_one("span.date")
            date_str = date_el.get_text(strip=True) if date_el else ""

            cat_el = div.select_one("span.category")
            category = cat_el.get_text(strip=True) if cat_el else ""

            # Get description text (loose text in text-container)
            desc = ""
            tc = div.select_one("div.text-container")
            if tc:
                for child in tc.children:
                    if hasattr(child, "name") and child.name in ("h3", "div", "a", "br"):
                        continue
                    t = child.string if child.string else ""
                    t = t.strip()
                    if t:
                        desc = t
                        break

            # Find PDF download link
            pdf_link = None
            for a in div.select("a[href]"):
                href = a.get("href", "")
                if href.endswith(".pdf") or "/download/" in href:
                    pdf_link = href if href.startswith("http") else f"{BASE_URL}/{href}"
                    break

            if not pdf_link:
                continue

            items.append({
                "title": title,
                "description": desc,
                "date": parse_hu_date(date_str),
                "date_raw": date_str,
                "category": category,
                "pdf_url": pdf_link,
            })

        return items

    def extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download a PDF and extract its text content."""
        try:
            pdf_bytes = self.http.download(pdf_url)
            if len(pdf_bytes) < 100 or not pdf_bytes[:5].startswith(b"%PDF"):
                return None
            text = pdf_extract_text(io.BytesIO(pdf_bytes))
            text = text.strip()
            return text if len(text) > 50 else None
        except Exception as e:
            print(f"  PDF extraction error: {e}", file=sys.stderr)
            return None

    def fetch_all(self, max_pages: Optional[int] = None) -> Generator[dict, None, None]:
        """Yield all normalized documents."""
        total = self.init_search()
        if total == 0:
            print("No results found", file=sys.stderr)
            return

        num_pages = (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
        if max_pages:
            num_pages = min(num_pages, max_pages)

        seen_urls = set()
        doc_count = 0

        for page in range(1, num_pages + 1):
            print(f"Listing page {page}/{num_pages}...", file=sys.stderr)
            items = self.list_page(page)

            for item in items:
                pdf_url = item["pdf_url"]
                if pdf_url in seen_urls:
                    continue
                seen_urls.add(pdf_url)

                time.sleep(RATE_LIMIT_DELAY)
                print(f"  Fetching PDF: {pdf_url.split('/')[-1][:60]}", file=sys.stderr)
                text = self.extract_pdf_text(pdf_url)
                if not text:
                    print(f"  Skipped (no text extracted)", file=sys.stderr)
                    continue

                doc_id = hashlib.sha256(pdf_url.encode()).hexdigest()[:16]
                record = normalize(item, doc_id, text, pdf_url)
                doc_count += 1
                yield record

            time.sleep(RATE_LIMIT_DELAY)

        print(f"Total documents fetched: {doc_count}", file=sys.stderr)
        self.http.cleanup()


def normalize(item: dict, doc_id: str, text: str, pdf_url: str) -> dict:
    """Transform a raw document into the standard schema."""
    return {
        "_id": doc_id,
        "_source": "HU/MEKH",
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": item["title"],
        "description": item.get("description", ""),
        "text": text,
        "date": item["date"],
        "url": pdf_url,
        "category": item.get("category", ""),
    }


def bootstrap_sample(num_pages: int = 3):
    """Fetch a sample of documents and save to sample/."""
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    fetcher = MEKHFetcher()
    count = 0

    for record in fetcher.fetch_all(max_pages=num_pages):
        count += 1
        out_path = sample_dir / f"{record['_id']}.json"
        out_path.write_text(json.dumps(record, ensure_ascii=False, indent=2))
        print(f"  Saved sample {count}: {record['title'][:60]}", file=sys.stderr)
        if count >= 15:
            break

    print(f"\nSample complete: {count} documents saved to {sample_dir}",
          file=sys.stderr)
    return count


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse
    parser = argparse.ArgumentParser(description="MEKH data fetcher")
    parser.add_argument("command", choices=["bootstrap"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample data only")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="Maximum number of listing pages to process")
    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample()
        else:
            fetcher = MEKHFetcher()
            for record in fetcher.fetch_all(max_pages=args.max_pages):
                print(json.dumps(record, ensure_ascii=False))
