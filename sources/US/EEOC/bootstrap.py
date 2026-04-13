#!/usr/bin/env python3
"""
US/EEOC -- Equal Employment Opportunity Commission Appellate Decisions

Fetches EEOC federal-sector appellate decisions via paginated Drupal listing.
112,000+ decisions available as .txt (older) and .pdf (newer) files.

Data access:
  - Paginated HTML listing at /federal-sector/appellate-decisions?page=N
  - 10 results per page, ~11,234 pages total
  - Decision files: .txt (plain text) or .pdf (requires pdfplumber)

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
import tempfile
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.EEOC")

BASE_URL = "https://www.eeoc.gov"
SEARCH_URL = BASE_URL + "/federal-sector/appellate-decisions"
DELAY = 2.0


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return session


def clean_text(text: str) -> str:
    """Clean extracted text."""
    if not text:
        return ""
    text = re.sub(r'\r\n', '\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="US/EEOC",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""

def extract_appeal_number(filename: str) -> str:
    """Extract appeal number from filename like '0120120123.txt' or '0120120123.pdf'."""
    base = Path(filename).stem
    return base


def extract_date_from_path(url: str) -> Optional[str]:
    """Try to extract date from URL path like /decisions/2023_01_15/."""
    match = re.search(r'/decisions/(\d{4})_(\d{2})_(\d{2})/', url)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    return None


def parse_listing_page(html: str) -> List[Dict[str, str]]:
    """Parse the search results page to extract decision file links."""
    if not HAS_BS4:
        logger.error("BeautifulSoup required")
        return []

    soup = BeautifulSoup(html, "html.parser")
    results = []

    # Find all links to decision files
    for link in soup.find_all("a", href=True):
        href = link["href"]

        # Match decision file patterns
        if not re.search(r'/sites/default/files/(decisions|migrated_files/decisions)/', href):
            continue
        if not (href.endswith(".txt") or href.endswith(".pdf")):
            continue

        # Build absolute URL
        if href.startswith("/"):
            file_url = BASE_URL + href
        elif href.startswith("http"):
            file_url = href
        else:
            file_url = urljoin(BASE_URL, href)

        filename = href.split("/")[-1]
        appeal_number = extract_appeal_number(filename)
        date = extract_date_from_path(href)
        link_text = link.get_text(strip=True)

        results.append({
            "file_url": file_url,
            "filename": filename,
            "appeal_number": appeal_number,
            "date": date,
            "link_text": link_text,
        })

    return results


def fetch_decision_text(session: requests.Session, file_url: str) -> str:
    """Download a decision file and extract text."""
    try:
        resp = session.get(file_url, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning(f"Failed to fetch {file_url}: {e}")
        return ""

    if file_url.endswith(".txt"):
        # Try to decode as text
        try:
            return clean_text(resp.content.decode("utf-8", errors="replace"))
        except Exception:
            return clean_text(resp.text)
    elif file_url.endswith(".pdf"):
        return extract_pdf_text(resp.content)

    return ""


def normalize(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a raw decision record."""
    appeal_num = raw.get("appeal_number", "")
    _id = f"US-EEOC-{appeal_num}" if appeal_num else f"US-EEOC-{raw['filename']}"

    title = raw.get("link_text", "") or f"EEOC Decision {appeal_num}"
    if title == raw.get("filename"):
        title = f"EEOC Decision {appeal_num}"

    return {
        "_id": _id,
        "_source": "US/EEOC",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": raw.get("text", ""),
        "date": raw.get("date"),
        "url": raw.get("file_url", ""),
        "appeal_number": appeal_num,
        "court": "US Equal Employment Opportunity Commission",
    }


def fetch_all(sample: bool = False) -> Generator[Dict[str, Any], None, None]:
    """Fetch EEOC decisions via paginated listing."""
    session = get_session()
    max_pages = 3 if sample else 12000
    total_yielded = 0

    for page_num in range(max_pages):
        url = f"{SEARCH_URL}?appellate_keywords=&page={page_num}"
        logger.info(f"Fetching page {page_num}: {url}")

        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch page {page_num}: {e}")
            continue

        entries = parse_listing_page(resp.text)
        if not entries:
            logger.info(f"No entries on page {page_num}, stopping")
            break

        logger.info(f"  Found {len(entries)} entries")

        for entry in entries:
            time.sleep(DELAY)
            logger.info(f"  Fetching: {entry['filename']}")
            text = fetch_decision_text(session, entry["file_url"])
            if not text or len(text) < 100:
                logger.warning(f"  Insufficient text for {entry['filename']}")
                continue

            entry["text"] = text
            record = normalize(entry)
            yield record
            total_yielded += 1
            logger.info(f"  Record {total_yielded}: {record['appeal_number']} ({len(text)} chars)")

            if sample and total_yielded >= 12:
                logger.info(f"Sample complete: {total_yielded} records")
                return

        time.sleep(DELAY)

    logger.info(f"Total records: {total_yielded}")


def fetch_updates(since: str) -> Generator[Dict[str, Any], None, None]:
    """Fetch recent decisions (newest first by page order)."""
    since_date = datetime.fromisoformat(since).date()
    for record in fetch_all():
        if record.get("date"):
            try:
                rec_date = datetime.fromisoformat(record["date"]).date()
                if rec_date < since_date:
                    return
            except ValueError:
                pass
        yield record


def test_connectivity() -> bool:
    """Quick connectivity test."""
    session = get_session()
    try:
        resp = session.get(f"{SEARCH_URL}?appellate_keywords=&page=0", timeout=15)
        resp.raise_for_status()
        entries = parse_listing_page(resp.text)
        logger.info(f"OK: {resp.status_code}, {len(entries)} entries on page 0")
        return len(entries) > 0
    except Exception as e:
        logger.error(f"Failed: {e}")
        return False


def main():
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        ok = test_connectivity()
        sys.exit(0 if ok else 1)

    if command == "bootstrap":
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in fetch_all(sample=sample):
            fname = re.sub(r'[^\w\-]', '_', record["_id"])[:80] + ".json"
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            print(f"  [{count}] {record['appeal_number']}")
        print(f"\nDone: {count} records saved to {sample_dir}/")

    elif command == "update":
        since = sys.argv[2] if len(sys.argv) > 2 else "2024-01-01"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in fetch_updates(since):
            fname = re.sub(r'[^\w\-]', '_', record["_id"])[:80] + ".json"
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        print(f"Updated: {count} records since {since}")


if __name__ == "__main__":
    main()
