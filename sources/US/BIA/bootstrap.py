#!/usr/bin/env python3
"""
US/BIA -- Board of Immigration Appeals Precedent Decisions

Fetches BIA precedent decisions from justice.gov EOIR volume pages.
29 volumes spanning decades of immigration law decisions.

Strategy:
  - GET /eoir/volume-{N} for volumes 27-29 (modern format)
  - GET /eoir/vll/intdec/nfvol{N}.html for volumes 1-26 (legacy format)
  - Parse HTML for decision metadata and PDF URLs
  - Download PDFs and extract full text via pdfplumber

Usage:
  python bootstrap.py bootstrap --sample   # ~15 sample decisions
  python bootstrap.py bootstrap             # Full extraction
  python bootstrap.py test-api              # Test connectivity
"""

import argparse
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional, Union

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "US/BIA"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.BIA")

BASE_URL = "https://www.justice.gov"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal data research; +https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/pdf",
    "Accept-Language": "en-US,en;q=0.9",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

CRAWL_DELAY = 2.0  # seconds between requests


def fetch_url(url: str, binary: bool = False) -> Optional[Union[bytes, str]]:
    """Fetch a URL with retry logic."""
    for attempt in range(3):
        try:
            resp = SESSION.get(url, timeout=60)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.content if binary else resp.text
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < 2:
                time.sleep(5 * (attempt + 1))
            else:
                logger.warning(f"Failed to fetch {url}: {e}")
                return None
        except Exception as e:
            logger.warning(f"Error fetching {url}: {e}")
            return None


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="US/BIA",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""

def parse_modern_volume(html: str, volume_num: int) -> list:
    """Parse decisions from modern volume pages (volumes 27+)."""
    decisions = []

    # Pattern: <strong>NAME</strong>, VOL I&N Dec. PAGE (AUTHORITY YEAR)
    # followed by <a href="PDF_URL">ID NNNN</a>
    pattern = re.compile(
        r'<strong>([^<]+)</strong>,\s*(\d+)\s*I&amp;N\s*Dec\.\s*(\d+)\s*'
        r'\(([^)]+)\)'
        r'.*?<a\s+href="([^"]*(?:/dl\?inline|\.pdf)[^"]*)"[^>]*>\s*ID\s*(\d+)',
        re.DOTALL | re.IGNORECASE
    )

    for m in pattern.finditer(html):
        name = unescape(m.group(1)).strip()
        vol = int(m.group(2))
        page = int(m.group(3))
        authority_year = unescape(m.group(4)).strip()
        pdf_path = m.group(5)
        decision_id = m.group(6)

        # Parse year from authority string like "BIA 2026" or "A.G. 2024"
        year_match = re.search(r'(\d{4})', authority_year)
        year = int(year_match.group(1)) if year_match else None

        # Parse authority
        authority = authority_year.replace(str(year), '').strip().rstrip(', ') if year else authority_year

        pdf_url = pdf_path if pdf_path.startswith('http') else f"{BASE_URL}{pdf_path}"

        decisions.append({
            "name": name,
            "volume": vol,
            "page": page,
            "authority": authority,
            "year": year,
            "decision_id": decision_id,
            "pdf_url": pdf_url,
        })

    return decisions


def parse_legacy_volume(html: str, volume_num: int) -> list:
    """Parse decisions from legacy volume pages (volumes 1-26)."""
    decisions = []

    # Legacy pages have simpler HTML with links to PDFs
    # Pattern: <a href="vol{N}/{ID}.pdf">text</a> or similar
    for m in re.finditer(
        r'<a\s+href="([^"]*vol\d+/(\d+)\.pdf)"[^>]*>([^<]+)</a>',
        html, re.IGNORECASE
    ):
        pdf_path = m.group(1)
        decision_id = m.group(2)
        link_text = unescape(m.group(3)).strip()

        # Make absolute URL
        if pdf_path.startswith('/'):
            pdf_url = f"{BASE_URL}{pdf_path}"
        elif pdf_path.startswith('http'):
            pdf_url = pdf_path
        else:
            pdf_url = f"{BASE_URL}/eoir/vll/intdec/{pdf_path}"

        decisions.append({
            "name": link_text,
            "volume": volume_num,
            "page": None,
            "authority": "BIA",
            "year": None,
            "decision_id": decision_id,
            "pdf_url": pdf_url,
        })

    return decisions


def get_volume_decisions(volume_num: int) -> list:
    """Fetch and parse a volume page to get all decisions."""
    if volume_num >= 27:
        url = f"{BASE_URL}/eoir/volume-{volume_num}"
    else:
        url = f"{BASE_URL}/eoir/vll/intdec/nfvol{volume_num}.html"

    html = fetch_url(url)
    if not html:
        logger.warning(f"Failed to fetch volume {volume_num}")
        return []

    if volume_num >= 27:
        decisions = parse_modern_volume(html, volume_num)
    else:
        decisions = parse_legacy_volume(html, volume_num)

    return decisions


def normalize(raw: dict, text: str) -> dict:
    """Normalize a BIA decision record."""
    decision_id = raw["decision_id"]
    name = raw["name"]
    volume = raw["volume"]
    page = raw.get("page")
    year = raw.get("year")

    cite = f"{volume} I&N Dec. {page}" if page else f"ID {decision_id}"
    title = f"Matter of {name}, {cite}"
    if year:
        title += f" ({year})"

    date_str = f"{year}-01-01" if year else None

    return {
        "_id": f"US/BIA/ID-{decision_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_str,
        "url": raw["pdf_url"],
        "respondent": name,
        "volume": volume,
        "page": page,
        "decision_number": f"ID {decision_id}",
        "authority": raw.get("authority", "BIA"),
        "jurisdiction": "US",
        "language": "en",
    }


def fetch_all() -> Generator[dict, None, None]:
    """Yield all BIA precedent decisions with full text."""
    total = 0
    # Process volumes from newest to oldest
    for vol in range(29, 0, -1):
        logger.info(f"Processing Volume {vol}...")
        decisions = get_volume_decisions(vol)
        time.sleep(CRAWL_DELAY)

        if not decisions:
            logger.warning(f"  No decisions found for volume {vol}")
            continue

        logger.info(f"  Found {len(decisions)} decisions")

        for raw in decisions:
            pdf_bytes = fetch_url(raw["pdf_url"], binary=True)
            time.sleep(CRAWL_DELAY)

            if not pdf_bytes:
                logger.warning(f"  Failed to download PDF for {raw['name']}")
                continue

            text = extract_text_from_pdf(pdf_bytes)
            if not text or len(text) < 100:
                logger.warning(f"  No text extracted for {raw['name']}")
                continue

            record = normalize(raw, text)
            total += 1
            if total % 50 == 0:
                logger.info(f"  Progress: {total} decisions fetched")
            yield record

    logger.info(f"Total decisions with full text: {total}")


def fetch_sample(count: int = 15) -> list:
    """Fetch sample records from recent volumes."""
    records = []

    # Sample from volumes 29, 28, 27
    for vol in [29, 28, 27]:
        if len(records) >= count:
            break

        logger.info(f"Sampling Volume {vol}...")
        decisions = get_volume_decisions(vol)
        time.sleep(CRAWL_DELAY)

        if not decisions:
            continue

        # Take first few from each volume
        take = min(6, count - len(records), len(decisions))
        for raw in decisions[:take]:
            pdf_bytes = fetch_url(raw["pdf_url"], binary=True)
            time.sleep(CRAWL_DELAY)

            if not pdf_bytes:
                continue

            text = extract_text_from_pdf(pdf_bytes)
            if not text or len(text) < 100:
                continue

            record = normalize(raw, text)
            records.append(record)
            logger.info(f"  [{len(records)}] {record['title'][:60]} - {len(text)} chars")

    return records


def test_api():
    """Test connectivity to justice.gov EOIR pages."""
    logger.info("Testing BIA connectivity...")

    # Test volume 29 page
    html = fetch_url(f"{BASE_URL}/eoir/volume-29")
    if not html:
        logger.error("Volume 29 page unreachable")
        return False
    logger.info(f"Volume 29 page OK - {len(html)} bytes")

    decisions = parse_modern_volume(html, 29)
    if not decisions:
        logger.error("No decisions parsed from volume 29")
        return False
    logger.info(f"Parsed {len(decisions)} decisions from volume 29")

    # Test PDF download
    time.sleep(CRAWL_DELAY)
    raw = decisions[0]
    pdf_bytes = fetch_url(raw["pdf_url"], binary=True)
    if not pdf_bytes:
        logger.error("PDF download failed")
        return False
    logger.info(f"PDF download OK - {len(pdf_bytes)} bytes")

    text = extract_text_from_pdf(pdf_bytes)
    if not text:
        logger.error("PDF text extraction failed")
        return False
    logger.info(f"Text extraction OK - {len(text)} chars")
    logger.info(f"Preview: {text[:200]}...")
    return True


def bootstrap_sample():
    """Fetch and save sample records."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = fetch_sample(count=15)

    if not records:
        logger.error("No records fetched!")
        return False

    for i, record in enumerate(records, 1):
        safe_id = re.sub(r'[^\w\-]', '_', record["_id"])[:80]
        filename = f"sample_{i:02d}_{safe_id}.json"
        filepath = SAMPLE_DIR / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    logger.info(f"\nSaved {len(records)} sample records to {SAMPLE_DIR}")

    text_lengths = [len(r.get("text", "")) for r in records]
    avg_text = sum(text_lengths) / len(text_lengths) if text_lengths else 0

    logger.info("Validation:")
    logger.info(f"  - Records with text: {sum(1 for t in text_lengths if t > 0)}/{len(records)}")
    logger.info(f"  - Avg text length: {avg_text:.0f} chars")
    logger.info(f"  - Min text length: {min(text_lengths) if text_lengths else 0}")
    logger.info(f"  - Max text length: {max(text_lengths) if text_lengths else 0}")

    return len(records) >= 10 and avg_text > 500


def main():
    parser = argparse.ArgumentParser(description="US/BIA Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true")

    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        if args.sample:
            success = bootstrap_sample()
            sys.exit(0 if success else 1)
        else:
            logger.info("Full bootstrap mode")
            count = 0
            SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
            for record in fetch_all():
                count += 1
                safe_id = re.sub(r'[^\w\-]', '_', record["_id"])[:80]
                filepath = SAMPLE_DIR / f"record_{safe_id}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"Processed {count} records")


if __name__ == "__main__":
    main()
