#!/usr/bin/env python3
"""
BB/ParliamentGazette — Barbados Official Gazette (Parliament)

Fetches Official Gazette PDFs from the Barbados Parliament website
and extracts full text using pdfplumber.

Strategy:
  - Crawl gazette listing pages via POST-based offset pagination
  - Extract PDF links and metadata (title, date, volume, issue, part)
  - Download each PDF and extract full text via pdfplumber
  - ~2550 gazette issues from 2013-2026

Data:
  - Part A: Acts, Statutory Instruments, Bills
  - Part B: Notices, Trademarks
  - Part C: Miscellaneous
  - Language: English

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
"""

import argparse
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple
from urllib.parse import unquote

import requests

try:
    import pdfplumber
    HAS_PDF = True
except ImportError:
    HAS_PDF = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://www.barbadosparliament.com"
SOURCE_ID = "BB/ParliamentGazette"
SAMPLE_DIR = Path(__file__).parent / "sample"
REQUEST_DELAY = 2.0
PAGE_SIZE = 50
MAX_PAGES = 60  # Safety limit (~3000 gazettes)


def _parse_date_from_title(title: str) -> Optional[str]:
    """Extract date from gazette title like 'Gazette May 11, 2026 Part A ...'"""
    m = re.search(
        r'Gazette\s+(\w+)\s+(\d{1,2}),?\s+(\d{4})',
        title, re.IGNORECASE
    )
    if m:
        month_str, day, year = m.group(1), m.group(2), m.group(3)
        try:
            dt = datetime.strptime(f"{month_str} {day} {year}", "%B %d %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def _parse_volume(title: str) -> Optional[str]:
    """Extract volume from title like 'VOL. CLXI'"""
    m = re.search(r'VOL\.?\s+([IVXLCDM]+)', title, re.IGNORECASE)
    return m.group(1) if m else None


def _parse_issue_number(title: str) -> Optional[str]:
    """Extract issue number from title like 'No. 54'"""
    m = re.search(r'No\.?\s+(\d+)', title, re.IGNORECASE)
    return m.group(1) if m else None


def _parse_part(title: str) -> Optional[str]:
    """Extract part from title like 'Part A' or 'Part B & C'"""
    m = re.search(r'Part\s+([A-C](?:\s*[&,]\s*[A-C])*)', title, re.IGNORECASE)
    return m.group(1).upper() if m else None


def _id_from_url(url: str) -> str:
    """Generate a stable ID from the PDF filename."""
    filename = url.rsplit("/", 1)[-1]
    filename = unquote(filename)
    if filename.endswith(".pdf"):
        filename = filename[:-4]
    return filename


def _extract_pdf_text(content: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    if not HAS_PDF:
        return ""
    try:
        pdf = pdfplumber.open(io.BytesIO(content))
        pages = []
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                pages.append(text)
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
        pdf.close()
        return "\n".join(pages)
    except Exception as e:
        logger.warning("PDF extraction failed: %s", e)
        return ""


def _fetch_listing_page(session: requests.Session, offset: int) -> List[Tuple[str, str]]:
    """Fetch one page of gazette listings. Returns list of (url, title) tuples."""
    if offset == 0:
        r = session.get(f"{BASE_URL}/gazette", timeout=30)
    else:
        r = session.post(f"{BASE_URL}/gazette/search", data={
            "COL_ID": -1,
            "ORD_ID": 0,
            "OFF_SET": offset,
        }, timeout=30)
    r.raise_for_status()

    entries = re.findall(
        r'<a\s+href="(https://www\.barbadosparliament\.com/uploads/gazette/[^"]+\.pdf)"[^>]*>(.*?)</a>',
        r.text, re.DOTALL
    )
    seen = set()
    results = []
    for url, raw_title in entries:
        if url in seen:
            continue
        seen.add(url)
        title = re.sub(r'<[^>]+>', '', raw_title).strip()
        title = re.sub(r'\s+', ' ', title)
        if title:
            results.append((url, title))
    return results


def fetch_all(session: requests.Session, sample: bool = False) -> Iterator[Dict[str, Any]]:
    """Yield all gazette entries with full text."""
    if not HAS_PDF:
        logger.error("pdfplumber not installed — cannot extract gazette text")
        return

    sample_limit = 15 if sample else None
    count = 0

    for page_num in range(MAX_PAGES):
        offset = page_num * PAGE_SIZE
        logger.info("Fetching listing page %d (offset %d)...", page_num + 1, offset)

        try:
            entries = _fetch_listing_page(session, offset)
        except Exception as e:
            logger.error("Failed to fetch listing page %d: %s", page_num + 1, e)
            break

        if not entries:
            logger.info("No more entries at offset %d, stopping.", offset)
            break

        for url, title in entries:
            if sample_limit and count >= sample_limit:
                return

            logger.info("  [%d] %s", count + 1, title[:80])
            time.sleep(REQUEST_DELAY)

            try:
                pdf_resp = session.get(url, timeout=120)
                pdf_resp.raise_for_status()
            except Exception as e:
                logger.warning("  Failed to download PDF: %s", e)
                continue

            text = _extract_pdf_text(pdf_resp.content)
            if not text or len(text) < 50:
                logger.warning("  Insufficient text extracted (%d chars), skipping", len(text) if text else 0)
                continue

            record = normalize(url, title, text)
            count += 1
            yield record

        time.sleep(REQUEST_DELAY)

    logger.info("Finished: %d gazette records fetched.", count)


def normalize(url: str, title: str, text: str) -> Dict[str, Any]:
    """Normalize a gazette record."""
    return {
        "_id": _id_from_url(url),
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": _parse_date_from_title(title),
        "url": url,
        "volume": _parse_volume(title),
        "issue_number": _parse_issue_number(title),
        "part": _parse_part(title),
    }


def save_samples(records: List[Dict[str, Any]]) -> None:
    """Save sample records to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    for i, rec in enumerate(records):
        path = SAMPLE_DIR / f"record_{i:04d}.json"
        path.write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("Saved %s", path.name)

    all_path = SAMPLE_DIR / "all_samples.json"
    all_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Saved all_samples.json (%d records)", len(records))


def main():
    parser = argparse.ArgumentParser(description="BB/ParliamentGazette bootstrap")
    parser.add_argument("command", choices=["bootstrap"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    session = requests.Session()
    session.headers.update({
        "User-Agent": "LegalDataHunter/1.0 (open-data research; contact@legaldatahunter.com)"
    })

    records = []
    for record in fetch_all(session, sample=args.sample):
        records.append(record)
        if args.sample and len(records) >= 15:
            break

    if not records:
        logger.error("No records fetched!")
        sys.exit(1)

    if args.sample:
        save_samples(records)

    logger.info("Total records: %d", len(records))
    text_lengths = [len(r.get("text", "")) for r in records]
    if text_lengths:
        logger.info("Text length — min: %d, max: %d, avg: %d",
                     min(text_lengths), max(text_lengths),
                     sum(text_lengths) // len(text_lengths))


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
