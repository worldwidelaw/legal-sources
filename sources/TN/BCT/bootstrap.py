#!/usr/bin/env python3
"""
TN/BCT — Central Bank of Tunisia: Circulars & Regulations

Fetches circulars and regulatory notes from the BCT circulars listing page,
downloads each PDF, and extracts full text with pdfplumber.

Strategy:
  1. Scrape the BCT circulars page for all PDF links.
  2. Parse reference numbers and dates from link text.
  3. Download each PDF and extract text with pdfplumber.
  4. Skip scanned/image PDFs that yield < 50 characters of text.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch ~15 sample records
"""

import argparse
import hashlib
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

try:
    import pdfplumber
    HAS_PDF = True
except ImportError:
    HAS_PDF = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SOURCE_ID = "TN/BCT"
SAMPLE_DIR = Path(__file__).parent / "sample"
CIRCULARS_URL = "https://www.bct.gov.tn/bct/siteprod/page.jsp?id=226&la=AN"
BASE_URL = "https://www.bct.gov.tn/bct/siteprod/"
REQUEST_DELAY = 2.0
MIN_TEXT_CHARS = 50

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _parse_date(text: str) -> Optional[str]:
    """Extract ISO date from link text like 'of 30 December 2016'."""
    # Normalize non-breaking spaces
    text = text.replace("\xa0", " ")
    m = re.search(r"of\s+(\d{1,2})\s*(?:st|nd|rd|th)?\s+(\w+)\s+(\d{4})", text, re.I)
    if m:
        day, month_name, year = m.groups()
        month_num = MONTHS.get(month_name.lower())
        if month_num:
            try:
                return f"{year}-{month_num:02d}-{int(day):02d}"
            except ValueError:
                pass
    return None


def _parse_ref(text: str) -> Optional[str]:
    """Extract reference number like '2016-10' from link text."""
    text = text.replace("\xa0", " ")
    m = re.search(r"n[°o]\s*(\d{4}-\d+)", text, re.I)
    if m:
        return m.group(1)
    return None


def _doc_type(href: str) -> str:
    """Determine document type from filename."""
    fname = href.split("/")[-1].lower()
    if fname.startswith("cir"):
        return "circular"
    elif fname.startswith("note"):
        return "note"
    return "other"


def _make_id(href: str) -> str:
    """Create a stable unique ID from the PDF path."""
    fname = href.split("/")[-1]
    # Remove .pdf extension for cleaner ID
    if fname.lower().endswith(".pdf"):
        fname = fname[:-4]
    return fname


def _resolve_url(href: str) -> str:
    """Resolve relative or absolute href to full URL."""
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return "https://www.bct.gov.tn" + href
    return urljoin(BASE_URL, href)


def _extract_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    if not HAS_PDF:
        return ""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            parts = []
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    parts.append(t)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            return "\n".join(parts).strip()
    except Exception as e:
        logger.debug("PDF extraction failed: %s", e)
        return ""


def _get_pdf_links(session: requests.Session) -> List[Dict[str, str]]:
    """Scrape the circulars page for all PDF links."""
    r = session.get(CIRCULARS_URL, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    results = []
    for link in soup.find_all("a", href=re.compile(r"\.pdf$", re.I)):
        href = link.get("href", "")
        text = link.get_text(strip=True)

        # Skip non-circular/note PDFs (like terms of use)
        fname = href.split("/")[-1].lower()
        if not (fname.startswith("cir") or fname.startswith("note")):
            continue

        results.append({"href": href, "text": text})

    return results


def fetch_all(sample: bool = False) -> Iterator[Dict[str, Any]]:
    """Yield normalized circular/note records."""
    if not HAS_PDF:
        logger.error("pdfplumber not installed — cannot extract PDF text")
        return

    session = requests.Session()
    session.headers.update({
        "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/ZachLaik/LegalDataHunter)",
    })

    logger.info("Fetching circulars listing from %s", CIRCULARS_URL)
    pdf_links = _get_pdf_links(session)
    logger.info("Found %d circular/note PDF links", len(pdf_links))

    # Sort newest first (higher ref numbers tend to be more recent)
    pdf_links.reverse()

    count = 0
    sample_limit = 15
    errors = 0

    for entry in pdf_links:
        href = entry["href"]
        link_text = entry["text"]

        doc_id = _make_id(href)
        doc_url = _resolve_url(href)
        doc_date = _parse_date(link_text)
        doc_ref = _parse_ref(link_text)
        dtype = _doc_type(href)

        # Download PDF
        time.sleep(REQUEST_DELAY)
        try:
            r = session.get(doc_url, timeout=60)
            if r.status_code != 200:
                logger.warning("HTTP %d for %s", r.status_code, doc_id)
                errors += 1
                continue
        except Exception as e:
            logger.warning("Download failed for %s: %s", doc_id, e)
            errors += 1
            continue

        # Extract text
        text = _extract_text(r.content)
        if len(text) < MIN_TEXT_CHARS:
            logger.debug("Skipping scanned PDF: %s (%d chars)", doc_id, len(text))
            continue

        # Clean title from link text
        title = link_text.strip()
        if not title:
            title = doc_id

        record = {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": doc_date,
            "url": doc_url,
            "reference": doc_ref,
            "document_type": dtype,
        }

        count += 1
        yield record

        if sample and count >= sample_limit:
            logger.info("Sample limit reached (%d records)", count)
            return

    logger.info("Total records yielded: %d (errors: %d)", count, errors)


def bootstrap(sample: bool = False) -> None:
    """Run bootstrap and save records to sample/ directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    saved = 0
    for record in fetch_all(sample=sample):
        out_path = SAMPLE_DIR / f"{record['_id']}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        saved += 1
        logger.info(
            "[%d] %s — %d chars",
            saved,
            record["title"][:60],
            len(record.get("text", "")),
        )

    logger.info("Bootstrap complete: %d records saved to %s", saved, SAMPLE_DIR)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    parser = argparse.ArgumentParser(description="TN/BCT bootstrap")
    parser.add_argument("command", choices=["bootstrap"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (~15 records)")
    args = parser.parse_args()

    if args.command == "bootstrap":
        bootstrap(sample=args.sample)
