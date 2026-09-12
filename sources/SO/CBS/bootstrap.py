#!/usr/bin/env python3
"""
SO/CBS — Central Bank of Somalia: Regulations

Scrapes PDF regulation documents from the CBS regulatory-guidelines page,
downloads each PDF, and extracts full text with PyMuPDF.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch ~15 sample records
"""

import argparse
import hashlib
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List

import requests

try:
    import fitz  # PyMuPDF
    HAS_PDF = True
except ImportError:
    HAS_PDF = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SOURCE_ID = "SO/CBS"
SAMPLE_DIR = Path(__file__).parent / "sample"
BASE_URL = "https://centralbank.gov.so"
GUIDELINES_URL = BASE_URL + "/regulatory-guidelines/"
REQUEST_DELAY = 1.5
MIN_TEXT_CHARS = 50
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def _session() -> requests.Session:
    s = requests.Session()
    s.verify = False
    s.headers["User-Agent"] = USER_AGENT
    return s


def _get_pdf_links(sess: requests.Session) -> List[str]:
    """Scrape all PDF links from the regulatory guidelines page."""
    r = sess.get(GUIDELINES_URL, timeout=30)
    r.raise_for_status()
    links = re.findall(r'href="([^"]*\.pdf[^"]*)"', r.text)
    unique = sorted(set(links))
    logger.info("Found %d unique PDF links", len(unique))
    return unique


def _clean_title(filename: str) -> str:
    """Derive a readable title from a PDF filename."""
    title = filename
    if title.lower().endswith(".pdf"):
        title = title[:-4]
    # Replace hyphens and underscores with spaces
    title = title.replace("-", " ").replace("_", " ")
    # Clean up numbering prefixes like "4. "
    title = re.sub(r"^\d+\.\s*", "", title)
    # Collapse whitespace
    title = re.sub(r"\s+", " ", title).strip()
    return title or filename


def _extract_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using PyMuPDF."""
    if not HAS_PDF:
        return ""
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        parts = []
        for page in doc:
            t = page.get_text()
            if t:
                parts.append(t)
        doc.close()
        return "\n".join(parts).strip()
    except Exception as e:
        logger.debug("PDF extraction failed: %s", e)
        return ""


def _make_id(url: str) -> str:
    """Create a stable unique ID from the URL."""
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def fetch_all(sample: bool = False) -> Iterator[Dict[str, Any]]:
    """Yield normalized regulation records."""
    if not HAS_PDF:
        logger.error("PyMuPDF (fitz) not installed — cannot extract PDF text")
        return

    import warnings
    warnings.filterwarnings("ignore", message=".*urllib3.*")
    warnings.filterwarnings("ignore", category=DeprecationWarning)

    sess = _session()
    pdf_links = _get_pdf_links(sess)
    sample_limit = 15
    count = 0

    for url in pdf_links:
        filename = url.split("/")[-1]

        time.sleep(REQUEST_DELAY)
        try:
            r = sess.get(url, timeout=60)
            if r.status_code != 200:
                logger.warning("HTTP %d for %s", r.status_code, filename)
                continue
        except Exception as e:
            logger.warning("Download failed for %s: %s", filename, e)
            continue

        text = _extract_text(r.content)
        if len(text) < MIN_TEXT_CHARS:
            logger.debug("Skipping scanned PDF: %s (%d chars)", filename, len(text))
            continue

        title = _clean_title(filename)
        doc_id = _make_id(url)

        record = {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "url": url,
            "filename": filename,
        }

        count += 1
        yield record

        if sample and count >= sample_limit:
            logger.info("Sample limit reached (%d records)", count)
            return

    logger.info("Total records yielded: %d", count)


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
    parser = argparse.ArgumentParser(description="SO/CBS bootstrap")
    parser.add_argument("command", choices=["bootstrap"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (~15 records)")
    args = parser.parse_args()

    if args.command == "bootstrap":
        bootstrap(sample=args.sample)
