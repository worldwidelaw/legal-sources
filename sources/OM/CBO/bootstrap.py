#!/usr/bin/env python3
"""
OM/CBO — Central Bank of Oman: Regulations & Circulars

Fetches circulars from the CBO SharePoint document libraries (English + Global),
downloads each PDF, and extracts full text with PyMuPDF.

Strategy:
  1. Use SharePoint REST API to enumerate year-folders under English/Circulars
     and Global/Circulars.
  2. For each folder, list PDF files via the Files endpoint.
  3. Download each PDF and extract text with fitz (PyMuPDF).
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
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import requests

try:
    import fitz  # PyMuPDF
    HAS_PDF = True
except ImportError:
    HAS_PDF = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SOURCE_ID = "OM/CBO"
SAMPLE_DIR = Path(__file__).parent / "sample"
BASE_URL = "https://cbo.gov.om"
SP_API = BASE_URL + "/sites/assets/_api/web"
REQUEST_DELAY = 1.5
MIN_TEXT_CHARS = 50

LIBRARIES = [
    "English/Circulars",
    "Global/Circulars",
]


def _sp_headers() -> dict:
    return {"Accept": "application/json;odata=verbose"}


def _session() -> requests.Session:
    s = requests.Session()
    s.verify = False
    s.headers.update(_sp_headers())
    return s


def _list_subfolders(sess: requests.Session, library: str) -> List[str]:
    """List year-subfolders under a document library."""
    encoded = urllib.parse.quote(f"/sites/assets/Documents/{library}", safe="")
    url = f"{SP_API}/GetFolderByServerRelativeUrl('{encoded}')/Folders"
    try:
        r = sess.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        results = data.get("d", {}).get("results", [])
        return [f["Name"] for f in results if f.get("Name")]
    except Exception as e:
        logger.warning("Failed to list folders for %s: %s", library, e)
        return []


def _list_files(sess: requests.Session, library: str, folder: str) -> List[Dict]:
    """List files in a specific year-folder."""
    path = f"/sites/assets/Documents/{library}/{folder}"
    encoded = urllib.parse.quote(path, safe="")
    url = f"{SP_API}/GetFolderByServerRelativeUrl('{encoded}')/Files"
    try:
        r = sess.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        return data.get("d", {}).get("results", [])
    except Exception as e:
        logger.warning("Failed to list files for %s/%s: %s", library, folder, e)
        return []


def _extract_date_from_name(name: str) -> Optional[str]:
    """Try to extract an ISO date from the filename."""
    # Pattern: YYYY-MM-DD at start
    m = re.match(r"(\d{4}-\d{2}-\d{2})", name)
    if m:
        return m.group(1)
    # Pattern: D-M-YYYY at start
    m = re.match(r"(\d{1,2})-(\d{1,2})-(\d{4})", name)
    if m:
        day, month, year = m.groups()
        try:
            return f"{year}-{int(month):02d}-{int(day):02d}"
        except ValueError:
            pass
    return None


def _clean_title(name: str) -> str:
    """Derive a human-readable title from the PDF filename."""
    title = name
    if title.lower().endswith(".pdf"):
        title = title[:-4]
    # Remove leading date pattern
    title = re.sub(r"^\d{4}-\d{2}-\d{2}\s*", "", title)
    title = re.sub(r"^\d{1,2}-\d{1,2}-\d{4}\s*", "", title)
    # Clean up multiple spaces and dashes
    title = re.sub(r"\s+", " ", title).strip()
    title = re.sub(r"^[-–—]+\s*", "", title).strip()
    return title or name


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


def _make_id(library: str, folder: str, name: str) -> str:
    """Create a stable unique ID from library/folder/filename."""
    raw = f"{library}/{folder}/{name}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def fetch_all(sample: bool = False) -> Iterator[Dict[str, Any]]:
    """Yield normalized circular records."""
    if not HAS_PDF:
        logger.error("PyMuPDF (fitz) not installed — cannot extract PDF text")
        return

    import warnings
    warnings.filterwarnings("ignore", message=".*urllib3.*")
    warnings.filterwarnings("ignore", category=DeprecationWarning)

    sess = _session()
    count = 0
    sample_limit = 15

    for library in LIBRARIES:
        folders = _list_subfolders(sess, library)
        # Sort year folders descending so newest first for sampling
        year_folders = sorted(
            [f for f in folders if re.match(r"\d{4}$", f)],
            reverse=True,
        )
        # Also include non-year folders like "Booklet", "QR" — skip them
        logger.info("Library %s: %d year-folders", library, len(year_folders))

        for folder in year_folders:
            files = _list_files(sess, library, folder)
            logger.info("  %s/%s: %d files", library, folder, len(files))

            for fmeta in files:
                name = fmeta.get("Name", "")
                if not name.lower().endswith(".pdf"):
                    continue

                server_url = fmeta.get("ServerRelativeUrl", "")
                download_url = BASE_URL + server_url
                modified = fmeta.get("TimeLastModified", "")

                # Download PDF
                time.sleep(REQUEST_DELAY)
                try:
                    r = sess.get(download_url, timeout=60, headers={"Accept": "*/*"})
                    if r.status_code != 200:
                        logger.warning("HTTP %d for %s", r.status_code, name)
                        continue
                except Exception as e:
                    logger.warning("Download failed for %s: %s", name, e)
                    continue

                # Extract text
                text = _extract_text(r.content)
                if len(text) < MIN_TEXT_CHARS:
                    logger.debug("Skipping scanned PDF: %s (%d chars)", name, len(text))
                    continue

                # Build record
                date_str = _extract_date_from_name(name) or (
                    modified[:10] if modified else None
                )
                title = _clean_title(name)
                circ_id = _make_id(library, folder, name)

                record = {
                    "_id": circ_id,
                    "_source": SOURCE_ID,
                    "_type": "doctrine",
                    "_fetched_at": datetime.now(timezone.utc).isoformat(),
                    "title": title,
                    "text": text,
                    "date": date_str,
                    "url": download_url,
                    "library": library,
                    "year_folder": folder,
                    "filename": name,
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
    parser = argparse.ArgumentParser(description="OM/CBO bootstrap")
    parser.add_argument("command", choices=["bootstrap"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (~15 records)")
    args = parser.parse_args()

    if args.command == "bootstrap":
        bootstrap(sample=args.sample)
