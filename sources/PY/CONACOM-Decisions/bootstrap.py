#!/usr/bin/env python3
"""
PY/CONACOM-Decisions -- Paraguay Competition Authority Decisions

Fetches competition-law case files (expedientes) from the Comisión Nacional de
la Competencia (CONACOM), Paraguay's competition authority, which enforces
Ley N° 4956/2013 de Defensa de la Competencia.

Data source: https://conacom.gov.py/
License: Open Government Data (Paraguay)

Strategy:
  - Two public "historial de expedientes" listing pages publish every case file
    that has documents already released:
      * Control de concentraciones: /concentraciones/expedientes/
      * Prácticas restrictivas:     /practicas/historial/
  - Each page is a set of accordion cards (one per expediente). Each card holds
    a tablepress table whose rows are individual documents:
        Fecha | ID (e.g. RAL 2017/001) | Descripción | Enlace (Google Drive PDF)
  - For each row we download the linked Google Drive PDF and extract its text.
  - CONACOM publishes a mix of digital (text-layer) PDFs and scanned image PDFs.
    Scanned PDFs yield no text and are skipped — only documents whose full text
    can actually be extracted are emitted.

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py bootstrap            # Full bootstrap
  python bootstrap.py test-api             # Quick connectivity test
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: beautifulsoup4 not installed. Run: pip3 install beautifulsoup4")
    sys.exit(1)

# Setup
SOURCE_ID = "PY/CONACOM-Decisions"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PY.CONACOM-Decisions")

# Configuration
BASE_URL = "https://conacom.gov.py"
# (path, category) for each listing page
LISTING_PAGES = [
    ("/practicas/historial/", "practicas"),
    ("/concentraciones/expedientes/", "concentraciones"),
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

RATE_LIMIT = 1.0            # seconds between PDF downloads
MIN_TEXT_CHARS = 500        # below this we treat the PDF as scanned / empty
SAMPLE_TARGET = 14          # how many text-bearing records to collect in sample mode
SAMPLE_MAX_ATTEMPTS = 170   # cap PDF downloads in sample mode so it terminates
SAMPLE_MAX_PDF_BYTES = 18_000_000   # skip huge (almost-certainly-scanned) PDFs in sample
FULL_MAX_PDF_BYTES = 48_000_000

DRIVE_RE = re.compile(r"drive\.google\.com/file/d/([A-Za-z0-9_-]+)")


def _drive_download_url(file_id: str) -> str:
    return f"https://drive.google.com/uc?export=download&id={file_id}"


def _norm_date(raw: str) -> Optional[str]:
    """CONACOM dates are 'YYYY/MM/DD'. Return ISO 8601 (YYYY-MM-DD) or None."""
    raw = (raw or "").strip()
    m = re.match(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", raw)
    if not m:
        return None
    y, mo, d = m.groups()
    try:
        return datetime(int(y), int(mo), int(d)).date().isoformat()
    except ValueError:
        return None


def _slug(text: str) -> str:
    text = re.sub(r"[^A-Za-z0-9]+", "-", text).strip("-")
    return text[:60] or "doc"


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def _parse_listing(session: requests.Session, path: str, category: str) -> list[dict]:
    """Parse one listing page into a list of document metadata dicts."""
    url = BASE_URL + path
    try:
        resp = session.get(url, timeout=(15, 60))
        resp.raise_for_status()
    except Exception as e:
        logger.warning("Failed to fetch listing %s: %s", url, e)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    cards = soup.select(".ea-card") or soup.select(".sp-ea-single")
    logger.info("%s — %d expediente cards", path, len(cards))

    rows: list[dict] = []
    for card in cards:
        header = card.select_one(".ea-header a") or card.select_one("h3")
        case_title = (
            re.sub(r"\s+", " ", header.get_text(strip=True)) if header else ""
        )
        for table in card.select("table"):
            for tr in table.select("tbody tr"):
                tds = tr.find_all("td")
                if len(tds) < 4:
                    continue
                link = tds[3].find("a", href=True)
                if not link:
                    continue
                m = DRIVE_RE.search(link["href"])
                if not m:
                    continue
                rows.append({
                    "category": category,
                    "case_title": case_title,
                    "date": tds[0].get_text(strip=True),
                    "doc_id": tds[1].get_text(strip=True),
                    "description": tds[2].get_text(strip=True),
                    "file_id": m.group(1),
                    "url": link["href"],
                })
    return rows


def list_documents(session: requests.Session) -> Generator[dict, None, None]:
    """Yield document metadata dicts, interleaving the two listing pages so a
    bounded sample spans both restrictive-practices and concentration cases.

    Each dict: {category, case_title, date, doc_id, description, file_id, url}
    """
    per_page = [_parse_listing(session, path, cat) for path, cat in LISTING_PAGES]
    i = 0
    while any(i < len(rows) for rows in per_page):
        for rows in per_page:
            if i < len(rows):
                yield rows[i]
        i += 1


def _download_pdf(session: requests.Session, file_id: str, max_bytes: int) -> Optional[bytes]:
    """Download a Google Drive PDF. Returns bytes, or None if too large /
    not a PDF (Drive virus-scan interstitial for big files) / failed."""
    url = _drive_download_url(file_id)
    try:
        resp = session.get(url, timeout=(15, 90), stream=True)
        resp.raise_for_status()
    except Exception as e:
        logger.warning("PDF fetch failed %s: %s", file_id, e)
        return None
    buf = bytearray()
    try:
        for chunk in resp.iter_content(chunk_size=65536):
            if not chunk:
                continue
            buf.extend(chunk)
            if len(buf) > max_bytes:
                logger.info("Skip %s — exceeds %d bytes (likely scanned)", file_id, max_bytes)
                return None
    except Exception as e:
        logger.warning("PDF read failed %s: %s", file_id, e)
        return None
    data = bytes(buf)
    if not data[:5].startswith(b"%PDF"):
        # Drive returned an HTML confirmation/interstitial page, not the PDF.
        logger.info("Skip %s — not a direct PDF (Drive interstitial)", file_id)
        return None
    return data


def normalize(meta: dict, text: str) -> dict:
    """Transform raw metadata + extracted text into the standard schema."""
    title_bits = [b for b in (meta.get("case_title"), meta.get("doc_id"),
                              meta.get("description")) if b]
    title = " — ".join(title_bits) if title_bits else (meta.get("doc_id") or "CONACOM")

    doc_slug = _slug(f"{meta.get('doc_id','')}-{meta.get('file_id','')}")
    _id = f"{meta['category']}-{doc_slug}"

    return {
        "_id": _id,
        "_source": "PY/CONACOM-Decisions",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": _norm_date(meta.get("date", "")),
        "url": meta.get("url"),
        "case": meta.get("case_title") or None,
        "resolution_id": meta.get("doc_id") or None,
        "description": meta.get("description") or None,
        "category": meta["category"],
        "jurisdiction": "PY",
        "language": "es",
        "publisher": "Comisión Nacional de la Competencia (CONACOM)",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    session = _session()
    max_bytes = SAMPLE_MAX_PDF_BYTES if sample else FULL_MAX_PDF_BYTES
    attempts = 0
    yielded = 0
    seen_ids = set()

    for meta in list_documents(session):
        # de-dup on file_id (same drive doc can appear under multiple rows)
        if meta["file_id"] in seen_ids:
            continue
        seen_ids.add(meta["file_id"])

        if sample and attempts >= SAMPLE_MAX_ATTEMPTS:
            break

        attempts += 1
        pdf_bytes = _download_pdf(session, meta["file_id"], max_bytes)
        time.sleep(RATE_LIMIT)
        if pdf_bytes is None:
            continue

        text = extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=normalize(meta, "")["_id"],
            pdf_bytes=pdf_bytes,
            table="case_law",
        )
        if not text or len(text.strip()) < MIN_TEXT_CHARS:
            logger.info("Skip %s (%s) — no extractable text (scanned)",
                        meta.get("doc_id"), meta["file_id"][:10])
            continue

        yielded += 1
        yield normalize(meta, text.strip())

        if sample and yielded >= SAMPLE_TARGET:
            break

    logger.info("Total records yielded: %d (from %d download attempts)", yielded, attempts)


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Yield documents whose date is on/after `since` (ISO 8601 YYYY-MM-DD)."""
    for record in fetch_all(sample=False):
        d = record.get("date")
        if d and since and d < since:
            continue
        yield record


def test_api() -> bool:
    """Quick connectivity test: list documents and extract one PDF."""
    session = _session()
    docs = list(list_documents(session))
    logger.info("Discovered %d document rows across listing pages", len(docs))
    if not docs:
        logger.error("No documents discovered — page structure may have changed")
        return False

    for meta in docs[:30]:
        pdf = _download_pdf(session, meta["file_id"], SAMPLE_MAX_PDF_BYTES)
        time.sleep(RATE_LIMIT)
        if not pdf:
            continue
        text = extract_pdf_markdown(
            source=SOURCE_ID, source_id="test", pdf_bytes=pdf, table="case_law"
        )
        if text and len(text.strip()) >= MIN_TEXT_CHARS:
            logger.info("Extracted %d chars from %s (%s)",
                        len(text), meta.get("doc_id"), meta["file_id"][:10])
            logger.info("Preview: %s", text.strip()[:200].replace("\n", " "))
            logger.info("API test passed!")
            return True
    logger.error("No text-bearing PDF found in first 30 documents")
    return False


def bootstrap(sample: bool = False) -> int:
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    output_dir = SAMPLE_DIR if sample else (SOURCE_DIR / "data")
    output_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for record in fetch_all(sample=sample):
        fname = f"{record['_id'].replace('/', '_')}.json"
        with open(output_dir / fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info("[%d] Saved: %s — %s", count, record["_id"], record["title"][:70])

    logger.info("Bootstrap complete: %d records saved to %s", count, output_dir)
    return count


def main():
    parser = argparse.ArgumentParser(description="PY/CONACOM-Decisions data fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch a small sample for validation")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command == "test-api":
        sys.exit(0 if test_api() else 1)
    elif args.command == "bootstrap":
        count = bootstrap(sample=args.sample)
        if count == 0:
            logger.error("No records fetched!")
            sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
