#!/usr/bin/env python3
"""
PA/SBP-Regulations -- Superintendencia de Bancos de Panamá (banking supervisor)

Fetches the binding prudential regulations ("Acuerdos") issued by the Junta
Directiva of Panama's banking superintendency, covering banking, fiduciary, and
AML/CFT-prevention rulebooks. Each acuerdo is published as a born-digital PDF on
superbancos.gob.pa, listed in Drupal "accordion" views at:
  /acuerdos/bancarios   (banking acuerdos)
  /acuerdos/fiduciarios (fiduciary acuerdos)
  /acuerdos/prevencion  (AML/CFT prevention acuerdos)

The scraper parses each listing page to recover, per row, the acuerdo number,
the official description (purpose + Gaceta Oficial reference), and the PDF URL,
then downloads the PDF and extracts its full text with pdfplumber.

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
"""

import argparse
import html as ihtml
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

try:
    import pdfplumber
except ImportError:
    print("ERROR: pdfplumber not installed. Run: pip3 install pdfplumber")
    sys.exit(1)

SOURCE_ID = "PA/SBP-Regulations"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PA.SBP-Regulations")

BASE = "https://www.superbancos.gob.pa"

# Listing pages (relative path -> human label).
LISTINGS = {
    "/acuerdos/bancarios": "Acuerdos Bancarios",
    "/acuerdos/fiduciarios": "Acuerdos Fiduciarios",
    "/acuerdos/prevencion": "Acuerdos Prevención",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (LegalDataHunter/1.0; legal research project)",
    "Accept": "text/html,application/xhtml+xml",
}

REQUEST_DELAY = 1.0
MIN_TEXT_CHARS = 400

# One accordion row = number (col-sm-2) + description (col-sm-9) + own PDF (col-sm-1).
ROW_RE = re.compile(
    r'col-sm-2"><b>(?P<num>.*?)</b>.*?'
    r'col-sm-9">(?P<desc>.*?)</div>\s*'
    r'<div class="col-sm-1">\s*<a href="(?P<pdf>[^"]+\.pdf)"',
    re.S,
)

MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}


def strip_html(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = ihtml.unescape(s)
    return re.sub(r"\s+", " ", s).strip()


def clean_text(s: str) -> str:
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n\s*\n+", "\n\n", s)
    return s.strip()


def _date_from_match(m) -> Optional[str]:
    day, month_name, year = m.group(1), m.group(2).lower(), m.group(3)
    month = MONTHS.get(month_name)
    if month:
        try:
            return datetime(int(year), month, int(day)).date().isoformat()
        except ValueError:
            return None
    return None


def parse_spanish_date(text: str) -> Optional[str]:
    """Find a '(DD de MONTH de YYYY)' date and return ISO format."""
    m = re.search(r"(\d{1,2})\s+de\s+([a-záéíóú]+)\s+de\s+(\d{4})", text, re.I)
    return _date_from_match(m) if m else None


def parse_acuerdo_date(text: str) -> Optional[str]:
    """Extract the acuerdo's own enactment date: the parenthetical date that
    immediately follows the 'ACUERDO No. NN-YYYY' header. Avoids grabbing dates
    of laws/decrees merely cited in the body."""
    m = re.search(
        r"ACUERDO\s+No\.?\s*[\w\-]+\s*\(?\s*(\d{1,2})\s+de\s+([a-záéíóú]+)\s+de\s+(\d{4})",
        text, re.I,
    )
    return _date_from_match(m) if m else None


def extract_pdf_text(pdf_bytes: bytes) -> str:
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            parts = [(pg.extract_text() or "").strip() for pg in pdf.pages]
            return clean_text("\n\n".join(p for p in parts if p))
    except Exception as e:  # noqa: BLE001
        logger.warning(f"PDF parse failed: {e}")
        return ""


def get_listing_rows(path: str) -> list:
    """Return [(number, description, pdf_url), ...] for one listing page."""
    try:
        resp = requests.get(BASE + path, headers=HEADERS, timeout=90)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to load listing {path}: {e}")
        return []

    rows = []
    seen = set()
    for m in ROW_RE.finditer(resp.text):
        pdf = m.group("pdf")
        if pdf in seen:
            continue
        seen.add(pdf)
        rows.append((strip_html(m.group("num")), strip_html(m.group("desc")), pdf))
    return rows


def normalize(number: str, desc: str, pdf_url: str, text: str, listing: str) -> dict:
    url = pdf_url if pdf_url.startswith("http") else BASE + pdf_url
    # Title = "Acuerdo No. N (YYYY) — purpose" (purpose = quoted lead of desc).
    purpose = desc
    qm = re.search(r"[\"“](.+?)[\"”]", desc)
    if qm:
        purpose = qm.group(1).strip()
    title = f"{number} — {purpose}" if purpose else number

    # Gaceta Oficial reference, if present.
    go = ""
    gm = re.search(r"G\.?O\.?\s*([\d\-A-Za-z]+(?:\s+de\s+[^.]+?\d{4})?)", desc)
    if gm:
        go = gm.group(0).strip()

    # Stable id from filename.
    fname = pdf_url.rsplit("/", 1)[-1].replace(".pdf", "")

    # Date: prefer the acuerdo's own header date, then the G.O. date in the
    # listing description, then any date near the top of the PDF.
    header_date = parse_acuerdo_date(text[:1200])
    go_date = parse_spanish_date(desc)
    # Sanity guard: the header regex occasionally locks onto a date cited in the
    # body (e.g. "Decreto Ley No. 2 de 22 de febrero de 2008"). If the parsed
    # year disagrees with the acuerdo's own year (taken from the filename
    # "Acuerdo_NN-YYYY"), fall back to the Gaceta Oficial publication date.
    ym = re.search(r"(\d{4})", fname)
    expected_year = ym.group(1) if ym else None
    if header_date and expected_year and header_date[:4] != expected_year:
        header_date = None
    date_iso = header_date or go_date or parse_spanish_date(text[:300])
    return {
        "_id": f"PA-SBP-{fname}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "description": desc,
        "date": date_iso,
        "url": url,
        "number": number,
        "gaceta_oficial": go,
        "category": listing,
        "language": "spa",
        "publisher": "Superintendencia de Bancos de Panamá",
        "country": "PA",
    }


def fetch_all(limit: Optional[int] = None) -> Generator[dict, None, None]:
    total = 0
    seen_pdfs = set()
    for path, label in LISTINGS.items():
        logger.info(f"Loading listing: {label} ({path})")
        rows = get_listing_rows(path)
        logger.info(f"  {len(rows)} acuerdos listed")
        for number, desc, pdf in rows:
            if pdf in seen_pdfs:
                continue
            seen_pdfs.add(pdf)
            url = pdf if pdf.startswith("http") else BASE + pdf
            try:
                resp = requests.get(url, headers={"User-Agent": HEADERS["User-Agent"]}, timeout=90)
                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.warning(f"PDF download failed {url}: {e}")
                time.sleep(REQUEST_DELAY)
                continue
            text = extract_pdf_text(resp.content)
            time.sleep(REQUEST_DELAY)
            if len(text) < MIN_TEXT_CHARS:
                logger.debug(f"Skipping (no text layer): {url}")
                continue
            yield normalize(number, desc, pdf, text, label)
            total += 1
            if limit and total >= limit:
                logger.info(f"Reached limit of {limit} records")
                return


def fetch_updates(since: str, limit: Optional[int] = None) -> Generator[dict, None, None]:
    """SBP listings carry no per-document timestamps; emit acuerdos whose
    parsed date is on/after `since`."""
    for rec in fetch_all(limit=None):
        if rec.get("date") and rec["date"] >= since:
            yield rec


def test_api():
    logger.info("Testing SBP listing pages...")
    try:
        grand = 0
        for path, label in LISTINGS.items():
            rows = get_listing_rows(path)
            grand += len(rows)
            logger.info(f"  {label}: {len(rows)} acuerdos")
        logger.info(f"Total acuerdos listed: {grand}")

        logger.info("\nFetching one sample acuerdo with full text...")
        for rec in fetch_all(limit=1):
            logger.info(f"  {rec['title'][:90]}  ({len(rec['text']):,} chars)")
            logger.info(f"  date={rec['date']}  GO={rec['gaceta_oficial']}")
            logger.info(f"  Preview: {rec['text'][:200]}...")
    except Exception as e:  # noqa: BLE001
        logger.error(f"API test failed: {e}")
        sys.exit(1)


def bootstrap(sample: bool = False, full: bool = False):
    limit = 15 if sample else None
    out_dir = SAMPLE_DIR if sample else SOURCE_DIR / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    records, text_lengths = [], []
    for record in fetch_all(limit=limit):
        records.append(record)
        text_lengths.append(len(record.get("text", "")))
        safe_id = record["_id"].replace("/", "_").replace(" ", "_")
        with open(out_dir / f"{safe_id}.json", "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {record['_id']} ({len(record.get('text', '')):,} chars)")

    if records:
        avg = sum(text_lengths) / len(text_lengths)
        logger.info(f"\n{'='*60}")
        logger.info(f"Total records: {len(records)}")
        logger.info(f"Avg text length: {avg:,.0f} chars")
        logger.info(f"Min/Max: {min(text_lengths):,}/{max(text_lengths):,} chars")
        logger.info(f"Records with text: {sum(1 for t in text_lengths if t > 0)}/{len(records)}")
        logger.info(f"Output directory: {out_dir}")
    else:
        logger.warning("No records fetched!")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    parser = argparse.ArgumentParser(description="PA/SBP-Regulations data fetcher")
    parser.add_argument("command", choices=["test-api", "bootstrap"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--since", help="For updates: ISO date (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.command == "test-api":
        test_api()
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample, full=args.full)
