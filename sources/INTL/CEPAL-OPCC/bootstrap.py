#!/usr/bin/env python3
"""INTL/CEPAL-OPCC — CEPAL Parliamentary Observatory on Climate Change.

Fetches environmental legislation records from the CEPAL/ECLAC OPCC
KoboToolbox API, then extracts full text from attached PDFs hosted on
geo.cepal.org.
"""

import argparse
import json
import io
import os
import sys
import time
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests

SOURCE_ID = "INTL/CEPAL-OPCC"
API_URL = "https://api-kobo.cepal.org/api-kobo/data"
DATASET_ID = "auxwim84jdna7n95PoDqhG"
PDF_BASE = f"https://geo.cepal.org/kbtx/{DATASET_ID}"

SAMPLE_DIR = Path(__file__).parent / "sample"
RATE_LIMIT = 1.5  # seconds between PDF downloads

logger = logging.getLogger(__name__)


def _extract_pdf_text(pdf_bytes: bytes, max_pages: int = 200) -> str:
    """Extract text from PDF bytes using pdfminer."""
    try:
        from pdfminer.high_level import extract_text as _pdfminer_extract
        text = _pdfminer_extract(io.BytesIO(pdf_bytes), page_numbers=list(range(max_pages)))
        return (text or "").strip()
    except Exception as e:
        logger.warning("pdfminer failed: %s", e)
    # Fallback: PyPDF2
    try:
        import PyPDF2
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        parts = []
        for page in reader.pages[:max_pages]:
            t = page.extract_text()
            if t:
                parts.append(t)
        return "\n".join(parts).strip()
    except Exception as e:
        logger.warning("PyPDF2 failed: %s", e)
    return ""


def _get_file_field(rec: dict) -> str:
    """Return the PDF filename for a record, checking area-specific fields."""
    area = rec.get("legislative_area", "")
    if area == "law":
        return rec.get("environmental_legislation/file_law", "")
    elif area == "ccf":
        return rec.get("climate_change_framework/file_ccf", "")
    elif area == "bill":
        return rec.get("bill/file_bill", "")
    return ""


def _get_link_field(rec: dict) -> str:
    """Return the external link for a record."""
    area = rec.get("legislative_area", "")
    if area == "law":
        return rec.get("environmental_legislation/related_link_law", "")
    elif area == "bill":
        return rec.get("bill/related_link_bill", "")
    return ""


def _get_title(rec: dict) -> str:
    area = rec.get("legislative_area", "")
    if area == "law":
        return rec.get("environmental_legislation/title_law", "")
    elif area == "ccf":
        return rec.get("climate_change_framework/title_ccf", "")
    elif area == "bill":
        return rec.get("bill/title_bill", "")
    return ""


def _get_number(rec: dict) -> str:
    area = rec.get("legislative_area", "")
    if area == "law":
        return rec.get("environmental_legislation/number_law", "")
    elif area == "ccf":
        return rec.get("climate_change_framework/number_ccf", "")
    elif area == "bill":
        return rec.get("bill/number_bill", "")
    return ""


def _get_date(rec: dict) -> str:
    area = rec.get("legislative_area", "")
    if area == "law":
        return rec.get("environmental_legislation/date_start_law", "")
    elif area == "ccf":
        return rec.get("climate_change_framework/date_start_ccf", "")
    elif area == "bill":
        return rec.get("bill/date_start_bill", "")
    return ""


def _get_status(rec: dict) -> str:
    area = rec.get("legislative_area", "")
    if area == "law":
        return rec.get("environmental_legislation/status_law", "")
    elif area == "ccf":
        return rec.get("climate_change_framework/status_ccf", "")
    elif area == "bill":
        return rec.get("bill/status_bill", "")
    return ""


def _get_topics(rec: dict) -> str:
    area = rec.get("legislative_area", "")
    if area == "law":
        return rec.get("environmental_legislation/topics_law", "")
    elif area == "ccf":
        return rec.get("climate_change_framework/topics_ccf", "")
    elif area == "bill":
        return rec.get("bill/topics_bill", "")
    return ""


# Map 3-letter ISO codes from the API to 2-letter ISO codes
COUNTRY_MAP = {
    "arg": "AR", "brb": "BB", "bol": "BO", "bra": "BR", "vgb": "VG",
    "chl": "CL", "col": "CO", "cri": "CR", "cuw": "CW", "grd": "GD",
    "gtm": "GT", "guy": "GY", "msr": "MS", "lca": "LC", "tto": "TT",
    "tca": "TC", "ury": "UY", "mex": "MX", "pry": "PY", "ecu": "EC",
    "pan": "PA", "per": "PE", "ven": "VE", "hnd": "HN", "nic": "NI",
    "slv": "SV", "sur": "SR", "blz": "BZ",
}


def fetch_all_records(session: requests.Session) -> list[dict]:
    """Fetch all records from the CEPAL KoboToolbox API."""
    params = {
        "dataset": DATASET_ID,
        "content": "1",
        "survey": "1",
    }
    logger.info("Fetching all records from CEPAL OPCC API...")
    resp = session.get(API_URL, params=params, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    results = data.get("results", [])
    # Filter to approved records only
    approved = [
        r for r in results
        if isinstance(r.get("_validation_status"), dict)
        and r["_validation_status"].get("label") == "Approved"
    ]
    logger.info("Fetched %d total records, %d approved", len(results), len(approved))
    return approved


def download_pdf_text(session: requests.Session, filename: str) -> str:
    """Download a PDF from geo.cepal.org and extract text."""
    if not filename:
        return ""
    url = f"{PDF_BASE}/{filename}"
    try:
        resp = session.get(url, timeout=60)
        if resp.status_code != 200:
            logger.debug("PDF %s returned %d", filename, resp.status_code)
            return ""
        text = _extract_pdf_text(resp.content)
        if len(text) < 50:
            logger.debug("PDF %s: extracted only %d chars (likely scanned)", filename, len(text))
            return ""
        return text
    except Exception as e:
        logger.warning("PDF download/extract failed for %s: %s", filename, e)
        return ""


def normalize(rec: dict, text: str = "") -> dict:
    """Normalize a raw API record into the standard schema."""
    rec_id = str(rec.get("_id", ""))
    title = _get_title(rec)
    date = _get_date(rec)
    country_iso3 = rec.get("country_isocode", rec.get("country", ""))
    country_code = COUNTRY_MAP.get(country_iso3, country_iso3.upper()[:2])
    link = _get_link_field(rec)

    return {
        "_id": f"cepal-opcc-{rec_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date or None,
        "url": link or f"https://opcc.cepal.org/en/legislative-resources",
        "country_code": country_code,
        "legislative_area": rec.get("legislative_area", ""),
        "law_number": _get_number(rec),
        "status": _get_status(rec),
        "topics": _get_topics(rec),
    }


def fetch_all(session: requests.Session = None):
    """Yield all normalized records with full text from PDFs."""
    if session is None:
        session = requests.Session()
        session.headers["User-Agent"] = "LegalDataHunter/1.0 (research)"
    records = fetch_all_records(session)
    for rec in records:
        filename = _get_file_field(rec)
        text = ""
        if filename:
            text = download_pdf_text(session, filename)
            time.sleep(RATE_LIMIT)
        yield normalize(rec, text)


def fetch_updates(since: str, session: requests.Session = None):
    """Yield records submitted after `since` date."""
    if session is None:
        session = requests.Session()
        session.headers["User-Agent"] = "LegalDataHunter/1.0 (research)"
    records = fetch_all_records(session)
    for rec in records:
        sub_time = rec.get("_submission_time", "")
        if sub_time and sub_time >= since:
            filename = _get_file_field(rec)
            text = ""
            if filename:
                text = download_pdf_text(session, filename)
                time.sleep(RATE_LIMIT)
            yield normalize(rec, text)


def run_sample(max_records: int = 15):
    """Download sample records with full text from PDFs.

    Tries multiple PDFs to find ones with extractable text, aiming for
    variety across countries and legislative areas.
    """
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    # Clear old samples
    for f in SAMPLE_DIR.glob("*.json"):
        f.unlink()

    session = requests.Session()
    session.headers["User-Agent"] = "LegalDataHunter/1.0 (research)"

    records = fetch_all_records(session)
    with_pdf = [r for r in records if _get_file_field(r)]

    saved = 0
    tried = 0
    seen_countries = set()
    max_tries = min(len(with_pdf), max_records * 5)  # Try up to 5x target

    # First pass: one per country for diversity
    for rec in with_pdf:
        if saved >= max_records or tried >= max_tries:
            break
        country = rec.get("country_isocode", "")
        if country in seen_countries:
            continue
        filename = _get_file_field(rec)
        tried += 1
        text = download_pdf_text(session, filename)
        time.sleep(0.8)
        if not text:
            continue
        seen_countries.add(country)
        normalized = normalize(rec, text)
        out_path = SAMPLE_DIR / f"{normalized['_id']}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(normalized, f, ensure_ascii=False, indent=2)
        saved += 1
        logger.info(
            "[%d/%d] %s — %s (%d chars)",
            saved, max_records, normalized["country_code"],
            normalized["title"][:50], len(text),
        )

    # Second pass: fill remaining from any country
    for rec in with_pdf:
        if saved >= max_records or tried >= max_tries:
            break
        rec_id = rec.get("_id")
        if any(rec_id == r.get("_id") for r in with_pdf[:tried]):
            # Skip already-tried in first pass (approximate)
            pass
        filename = _get_file_field(rec)
        tried += 1
        text = download_pdf_text(session, filename)
        time.sleep(0.8)
        if not text:
            continue
        normalized = normalize(rec, text)
        out_path = SAMPLE_DIR / f"{normalized['_id']}.json"
        if out_path.exists():
            continue
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(normalized, f, ensure_ascii=False, indent=2)
        saved += 1
        logger.info(
            "[%d/%d] %s — %s (%d chars)",
            saved, max_records, normalized["country_code"],
            normalized["title"][:50], len(text),
        )

    logger.info("Saved %d sample records (%d PDFs tried) to %s", saved, tried, SAMPLE_DIR)
    return saved


def main():
    parser = argparse.ArgumentParser(description="CEPAL OPCC bootstrap")
    parser.add_argument("command", choices=["bootstrap"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Run in sample mode")
    parser.add_argument("--full", action="store_true", help="Run full fetch")
    parser.add_argument("--max", type=int, default=15, help="Max sample records")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.command == "bootstrap":
        if args.sample or not args.full:
            count = run_sample(max_records=args.max)
            print(f"Sample complete: {count} records saved")
        else:
            count = 0
            for rec in fetch_all():
                count += 1
                if count % 100 == 0:
                    logger.info("Processed %d records", count)
            print(f"Full fetch complete: {count} records")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
