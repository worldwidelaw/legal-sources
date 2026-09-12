#!/usr/bin/env python3
"""
LY/LibyanLawSociety — Libyan Law and Society Academic Collection

Fetches Libyan legal documents from the Leiden/Benghazi academic collaboration.
Constitutional declarations, court rulings, decrees, laws. PDFs in English.
~17 hard-to-find documents covering Libya's constitutional and legislative history.

Data source: https://www.libyanlawandsociety.org/legal-documents
Method: PDF download + text extraction
License: Academic/Open Access

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import json
import re
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

import pdfplumber
import requests

BASE_URL = "https://www.libyanlawandsociety.org"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "LY/LibyanLawSociety"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
}

RATE_LIMIT_DELAY = 2.0

# All documents with titles, years, types, and PDF URLs
DOCUMENTS = [
    ("Challenges for the Libyan Judiciary", 2016, "doctrine",
     "assets/legal-documents/ICJ-2016-Challenges-for-the-Libyan-Judiciary-ensuring-independence-accountability-and-gender-equality.pdf"),
    ("Constitutional Declaration of 1969", 1969, "legislation",
     "assets/legal-documents/6-Constitutional-Declaration-of-1969_EN.pdf"),
    ("Constitutional Declaration of 2011 (Consolidated)", 2014, "legislation",
     "assets/legal-documents/2-Constitutional-Declaration-of-2011_EN_Consolidated.pdf"),
    ("Decree 33/2014 on Assigning Revolutionaries", 2014, "legislation",
     "assets/legal-documents/GNC-2014-Decree-33-on-assigning-revolutionaries-affiliated-under-the-legitimacy-of-the-Libyan-state-to-protect-the-revolution.pdf"),
    ("Draft Law on Reconciliation", 2022, "legislation",
     "assets/legal-documents/Draft-law-on-reconciliation.pdf"),
    ("Independence Constitution of 1951", 1951, "legislation",
     "assets/legal-documents/13-Constitution-of-1951_EN.pdf"),
    ("Law 11/2012 on Powers of Command Levels in the Libyan Army", 2012, "legislation",
     "assets/legal-documents/NTC-2012-Law-11-Concerning-powers-of-command-levels-in-Libyan-Army.pdf"),
    ("Law 17/2012 on National Reconciliation and Transitional Justice", 2012, "legislation",
     "assets/legal-documents/311-Law-No.-17-of-2012_EN.pdf"),
    ("Law 29/2013 on Transitional Justice", 2013, "legislation",
     "assets/legal-documents/631-Law-No.-29-of-2013_EN.pdf"),
    ("Law 59/2012 on the Local Administration System", 2012, "legislation",
     "assets/legal-documents/62-Law-No.59-of-2012_EN.pdf"),
    ("Libya - Supporting Justice and Security Through Property Rights", 2013, "doctrine",
     "assets/legal-documents/USAID-2013-Libya-supporting-the-justice-and-security-sector-through-property-rights.pdf"),
    ("Proposal of a Consolidated Draft Constitution (2017)", 2017, "legislation",
     "assets/legal-documents/229-2017-Draft-Constitution_ENG.pdf"),
    ("Resolution 127/2013 on Dissolving Armed Units", 2013, "legislation",
     "assets/legal-documents/GNC-2013-Res-127-On-dissolving-armed-units-and-adopting-certain-provisions.pdf"),
    ("Constitution of the Libyan Arab Jamahiriya", 1977, "legislation",
     "assets/legal-documents/8-Constitution-of-the-Great-Socialist-Peoples-Republic-of-the-Libyan-Arab-Jamahiriya_EN.pdf"),
    ("Great Green Charter of Human Rights (1988)", 1988, "legislation",
     "assets/legal-documents/10-Green-charter_EN.pdf"),
    ("Strategic Vision for National Reconciliation", 2022, "legislation",
     "assets/legal-documents/Reconciliation-Vision-PC2-English.pdf"),
    ("Transitional Justice: Evolutions, Challenges and Way Forward in Libya", 2021, "doctrine",
     "assets/legal-documents/Maghur-and-Marghani-2020-Transitional-Justice.pdf"),
]


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    with tempfile.NamedTemporaryFile(suffix=".pdf") as tmp:
        tmp.write(pdf_bytes)
        tmp.flush()
        with pdfplumber.open(tmp.name) as pdf:
            pages = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages.append(text.strip())
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            return "\n\n".join(pages)


def normalize(title: str, year: int, doc_type: str, pdf_path: str, text: str, idx: int) -> dict:
    """Create a normalized record."""
    doc_id = re.sub(r'[^\w-]', '_', pdf_path.split("/")[-1].replace(".pdf", ""))

    return {
        "_id": f"ly-lls-{idx:02d}-{doc_id}",
        "_source": SOURCE_ID,
        "_type": doc_type,
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": f"{year}-01-01",
        "url": f"{BASE_URL}/{pdf_path}",
        "country": "LY",
        "language": "en",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all Libyan legal document PDFs and extract text."""
    session = requests.Session()
    max_docs = 12 if sample else len(DOCUMENTS)
    count = 0

    print(f"Fetching Libyan legal documents ({'sample' if sample else 'full'} mode)...")

    for idx, (title, year, doc_type, pdf_path) in enumerate(DOCUMENTS):
        if count >= max_docs:
            break

        url = f"{BASE_URL}/{pdf_path}"
        print(f"  [{idx+1}/{len(DOCUMENTS)}] Downloading: {title}")

        try:
            resp = session.get(url, headers=HEADERS, timeout=120)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"    ERROR downloading: {e}")
            continue

        if len(resp.content) < 1000:
            print(f"    SKIP: file too small ({len(resp.content)} bytes)")
            continue

        try:
            text = extract_pdf_text(resp.content)
        except Exception as e:
            print(f"    ERROR extracting text: {e}")
            continue

        if not text or len(text) < 100:
            print(f"    SKIP: insufficient text ({len(text) if text else 0} chars)")
            continue

        record = normalize(title, year, doc_type, pdf_path, text, idx + 1)
        count += 1
        print(f"    OK: {len(text)} chars extracted")
        yield record

        time.sleep(RATE_LIMIT_DELAY)

    print(f"\nTotal: {count} records with full text")


def test_connectivity() -> bool:
    """Test that the site is accessible."""
    try:
        resp = requests.get(f"{BASE_URL}/legal-documents", headers=HEADERS, timeout=30)
        resp.raise_for_status()
        print(f"Connectivity OK: status {resp.status_code}")
        return True
    except Exception as e:
        print(f"Connectivity FAILED: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="LY/LibyanLawSociety bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    parser.add_argument("--full", action="store_true", help="Full bootstrap")
    args = parser.parse_args()

    if args.command == "test":
        ok = test_connectivity()
        sys.exit(0 if ok else 1)

    if args.command == "bootstrap":
        sample_mode = args.sample or not args.full
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

        records = []
        for record in fetch_all(sample=sample_mode):
            records.append(record)
            safe_id = re.sub(r'[^\w-]', '_', record["_id"])
            out_path = SAMPLE_DIR / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"\nSaved {len(records)} records to {SAMPLE_DIR}/")

        if records:
            text_lens = [len(r["text"]) for r in records]
            print(f"Text lengths: min={min(text_lens)}, max={max(text_lens)}, avg={sum(text_lens)//len(text_lens)}")
        else:
            print("WARNING: No records fetched!")
            sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
