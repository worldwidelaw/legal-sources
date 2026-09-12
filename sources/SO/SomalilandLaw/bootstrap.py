#!/usr/bin/env python3
"""
SO/SomalilandLaw — Somaliland Law (somalilandlaw.com)

Crawls somalilandlaw.com — a comprehensive collection of Somaliland legislation
maintained by the International Journal of Somaliland Law. Covers constitution,
penal code, civil code, election laws, judiciary laws, and 600+ legal PDFs.

Data source: http://www.somalilandlaw.com/
Method: Crawl topic pages → discover PDF links → download + extract text
License: Open academic access (government legislation)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 key English law PDFs
  python bootstrap.py bootstrap --full      # Full bootstrap (all PDFs)
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

BASE_URL = "http://www.somalilandlaw.com"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "SO/SomalilandLaw"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
}

RATE_LIMIT_DELAY = 1.5

# Topic pages to crawl for PDF links
TOPIC_PAGES = [
    "somaliland_constitution.htm",
    "somaliland_constitution_1960.HTM",
    "criminal_law.html",
    "criminal_procedure_law.html",
    "civil_law_.html",
    "civil_procedure_law.html",
    "organisation_of_judiciary_law.HTM",
    "electoral_laws.html",
    "administrative_law.html",
    "banking___finance_laws.html",
    "business___the_law.html",
    "citizenship_law.htm",
    "commercial_law.htm",
    "communications_laws.html",
    "education_law.html",
    "environmental_laws.html",
    "evidence_law.html",
    "family___personal_law.html",
    "labour_employment_law.html",
    "land___planning_law.htm",
    "local_government_law.htm",
    "maritime_law.htm",
    "military_law.html",
    "miscellaneous_laws.html",
    "police_law_.html",
    "press___media__law.htm",
    "prison_law.html",
    "somaliland_company_law.html",
    "somaliland_human_rights_law.html",
    "somaliland_intellectual_proper.html",
    "somaliland_livestock_laws.html",
    "somaliland_mining_laws.html",
    "somaliland_ngos_law.html",
    "somaliland_public_finance_law.html",
    "somaliland_public_safety_laws.html",
    "somaliland_utilities_laws.html",
    "transport___traffic_law.html",
    "foreign_investment_law_2004.htm",
    "standing_rules_of_parliament_.html",
    "voter_registration_law.HTM",
    "insurance_companies_bill.htm",
    "somaliland_official_gazette.html",
    "somaliland_bills.html",
    "sharia__-_source_of_law.html",
    "somaliland_protectorate_laws.html",
    "somali_rep_60-89_laws.html",
    "somaliland_customary_law.html",
    "_international_law.html",
]

# Key English-language legislation PDFs for sample mode
SAMPLE_PDFS = [
    "Somaliland_Constitution_Text_only_Eng_IJSLL2.pdf",
    "Penal_Code_English.pdf",
    "Organisation_of_Judiciary_Law_1962_English.pdf",
    "Somaliland_Companies_Law_2004_SLLaw_text.pdf",
    "NGO_LAW_ENGLISH_VERSION_FINAL.pdf",
    "Election_Law_no_20_As_amended_2009.pdf",
    "Somaliland_Civil_Service_Law_1996_Eng.pdf",
    "Somaliland_Telecommunications_Law_as_passed_by_HR_April_2011.pdf",
    "Somaliland_Police_Force_2017_Law___Compilation_300319F.pdf",
    "General_Law_National_Elections___Voter_Registration_No_91-2020_Unofficial-translation.pdf",
    "Somaliland_Transfer_of_Prisoners_Law_Eng_21032012.pdf",
    "Somaliland_Press___Publications_Bill_English.pdf",
    "Military_Criminal_Code_in_Peace_1963.pdf",
    "Election_Law_2001___all_Amends_1-5_2012Final.pdf",
    "Somaliland_Voter_Registration_Law_2007_As_amended.pdf",
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


def discover_pdfs(session: requests.Session) -> list:
    """Crawl all topic pages and discover unique PDF links."""
    all_pdfs = set()
    for page in TOPIC_PAGES:
        try:
            resp = session.get(f"{BASE_URL}/{page}", headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                continue
            pdfs = re.findall(r'href="([^"]*\.pdf)"', resp.text, re.IGNORECASE)
            for p in pdfs:
                p = p.replace("./", "")
                if p.startswith("http"):
                    continue
                if p.startswith("C:\\"):
                    continue
                all_pdfs.add(p)
            time.sleep(0.5)
        except Exception as e:
            print(f"  WARN: Failed to crawl {page}: {e}")
    return sorted(all_pdfs)


def title_from_filename(filename: str) -> str:
    """Generate a readable title from a PDF filename."""
    name = filename.replace(".pdf", "")
    name = re.sub(r'_+', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def normalize(filename: str, text: str, idx: int) -> dict:
    """Create a normalized record."""
    doc_id = re.sub(r'[^\w-]', '_', filename.replace(".pdf", ""))
    title = title_from_filename(filename)

    year_match = re.search(r'(19\d{2}|20\d{2})', filename)
    date = f"{year_match.group(1)}-01-01" if year_match else None

    return {
        "_id": f"so-sll-{idx:04d}-{doc_id[:80]}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date,
        "url": f"{BASE_URL}/{filename}",
        "country": "SO",
        "language": "en",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch Somaliland law PDFs and extract text."""
    session = requests.Session()

    if sample:
        pdf_list = SAMPLE_PDFS
        print(f"Sample mode: fetching {len(pdf_list)} key English legislation PDFs...")
    else:
        print("Discovering PDFs from all topic pages...")
        pdf_list = discover_pdfs(session)
        print(f"Found {len(pdf_list)} unique PDFs. Downloading...")

    count = 0
    errors = 0

    for idx, filename in enumerate(pdf_list):
        url = f"{BASE_URL}/{filename}"
        print(f"  [{idx+1}/{len(pdf_list)}] {filename}")

        try:
            resp = session.get(url, headers=HEADERS, timeout=120)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"    ERROR downloading: {e}")
            errors += 1
            continue

        if len(resp.content) < 500:
            print(f"    SKIP: too small ({len(resp.content)} bytes)")
            continue

        try:
            text = extract_pdf_text(resp.content)
        except Exception as e:
            print(f"    ERROR extracting: {e}")
            errors += 1
            continue

        if not text or len(text) < 50:
            print(f"    SKIP: insufficient text ({len(text) if text else 0} chars)")
            continue

        record = normalize(filename, text, count + 1)
        count += 1
        print(f"    OK: {len(text)} chars")
        yield record

        time.sleep(RATE_LIMIT_DELAY)

    print(f"\nTotal: {count} records with full text ({errors} errors)")


def test_connectivity() -> bool:
    """Test that the site is accessible."""
    try:
        resp = requests.get(f"{BASE_URL}/", headers=HEADERS, timeout=15)
        resp.raise_for_status()
        print(f"Connectivity OK: status {resp.status_code}, {len(resp.content)} bytes")
        return True
    except Exception as e:
        print(f"Connectivity FAILED: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="SO/SomalilandLaw bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample records")
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
            safe_id = re.sub(r'[^\w-]', '_', record["_id"])[:120]
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
