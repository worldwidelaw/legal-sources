#!/usr/bin/env python3
"""
LA/UNDP-LaoLaws — UNDP Official English Translations of Lao PDR Laws

Fetches 33 major Lao PDR laws officially translated into English by UNDP/Singapore/NUS.
Covers constitution, business, criminal, civil procedure, family, property, tax law, etc.

Data source: https://www.luangprabang-laos.com/Lao-laws
Method: PDF download + text extraction
License: Public domain (official government legislation translations)

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

BASE_URL = "https://www.luangprabang-laos.com"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "LA/UNDP-LaoLaws"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
}

RATE_LIMIT_DELAY = 2.0

# All 33 law PDFs with titles and PDF paths
LAWS = [
    ("Explanatory Notes to Translation", "IMG/pdf/lao_laws_01_explanatory_notes_to_translation.pdf"),
    ("Business Law", "IMG/pdf/lao_laws_02_business_law.pdf"),
    ("Law on Civil Procedure", "IMG/pdf/lao_laws_03_law_on_civil_procedure.pdf"),
    ("Amended Constitution of the Lao PDR", "IMG/pdf/lao_laws_04_amended_constitution_of_the_lao_pdr.pdf"),
    ("Contract Law", "IMG/pdf/lao_laws_05_contract_law.pdf"),
    ("Family Law", "IMG/pdf/lao_laws_06_family_law.pdf"),
    ("Foreign Investment Law", "IMG/pdf/lao_laws_07_foreign_investment_law.pdf"),
    ("Law on Judgement Enforcement", "IMG/pdf/lao_laws_08_law_on_judgement_enforcement.pdf"),
    ("Law on Local Administration", "IMG/pdf/lao_laws_09_law_on_local_administration.pdf"),
    ("Law on the National Assembly", "IMG/pdf/lao_laws_10_law_on_the_national_assembly.pdf"),
    ("Law on Lao Nationality", "IMG/pdf/lao_laws_11_law_on_lao_nationality.pdf"),
    ("Penal Law", "IMG/pdf/lao_laws_12_penal_law.pdf"),
    ("Law on the People's Court", "IMG/pdf/lao_laws_13_law_on_the_people_court.pdf"),
    ("Property Law", "IMG/pdf/lao_laws_14_property_law.pdf"),
    ("Anti-Corruption Law and Decree", "IMG/pdf/lao_laws_15_anti_corruption_law_and_decree_final_140306.pdf"),
    ("Customs Law and Decree", "IMG/pdf/lao_laws_16_customs_law_and_decree_final_200306.pdf"),
    ("Insurance Law and Decree", "IMG/pdf/lao_laws_17_insurance_law_and_decree_final.pdf"),
    ("Labour Law and Decree", "IMG/pdf/lao_laws_18_labour_law_and_decree_final.pdf"),
    ("Land Law and Decree", "IMG/pdf/lao_laws_19_land_law_and_decree_final_200306.pdf"),
    ("Law on Agriculture and Decree", "IMG/pdf/lao_laws_20_law_on_agriculture_and_decree_final.pdf"),
    ("Criminal Procedure Law and Decree", "IMG/pdf/lao_laws_21_crim_pro_law_and_decree_final_200306.pdf"),
    ("Electricity Law and Decree", "IMG/pdf/lao_laws_22_electricity_law_and_decree_final.pdf"),
    ("Resolution of Economic Disputes Law and Decree", "IMG/pdf/lao_laws_23_resolution_ofecon_disp_law_and_decree_final_200306.pdf"),
    ("Secured Transaction Law and Decree", "IMG/pdf/lao_laws_24_secured_transaction_law_and_decree_final.pdf"),
    ("Telecommunications Law and Decree", "IMG/pdf/lao_laws_25_telecommunications_law_and_decree_final_200306.pdf"),
    ("Bankruptcy Law and Decree", "IMG/pdf/lao_laws_26_bankruptcy_law_and_decree_final_200306.pdf"),
    ("Women and Children Law and Decree", "IMG/pdf/lao_laws_27_women_and_children_law_and_decree_final.pdf"),
    ("Law on the Government of the Lao PDR and Decree", "IMG/pdf/lao_laws_28_law_on_the_government_of_the_lao_pdr_and_decree_final.pdf"),
    ("Prosecutor Law and Decree", "IMG/pdf/lao_laws_29_prosecutor_law_and_decree_final.pdf"),
    ("National Assembly Oversight and Decree", "IMG/pdf/lao_laws_30_na_oversight_and_decree_final_200306.pdf"),
    ("Law on Industrial Processing and Decree", "IMG/pdf/lao_laws_31_law_on_the_industrial_processing_and_decree_final.pdf"),
    ("National Budget Law and Decree", "IMG/pdf/lao_laws_32_national_budget_law_and_decree_final.pdf"),
    ("Tax Law and Decree", "IMG/pdf/lao_laws_33_tax_law_and_decree_final_200306.pdf"),
    ("Customary Law in Laos (2011)", "IMG/pdf/customary_law_laos_2011_english_master1.pdf"),
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


def normalize(title: str, pdf_path: str, text: str, idx: int) -> dict:
    """Create a normalized record from extracted PDF text."""
    doc_id = re.sub(r'[^\w-]', '_', pdf_path.split("/")[-1].replace(".pdf", ""))
    url = f"{BASE_URL}/{pdf_path}"

    return {
        "_id": f"la-undp-{idx:02d}-{doc_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": "2006-03-20",  # UNDP translation project date (March 2006)
        "url": url,
        "country": "LA",
        "language": "en",
        "translation_note": "Official English translation by UNDP/Singapore/NUS collaboration",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all Lao law PDFs, extract text, and yield normalized records."""
    session = requests.Session()
    max_docs = 15 if sample else len(LAWS)
    count = 0

    print(f"Fetching Lao PDR laws ({'sample' if sample else 'full'} mode)...")

    for idx, (title, pdf_path) in enumerate(LAWS):
        if count >= max_docs:
            break

        url = f"{BASE_URL}/{pdf_path}"
        print(f"  [{idx+1}/{len(LAWS)}] Downloading: {title}")

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

        record = normalize(title, pdf_path, text, idx + 1)
        count += 1
        print(f"    OK: {len(text)} chars extracted")
        yield record

        time.sleep(RATE_LIMIT_DELAY)

    print(f"\nTotal: {count} records with full text")


def test_connectivity() -> bool:
    """Test that the PDF download site is accessible."""
    session = requests.Session()
    try:
        url = f"{BASE_URL}/{LAWS[3][1]}"  # Constitution PDF
        resp = session.get(url, headers=HEADERS, timeout=30, stream=True)
        resp.raise_for_status()
        size = int(resp.headers.get("content-length", 0))
        print(f"Connectivity OK: Constitution PDF accessible ({size/1024:.0f} KB)")
        resp.close()
        return True
    except Exception as e:
        print(f"Connectivity FAILED: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="LA/UNDP-LaoLaws bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    parser.add_argument("--full", action="store_true", help="Full bootstrap (all records)")
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
