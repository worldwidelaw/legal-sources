#!/usr/bin/env python3
"""
KW/Legislation - Kuwait e-Government Laws and Regulations

Data source: https://e.gov.kw/sites/kgoenglish/Pages/Business/InfoSubPages/LawsAndRegulations.aspx
Format: Static PDF downloads
License: Public Domain (government legislation)
Records: ~20 key laws (Constitution, Labor, Commercial, Civil, Criminal, etc.)

The Kuwait e-Government portal publishes major laws as PDF files.
These cover commercial, civil, criminal, labor, environmental, and other areas.
Content is primarily in Arabic.
"""

import argparse
import io
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Generator, List

import pdfplumber
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

SOURCE_ID = "KW/Legislation"
BASE_URL = "https://e.gov.kw"
REQUEST_DELAY = 2.0

# All known law PDFs from the e.gov.kw Laws and Regulations page
LAWS = [
    {
        "id": "constitution",
        "title": "دستور الكويت (Constitution of Kuwait)",
        "title_en": "Constitution of Kuwait",
        "url": f"{BASE_URL}/sites/kgoEnglish/Forms/DastoorKuwaity.pdf",
        "category": "constitution",
    },
    {
        "id": "labor-law",
        "title": "قانون العمل في القطاع الأهلي (Labor Law in the Private Sector)",
        "title_en": "Labor Law (No. 6/2010)",
        "url": f"{BASE_URL}/sites/kgoenglish/Forms/KuwaitLaborLaw.pdf",
        "category": "labor",
    },
    {
        "id": "commercial-law",
        "title": "القانون التجاري (Commercial Law)",
        "title_en": "Commercial Law",
        "url": f"{BASE_URL}/sites/kgoenglish/Forms/KuwaitCommercialLaw.pdf",
        "category": "commercial",
    },
    {
        "id": "civil-law",
        "title": "القانون المدني (Civil Law)",
        "title_en": "Civil Law",
        "url": f"{BASE_URL}/sites/kgoenglish/Forms/QanoonMadani.pdf",
        "category": "civil",
    },
    {
        "id": "civil-procedures",
        "title": "قانون المرافعات المدنية والتجارية (Civil and Commercial Procedures Law)",
        "title_en": "Civil and Commercial Procedures Law",
        "url": f"{BASE_URL}/sites/kgoenglish/Forms/QanoonAlMurafaatAlMadaniyah.pdf",
        "category": "procedural",
    },
    {
        "id": "criminal-procedures",
        "title": "قانون الإجراءات والمحاكمات الجزائية (Criminal Procedures and Penal Trials Law)",
        "title_en": "Criminal Procedures and Penal Trials Law",
        "url": f"{BASE_URL}/sites/kgoenglish/Forms/QanoonAlJajaa.pdf",
        "category": "criminal",
    },
    {
        "id": "personal-status",
        "title": "قانون الأحوال الشخصية (Personal Status Law)",
        "title_en": "Personal Status Law",
        "url": f"{BASE_URL}/sites/kgoenglish/Forms/QanoonAlAhwalAlMadaniyah.pdf",
        "category": "family",
    },
    {
        "id": "commercial-companies",
        "title": "قانون الشركات التجارية (Commercial Companies Law)",
        "title_en": "Commercial Companies Law",
        "url": f"{BASE_URL}/sites/kgoenglish/Forms/QanoonSharekatTejaryieah.pdf",
        "category": "commercial",
    },
    {
        "id": "maritime-trade",
        "title": "قانون التجارة البحرية (Maritime Trade Law)",
        "title_en": "Maritime Trade Law",
        "url": f"{BASE_URL}/sites/kgoenglish/Forms/QanoonAlTejaraAlBahriyah.pdf",
        "category": "commercial",
    },
    {
        "id": "commerce-regulation",
        "title": "القوانين المنظمة للتجارة (Commerce and Standardization Law)",
        "title_en": "Commerce and Standardization Regulation",
        "url": f"{BASE_URL}/sites/kgoenglish/Forms/AlQawaneenAlMonadhama.pdf",
        "category": "commercial",
    },
    {
        "id": "copyright-law",
        "title": "قانون حقوق المؤلف (Copyright Act)",
        "title_en": "Copyright Act",
        "url": f"{BASE_URL}/sites/kgoenglish/Forms/KuwaitCopyrightLaw.pdf",
        "category": "intellectual_property",
    },
    {
        "id": "customs-law",
        "title": "قانون الجمارك (Customs Law)",
        "title_en": "Customs Law",
        "url": f"{BASE_URL}/sites/kgoenglish/Forms/KuwaitCustomsLaw.pdf",
        "category": "customs",
    },
    {
        "id": "cybercrime-law",
        "title": "قانون مكافحة جرائم تقنية المعلومات (Cybercrime Law No. 63/2015)",
        "title_en": "Cybercrime Law (No. 63/2015)",
        "url": f"{BASE_URL}/sites/kgoenglish/Forms/CAITLawNo.63of2015oncombatingInformationTechnologyCrimes.pdf",
        "category": "technology",
    },
    {
        "id": "environmental-protection",
        "title": "قانون حماية البيئة (Environmental Protection Law)",
        "title_en": "Environmental Protection Law",
        "url": f"{BASE_URL}/sites/kgoenglish/Forms/EPL.pdf",
        "category": "environment",
    },
    {
        "id": "capital-markets",
        "title": "قانون إنشاء هيئة أسواق المال (Capital Markets Authority Establishment Law)",
        "title_en": "Capital Markets Authority Establishment Law",
        "url": f"{BASE_URL}/sites/kgoenglish/Forms/CMACapitalMarketEstablishmentLaw.pdf",
        "category": "financial",
    },
    {
        "id": "electronic-transactions",
        "title": "قانون المعاملات الإلكترونية (Electronic Transactions Law No. 20/2014)",
        "title_en": "Electronic Transactions Law (No. 20/2014)",
        "url": f"{BASE_URL}/sites/kgoenglish/Forms/CAITLawNo20of2014electronictransactions.pdf",
        "category": "technology",
    },
    {
        "id": "special-needs",
        "title": "قانون ذوي الاحتياجات الخاصة (People with Special Needs Law)",
        "title_en": "People with Special Needs Law",
        "url": f"{BASE_URL}/sites/kgoenglish/Forms/GODPLAW.pdf",
        "category": "social",
    },
    {
        "id": "electronic-media",
        "title": "قانون الإعلام الإلكتروني (Electronic Media Law No. 8/2016)",
        "title_en": "Electronic Media Law (No. 8/2016)",
        "url": f"{BASE_URL}/sites/kgoenglish/Forms/MediaLaw082016.pdf",
        "category": "media",
    },
    {
        "id": "import-law",
        "title": "قانون الاستيراد (Import Law)",
        "title_en": "Import Law",
        "url": f"{BASE_URL}/sites/kgoenglish/Forms/ImportLaw.pdf",
        "category": "trade",
    },
    {
        "id": "commerce-law-alt",
        "title": "قانون التجارة (Commerce Law)",
        "title_en": "Commerce Law",
        "url": f"{BASE_URL}/sites/kgoenglish/Forms/QanoonAlTejara.pdf",
        "category": "commercial",
    },
]


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "application/pdf,*/*",
    })
    return session


def extract_pdf_text(pdf_bytes: bytes, source_id: str) -> str:
    """Extract full text from PDF bytes via the shared helper.

    This used to call pdfplumber directly. pdfplumber emits glyphs in the order
    the content stream lists them, which for these PDFs is *visual* order, so
    every Arabic line came out character-reversed — ``قانون`` stored as
    ``نوناق`` (issue #1560). Keyword search over the corpus then matches
    nothing, silently: the semantic half of a hybrid index still returns
    something, so the source looks healthy while ranking against gibberish.

    ``common.pdf_extract`` routes RTL-heavy output back through
    ``common.arabic_pdf``, which orders glyph clusters by x-geometry instead of
    trusting the emitted sequence, so going through the shared helper is what
    fixes the reading order. force=True because this corpus is 20 documents and
    a refresh must re-extract all of them rather than skip the ones already in
    Neon with (reversed) text.
    """
    try:
        text = extract_pdf_markdown(
            SOURCE_ID,
            source_id,
            pdf_bytes=pdf_bytes,
            table="legislation",
            force=True,
        )
    except Exception as e:
        print(f"  PDF extraction error: {e}", file=sys.stderr)
        return ""
    return text or ""


def normalize(law_info: Dict, full_text: str, pdf_pages: int) -> Dict:
    """Transform law data into standard schema."""
    return {
        "_id": f"KW-LEG-{law_info['id']}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": law_info["title"],
        "title_en": law_info.get("title_en", ""),
        "text": full_text,
        "date": None,
        "url": law_info["url"],
        "category": law_info.get("category", ""),
        "pages": pdf_pages,
        "language": "ar",
    }


def fetch_all() -> Generator[Dict, None, None]:
    """Fetch all law PDFs and extract full text."""
    session = get_session()

    for law in LAWS:
        print(f"  Downloading: {law['title_en']}...")
        try:
            resp = session.get(law["url"], timeout=60)
            resp.raise_for_status()
        except Exception as e:
            print(f"  ERROR downloading {law['id']}: {e}", file=sys.stderr)
            continue

        pdf_bytes = resp.content
        print(f"    Size: {len(pdf_bytes):,} bytes")

        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pdf_pages = len(pdf.pages)
        except Exception:
            pdf_pages = 0

        full_text = extract_pdf_text(pdf_bytes, f"KW-LEG-{law['id']}")
        if not full_text or len(full_text) < 100:
            print(f"  SKIPPING {law['id']} — insufficient text ({len(full_text)} chars)")
            continue

        print(f"    Pages: {pdf_pages} | Text: {len(full_text):,} chars")
        record = normalize(law, full_text, pdf_pages)
        yield record
        time.sleep(REQUEST_DELAY)


def fetch_updates(since: datetime) -> Generator[Dict, None, None]:
    """Static source — returns all records (no update tracking)."""
    yield from fetch_all()


def bootstrap_sample(sample_dir: Path, count: int = 15):
    """Fetch sample records for validation."""
    session = get_session()
    sample_dir.mkdir(parents=True, exist_ok=True)

    records_saved = 0
    total_text_chars = 0

    for law in LAWS:
        if records_saved >= count:
            break

        print(f"  [{records_saved + 1}/{count}] Downloading: {law['title_en']}...")
        try:
            resp = session.get(law["url"], timeout=60)
            resp.raise_for_status()
        except Exception as e:
            print(f"    ERROR: {e}")
            continue

        pdf_bytes = resp.content
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pdf_pages = len(pdf.pages)
        except Exception:
            pdf_pages = 0

        full_text = extract_pdf_text(pdf_bytes, f"KW-LEG-{law['id']}")
        text_len = len(full_text)

        if text_len < 100:
            print(f"    SKIPPED — insufficient text ({text_len} chars)")
            continue

        record = normalize(law, full_text, pdf_pages)
        total_text_chars += text_len
        records_saved += 1

        safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", record["_id"])[:100]
        filename = f"{safe_name}.json"
        filepath = sample_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"    Pages: {pdf_pages} | Text: {text_len:,} chars")

        time.sleep(REQUEST_DELAY)

    # Summary
    print("\n" + "=" * 60)
    print("SAMPLE SUMMARY")
    print("=" * 60)
    print(f"Records saved: {records_saved}")
    if records_saved > 0:
        avg_chars = total_text_chars // records_saved
        print(f"Total text chars: {total_text_chars:,}")
        print(f"Average text length: {avg_chars:,} chars/doc")
    print(f"Total laws in database: {len(LAWS)}")
    print(f"Sample directory: {sample_dir}")

    if records_saved >= 10:
        print("\nSUCCESS: 10+ sample records with full text")
    else:
        print(f"\nWARNING: Only {records_saved} records saved (need 10+)")


def main():
    parser = argparse.ArgumentParser(description="Kuwait Laws and Regulations Fetcher")
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records for validation")
    parser.add_argument("--count", type=int, default=15,
                        help="Number of sample records to fetch")
    parser.add_argument("--since", type=str,
                        help="Fetch updates since date (ISO format)")
    parser.add_argument("--full", action="store_true",
                        help="Run full bootstrap (all records)")

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(sample_dir, args.count)
        else:
            print("Running full bootstrap...")
            sample_dir.mkdir(parents=True, exist_ok=True)
            records_saved = 0
            for record in fetch_all():
                safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", record["_id"])[:100]
                filename = f"{safe_name}.json"
                filepath = sample_dir / filename

                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

                records_saved += 1

            print(f"\nFull bootstrap complete: {records_saved} records saved")

    elif args.command == "fetch":
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))

    elif args.command == "updates":
        if not args.since:
            print("ERROR: --since required for updates command")
            sys.exit(1)
        since = datetime.fromisoformat(args.since)
        for record in fetch_updates(since):
            print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
