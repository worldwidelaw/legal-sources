#!/usr/bin/env python3
"""
UN/TreatyCollection - United Nations Treaty Collection (UNTS) Fetcher

Fetches multilateral treaties deposited with the UN Secretary-General.
~419 treaties across 29 chapters (human rights, environment, trade, etc.).

Data sources:
  - Chapter listings: treaties.un.org/Pages/Treaties.aspx
  - Metadata XML: treaties.un.org/doc/Publication/MTDSG/Volume%20I/...
  - Treaty text PDFs: CTC PDFs from ViewDetails pages
  - Fallback: MTDSG status PDFs

Method: HTML scraping for treaty IDs → XML for metadata → PDF for full text
License: UN public domain
Auth: None

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample records
  python bootstrap.py bootstrap             # Full bootstrap (all ~419 treaties)
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import json
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

BASE_URL = "https://treaties.un.org"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "UN/TreatyCollection"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LegalDataHunter/1.0",
    "Accept": "text/html,application/xhtml+xml,application/xml",
}

RATE_LIMIT_DELAY = 2.0
NUM_CHAPTERS = 29

# Roman numeral conversion for chapter paths
ROMAN_NUMERALS = {
    1: "I", 2: "II", 3: "III", 4: "IV", 5: "V",
    6: "VI", 7: "VII", 8: "VIII", 9: "IX", 10: "X",
    11: "XI", 12: "XII", 13: "XIII", 14: "XIV", 15: "XV",
    16: "XVI", 17: "XVII", 18: "XVIII", 19: "XIX", 20: "XX",
    21: "XXI", 22: "XXII", 23: "XXIII", 24: "XXIV", 25: "XXV",
    26: "XXVI", 27: "XXVII", 28: "XXVIII", 29: "XXIX",
}


def fetch_url(url: str, timeout: int = 60) -> Optional[requests.Response]:
    """Fetch a URL with error handling."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        resp.raise_for_status()
        return resp
    except requests.RequestException as e:
        print(f"  Error fetching {url}: {e}")
        return None


def get_all_treaty_ids() -> list[dict]:
    """Fetch all treaty IDs and titles from chapter listing pages."""
    treaties = []
    for ch in range(1, NUM_CHAPTERS + 1):
        url = f"{BASE_URL}/Pages/Treaties.aspx?id={ch}&subid=A&clang=_en"
        resp = fetch_url(url)
        if not resp:
            continue

        # Extract mtdsg_no and title from links
        entries = re.findall(r'mtdsg_no=([^&"]+)[^>]*>([^<]+)</a>', resp.text)
        seen = set()
        for mtdsg_no, raw_title in entries:
            if mtdsg_no in seen:
                continue
            seen.add(mtdsg_no)
            # Clean title: remove &nbsp; and trailing location/date
            title = re.sub(r"&nbsp;", " ", raw_title).strip()
            treaties.append({"mtdsg_no": mtdsg_no, "chapter": ch, "listing_title": title})

        print(f"  Chapter {ch}: {len(seen)} treaties")
        time.sleep(RATE_LIMIT_DELAY)

    print(f"  Total: {len(treaties)} treaties")
    return treaties


def parse_xml_metadata(xml_text: str) -> dict:
    """Parse treaty metadata from MTDSG XML."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return {}

    meta = {}

    # Chapter info
    chapter_header = root.find(".//Chapter/Header")
    if chapter_header is not None:
        meta["chapter_label"] = chapter_header.text or ""

    chapter_name = root.find(".//Chapter/Name")
    if chapter_name is not None:
        meta["chapter_name"] = chapter_name.text or ""

    ext = root.find(".//ExternalData")
    if ext is None:
        return meta

    # Title
    title_el = ext.find("Titlesect")
    if title_el is not None and title_el.text:
        meta["title"] = title_el.text.strip()

    # Conclusion (place and date)
    conclusion_el = ext.find("Conclusion")
    if conclusion_el is not None and conclusion_el.text:
        raw = re.sub(r"<[^>]+>", "", conclusion_el.text).strip()
        meta["conclusion"] = raw
        # Extract date from conclusion text
        date_match = re.search(r"(\d{1,2}\s+\w+\s+\d{4})", raw)
        if date_match:
            try:
                dt = datetime.strptime(date_match.group(1), "%d %B %Y")
                meta["date"] = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

    # Entry into force
    eif = ext.find(".//EIF/Labeltext")
    if eif is not None and eif.text:
        meta["entry_into_force"] = re.sub(r"<[^>]+>", "", eif.text).strip()

    # Registration
    reg = ext.find(".//Registration/Labeltext")
    if reg is not None and reg.text:
        meta["registration"] = reg.text.strip()
        reg_match = re.search(r"No\.\s*(\d+)", reg.text)
        if reg_match:
            meta["registration_number"] = reg_match.group(1)

    # Status counts
    sig = ext.find(".//Status/Signatories")
    if sig is not None and sig.text:
        meta["signatories_count"] = int(sig.text)

    parties = ext.find(".//Status/Parties")
    if parties is not None and parties.text:
        meta["parties_count"] = int(parties.text)

    # Treaty text reference
    text_ref = ext.find(".//TreatyText/Text")
    if text_ref is not None and text_ref.text:
        meta["text_reference"] = re.sub(r"<[^>]+>", "", text_ref.text).strip()

    # Notes
    notes = []
    for note in root.findall(".//TreatyNote/Text"):
        if note.text:
            notes.append(re.sub(r"<[^>]+>", "", note.text).strip())
    if notes:
        meta["notes"] = " ".join(notes)

    # Participants
    participants = []
    for row in root.findall(".//Participants//Row"):
        entries = row.findall("Entry")
        if entries and entries[0].text:
            participants.append(entries[0].text.strip())
    if participants:
        meta["participants"] = participants

    return meta


def fetch_treaty_metadata(mtdsg_no: str, chapter: int) -> dict:
    """Fetch XML metadata for a treaty."""
    roman = ROMAN_NUMERALS.get(chapter, str(chapter))
    xml_url = (
        f"{BASE_URL}/doc/Publication/MTDSG/Volume%20I/"
        f"Chapter%20{roman}/{mtdsg_no}.en.xml"
    )
    resp = fetch_url(xml_url, timeout=30)
    if not resp or not resp.text.strip().startswith("<?xml"):
        return {}
    return parse_xml_metadata(resp.text)


def find_treaty_pdf_url(mtdsg_no: str, chapter: int) -> tuple[Optional[str], Optional[str]]:
    """Find the best PDF URL for the treaty text from the ViewDetails page.
    Returns (pdf_url, page_title) tuple."""
    url = (
        f"{BASE_URL}/Pages/ViewDetails.aspx"
        f"?src=TREATY&mtdsg_no={mtdsg_no}&chapter={chapter}&clang=_en"
    )
    resp = fetch_url(url, timeout=60)
    if not resp:
        return None, None

    # Extract title from page as fallback
    page_title = None
    title_match = re.search(r'<span[^>]*id="ctl00_ctl00[^"]*lblTitle"[^>]*>([^<]+)</span>', resp.text)
    if not title_match:
        title_match = re.search(r'<title>([^<]+)</title>', resp.text)
    if title_match:
        page_title = title_match.group(1).strip()
        # Clean up generic titles
        if page_title in ("United Nations Treaty Collection", ""):
            page_title = None

    # Priority 1: CTC treaty text PDFs (/doc/Treaties/...)
    ctc_pdfs = re.findall(r'/doc/Treaties/[^"\'>\s]+\.pdf', resp.text)
    if ctc_pdfs:
        pdf_path = ctc_pdfs[0].replace("&amp;", "&")
        pdf_url = f"{BASE_URL}{pdf_path}" if not pdf_path.startswith("http") else pdf_path
        return pdf_url, page_title

    # Priority 2: CTC Publication PDFs (/doc/Publication/CTC/...)
    ctc_pub = re.findall(r'/doc/Publication/CTC/[^"\'>\s]+\.pdf', resp.text)
    if ctc_pub:
        pdf_path = ctc_pub[0]
        return f"{BASE_URL}{pdf_path}", page_title

    # Priority 3: MTDSG status PDF (has metadata + participant info, not full text)
    roman = ROMAN_NUMERALS.get(chapter, str(chapter))
    mtdsg_pdf = f"{BASE_URL}/doc/Publication/MTDSG/Volume%20I/Chapter%20{roman}/{mtdsg_no}.en.pdf"
    return mtdsg_pdf, page_title


def extract_treaty_text(pdf_url: str, mtdsg_no: str) -> str:
    """Download and extract text from treaty PDF."""
    resp = fetch_url(pdf_url, timeout=120)
    if not resp:
        return ""

    if len(resp.content) > 50_000_000:  # Skip >50MB
        print(f"    PDF too large: {len(resp.content)} bytes")
        return ""

    if not resp.content[:5] == b"%PDF-":
        print(f"    Not a valid PDF")
        return ""

    text = extract_pdf_markdown(
        source=SOURCE_ID,
        source_id=mtdsg_no,
        pdf_bytes=resp.content,
        table="legislation",
    ) or ""

    return text


def normalize(meta: dict, full_text: str, mtdsg_no: str, chapter: int) -> dict:
    """Normalize treaty data into standard schema."""
    title = meta.get("title", f"Treaty {mtdsg_no}")
    treaty_url = (
        f"{BASE_URL}/Pages/ViewDetails.aspx"
        f"?src=TREATY&mtdsg_no={mtdsg_no}&chapter={chapter}&clang=_en"
    )

    return {
        "_id": f"UNTS-{mtdsg_no}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": meta.get("date"),
        "url": treaty_url,
        "mtdsg_no": mtdsg_no,
        "chapter": chapter,
        "chapter_name": meta.get("chapter_name"),
        "conclusion": meta.get("conclusion"),
        "entry_into_force": meta.get("entry_into_force"),
        "registration_number": meta.get("registration_number"),
        "signatories_count": meta.get("signatories_count"),
        "parties_count": meta.get("parties_count"),
        "text_reference": meta.get("text_reference"),
        "participants": meta.get("participants"),
        "notes": meta.get("notes"),
    }


def fetch_treaties(limit: int = 0) -> Generator[dict, None, None]:
    """Fetch treaties with full text."""
    print("Fetching treaty IDs from chapter listings...")
    treaty_ids = get_all_treaty_ids()

    if limit > 0:
        treaty_ids = treaty_ids[:limit]

    fetched = 0
    for i, entry in enumerate(treaty_ids):
        mtdsg_no = entry["mtdsg_no"]
        chapter = entry["chapter"]
        print(f"\n  [{i+1}/{len(treaty_ids)}] {mtdsg_no} (Chapter {chapter})")

        # Fetch XML metadata
        meta = fetch_treaty_metadata(mtdsg_no, chapter)
        # Use listing title as fallback
        if not meta.get("title"):
            meta["title"] = entry.get("listing_title", mtdsg_no)
        title = meta.get("title", mtdsg_no)
        print(f"    Title: {title[:80]}")
        time.sleep(RATE_LIMIT_DELAY)

        # Find and download treaty text PDF
        pdf_url, page_title = find_treaty_pdf_url(mtdsg_no, chapter)
        if not meta.get("title") and page_title:
            meta["title"] = page_title
        full_text = ""
        if pdf_url:
            print(f"    PDF: {pdf_url[:100]}...")
            full_text = extract_treaty_text(pdf_url, mtdsg_no)
            print(f"    Text: {len(full_text)} chars")
        time.sleep(RATE_LIMIT_DELAY)

        record = normalize(meta, full_text, mtdsg_no, chapter)
        yield record
        fetched += 1


def bootstrap_sample():
    """Fetch sample records for validation."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    total = 0

    # Sample from different chapters for variety
    sample_treaties = [
        {"mtdsg_no": "I-1", "chapter": 1, "listing_title": "Charter of the United Nations"},
        {"mtdsg_no": "IV-8", "chapter": 4, "listing_title": "Convention on the Elimination of All Forms of Discrimination against Women"},
        {"mtdsg_no": "IV-4", "chapter": 4, "listing_title": "International Covenant on Civil and Political Rights"},
        {"mtdsg_no": "IV-3", "chapter": 4, "listing_title": "International Covenant on Economic, Social and Cultural Rights"},
        {"mtdsg_no": "III-3", "chapter": 3, "listing_title": "Vienna Convention on Diplomatic Relations"},
        {"mtdsg_no": "VI-1", "chapter": 6, "listing_title": "Protocol amending the Agreements, Conventions and Protocols on Narcotic Drugs"},
        {"mtdsg_no": "X-1-a", "chapter": 10, "listing_title": "General Agreement on Tariffs and Trade"},
        {"mtdsg_no": "XVIII-10", "chapter": 18, "listing_title": "Rome Statute of the International Criminal Court"},
        {"mtdsg_no": "XXVI-2", "chapter": 26, "listing_title": "Convention on Certain Conventional Weapons"},
        {"mtdsg_no": "XXVII-7", "chapter": 27, "listing_title": "United Nations Framework Convention on Climate Change"},
        {"mtdsg_no": "IV-11", "chapter": 4, "listing_title": "Convention on the Rights of the Child"},
        {"mtdsg_no": "IV-15", "chapter": 4, "listing_title": "Convention on the Rights of Persons with Disabilities"},
    ]

    for entry in sample_treaties:
        mtdsg_no = entry["mtdsg_no"]
        chapter = entry["chapter"]
        print(f"\n  [{total+1}/{len(sample_treaties)}] {mtdsg_no} (Chapter {chapter})")

        meta = fetch_treaty_metadata(mtdsg_no, chapter)
        if not meta.get("title"):
            meta["title"] = entry.get("listing_title", mtdsg_no)
        title = meta.get("title", mtdsg_no)
        print(f"    Title: {title[:80]}")
        time.sleep(RATE_LIMIT_DELAY)

        pdf_url, page_title = find_treaty_pdf_url(mtdsg_no, chapter)
        if not meta.get("title") and page_title:
            meta["title"] = page_title
        full_text = ""
        if pdf_url:
            print(f"    PDF: {pdf_url[:100]}...")
            full_text = extract_treaty_text(pdf_url, mtdsg_no)
            print(f"    Text: {len(full_text)} chars")
        time.sleep(RATE_LIMIT_DELAY)

        record = normalize(meta, full_text, mtdsg_no, chapter)
        fname = SAMPLE_DIR / f"{record['_id']}.json"
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        total += 1
        print(f"    Saved {fname.name} ({len(full_text)} chars text)")

    print(f"\nSample complete: {total} records saved to {SAMPLE_DIR}")
    validate_sample()


def bootstrap_full():
    """Fetch all records."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    total = 0

    for record in fetch_treaties():
        fname = SAMPLE_DIR / f"{record['_id']}.json"
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        total += 1

    print(f"\nFull bootstrap complete: {total} records saved.")


def validate_sample():
    """Validate sample records."""
    files = list(SAMPLE_DIR.glob("*.json"))
    if not files:
        print("FAIL: No sample files found")
        return False

    print(f"\nValidating {len(files)} sample records...")
    issues = []
    text_present = 0
    for f in files:
        with open(f, "r", encoding="utf-8") as fh:
            rec = json.load(fh)
        name = f.name
        if not rec.get("text"):
            issues.append(f"{name}: missing or empty 'text' field")
        elif len(rec["text"]) < 100:
            issues.append(f"{name}: text too short ({len(rec['text'])} chars)")
        else:
            text_present += 1
        if not rec.get("title"):
            issues.append(f"{name}: missing title")
        if not rec.get("_id"):
            issues.append(f"{name}: missing _id")

    if issues:
        print("ISSUES FOUND:")
        for i in issues:
            print(f"  - {i}")

    print(f"\n{text_present}/{len(files)} records have full text")
    if text_present >= len(files) * 0.7:
        print("VALIDATION PASSED (>=70% have text)")
        return True
    else:
        print("VALIDATION FAILED (<70% have text)")
        return False


def test_connectivity():
    """Test connectivity to UN Treaty Collection."""
    print("Testing UN Treaty Collection connectivity...\n")

    # Test chapter listing
    resp = fetch_url(f"{BASE_URL}/Pages/Treaties.aspx?id=4&subid=A&clang=_en")
    if resp:
        ids = re.findall(r'mtdsg_no=([^&"]+)', resp.text)
        print(f"  Chapter listing: OK ({len(set(ids))} treaties in Chapter IV)")
    else:
        print("  Chapter listing: FAILED")
        return False

    # Test XML metadata
    resp = fetch_url(
        f"{BASE_URL}/doc/Publication/MTDSG/Volume%20I/Chapter%20IV/IV-8.en.xml"
    )
    if resp:
        meta = parse_xml_metadata(resp.text)
        print(f"  XML metadata: OK (Title: {meta.get('title', 'N/A')[:60]})")
    else:
        print("  XML metadata: FAILED")
        return False

    # Test ViewDetails page
    pdf_url, page_title = find_treaty_pdf_url("IV-8", 4)
    if pdf_url:
        print(f"  ViewDetails: OK (PDF found, title: {page_title or 'N/A'})")
    else:
        print("  ViewDetails: FAILED")
        return False

    # Test PDF download
    if pdf_url:
        resp = fetch_url(pdf_url, timeout=60)
        if resp and resp.content[:5] == b"%PDF-":
            print(f"  PDF download: OK ({len(resp.content)} bytes)")
        else:
            print("  PDF download: FAILED")

    print(f"\n  PDF extraction: via common/pdf_extract.extract_pdf_markdown")
    print("\nConnectivity test complete.")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UN/TreatyCollection data fetcher")
    parser.add_argument("command", choices=["bootstrap", "test"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    if args.command == "test":
        success = test_connectivity()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        if args.sample:
            bootstrap_sample()
        else:
            bootstrap_full()
