#!/usr/bin/env python3
"""
DK/Hoejesteret — Danish Supreme Court Decisions (Højesteret)

Fetches ~2,850 selected judgments and orders from the Danish Supreme Court
since September 2009. Full text extracted from PDF documents.

Source: https://domstol.fe1.tangora.com/Domsoversigt-(Højesteret).31478.aspx
License: Public domain (Danish court decisions)
"""

import argparse
import io
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from html import unescape

import requests
from pdfminer.high_level import extract_text as pdf_extract_text

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

SOURCE_ID = "DK/Hoejesteret"
SAMPLE_DIR = Path(__file__).parent / "sample"

BASE_URL = "https://domstol.fe1.tangora.com"
LISTING_URL = f"{BASE_URL}/Domsoversigt-(H%C3%B8jesteret).31478.aspx"
AJAX_URL = f"{BASE_URL}/listediting.ashx"
RECORD_URL = f"{BASE_URL}/Domsoversigt-(H%C3%B8jesteret).31478.aspx"
PAGE_SIZE = 15
PAGE_ID = 31478


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "LegalDataHunter/1.0 (research; https://github.com/ZachLaik/LegalDataHunter)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return session


def strip_html(html_str: str) -> str:
    """Remove HTML tags and decode entities."""
    if not html_str:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", html_str)
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"&nbsp;", " ", text)
    return text.strip()


def parse_listing_html(html: str) -> list[dict]:
    """Parse a listing page (or AJAX fragment) to extract record entries."""
    records = []
    # Find all record links: recordid31478=NNNN
    record_ids = list(dict.fromkeys(re.findall(r"recordid31478=(\d+)", html)))

    for rid in record_ids:
        entry = {"record_id": rid}

        # Extract case number (sagsnr) near this record ID
        pattern = rf"recordid31478={rid}['\"]?>([^<]+)<"
        match = re.search(pattern, html)
        if match:
            entry["case_number"] = match.group(1).strip()

        # Extract date near this record
        date_pattern = rf"recordid31478={rid}['\"]?>.*?<i>.*?recordid31478={rid}['\"]?>(\d{{2}}-\d{{2}}-\d{{4}})<"
        date_match = re.search(date_pattern, html, re.S)
        if date_match:
            entry["date_raw"] = date_match.group(1)

        # Extract summary (resumé) - the long text block after this record
        resume_pattern = (
            rf"recordid31478={rid}['\"]?>.*?"
            r"Resumé.*?<div class=\"vdcontent\">(.*?)</div>\s*</div>\s*</li>"
        )
        resume_match = re.search(resume_pattern, html, re.S)
        if resume_match:
            entry["summary"] = strip_html(resume_match.group(1))

        records.append(entry)

    return records


def parse_record_page(html: str, record_id: str) -> dict:
    """Parse an individual record page for metadata and PDF link."""
    data = {"record_id": record_id}

    # Case number
    case_match = re.search(
        r"Sagsnr.*?<div class=\"vdcontent\">.*?<b>(.*?)</b>", html, re.S
    )
    if case_match:
        data["case_number"] = strip_html(case_match.group(1))

    # Date
    date_match = re.search(
        r"Afgørelsesdato.*?<div class=\"vdcontent\">.*?<span[^>]*>([\d-]+)</span>",
        html, re.S,
    )
    if date_match:
        data["date_raw"] = date_match.group(1).strip()

    # Summary
    resume_match = re.search(
        r"Resumé.*?<div class=\"vdcontent\">(.*?)</div>\s*</div>\s*</li>",
        html, re.S,
    )
    if resume_match:
        data["summary"] = strip_html(resume_match.group(1))

    # PDF link
    pdf_match = re.search(r"href=['\"](/ref\.aspx\?[^'\"]+)['\"]", html)
    if pdf_match:
        data["pdf_path"] = pdf_match.group(1)

    # Subject matter
    subject_match = re.search(
        r"Sagsemne.*?<div class=\"vdcontent\">(.*?)</div>", html, re.S
    )
    if subject_match:
        data["subjects"] = strip_html(subject_match.group(1))

    return data


def extract_pdf_text(session: requests.Session, pdf_path: str) -> str:
    """Download PDF and extract text."""
    url = BASE_URL + pdf_path
    resp = session.get(url, timeout=60)
    resp.raise_for_status()
    if b"%PDF" not in resp.content[:10]:
        return ""
    text = pdf_extract_text(io.BytesIO(resp.content))
    # Clean up spacing artifacts from PDF extraction
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def fetch_listing_page(session: requests.Session, page_number: int) -> str:
    """Fetch a page of listings. Page 1 is the initial page, 2+ use AJAX."""
    if page_number == 1:
        resp = session.get(LISTING_URL, timeout=30)
        resp.raise_for_status()
        return resp.text
    else:
        resp = session.post(
            AJAX_URL,
            params={
                "action": "autofetchdata",
                "pageid": PAGE_ID,
                "pagenumber": page_number,
                "moduleid": 17,
                "navigateuse": 1,
            },
            data={"sourceurl": LISTING_URL},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.text


def fetch_record(session: requests.Session, record_id: str) -> dict:
    """Fetch individual record page and extract metadata + PDF text."""
    url = f"{RECORD_URL}?recordid{PAGE_ID}={record_id}"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    data = parse_record_page(resp.text, record_id)

    # Download and extract PDF text
    if "pdf_path" in data:
        try:
            data["full_text"] = extract_pdf_text(session, data["pdf_path"])
        except Exception as e:
            print(f"  PDF extraction failed for record {record_id}: {e}", file=sys.stderr)
            data["full_text"] = ""
    else:
        data["full_text"] = ""

    return data


def normalize(raw: dict) -> dict:
    """Normalize a raw record into standard schema."""
    record_id = raw.get("record_id", "")
    case_number = raw.get("case_number", "")
    date_raw = raw.get("date_raw", "")
    summary = raw.get("summary", "")
    full_text = raw.get("full_text", "")
    subjects = raw.get("subjects", "")

    # Parse date (DD-MM-YYYY → YYYY-MM-DD)
    date_iso = None
    if date_raw:
        try:
            dt = datetime.strptime(date_raw, "%d-%m-%Y")
            date_iso = dt.strftime("%Y-%m-%d")
        except ValueError:
            date_iso = date_raw

    # Use full PDF text if available, otherwise fall back to summary
    text = full_text if full_text else summary

    title = case_number if case_number else f"Record {record_id}"
    if summary and not full_text:
        # If only summary, prepend case number context
        title = f"Højesteret {case_number}" if case_number else title

    return {
        "_id": f"DK-HJR-{record_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_iso,
        "url": f"{BASE_URL}/Domsoversigt-(H%C3%B8jesteret).31478.aspx?recordid{PAGE_ID}={record_id}",
        "case_number": case_number,
        "subjects": subjects,
        "summary": summary,
        "has_full_text": bool(full_text),
    }


def fetch_all(session: requests.Session, limit: Optional[int] = None) -> Generator[dict, None, None]:
    """Yield all decisions from the Højesteret database."""
    page = 1
    count = 0
    seen_ids = set()

    while True:
        print(f"  Fetching listing page {page}...", file=sys.stderr)
        html = fetch_listing_page(session, page)
        entries = parse_listing_html(html)

        if not entries:
            break

        new_ids = [e for e in entries if e["record_id"] not in seen_ids]
        if not new_ids:
            break

        for entry in new_ids:
            rid = entry["record_id"]
            seen_ids.add(rid)

            print(f"  Fetching record {rid} ({count + 1})...", file=sys.stderr)
            record = fetch_record(session, rid)
            # Merge listing data with record data (record page has more detail)
            if "summary" not in record and "summary" in entry:
                record["summary"] = entry["summary"]
            if "date_raw" not in record and "date_raw" in entry:
                record["date_raw"] = entry["date_raw"]
            if "case_number" not in record and "case_number" in entry:
                record["case_number"] = entry["case_number"]

            yield normalize(record)
            count += 1

            if limit and count >= limit:
                return

            time.sleep(1.5)  # Rate limit

        page += 1
        time.sleep(1.0)


def fetch_updates(session: requests.Session, since: str) -> Generator[dict, None, None]:
    """Yield decisions modified since the given date."""
    try:
        since_dt = datetime.strptime(since, "%Y-%m-%d")
    except ValueError:
        print(f"Invalid date format: {since}", file=sys.stderr)
        return

    for record in fetch_all(session):
        if record.get("date"):
            try:
                rec_dt = datetime.strptime(record["date"], "%Y-%m-%d")
                if rec_dt >= since_dt:
                    yield record
                else:
                    # Records are sorted newest first; stop when we pass the date
                    return
            except ValueError:
                yield record


def bootstrap_sample():
    """Fetch sample records and save to sample/ directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    session = get_session()
    count = 0
    target = 12

    print(f"Fetching {target} sample records from DK/Hoejesteret...")

    for record in fetch_all(session, limit=target):
        count += 1
        fname = SAMPLE_DIR / f"{record['_id']}.json"
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        text_len = len(record.get("text", ""))
        has_ft = record.get("has_full_text", False)
        print(f"  [{count}/{target}] {record['_id']}: "
              f"{record.get('case_number', '?')} | "
              f"text={text_len} chars | full_text={has_ft}")

    print(f"\nDone: {count} records saved to {SAMPLE_DIR}/")
    return count


def main():
    parser = argparse.ArgumentParser(description="DK/Hoejesteret bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot_parser = sub.add_parser("bootstrap", help="Fetch sample data")
    boot_parser.add_argument("--sample", action="store_true", default=True)
    full_parser = sub.add_parser("full", help="Fetch all records")
    full_parser.add_argument("--limit", type=int, help="Max records")

    update_parser = sub.add_parser("updates", help="Fetch updates since date")
    update_parser.add_argument("--since", required=True, help="YYYY-MM-DD")

    args = parser.parse_args()

    if args.command == "bootstrap":
        bootstrap_sample()
    elif args.command == "full":
        session = get_session()
        for record in fetch_all(session, limit=args.limit):
            print(json.dumps(record, ensure_ascii=False))
    elif args.command == "updates":
        session = get_session()
        for record in fetch_updates(session, args.since):
            print(json.dumps(record, ensure_ascii=False))
    else:
        bootstrap_sample()


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
