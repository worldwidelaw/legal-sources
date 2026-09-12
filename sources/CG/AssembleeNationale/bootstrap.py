#!/usr/bin/env python3
"""
CG/AssembleeNationale — Congo-Brazzaville Official Journal (Journal Officiel)

Fetches legislation from the Secrétariat Général du Gouvernement (SGG) website.
Each record is one Journal Officiel issue with full extracted text from PDF.

Source: https://www.sgg.cg/fr/journal-officiel/le-journal-officiel.html
Total: ~1,833 issues (1946-2026), text extractable from ~2010 onwards.
"""

import argparse
import io
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

try:
    import pdfplumber
except ImportError:
    print("ERROR: pdfplumber required. Install: pip install pdfplumber")
    sys.exit(1)

SOURCE_ID = "CG/AssembleeNationale"
BASE_URL = "https://www.sgg.cg"
LISTING_URL = "https://www.sgg.cg/fr/journal-officiel/le-journal-officiel.html"
ITEMS_PER_PAGE = 20
DELAY_BETWEEN_REQUESTS = 2
DELAY_BETWEEN_PDFS = 3
MIN_TEXT_CHARS = 200


def get_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; academic research)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return s


def fetch_listing_page(session, page_num):
    """Fetch one page of the Journal Officiel listing."""
    params = {"page": page_num, "row": "1833"}
    resp = session.get(LISTING_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.text


def parse_listing_page(html):
    """Extract PDF links and metadata from a listing page."""
    soup = BeautifulSoup(html, "html.parser")
    results = []

    for link in soup.find_all("a", href=True):
        href = link["href"]
        if ".pdf" not in href:
            continue

        text = link.get_text(strip=True)
        # Normalize URL
        if href.startswith("/"):
            pdf_url = BASE_URL + href
        elif not href.startswith("http"):
            pdf_url = "https://" + href
        else:
            pdf_url = href

        # Parse issue number and date from text like:
        # "JOJournal officiel n°2026-18 du 30/04/2026"
        issue_match = re.search(r'n[°º]?\s*(\d{4}-\d+(?:-\d+)?)', text)
        date_match = re.search(r'du\s+(\d{2}/\d{2}/\d{4})', text)

        issue_number = issue_match.group(1) if issue_match else None
        date_str = None
        if date_match:
            try:
                d = datetime.strptime(date_match.group(1), "%d/%m/%Y")
                date_str = d.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Extract year from URL or issue number
        year_match = re.search(r'/(\d{4})/', pdf_url)
        year = int(year_match.group(1)) if year_match else None

        results.append({
            "pdf_url": pdf_url,
            "title": text,
            "issue_number": issue_number,
            "date": date_str,
            "year": year,
        })

    return results


def extract_text_from_pdf(session, pdf_url):
    """Download PDF and extract text using pdfplumber."""
    resp = session.get(pdf_url, timeout=60)
    resp.raise_for_status()

    text_parts = []
    with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass

    return "\n\n".join(text_parts)


def normalize(raw, text):
    """Transform raw listing data + extracted text into standard schema."""
    issue_id = raw.get("issue_number") or raw["pdf_url"].split("/")[-1].replace(".pdf", "")
    return {
        "_id": f"CG-JO-{issue_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": f"Journal Officiel de la République du Congo n°{issue_id}",
        "text": text,
        "date": raw.get("date"),
        "url": raw["pdf_url"],
        "issue_number": raw.get("issue_number"),
        "year": raw.get("year"),
    }


def fetch_all(session, min_year=2010, max_pages=92):
    """Yield all Journal Officiel issues with extractable text."""
    for page_num in range(max_pages):
        print(f"Fetching listing page {page_num + 1}/{max_pages}...")
        html = fetch_listing_page(session, page_num)
        items = parse_listing_page(html)

        if not items:
            print(f"  No items on page {page_num + 1}, stopping.")
            break

        for item in items:
            if item.get("year") and item["year"] < min_year:
                continue

            print(f"  Downloading: {item['pdf_url']}")
            try:
                text = extract_text_from_pdf(session, item["pdf_url"])
            except Exception as e:
                print(f"    ERROR extracting PDF: {e}")
                continue

            if len(text) < MIN_TEXT_CHARS:
                print(f"    Skipped: only {len(text)} chars (likely scanned)")
                continue

            record = normalize(item, text)
            yield record
            time.sleep(DELAY_BETWEEN_PDFS)

        time.sleep(DELAY_BETWEEN_REQUESTS)


def fetch_updates(session, since, min_year=2010):
    """Yield issues published since a given date."""
    since_date = datetime.fromisoformat(since).date() if isinstance(since, str) else since

    for page_num in range(10):  # Only check recent pages
        html = fetch_listing_page(session, page_num)
        items = parse_listing_page(html)

        if not items:
            break

        for item in items:
            if not item.get("date"):
                continue
            item_date = datetime.strptime(item["date"], "%Y-%m-%d").date()
            if item_date < since_date:
                return  # Listing is chronological, stop when we pass since

            if item.get("year") and item["year"] < min_year:
                continue

            try:
                text = extract_text_from_pdf(session, item["pdf_url"])
            except Exception as e:
                print(f"  ERROR: {e}")
                continue

            if len(text) < MIN_TEXT_CHARS:
                continue

            yield normalize(item, text)
            time.sleep(DELAY_BETWEEN_PDFS)

        time.sleep(DELAY_BETWEEN_REQUESTS)


def bootstrap_sample(n=12):
    """Download a sample of recent issues for validation."""
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    session = get_session()
    html = fetch_listing_page(session, 0)
    items = parse_listing_page(html)

    count = 0
    for item in items:
        if count >= n:
            break
        if item.get("year") and item["year"] < 2010:
            continue

        print(f"[{count + 1}/{n}] Downloading: {item['pdf_url']}")
        try:
            text = extract_text_from_pdf(session, item["pdf_url"])
        except Exception as e:
            print(f"  ERROR: {e}")
            continue

        if len(text) < MIN_TEXT_CHARS:
            print(f"  Skipped: insufficient text ({len(text)} chars)")
            continue

        record = normalize(item, text)
        fname = f"{record['_id']}.json"
        with open(sample_dir / fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"  Saved: {fname} ({len(text):,} chars)")
        count += 1
        time.sleep(DELAY_BETWEEN_PDFS)

    print(f"\nDone: {count} samples saved to {sample_dir}")
    return count


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    parser = argparse.ArgumentParser(description="CG/AssembleeNationale bootstrap")
    parser.add_argument("command", choices=["bootstrap", "fetch_all", "fetch_updates"])
    parser.add_argument("--sample", action="store_true", help="Run in sample mode (12 records)")
    parser.add_argument("--since", help="ISO date for fetch_updates")
    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample()
        else:
            session = get_session()
            count = 0
            for record in fetch_all(session):
                count += 1
                print(f"  [{count}] {record['title']}: {len(record['text']):,} chars")
            print(f"Total: {count} records")

    elif args.command == "fetch_updates":
        since = args.since or "2025-01-01"
        session = get_session()
        for record in fetch_updates(session, since):
            print(f"  {record['title']}: {len(record['text']):,} chars")

    elif args.command == "fetch_all":
        session = get_session()
        count = 0
        for record in fetch_all(session):
            count += 1
        print(f"Total: {count} records")
