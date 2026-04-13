#!/usr/bin/env python3
"""
CH/ZH-Legislation -- Kanton Zürich Gesetzessammlung (Loseblattsammlung, LS)

Fetches Zurich cantonal legislation from zh.ch JSON API + PDF full text.

Data source: https://www.zh.ch/de/politik-staat/gesetze-beschluesse/gesetzessammlung.html
License: Public domain (Swiss cantonal legislation)

Strategy:
  - Query JSON API with fileNumber 1-14 to list all ~951 active laws
  - For each law, fetch detail HTML page to extract PDF URL
  - Download PDF (handling JS redirect from notes.zh.ch)
  - Extract full text with pdfplumber
"""

import json
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


API_BASE = ("https://www.zh.ch/de/politik-staat/gesetze-beschluesse/gesetzessammlung/"
            "_jcr_content/main/lawcollectionsearch_312548694.zhweb-zhlex-ls.zhweb-cache.json")
DETAIL_BASE = "https://www.zh.ch"
RATE_LIMIT_DELAY = 1.5
CURL_TIMEOUT = 120

SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"


def curl_fetch(url, retries=2, binary=False):
    """Fetch URL using curl. Returns bytes if binary, else string."""
    for attempt in range(retries + 1):
        try:
            result = subprocess.run(
                ["curl", "-sL", "-m", str(CURL_TIMEOUT),
                 "-w", "\n%{http_code}", url],
                capture_output=True, timeout=CURL_TIMEOUT + 10
            )
            raw = result.stdout
            last_nl = raw.rfind(b"\n")
            if last_nl >= 0:
                body = raw[:last_nl]
                status = raw[last_nl + 1:].decode("ascii", errors="replace").strip()
            else:
                body = raw
                status = "000"

            if not status.startswith("2"):
                if attempt == retries:
                    print("HTTP %s for %s" % (status, url), file=sys.stderr)
                    return None
                time.sleep(3)
                continue

            if binary:
                return body
            return body.decode("utf-8", errors="replace")
        except Exception as e:
            if attempt == retries:
                print("Failed to fetch %s: %s" % (url, e), file=sys.stderr)
                return None
            time.sleep(3)
    return None


def get_all_laws():
    """Fetch all laws from the JSON API by querying each fileNumber group."""
    laws = []
    for fn in range(1, 15):
        page = 0
        while True:
            url = "%s?fileNumber=%d&page=%d" % (API_BASE, fn, page)
            raw = curl_fetch(url)
            if not raw:
                break

            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                print("JSON parse error for fileNumber=%d page=%d" % (fn, page), file=sys.stderr)
                break

            items = data.get("data", [])
            if not items:
                break

            for item in items:
                laws.append({
                    "link": item.get("link", ""),
                    "referenceNumber": item.get("referenceNumber", ""),
                    "title": item.get("enactmentTitle", "").strip(),
                    "date": item.get("enactmentDate", ""),
                })

            num_pages = data.get("numberOfResultPages", 1)
            page += 1
            if page >= num_pages:
                break
            time.sleep(0.5)

        print("FileNumber %d: %d laws so far" % (fn, len(laws)), file=sys.stderr)
        time.sleep(RATE_LIMIT_DELAY)

    return laws


def get_pdf_url(detail_link):
    """Fetch detail page and extract the PDF download URL."""
    url = DETAIL_BASE + detail_link
    html = curl_fetch(url)
    if not html:
        return None

    # Find the OpenAttachment PDF link
    m = re.search(
        r'href="(https?://www\.notes\.zh\.ch/appl/zhlex_r\.nsf/OpenAttachment[^"]*\.pdf[^"]*)"',
        html, re.IGNORECASE
    )
    if m:
        return m.group(1)

    # Fallback: any notes.zh.ch PDF link
    m = re.search(r'href="(https?://www\.notes\.zh\.ch[^"]*\.pdf[^"]*)"', html, re.IGNORECASE)
    if m:
        return m.group(1)

    return None


def resolve_pdf_redirect(open_attachment_url):
    """Resolve the JS redirect from OpenAttachment to actual PDF URL."""
    html = curl_fetch(open_attachment_url)
    if not html:
        return None

    m = re.search(r'window\.location="([^"]+)"', html)
    if m:
        path = m.group(1)
        return "https://www.notes.zh.ch" + path
    return None


def extract_text_from_pdf(pdf_bytes):
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="CH/ZH-Legislation",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="legislation",
    ) or ""

def parse_date(date_str):
    """Parse DD.MM.YYYY to ISO 8601."""
    if not date_str:
        return ""
    m = re.match(r'(\d{2})\.(\d{2})\.(\d{4})', date_str)
    if m:
        return "%s-%s-%s" % (m.group(3), m.group(2), m.group(1))
    return ""


def normalize(law, text):
    """Normalize a law record."""
    return {
        "_id": "CH/ZH-Legislation/%s" % law["referenceNumber"],
        "_source": "CH/ZH-Legislation",
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": law["title"],
        "text": text,
        "date": parse_date(law["date"]),
        "url": DETAIL_BASE + law["link"],
        "systematic_number": law["referenceNumber"],
        "jurisdiction": "CH-ZH",
    }


def fetch_all(sample=False):
    """Fetch all Zurich cantonal legislation."""
    laws = get_all_laws()
    if not laws:
        print("ERROR: Could not fetch law list", file=sys.stderr)
        return

    print("Found %d laws total" % len(laws), file=sys.stderr)

    count = 0
    errors = 0
    for law in laws:
        if not law.get("link"):
            continue

        # Step 1: Get PDF URL from detail page
        pdf_open_url = get_pdf_url(law["link"])
        time.sleep(RATE_LIMIT_DELAY)

        if not pdf_open_url:
            print("  No PDF link for %s" % law["referenceNumber"], file=sys.stderr)
            errors += 1
            continue

        # Step 2: Resolve the JS redirect
        pdf_url = resolve_pdf_redirect(pdf_open_url)
        time.sleep(0.5)

        if not pdf_url:
            print("  Could not resolve PDF redirect for %s" % law["referenceNumber"], file=sys.stderr)
            errors += 1
            continue

        # Step 3: Download PDF
        pdf_bytes = curl_fetch(pdf_url, binary=True)
        time.sleep(RATE_LIMIT_DELAY)

        if not pdf_bytes or len(pdf_bytes) < 100:
            print("  Empty PDF for %s" % law["referenceNumber"], file=sys.stderr)
            errors += 1
            continue

        # Step 4: Extract text
        text = extract_text_from_pdf(pdf_bytes)

        if not text or len(text) < 50:
            print("  Insufficient text for %s (%d chars)" % (
                law["referenceNumber"], len(text) if text else 0), file=sys.stderr)
            errors += 1
            continue

        record = normalize(law, text)
        yield record
        count += 1

        if count % 25 == 0:
            print("  Fetched %d records (%d errors)..." % (count, errors), file=sys.stderr)
        if sample and count >= 15:
            break

    print("Total records: %d (errors: %d)" % (count, errors), file=sys.stderr)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Zurich cantonal legislation fetcher")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Fetch documents")
    boot.add_argument("--sample", action="store_true", help="Fetch ~15 sample records")
    boot.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "bootstrap":
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        count = 0
        for record in fetch_all(sample=args.sample):
            out_path = SAMPLE_DIR / ("%04d.json" % count)
            out_path.write_text(json.dumps(record, ensure_ascii=False, indent=2))
            count += 1
        print("Done. Saved %d records to %s/" % (count, SAMPLE_DIR), file=sys.stderr)


if __name__ == "__main__":
    main()
