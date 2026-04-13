#!/usr/bin/env python3
"""
CH/GE-Legislation -- Canton de Genève Recueil systématique (rs/GE)

Fetches Geneva cantonal legislation from silgeneve.ch by scraping the HTML
index page and individual law pages. Full text is extracted from HTML.

Data source: https://silgeneve.ch/legis/
License: Public domain (Swiss cantonal legislation)

Strategy:
  - GET /program/books/RSG/content.htm for index of all ~858 laws
  - Parse <h3><a href="htm/rsg_XXX.htm">Title</a></h3> links
  - GET each law page and extract full text from HTML body
"""

import html as html_lib
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

INDEX_URL = "https://silgeneve.ch/legis/program/books/RSG/content.htm"
BASE_URL = "https://silgeneve.ch/legis/program/books/RSG/"
RATE_LIMIT_DELAY = 1.5
CURL_TIMEOUT = 120

SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"


def fetch_html(url: str, retries: int = 2):
    """Fetch HTML from URL using curl. Handles windows-1252 encoding."""
    for attempt in range(retries + 1):
        try:
            result = subprocess.run(
                ["curl", "-sL", "-m", str(CURL_TIMEOUT),
                 "-H", "Accept: text/html",
                 "-w", "\n%{http_code}", url],
                capture_output=True, timeout=CURL_TIMEOUT + 10
            )
            raw = result.stdout
            # Split off HTTP status code (last line)
            last_nl = raw.rfind(b"\n")
            if last_nl >= 0:
                body_bytes = raw[:last_nl]
                status = raw[last_nl + 1:].decode("ascii", errors="replace").strip()
            else:
                body_bytes = raw
                status = "000"

            if not status.startswith("2"):
                if attempt == retries:
                    print("HTTP %s for %s" % (status, url), file=sys.stderr)
                    return None
                time.sleep(3)
                continue

            # Try UTF-8 first, fall back to windows-1252
            try:
                return body_bytes.decode("utf-8")
            except UnicodeDecodeError:
                return body_bytes.decode("windows-1252", errors="replace")
        except Exception as e:
            if attempt == retries:
                print("Failed to fetch %s: %s" % (url, e), file=sys.stderr)
                return None
            time.sleep(3)
    return None


def strip_html(raw):
    """Strip HTML tags and decode entities from a string."""
    if not raw:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', raw, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</(?:p|div|h[1-6]|tr|li)>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = html_lib.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def get_law_index():
    """Parse the index page to get all law entries."""
    raw = fetch_html(INDEX_URL)
    if not raw:
        return []

    laws = []
    # Match <h3...><a href="htm/rsg_XXX.htm">Title</a></h3>
    pattern = re.compile(
        r'<h3[^>]*>\s*<a\s+href="(htm/rsg_[^"]+\.htm)"[^>]*>(.*?)</a>',
        re.DOTALL | re.IGNORECASE
    )
    for match in pattern.finditer(raw):
        href = match.group(1)
        title_raw = match.group(2).strip()
        title = strip_html(title_raw)

        # Extract systematic number from title (e.g., "A 1 01 Acte d'union...")
        sn_match = re.match(r'^([A-L]\s+\d+[\s.]+\d+(?:\.\d+)*)\s+(.+)', title)
        if sn_match:
            systematic_number = re.sub(r'\s+', ' ', sn_match.group(1)).strip()
            title_clean = sn_match.group(2).strip()
        else:
            # Use filename as fallback systematic number
            fn = href.split("/")[-1].replace("rsg_", "").replace(".htm", "")
            systematic_number = fn.replace("_", " ").replace("p", ".").upper()
            title_clean = title

        # Extract abbreviation from title if present (in parentheses at the end)
        abbr = ""
        abbr_match = re.search(r'\(([A-ZÀÂÄÉÈÊËÏÎÔÙÛÜŸÆŒÇ][A-ZÀÂÄÉÈÊËÏÎÔÙÛÜŸÆŒÇa-zàâäéèêëïîôùûüÿæœç-]+(?:\s*[-/]\s*[A-ZÀÂÄÉÈÊËÏÎÔÙÛÜŸÆŒÇa-z]+)*)\)\s*$', title_clean)
        if abbr_match:
            abbr = abbr_match.group(1)

        laws.append({
            "href": href,
            "systematic_number": systematic_number,
            "title": title_clean,
            "abbreviation": abbr,
        })

    return laws


def extract_body_text(raw_html):
    """Extract the main text content from a law page HTML."""
    # Extract body
    m = re.search(r'<body[^>]*>(.*)</body>', raw_html, re.DOTALL | re.IGNORECASE)
    if not m:
        return ""
    return strip_html(m.group(1))


def extract_date(raw_html):
    """Try to extract the enactment/adoption date from the law page."""
    # Look for "du DD mois YYYY" pattern
    months = {
        'janvier': '01', 'février': '02', 'mars': '03', 'avril': '04',
        'mai': '05', 'juin': '06', 'juillet': '07', 'août': '08',
        'septembre': '09', 'octobre': '10', 'novembre': '11', 'décembre': '12',
        # windows-1252 encoded variants
        'f\xe9vrier': '02', 'ao\xfbt': '08', 'd\xe9cembre': '12',
    }
    months_pattern = '|'.join(re.escape(m) for m in months.keys())
    pattern = re.compile(r'du\s+(\d{1,2})\s*(?:er\s+)?(%s)\s+(\d{4})' % months_pattern, re.IGNORECASE)
    match = pattern.search(raw_html)
    if match:
        day = int(match.group(1))
        month = months.get(match.group(2).lower(), "01")
        year = match.group(3)
        return "%s-%s-%02d" % (year, month, day)
    return ""


def fetch_law(law_info):
    """Fetch a single law page and extract full text."""
    url = BASE_URL + law_info["href"]
    raw = fetch_html(url)
    if not raw:
        return None, ""

    text = extract_body_text(raw)
    date = extract_date(raw)
    return {"date": date}, text


def normalize(law_info, text, meta):
    """Normalize a law record."""
    sn = law_info["systematic_number"]
    return {
        "_id": "CH/GE-Legislation/%s" % sn,
        "_source": "CH/GE-Legislation",
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": law_info["title"],
        "text": text,
        "date": meta.get("date", "") if meta else "",
        "url": BASE_URL + law_info["href"],
        "systematic_number": sn,
        "abbreviation": law_info.get("abbreviation", ""),
        "jurisdiction": "CH-GE",
    }


def fetch_all(sample=False):
    """Fetch all Geneva cantonal legislation."""
    laws = get_law_index()
    if not laws:
        print("ERROR: Could not fetch law index", file=sys.stderr)
        return

    print("Found %d laws in index" % len(laws), file=sys.stderr)

    count = 0
    for law_info in laws:
        meta, text = fetch_law(law_info)

        if not text or len(text) < 50:
            print("  Skipping %s: insufficient text (%d chars)" % (
                law_info["systematic_number"], len(text) if text else 0), file=sys.stderr)
            time.sleep(RATE_LIMIT_DELAY)
            continue

        record = normalize(law_info, text, meta)
        yield record
        count += 1

        if count % 25 == 0:
            print("  Fetched %d records..." % count, file=sys.stderr)
        if sample and count >= 15:
            break

        time.sleep(RATE_LIMIT_DELAY)

    print("Total records: %d" % count, file=sys.stderr)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Geneva cantonal legislation fetcher")
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
