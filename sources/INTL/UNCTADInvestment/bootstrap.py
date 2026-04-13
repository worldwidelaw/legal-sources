#!/usr/bin/env python3
"""
INTL/UNCTADInvestment - UNCTAD International Investment Agreements Fetcher

Fetches bilateral investment treaties (BITs) and treaties with investment
provisions (TIPs) from the UNCTAD IIA database. ~3,400 treaties with full text
PDFs for many.

Data source: https://investmentpolicy.unctad.org/international-investment-agreements
Method: Country page HTML parsing + PDF download/text extraction
License: UN public domain (personal non-commercial use)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
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

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


BASE_URL = "https://investmentpolicy.unctad.org"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "INTL/UNCTADInvestment"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

RATE_LIMIT_DELAY = 2.0

# Sample countries with many treaties and good text coverage
SAMPLE_COUNTRIES = [
    {"IiaId": 72, "Name": "France", "UrlName": "france"},
    {"IiaId": 78, "Name": "Germany", "UrlName": "germany"},
    {"IiaId": 221, "Name": "United Kingdom", "UrlName": "united-kingdom"},
]

SAMPLE_TREATIES_PER_COUNTRY = 5


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="INTL/UNCTADInvestment",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="legislation",
    ) or ""

def fetch_html(url: str) -> Optional[str]:
    """Fetch an HTML page."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=120)
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as e:
        print(f"  Error fetching {url}: {e}")
        return None


def fetch_pdf(url: str) -> Optional[bytes]:
    """Fetch a PDF file."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=120, allow_redirects=True)
        resp.raise_for_status()
        ct = resp.headers.get("Content-Type", "")
        if "pdf" not in ct and len(resp.content) < 500:
            return None
        if len(resp.content) > 50_000_000:
            print(f"    PDF too large: {len(resp.content)} bytes")
            return None
        return resp.content
    except requests.RequestException as e:
        print(f"    Error fetching PDF: {e}")
        return None


def get_country_catalog(html: str) -> list[dict]:
    """Extract the country catalog from a page's JavaScript."""
    match = re.search(r'window\.countryCatalog\s*=\s*(\[.*?\]);', html, re.DOTALL)
    if not match:
        return []
    try:
        catalog = json.loads(match.group(1))
        return [c for c in catalog if c.get("IiaId") is not None]
    except json.JSONDecodeError:
        return []


def parse_date(date_str: str) -> Optional[str]:
    """Parse DD/MM/YYYY to ISO format."""
    date_str = date_str.strip()
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def parse_treaties_from_html(html: str, country_name: str) -> list[dict]:
    """Parse all treaties (BITs + TIPs) from a country page HTML."""
    treaties = []

    # Find all table rows in BIT and TIP tables
    # Each row: <tr>...<td data-index="N">...</td>...</tr>
    row_pattern = re.compile(r'<tr>\s*<th[^>]*>(\d+)</th>(.*?)</tr>', re.DOTALL)

    for match in row_pattern.finditer(html):
        row_num = match.group(1)
        row_html = match.group(2)

        treaty = {"row_num": row_num}

        # Extract each cell by data-index
        cells = re.findall(
            r'<td\s+data-index="(\d+)"[^>]*>(.*?)</td>',
            row_html,
            re.DOTALL,
        )

        for idx, content in cells:
            idx = int(idx)
            content = content.strip()

            if idx == 1:  # Full title (hidden) + treaty URL
                link = re.search(r'href="([^"]+)"', content)
                if link:
                    treaty["detail_url"] = link.group(1)
                    # Extract treaty ID
                    tid = re.search(r'/(\d+)/', link.group(1))
                    if tid:
                        treaty["treaty_id"] = tid.group(1)

            elif idx == 2:  # Short title
                link = re.search(r'>([^<]+)</a>', content)
                if link:
                    treaty["title"] = link.group(1).strip()
                if not treaty.get("detail_url"):
                    link2 = re.search(r'href="([^"]+)"', content)
                    if link2:
                        treaty["detail_url"] = link2.group(1)
                        tid = re.search(r'/(\d+)/', link2.group(1))
                        if tid:
                            treaty["treaty_id"] = tid.group(1)

            elif idx == 3:  # Type
                treaty["treaty_type"] = re.sub(r'<[^>]+>', '', content).strip()

            elif idx == 4:  # Status
                treaty["status"] = re.sub(r'<[^>]+>', '', content).strip()

            elif idx == 5:  # Parties
                parties = re.findall(r'>([^<]+)</a>', content)
                treaty["other_parties"] = [p.strip() for p in parties if p.strip()]

            elif idx == 6:  # Date of signature
                treaty["date_signed"] = parse_date(re.sub(r'<[^>]+>', '', content))

            elif idx == 7:  # Date of entry into force
                treaty["date_in_force"] = parse_date(re.sub(r'<[^>]+>', '', content))

            elif idx == 8:  # Termination date
                treaty["date_terminated"] = parse_date(re.sub(r'<[^>]+>', '', content))

            elif idx == 9:  # Text download links
                file_links = re.findall(
                    r'treaty-files/(\d+)/download"[^>]*>([^<]+)</a>',
                    content,
                )
                treaty["files"] = [
                    {"file_id": fid, "language": lang.strip()}
                    for fid, lang in file_links
                    if fid != "0"
                ]

        treaty["country_page"] = country_name
        if treaty.get("title"):
            treaties.append(treaty)

    return treaties


def download_treaty_text(treaty: dict) -> str:
    """Download and extract text from a treaty's PDF file."""
    files = treaty.get("files", [])
    if not files:
        return ""

    # Prefer English, then French, then first available
    preferred = None
    for f in files:
        if f["language"].lower() == "en":
            preferred = f
            break
    if not preferred:
        for f in files:
            if f["language"].lower() == "fr":
                preferred = f
                break
    if not preferred:
        preferred = files[0]

    url = f"{BASE_URL}/international-investment-agreements/treaty-files/{preferred['file_id']}/download"
    print(f"    Downloading PDF ({preferred['language']})...")
    pdf_bytes = fetch_pdf(url)
    if not pdf_bytes:
        return ""

    text = extract_text_from_pdf(pdf_bytes)
    return text


def normalize(treaty: dict, full_text: str, country_name: str) -> dict:
    """Normalize treaty data into standard schema."""
    treaty_id = treaty.get("treaty_id", "")
    detail_url = treaty.get("detail_url", "")
    url = f"{BASE_URL}{detail_url}" if detail_url else ""

    # Extract parties from title (e.g., "France - Iraq BIT (2010)")
    title = treaty.get("title", "")
    title_parties = re.split(r'\s*-\s+', re.sub(r'\s*(?:BIT|TIP|FTA|EPA|TIFA|IFD).*', '', title))
    title_parties = [p.strip() for p in title_parties if p.strip()]
    all_parties = title_parties if title_parties else [country_name] + treaty.get("other_parties", [])

    return {
        "_id": f"UNCTAD-IIA-{treaty_id}" if treaty_id else f"UNCTAD-IIA-{hash(treaty.get('title', '')) % 10**8}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": treaty.get("title", ""),
        "text": full_text,
        "date": treaty.get("date_signed"),
        "url": url,
        "treaty_type": treaty.get("treaty_type", ""),
        "status": treaty.get("status", ""),
        "parties": all_parties,
        "date_signed": treaty.get("date_signed"),
        "date_in_force": treaty.get("date_in_force"),
        "date_terminated": treaty.get("date_terminated"),
        "language": next(
            (f["language"] for f in treaty.get("files", [])
             if f["language"].lower() == "en"),
            treaty.get("files", [{}])[0].get("language", "") if treaty.get("files") else "",
        ),
    }


def fetch_country_treaties(
    iia_id: int, url_name: str, country_name: str, limit: int = 0
) -> Generator[dict, None, None]:
    """Fetch all treaties for a country with full text."""
    url = f"{BASE_URL}/international-investment-agreements/countries/{iia_id}/{url_name}"
    print(f"\n  Fetching country page: {country_name}...")
    html = fetch_html(url)
    if not html:
        return

    treaties = parse_treaties_from_html(html, country_name)
    print(f"  Found {len(treaties)} treaties for {country_name}")

    # Filter to only those with downloadable text
    with_text = [t for t in treaties if t.get("files")]
    print(f"  {len(with_text)} have downloadable text")

    fetched = 0
    for treaty in with_text:
        title = treaty.get("title", "unknown")
        print(f"  [{fetched+1}] {title[:60]}...")
        full_text = download_treaty_text(treaty)
        if not full_text:
            print(f"    WARNING: No text extracted")
            continue

        record = normalize(treaty, full_text, country_name)
        yield record
        fetched += 1
        if limit > 0 and fetched >= limit:
            return
        time.sleep(RATE_LIMIT_DELAY)


def bootstrap_sample():
    """Fetch sample records for validation."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    total = 0

    for country in SAMPLE_COUNTRIES:
        for record in fetch_country_treaties(
            country["IiaId"],
            country["UrlName"],
            country["Name"],
            limit=SAMPLE_TREATIES_PER_COUNTRY,
        ):
            fname = SAMPLE_DIR / f"{record['_id']}.json"
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            total += 1
            text_len = len(record.get("text", ""))
            print(f"    Saved {fname.name} ({text_len} chars text)")

    print(f"\nSample complete: {total} records saved to {SAMPLE_DIR}")
    validate_sample()


def bootstrap_full():
    """Fetch all treaties from all countries."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    # First get country catalog
    print("Fetching country catalog...")
    url = f"{BASE_URL}/international-investment-agreements/countries/72/france"
    html = fetch_html(url)
    if not html:
        print("ERROR: Cannot fetch country catalog")
        return

    catalog = get_country_catalog(html)
    print(f"Found {len(catalog)} countries with IIA pages")

    total = 0
    seen_ids = set()

    for country in catalog:
        iia_id = country["IiaId"]
        url_name = country["UrlName"]
        name = country["Name"]

        for record in fetch_country_treaties(iia_id, url_name, name):
            if record["_id"] in seen_ids:
                continue
            seen_ids.add(record["_id"])

            fname = SAMPLE_DIR / f"{record['_id']}.json"
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            total += 1

        time.sleep(RATE_LIMIT_DELAY)

    print(f"\nFull bootstrap complete: {total} records saved.")


def validate_sample():
    """Validate sample records."""
    files = list(SAMPLE_DIR.glob("*.json"))
    if not files:
        print("FAIL: No sample files found")
        return False

    print(f"\nValidating {len(files)} sample records...")
    issues = []
    for f in files:
        with open(f, "r", encoding="utf-8") as fh:
            rec = json.load(fh)
        name = f.name
        if not rec.get("text"):
            issues.append(f"{name}: missing or empty 'text' field")
        elif len(rec["text"]) < 100:
            issues.append(f"{name}: text too short ({len(rec['text'])} chars)")
        if not rec.get("title"):
            issues.append(f"{name}: missing title")
        if not rec.get("_id"):
            issues.append(f"{name}: missing _id")

    if issues:
        print("ISSUES FOUND:")
        for i in issues:
            print(f"  - {i}")
        return False
    else:
        print("ALL CHECKS PASSED")
        for f in files[:3]:
            with open(f, "r", encoding="utf-8") as fh:
                rec = json.load(fh)
            print(f"  {rec['_id']}: {rec['title'][:50]}... ({len(rec.get('text',''))} chars)")
        return True


def test_connectivity():
    """Test connectivity to UNCTAD IIA database."""
    print("Testing UNCTAD IIA connectivity...\n")

    # Test country page
    url = f"{BASE_URL}/international-investment-agreements/countries/72/france"
    html = fetch_html(url)
    if not html:
        print("  Country page: FAILED")
        return False

    treaties = parse_treaties_from_html(html, "France")
    print(f"  Country page: OK ({len(treaties)} treaties for France)")

    with_text = [t for t in treaties if t.get("files")]
    print(f"  Treaties with text: {len(with_text)}")

    # Test country catalog
    catalog = get_country_catalog(html)
    print(f"  Country catalog: {len(catalog)} countries")

    # Test PDF download
    if with_text:
        t = with_text[0]
        f = t["files"][0]
        pdf_url = f"{BASE_URL}/international-investment-agreements/treaty-files/{f['file_id']}/download"
        pdf_bytes = fetch_pdf(pdf_url)
        if pdf_bytes:
            text = extract_text_from_pdf(pdf_bytes)
            print(f"\n  PDF download + extraction: OK ({len(text)} chars)")
        else:
            print("\n  PDF download: FAILED")

    print("\nConnectivity test complete.")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="INTL/UNCTADInvestment data fetcher")
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
