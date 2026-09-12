#!/usr/bin/env python3
"""
GU/SuperiorCourt -- Guam Superior Court Decisions & Orders Data Fetcher

Fetches full text of Superior Court decisions from guamcourts.gov.

The judiciary retired the classic ASP site; decisions now live in two places:
  * the current year on a Drupal page, and
  * 1998-2025 in a "legacy data" archive whose year picker is backed by a
    plain GET endpoint (?action=get_items&type=SUPDO&year=YYYY).
Each entry links to a PDF decision document.

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample records
  python bootstrap.py bootstrap --full     # Full bootstrap (all years)
  python bootstrap.py bootstrap-fast       # Alias for --full (fleet runner)
  python bootstrap.py test-api             # Quick connectivity test
"""

import argparse
import html as html_mod
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import quote, urlsplit, urlunsplit

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

try:
    import pdfplumber
except ImportError:
    print("ERROR: pdfplumber not installed. Run: pip3 install pdfplumber")
    sys.exit(1)

# Setup
SOURCE_ID = "GU/SuperiorCourt"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GU.SuperiorCourt")

BASE_URL = "https://guamcourts.gov"
# Current year, published straight onto the Drupal page.
DECISIONS_URL = f"{BASE_URL}/courts-council/superior-court/decisions-and-orders"
# Everything older, behind the legacy archive's year picker.
LEGACY_URL = f"{BASE_URL}/legacydata/superior-court-decisions"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

session = requests.Session()
session.headers.update(HEADERS)

# The legacy year picker offers 1998 through last year; the current year lives
# on DECISIONS_URL instead.
FIRST_LEGACY_YEAR = 1998
CURRENT_YEAR = datetime.now(timezone.utc).year

# Each decision is an <a> to a PDF; the case number sits just before it and the
# decision type + date just after, in both the Drupal and legacy layouts.
ANCHOR_RE = re.compile(r'<a\s[^>]*href="(?P<url>[^"]+?\.pdf)"[^>]*>(?P<name>.*?)</a>', re.I | re.S)
TAG_RE = re.compile(r"<[^>]+>")
# Some entries split the case number across an inline <span> ("CV0077<span>-26</span>"),
# so tolerate whitespace where tag-stripping left a gap.
CASE_RE = re.compile(r"[A-Z]{2,3}\s?\d{2,4}\s*-\s*\d{2}(?:\s*,\s*[A-Z]{0,3}\s?\d{2,4}\s*-\s*\d{2})*")
DATE_RE = re.compile(r"(\d{1,2})[-/.](\d{1,2})[-/.](\d{4})")


def strip_tags(fragment: str) -> str:
    """Turn an HTML fragment into collapsed plain text."""
    text = TAG_RE.sub(" ", fragment)
    return re.sub(r"\s+", " ", html_mod.unescape(text)).strip()


def absolute_pdf_url(href: str) -> str:
    """Resolve a PDF href to an absolute, correctly-encoded URL.

    Legacy filenames contain spaces and ampersands, so the path has to be
    percent-encoded before requests will send it.
    """
    href = html_mod.unescape(href.strip())
    if not href.startswith("http"):
        href = f"{BASE_URL}{href if href.startswith('/') else '/' + href}"
    parts = urlsplit(href)
    return urlunsplit((parts.scheme, parts.netloc, quote(parts.path, safe="/%"),
                       parts.query, parts.fragment))


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    text_parts = []
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
    except Exception as e:
        logger.warning(f"PDF extraction error: {e}")
    return "\n\n".join(text_parts)


def parse_decision_date(date_str: str) -> Optional[str]:
    """Parse a date like '04-03-2026' or '1-9-2026' to ISO 8601."""
    if not date_str:
        return None
    for fmt in ("%m-%d-%Y", "%m-%d-%y"):
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def parse_listing(page_html: str, year: int) -> list[dict]:
    """Parse decision entries out of a listing page.

    The Drupal page wraps each entry in a <p> and the legacy archive in a
    div.item_for_list, but both put the case number immediately before the PDF
    link and the decision type + date immediately after, so we read a window
    around each anchor rather than matching either layout exactly.
    """
    entries = []
    for m in ANCHOR_RE.finditer(page_html):
        pdf_url = absolute_pdf_url(m.group("url"))
        case_name = strip_tags(m.group("name"))

        before = strip_tags(page_html[max(0, m.start() - 400):m.start()])
        after = strip_tags(page_html[m.end():m.end() + 300])
        # Stop at the next entry so we don't absorb its posted date.
        after = re.split(r"Posted:", after)[0].strip()

        case_matches = CASE_RE.findall(before)
        case_num = re.sub(r"\s*-\s*", "-", case_matches[-1]).strip() if case_matches else ""

        date_match = DATE_RE.search(after)
        if date_match:
            decision_date = "-".join(date_match.groups())
            decision_type = after[:date_match.start()].strip().rstrip(",").strip()
        else:
            decision_date = None
            decision_type = after.strip().rstrip(",").strip()

        filename = pdf_url.rsplit("/", 1)[-1][:-4]
        entries.append({
            "case_number": case_num,
            "case_name": case_name,
            "decision_type": decision_type,
            "decision_date": decision_date,
            "year": year,
            "pdf_url": pdf_url,
            "filename": filename,
        })
    return entries


def fetch_decisions_for_year(year: int) -> list[dict]:
    """Fetch all decision entries for a given year."""
    logger.info(f"Fetching decisions for year {year}")
    if year >= CURRENT_YEAR:
        resp = session.get(DECISIONS_URL, timeout=60)
    else:
        resp = session.get(
            LEGACY_URL,
            params={"action": "get_items", "type": "SUPDO", "year": str(year)},
            timeout=60,
        )
    resp.raise_for_status()

    entries = parse_listing(resp.text, year)
    # The same PDF can be listed twice when a decision is re-posted.
    seen, unique = set(), []
    for entry in entries:
        if entry["pdf_url"] in seen:
            continue
        seen.add(entry["pdf_url"])
        unique.append(entry)

    logger.info(f"Year {year}: found {len(unique)} decisions")
    return unique


def normalize(raw: dict) -> dict:
    """Normalize a raw decision record into standard schema."""
    case_num = raw.get("case_number", "unknown")
    filename = raw.get("filename", case_num)
    doc_id = f"GU-SC-{filename}"

    case_name = raw.get("case_name", "")
    decision_type = raw.get("decision_type", "")
    title = case_name
    if decision_type:
        title = f"{case_name} - {decision_type}"

    iso_date = parse_decision_date(raw.get("decision_date"))

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": raw.get("text", ""),
        "date": iso_date,
        "url": raw.get("pdf_url", ""),
        "case_number": case_num,
        "case_name": case_name,
        "decision_type": decision_type,
        "jurisdiction": "GU",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield normalized records for all decisions."""
    count = 0
    sample_limit = 15 if sample else None
    errors = 0
    # A share of the legacy links are dead upstream, so tolerate misses over a
    # ~3,400-document corpus instead of aborting the run.
    max_errors = 20 if sample else 500

    # In sample mode, only fetch the current year.
    years = [CURRENT_YEAR] if sample else list(range(CURRENT_YEAR, FIRST_LEGACY_YEAR - 1, -1))

    for year in years:
        if sample_limit and count >= sample_limit:
            break

        try:
            entries = fetch_decisions_for_year(year)
        except Exception as e:
            logger.error(f"Failed to fetch year {year}: {e}")
            errors += 1
            continue

        time.sleep(1)

        for entry in entries:
            if sample_limit and count >= sample_limit:
                break
            if errors >= max_errors:
                logger.error("Too many errors, stopping")
                break

            pdf_url = entry["pdf_url"]
            logger.info(f"Downloading {entry['case_number']} ({entry['case_name'][:40]})")

            try:
                resp = None
                for attempt in range(3):
                    try:
                        resp = session.get(pdf_url, timeout=60)
                        resp.raise_for_status()
                        break
                    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as retry_e:
                        if attempt < 2:
                            logger.warning(f"Retry {attempt+1}: {retry_e}")
                            time.sleep(2)
                        else:
                            raise

                text = extract_pdf_text(resp.content)

                if not text.strip():
                    logger.warning(f"No text from {pdf_url} (may be scanned)")
                    errors += 1
                    continue

                entry["text"] = text
                yield normalize(entry)
                count += 1

            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP error for {pdf_url}: {e}")
                errors += 1
            except Exception as e:
                logger.error(f"Failed to fetch {pdf_url}: {e}")
                errors += 1

            time.sleep(1)

    logger.info(f"Total records yielded: {count} (errors: {errors})")


def save_records(records: list[dict], output_dir: Path) -> int:
    """Save records as individual JSON files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    saved = 0
    for rec in records:
        doc_id = rec["_id"].replace("/", "_")
        fname = f"record_{doc_id}.json"
        fpath = output_dir / fname
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False, indent=2)
        saved += 1
    return saved


def cmd_test_api():
    """Quick connectivity test."""
    print(f"Testing connection to {DECISIONS_URL}...")
    resp = session.get(DECISIONS_URL, timeout=15)
    print(f"Status: {resp.status_code}")

    entries = fetch_decisions_for_year(2025)
    print(f"Found {len(entries)} decisions for 2025")

    if entries:
        entry = entries[0]
        print(f"\nFirst entry:")
        print(f"  Case: {entry['case_number']}")
        print(f"  Name: {entry['case_name']}")
        print(f"  Type: {entry['decision_type']}")
        print(f"  Date: {entry['decision_date']}")
        print(f"  URL: {entry['pdf_url']}")

        print(f"\nTesting PDF download...")
        resp = session.get(entry["pdf_url"], timeout=30)
        print(f"PDF status: {resp.status_code}, size: {len(resp.content)} bytes")
        text = extract_pdf_text(resp.content)
        print(f"Extracted text: {len(text)} chars")
        if text:
            print(f"First 300 chars: {text[:300]}")

    print("\nConnectivity test PASSED")


def cmd_bootstrap(sample: bool = False, full: bool = False):
    """Bootstrap the data source."""
    mode = "sample" if sample else "full"
    logger.info(f"Starting bootstrap in {mode} mode")

    if sample:
        records = list(fetch_all(sample=True))
        if not records:
            logger.error("No records fetched!")
            sys.exit(1)
        saved = save_records(records, SAMPLE_DIR)
        texts_ok = sum(1 for r in records if len(r.get("text", "").strip()) > 100)
        output = SAMPLE_DIR
    else:
        # Stream to JSONL so a full run never holds the corpus in memory.
        output = SOURCE_DIR / "data" / "records.jsonl"
        output.parent.mkdir(parents=True, exist_ok=True)
        saved = texts_ok = 0
        with open(output, "w", encoding="utf-8") as f:
            for rec in fetch_all(sample=False):
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                saved += 1
                if len(rec.get("text", "").strip()) > 100:
                    texts_ok += 1
        if not saved:
            logger.error("No records fetched!")
            sys.exit(1)

    logger.info(f"Saved {saved} records to {output}")
    print(f"\n{'='*60}")
    print(f"Bootstrap complete ({mode} mode)")
    print(f"Records: {saved}")
    print(f"With substantial text: {texts_ok}/{saved}")
    print(f"Output: {output}")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="GU/SuperiorCourt bootstrapper")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Bootstrap data")
    boot.add_argument("--sample", action="store_true", help="Sample mode (~15 records)")
    boot.add_argument("--full", action="store_true", help="Full bootstrap")

    # The fleet runner invokes `bootstrap-fast`; route it to the full path.
    fast = sub.add_parser("bootstrap-fast", help="Alias for bootstrap --full")
    fast.add_argument("--sample", action="store_true", help="Sample mode (~15 records)")
    fast.add_argument("--full", action="store_true", help="Full bootstrap")

    sub.add_parser("test-api", help="Test API connectivity")

    args = parser.parse_args()

    if args.command == "test-api":
        cmd_test_api()
    elif args.command in ("bootstrap", "bootstrap-fast"):
        cmd_bootstrap(sample=args.sample, full=args.full)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
