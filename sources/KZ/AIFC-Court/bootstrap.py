#!/usr/bin/env python3
"""
KZ/AIFC-Court - Astana International Financial Centre Court

Data source: https://court.aifc.kz
Format: WordPress REST API + PDF judgments
License: Public Domain (court decisions)
Records: ~245 English-language judgments and orders

The AIFC Court is an English-language common law court in Kazakhstan,
handling international commercial disputes. Judgments are published as
PDFs on WordPress, accessible via the WP REST API for listing and
direct PDF download links on individual judgment pages.
"""

import argparse
import io
import json
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Dict, Generator, Optional

import pdfplumber
import requests
from bs4 import BeautifulSoup

SOURCE_ID = "KZ/AIFC-Court"
BASE_URL = "https://court.aifc.kz"
API_URL = f"{BASE_URL}/wp-json/wp/v2/judgment"
REQUEST_DELAY = 2.0
PER_PAGE = 100


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "application/json",
    })
    return session


def list_english_judgments(session: requests.Session) -> list:
    """List all English-language judgments from WP REST API."""
    all_judgments = []
    page = 1
    while True:
        try:
            resp = session.get(API_URL, params={"per_page": PER_PAGE, "page": page}, timeout=30)
            if resp.status_code != 200:
                break
            data = resp.json()
            if not data:
                break
        except Exception as e:
            print(f"  Error fetching page {page}: {e}", file=sys.stderr)
            break

        # Filter English (exclude /kk/ and /ru/ translated versions)
        english = [j for j in data if "/kk/" not in j.get("link", "") and "/ru/" not in j.get("link", "")]
        all_judgments.extend(english)

        if len(data) < PER_PAGE:
            break
        page += 1
        time.sleep(REQUEST_DELAY)

    return all_judgments


def scrape_judgment_page(session: requests.Session, url: str) -> Dict:
    """Scrape a judgment page for metadata and PDF link."""
    result = {"pdf_url": None, "case_no": None, "date": None, "judge": None}
    try:
        resp = session.get(url, timeout=30, headers={"Accept": "text/html"})
        resp.raise_for_status()
    except Exception as e:
        print(f"  Error scraping {url}: {e}", file=sys.stderr)
        return result

    soup = BeautifulSoup(resp.text, "html.parser")

    # PDF link
    pdf_link = soup.find("a", class_="link_doc")
    if pdf_link and pdf_link.get("href"):
        result["pdf_url"] = pdf_link["href"]
    else:
        # Fallback: find any PDF link in content
        for a in soup.find_all("a", href=True):
            if ".pdf" in a["href"].lower() and "wp-content" in a["href"]:
                result["pdf_url"] = a["href"]
                break

    # Case number
    text = soup.get_text()
    case_match = re.search(r"CASE\s*No:\s*(AIFC[^\n<]+)", text, re.IGNORECASE)
    if case_match:
        result["case_no"] = case_match.group(1).strip()

    # Date
    date_div = soup.find("div", class_="date_page")
    if date_div:
        result["date"] = date_div.get_text(strip=True)

    # Judge
    judge_div = soup.find("div", class_="judges_name")
    if judge_div:
        result["judge"] = judge_div.get_text(strip=True)

    return result


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract full text from PDF bytes."""
    text_parts = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text() or ""
                if page_text.strip():
                    text_parts.append(page_text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
    except Exception as e:
        print(f"  PDF extraction error: {e}", file=sys.stderr)
        return ""
    return "\n\n".join(text_parts)


def parse_date(date_str: str) -> Optional[str]:
    """Parse date from dd.mm.yyyy format to ISO."""
    if not date_str:
        return None
    match = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", date_str)
    if match:
        return f"{match.group(3)}-{match.group(2)}-{match.group(1)}"
    return None


def normalize(wp_data: Dict, page_data: Dict, full_text: str) -> Dict:
    """Transform judgment data into standard schema."""
    wp_id = wp_data.get("id", "")
    title_raw = wp_data.get("title", {}).get("rendered", "")
    title = unescape(BeautifulSoup(title_raw, "html.parser").get_text())

    case_no = page_data.get("case_no") or ""
    parsed_date = parse_date(page_data.get("date") or "")
    judge = page_data.get("judge") or ""
    link = wp_data.get("link") or f"{BASE_URL}/judgments/"

    return {
        "_id": f"KZ-AIFC-{wp_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": parsed_date,
        "url": link,
        "case_number": case_no,
        "judge": judge,
        "pdf_url": page_data.get("pdf_url") or "",
        "language": "en",
    }


def fetch_all() -> Generator[Dict, None, None]:
    """Fetch all English judgments with full text from PDFs."""
    session = get_session()

    print("  Listing English judgments from WP API...")
    judgments = list_english_judgments(session)
    print(f"  Found {len(judgments)} English judgments")

    total_yielded = 0
    for i, j in enumerate(judgments):
        link = j.get("link", "")
        title = unescape(BeautifulSoup(j.get("title", {}).get("rendered", ""), "html.parser").get_text())
        print(f"  [{i+1}/{len(judgments)}] {title[:60]}...")

        time.sleep(REQUEST_DELAY)
        page_data = scrape_judgment_page(session, link)

        if not page_data.get("pdf_url"):
            print(f"    No PDF found, skipping")
            continue

        try:
            resp = session.get(page_data["pdf_url"], timeout=60)
            resp.raise_for_status()
            pdf_bytes = resp.content
        except Exception as e:
            print(f"    PDF download error: {e}")
            continue

        full_text = extract_pdf_text(pdf_bytes)
        if not full_text or len(full_text) < 100:
            print(f"    Insufficient text ({len(full_text)} chars)")
            continue

        record = normalize(j, page_data, full_text)
        yield record
        total_yielded += 1
        time.sleep(REQUEST_DELAY)

    print(f"  Total records yielded: {total_yielded}")


def fetch_updates(since: datetime) -> Generator[Dict, None, None]:
    """Fetch judgments modified since a given date."""
    session = get_session()
    since_str = since.strftime("%Y-%m-%dT%H:%M:%S")

    judgments = []
    page = 1
    while True:
        try:
            resp = session.get(API_URL, params={
                "per_page": PER_PAGE, "page": page,
                "after": since_str, "orderby": "date", "order": "desc",
            }, timeout=30)
            if resp.status_code != 200:
                break
            data = resp.json()
            if not data:
                break
        except Exception:
            break

        english = [j for j in data if "/kk/" not in j.get("link", "") and "/ru/" not in j.get("link", "")]
        judgments.extend(english)
        if len(data) < PER_PAGE:
            break
        page += 1
        time.sleep(REQUEST_DELAY)

    for j in judgments:
        link = j.get("link", "")
        time.sleep(REQUEST_DELAY)
        page_data = scrape_judgment_page(session, link)
        if not page_data.get("pdf_url"):
            continue
        try:
            resp = session.get(page_data["pdf_url"], timeout=60)
            resp.raise_for_status()
        except Exception:
            continue
        full_text = extract_pdf_text(resp.content)
        if full_text and len(full_text) >= 100:
            yield normalize(j, page_data, full_text)


def bootstrap_sample(sample_dir: Path, count: int = 15):
    """Fetch sample records for validation."""
    session = get_session()
    sample_dir.mkdir(parents=True, exist_ok=True)

    print("Listing English judgments from WP API...")
    judgments = list_english_judgments(session)
    print(f"Found {len(judgments)} English judgments")

    records_saved = 0
    total_text_chars = 0

    for i, j in enumerate(judgments):
        if records_saved >= count:
            break

        link = j.get("link", "")
        title = unescape(BeautifulSoup(j.get("title", {}).get("rendered", ""), "html.parser").get_text())
        print(f"\n  [{records_saved+1}/{count}] {title[:70]}...")

        time.sleep(REQUEST_DELAY)
        page_data = scrape_judgment_page(session, link)

        if not page_data.get("pdf_url"):
            print(f"    No PDF found, skipping")
            continue

        print(f"    Downloading PDF...")
        try:
            resp = session.get(page_data["pdf_url"], timeout=60)
            resp.raise_for_status()
        except Exception as e:
            print(f"    PDF download error: {e}")
            continue

        full_text = extract_pdf_text(resp.content)
        text_len = len(full_text)

        if text_len < 100:
            print(f"    Insufficient text ({text_len} chars)")
            continue

        record = normalize(j, page_data, full_text)
        total_text_chars += text_len
        records_saved += 1

        safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", record["_id"])[:100]
        filename = f"{safe_name}.json"
        filepath = sample_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"    Case: {record.get('case_number', '?')} | Judge: {record.get('judge', '?')}")
        print(f"    Date: {record.get('date', '?')} | Text: {text_len:,} chars")

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
    print(f"Total English judgments available: {len(judgments)}")
    print(f"Sample directory: {sample_dir}")

    if records_saved >= 10:
        print("\nSUCCESS: 10+ sample records with full text")
    else:
        print(f"\nWARNING: Only {records_saved} records saved (need 10+)")


def main():
    parser = argparse.ArgumentParser(description="AIFC Court Judgments Fetcher")
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
                filepath = sample_dir / f"{safe_name}.json"
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
