#!/usr/bin/env python3
"""
TR/GIB-Ozelgeler -- Turkish Revenue Administration Tax Rulings (Özelgeler)

Fetches binding tax rulings from the Turkish Revenue Administration (GİB).
Uses vergibulustayi.com DataTables JSON API for listing (18,000+ records)
and detail pages for full text extraction.

GİB official site is a Next.js SPA with no accessible REST API.
vergibulustayi.com aggregates GİB özelge data with open access.

Usage:
  python bootstrap.py bootstrap --sample    # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Quick connectivity test
"""

import argparse
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("TR.GIB-Ozelgeler")

SOURCE_ID = "TR/GIB-Ozelgeler"
LIST_API = "https://www.vergibulustayi.com/ozelgeler/data"
DETAIL_URL = "https://www.vergibulustayi.com/ozelgeler/{id}"
GIB_URL = "https://gib.gov.tr/mevzuat/kanun/{kanun_id}/ozelge/{id}"

REQUEST_DELAY = 2.0
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
}


def http_get(url: str, retries: int = 3) -> Optional[str]:
    """Make HTTP GET request with retries."""
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            resp = urllib.request.urlopen(req, timeout=60)
            return resp.read().decode("utf-8", errors="replace")
        except (urllib.error.HTTPError, urllib.error.URLError, OSError) as e:
            code = getattr(e, 'code', None)
            if code and code < 500:
                logger.warning(f"Client error {code} for {url}")
                return None
            logger.warning(f"Attempt {attempt+1}/{retries} failed for {url}: {e}")
            if attempt < retries - 1:
                time.sleep(REQUEST_DELAY * (attempt + 1))
    return None


def fetch_list_page(start: int = 0, length: int = 100) -> Optional[dict]:
    """Fetch a page of özelge listings from the DataTables API."""
    params = urllib.parse.urlencode({
        "draw": 1,
        "start": start,
        "length": length,
    })
    url = f"{LIST_API}?{params}"
    text = http_get(url)
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error: {e}")
        return None


def parse_list_record(row: list) -> dict:
    """Parse a raw DataTables row into structured metadata."""
    record_id = row[0]
    ref_number = row[1]

    # Extract title from HTML anchor
    title_html = row[2]
    title_match = re.search(r'>([^<]+)<', title_html)
    title = title_match.group(1).strip() if title_match else ""

    kanun_code = row[3]

    # Extract kanun name from badge HTML
    kanun_html = row[4]
    kanun_match = re.search(r'title="([^"]*)"', kanun_html)
    kanun_name = kanun_match.group(1).strip() if kanun_match else ""
    kanun_badge = re.search(r'>([^<]+)<', kanun_html)
    kanun_short = kanun_badge.group(1).strip() if kanun_badge else ""

    date_str = row[5]

    # Extract GIB kanun_id from the link
    gib_link = ""
    gib_match = re.search(r'href="https://gib\.gov\.tr/mevzuat/kanun/(\d+)/ozelge/(\d+)"', row[7])
    kanun_id = gib_match.group(1) if gib_match else ""

    return {
        "id": record_id,
        "ref_number": ref_number,
        "title": title,
        "kanun_code": kanun_code,
        "kanun_name": kanun_name,
        "kanun_short": kanun_short,
        "date_str": date_str,
        "kanun_id": kanun_id,
    }


def fetch_detail(record_id: str) -> Optional[str]:
    """Fetch full text from the detail page."""
    url = DETAIL_URL.format(id=record_id)
    html = http_get(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # The main content is in div.content (full özelge text)
    # div.card-body only has metadata — must check div.content first
    content = None

    for selector in [
        "div.content",
        "div.ozelge-content",
        "article",
    ]:
        el = soup.select_one(selector)
        if el:
            text = el.get_text(separator="\n", strip=True)
            if len(text) > 200:
                content = text
                break

    if not content:
        # Fallback: find the largest text block on the page
        main = soup.find("main") or soup.find("body")
        if main:
            # Remove nav, header, footer, scripts
            for tag in main.find_all(["nav", "header", "footer", "script", "style"]):
                tag.decompose()
            text = main.get_text(separator="\n", strip=True)
            if len(text) > 200:
                content = text

    return content


def parse_date(date_str: str) -> str:
    """Parse DD.MM.YYYY -> YYYY-MM-DD."""
    m = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", date_str)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return ""


def normalize(meta: dict, full_text: str) -> dict:
    """Transform into standard schema."""
    date_iso = parse_date(meta.get("date_str", ""))
    record_id = meta["id"]
    kanun_id = meta.get("kanun_id", "")

    gib_url = GIB_URL.format(kanun_id=kanun_id, id=record_id) if kanun_id else ""

    title = meta.get("title", "")
    kanun_short = meta.get("kanun_short", "")
    if kanun_short and title:
        full_title = f"Özelge {record_id} - {kanun_short}: {title}"
    else:
        full_title = f"Özelge {record_id}: {title}"

    return {
        "_id": f"GIB-OZG-{record_id}",
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": full_title,
        "text": full_text,
        "date": date_iso,
        "url": gib_url or DETAIL_URL.format(id=record_id),
        "ref_number": meta.get("ref_number", ""),
        "kanun": kanun_short,
        "kanun_name": meta.get("kanun_name", ""),
        "language": "tr",
        "jurisdiction": "TR",
    }


def fetch_all(max_records: int = 0) -> Generator[dict, None, None]:
    """Fetch all özelge records with full text."""
    start = 0
    page_size = 100
    total = None
    fetched = 0

    while True:
        logger.info(f"Fetching list page start={start}...")
        data = fetch_list_page(start=start, length=page_size)
        if not data:
            break

        if total is None:
            total = data.get("recordsTotal", 0)
            logger.info(f"Total records: {total}")

        rows = data.get("data", [])
        if not rows:
            break

        for row in rows:
            meta = parse_list_record(row)
            record_id = meta["id"]

            time.sleep(REQUEST_DELAY)
            full_text = fetch_detail(record_id)

            if not full_text or len(full_text) < 100:
                logger.warning(f"No/short text for {record_id}: {meta.get('title', '')[:40]}")
                continue

            record = normalize(meta, full_text)
            yield record
            fetched += 1

            if fetched % 50 == 0:
                logger.info(f"Fetched {fetched} records...")

            if max_records > 0 and fetched >= max_records:
                return

        start += len(rows)
        if start >= (total or 0):
            break

        time.sleep(REQUEST_DELAY)

    logger.info(f"Total fetched: {fetched}")


def bootstrap_sample(count: int = 15) -> list:
    """Fetch sample records for testing."""
    samples = []

    logger.info(f"Fetching {count} sample records...")

    # Get first page of listings
    data = fetch_list_page(start=0, length=count + 5)
    if not data:
        logger.error("Cannot fetch listing")
        return samples

    total = data.get("recordsTotal", 0)
    logger.info(f"Total available: {total}")

    rows = data.get("data", [])
    for row in rows[:count + 5]:
        if len(samples) >= count:
            break

        meta = parse_list_record(row)
        record_id = meta["id"]

        time.sleep(REQUEST_DELAY)
        logger.info(f"Fetching detail for {record_id}: {meta.get('title', '')[:50]}...")
        full_text = fetch_detail(record_id)

        if not full_text or len(full_text) < 100:
            logger.warning(f"  No/short text, skipping")
            continue

        record = normalize(meta, full_text)
        text_len = len(record.get("text", ""))
        samples.append(record)
        logger.info(f"  OK: {text_len} chars")

    return samples


def main():
    parser = argparse.ArgumentParser(description="TR/GIB-Ozelgeler Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records only")
    parser.add_argument("--count", type=int, default=15,
                        help="Number of sample records")
    parser.add_argument("--max-records", type=int, default=0,
                        help="Maximum records to fetch (0=all)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command == "test":
        print("Testing TR/GIB-Ozelgeler connectivity...\n")

        print("1. Fetching listing API...")
        data = fetch_list_page(start=0, length=3)
        if not data:
            print("   FAILED")
            sys.exit(1)
        total = data.get("recordsTotal", 0)
        print(f"   OK: {total} total records")

        rows = data.get("data", [])
        if rows:
            meta = parse_list_record(rows[0])
            print(f"   First: ID={meta['id']}, {meta['title'][:60]}")

            time.sleep(REQUEST_DELAY)
            print("\n2. Fetching detail page...")
            full_text = fetch_detail(meta["id"])
            if full_text:
                print(f"   OK: {len(full_text)} chars")
                print(f"   Preview: {full_text[:300]}...")
            else:
                print("   FAILED - no text extracted")
                sys.exit(1)

        print("\nTest complete!")

    elif args.command == "bootstrap":
        if args.sample:
            samples = bootstrap_sample(count=args.count)

            if not samples:
                logger.error("No samples fetched!")
                sys.exit(1)

            for i, record in enumerate(samples):
                filepath = sample_dir / f"sample_{i + 1:03d}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"\n{'=' * 60}")
            print(f"TR/GIB-Ozelgeler Sample Bootstrap Complete")
            print(f"{'=' * 60}")
            print(f"Records: {len(samples)}")

            text_lengths = [len(r.get("text", "")) for r in samples]
            avg_len = sum(text_lengths) / len(text_lengths) if text_lengths else 0
            min_len = min(text_lengths) if text_lengths else 0
            max_len = max(text_lengths) if text_lengths else 0
            print(f"Text lengths: min={min_len:,}, avg={avg_len:,.0f}, max={max_len:,}")

            all_have_text = all(len(r.get("text", "")) > 200 for r in samples)
            print(f"All have text >200 chars: {'YES' if all_have_text else 'NO'}")

            print(f"\nSample IDs:")
            for r in samples:
                print(f"  {r['_id']} ({r.get('date', 'no date')}) "
                      f"- {len(r.get('text', '')):,} chars - {r.get('kanun', '')}")

            print(f"\nSamples saved to: {sample_dir}")

        else:
            output_file = script_dir / "records.jsonl"
            count = 0
            with open(output_file, "w", encoding="utf-8") as f:
                for record in fetch_all(max_records=args.max_records):
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    count += 1
            print(f"\nBootstrap complete: {count} records -> {output_file}")


if __name__ == "__main__":
    main()
