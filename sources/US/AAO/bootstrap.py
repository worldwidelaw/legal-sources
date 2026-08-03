#!/usr/bin/env python3
"""
US/AAO -- USCIS Administrative Appeals Office Non-Precedent Decisions

Fetches non-precedent decisions of the USCIS Administrative Appeals Office
(AAO), which adjudicates administrative appeals of denied immigration benefit
petitions/applications (I-140 employment petitions, T/U nonimmigrant status,
waivers, refugee travel documents, etc.). Each decision resolves a specific
case = case_law; US federal-government works, public domain (17 U.S.C. 105).

Strategy:
  - The non-precedent decisions listing at
    /administrative-appeals/aao-decisions/aao-non-precedent-decisions is a
    Drupal view paginated with ?page=N (0-indexed, 10 decisions/page, newest
    first). Iterate pages until a page yields no decision PDFs.
  - Each list entry is an anchor whose text is
    "{Category} - {MON}{DD}{YYYY}_{seq}{code} (PDF, {size})" and whose href is
    /sites/default/files/err/{CODE} - {Category}/Decisions_Issued_in_{YEAR}/
    {stem}.pdf. The category, benefit-type code, decision date and a stable id
    are recovered from the anchor text + href.
  - Download each born-digital decision PDF and extract full text via
    common.pdf_extract (text layer; no OCR needed).

Usage:
  python bootstrap.py bootstrap --sample   # ~15 sample decisions
  python bootstrap.py bootstrap             # Full extraction -> data/records.jsonl
  python bootstrap.py bootstrap-fast        # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api              # Test connectivity
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional, Union
from urllib.parse import unquote

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "US/AAO"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.AAO")

BASE_URL = "https://www.uscis.gov"
LISTING_URL = f"{BASE_URL}/administrative-appeals/aao-decisions/aao-non-precedent-decisions"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/pdf",
    "Accept-Language": "en-US,en;q=0.9",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

CRAWL_DELAY = 1.5  # seconds between requests
MAX_EMPTY_PAGES = 3  # stop after this many consecutive PDF-less pages

MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

PDF_ANCHOR_RE = re.compile(
    r'<a\s+[^>]*href="([^"]+\.pdf)"[^>]*>(.*?)</a>', re.IGNORECASE | re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")
DATE_STEM_RE = re.compile(r"([A-Za-z]{3})(\d{2})(\d{4})_(\w+)")


def fetch_url(url: str, binary: bool = False) -> Optional[Union[bytes, str]]:
    """Fetch a URL with retry logic."""
    for attempt in range(3):
        try:
            resp = SESSION.get(url, timeout=60, allow_redirects=True)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.content if binary else resp.text
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < 2:
                time.sleep(5 * (attempt + 1))
            else:
                logger.warning(f"Failed to fetch {url}: {e}")
                return None
        except Exception as e:
            logger.warning(f"Error fetching {url}: {e}")
            return None


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    return extract_pdf_markdown(
        source="US/AAO",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", unescape(TAG_RE.sub(" ", s or ""))).strip()


def _parse_stem(stem: str) -> tuple:
    """From a filename stem 'NOV172025_04D12101' recover (iso_date, code)."""
    m = DATE_STEM_RE.match(stem)
    if not m:
        return None, None
    mon = MONTHS.get(m.group(1).lower())
    day = int(m.group(2))
    year = int(m.group(3))
    tail = m.group(4)  # e.g. 04D12101 -> seq 04, then category code
    iso = None
    if mon:
        try:
            iso = datetime(year, mon, day).strftime("%Y-%m-%d")
        except ValueError:
            iso = None
    # Category code = leading letters+digits after the numeric sequence.
    cm = re.match(r"\d+([A-Za-z]+\d+)", tail)
    code = cm.group(1) if cm else None
    return iso, code


def _code_from_href(href: str) -> Optional[str]:
    """Recover the benefit-type code from the /err/{CODE} - {desc}/ folder."""
    m = re.search(r"/err/([A-Za-z0-9]+)\s*(?:-|%20-)", unquote(href))
    return m.group(1) if m else None


def parse_listing(html: str) -> list:
    """Parse decision entries from one non-precedent listing page."""
    out = []
    seen = set()
    for href, inner in PDF_ANCHOR_RE.findall(html):
        low = href.lower()
        if "/err/" not in low or "decisions_issued_in" not in low:
            continue
        url = href if href.startswith("http") else f"{BASE_URL}{href}"
        if url in seen:
            continue
        seen.add(url)

        text = _clean(inner)
        # Anchor text: "{Category} - {STEM} (PDF, {size})"
        category = None
        cm = re.match(r"(.*?)\s+-\s+[A-Za-z]{3}\d{2}\d{4}_", text)
        if cm:
            category = cm.group(1).strip()

        stem = unquote(url).rsplit("/", 1)[-1][:-4]  # drop .pdf
        iso_date, stem_code = _parse_stem(stem)
        # The /err/{CODE} - {desc}/ folder carries the authoritative benefit
        # code (e.g. "D12"); the filename tail duplicates it with the page
        # sequence appended ("D12101"), so prefer the folder code.
        code = _code_from_href(url) or stem_code
        if not category:
            # Fall back to the folder description.
            fm = re.search(r"/err/[^/]*?-\s*([^/]+?)/Decisions_Issued", unquote(url))
            category = fm.group(1).strip() if fm else (code or "AAO Decision")

        out.append({
            "stem": stem,
            "category": category,
            "code": code,
            "date": iso_date,
            "pdf_url": url,
        })
    return out


def _iter_pages(max_pages: Optional[int] = None) -> Generator[list, None, None]:
    """Yield parsed entries per listing page until the corpus is exhausted."""
    page = 0
    empty_streak = 0
    while True:
        if max_pages is not None and page >= max_pages:
            return
        url = LISTING_URL if page == 0 else f"{LISTING_URL}?page={page}"
        html = fetch_url(url)
        time.sleep(CRAWL_DELAY)
        entries = parse_listing(html) if html else []
        if not entries:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY_PAGES:
                return
            page += 1
            continue
        empty_streak = 0
        yield entries
        page += 1


def normalize(raw: dict, text: str) -> dict:
    """Normalize an AAO non-precedent decision record."""
    stem = raw["stem"]
    category = raw["category"]
    code = raw.get("code")

    # Pull the redacted "In Re: NNNNN" identifier from the body if present.
    in_re = None
    mr = re.search(r"In Re:\s*([0-9A-Za-z\-]+)", text)
    if mr:
        in_re = mr.group(1).strip()

    title = f"AAO Non-Precedent Decision — {category}"
    if raw.get("date"):
        title += f" ({raw['date']})"

    return {
        "_id": f"US/AAO/{stem}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": raw.get("date"),
        "url": raw["pdf_url"],
        "category": category,
        "benefit_code": code,
        "in_re": in_re,
        "authority": "USCIS Administrative Appeals Office",
        "precedential": False,
        "jurisdiction": "US",
        "language": "en",
    }


def fetch_all() -> Generator[dict, None, None]:
    """Yield every AAO non-precedent decision with full text."""
    total = 0
    seen_stems = set()
    for entries in _iter_pages():
        for raw in entries:
            if raw["stem"] in seen_stems:
                continue
            seen_stems.add(raw["stem"])
            pdf_bytes = fetch_url(raw["pdf_url"], binary=True)
            time.sleep(CRAWL_DELAY)
            if not pdf_bytes:
                logger.warning(f"  Failed to download {raw['pdf_url']}")
                continue
            text = extract_text_from_pdf(pdf_bytes)
            if not text or len(text) < 100:
                logger.warning(f"  No text extracted for {raw['pdf_url']}")
                continue
            record = normalize(raw, text)
            total += 1
            if total % 100 == 0:
                logger.info(f"  Progress: {total} decisions fetched")
            yield record
    logger.info(f"Total decisions with full text: {total}")


def fetch_sample(count: int = 15) -> list:
    """Fetch sample records from the first listing pages."""
    records = []
    seen_stems = set()
    for entries in _iter_pages(max_pages=8):
        for raw in entries:
            if len(records) >= count:
                return records
            if raw["stem"] in seen_stems:
                continue
            seen_stems.add(raw["stem"])
            pdf_bytes = fetch_url(raw["pdf_url"], binary=True)
            time.sleep(CRAWL_DELAY)
            if not pdf_bytes:
                continue
            text = extract_text_from_pdf(pdf_bytes)
            if not text or len(text) < 100:
                continue
            record = normalize(raw, text)
            records.append(record)
            logger.info(f"  [{len(records)}] {record['title'][:60]} - {len(text)} chars")
    return records


def test_api():
    """Test connectivity to the AAO listing + a decision PDF."""
    logger.info("Testing AAO connectivity...")
    html = fetch_url(LISTING_URL)
    if not html:
        logger.error("Listing page unreachable")
        return False
    entries = parse_listing(html)
    if not entries:
        logger.error("No decisions parsed from listing page 0")
        return False
    logger.info(f"Parsed {len(entries)} decisions from page 0")

    time.sleep(CRAWL_DELAY)
    raw = entries[0]
    logger.info(f"Sample entry: {raw['category']} / {raw['code']} / {raw['date']}")
    pdf_bytes = fetch_url(raw["pdf_url"], binary=True)
    if not pdf_bytes:
        logger.error("PDF download failed")
        return False
    logger.info(f"PDF download OK - {len(pdf_bytes)} bytes")
    text = extract_text_from_pdf(pdf_bytes)
    if not text:
        logger.error("PDF text extraction failed")
        return False
    logger.info(f"Text extraction OK - {len(text)} chars")
    logger.info(f"Preview: {text[:200]}...")
    return True


def bootstrap_sample():
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    records = fetch_sample(count=15)
    if not records:
        logger.error("No records fetched!")
        return False
    for i, record in enumerate(records, 1):
        safe_id = re.sub(r'[^\w\-]', '_', record["_id"])[:80]
        with open(SAMPLE_DIR / f"sample_{i:02d}_{safe_id}.json", "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
    logger.info(f"\nSaved {len(records)} sample records to {SAMPLE_DIR}")
    text_lengths = [len(r.get("text", "")) for r in records]
    avg_text = sum(text_lengths) / len(text_lengths) if text_lengths else 0
    logger.info("Validation:")
    logger.info(f"  - Records with text: {sum(1 for t in text_lengths if t > 0)}/{len(records)}")
    logger.info(f"  - Avg text length: {avg_text:.0f} chars")
    logger.info(f"  - Min text length: {min(text_lengths) if text_lengths else 0}")
    logger.info(f"  - Max text length: {max(text_lengths) if text_lengths else 0}")
    return len(records) >= 10 and avg_text > 500


def bootstrap_full():
    data_dir = SOURCE_DIR / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    out_path = data_dir / "records.jsonl"
    count = 0
    with open(out_path, "w", encoding="utf-8") as fh:
        for record in fetch_all():
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
    logger.info(f"Processed {count} records -> {out_path}")
    return count


def main():
    parser = argparse.ArgumentParser(description="US/AAO Data Fetcher")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command == "test-api":
        sys.exit(0 if test_api() else 1)
    else:
        if args.sample:
            sys.exit(0 if bootstrap_sample() else 1)
        logger.info("Full bootstrap mode")
        bootstrap_full()


if __name__ == "__main__":
    main()
