#!/usr/bin/env python3
"""
QA/AlMeezanCaseLaw -- Qatar Al Meezan Case Law Data Fetcher

Fetches Court of Cassation decisions from Qatar's official Al Meezan
legal portal (almeezan.qa), operated by the Ministry of Justice.

Strategy:
  - Enumerate ruling IDs from 1 to ~1931
  - Fetch metadata from RulingPage.aspx?id=X&language=ar
  - Fetch full text from RulingView.aspx?opt&RulID=X&language=ar
  - Also fetch English translation if available

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
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
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "QA/AlMeezanCaseLaw"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.QA.AlMeezanCaseLaw")

BASE_URL = "https://www.almeezan.qa"
MAX_ID = 2000  # Upper bound for ID enumeration (actual max ~1931)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "ar,en;q=0.9",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)
SESSION.verify = False  # SSL cert issues with almeezan.qa

# Suppress InsecureRequestWarning
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_meta(html: str, field_id: str) -> str:
    """Extract text from a span/element by its ASP.NET ID."""
    pattern = rf'id="{re.escape(field_id)}"[^>]*>(.*?)</(?:span|h3|div)'
    m = re.search(pattern, html, re.DOTALL)
    if m:
        return clean_html(m.group(1)).strip()
    return ""


def extract_block(html: str, field_id: str) -> str:
    """Extract content from a div block by its ASP.NET ID."""
    pattern = rf'id="{re.escape(field_id)}"[^>]*>(.*?)</div>\s*(?:</div>)?'
    m = re.search(pattern, html, re.DOTALL)
    if m:
        return clean_html(m.group(1)).strip()
    return ""


def fetch_ruling_metadata(ruling_id: int, language: str = "ar") -> Optional[dict]:
    """Fetch ruling metadata from the RulingPage."""
    url = f"{BASE_URL}/RulingPage.aspx?id={ruling_id}&language={language}"
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Failed to fetch metadata for ID {ruling_id}: {e}")
        return None

    html = resp.text
    prefix = "ContentPlaceHolder1_"

    title = extract_meta(html, f"{prefix}lblTitle")
    if not title:
        return None  # No ruling at this ID

    court = extract_meta(html, f"{prefix}lblcourtName")
    department = extract_meta(html, f"{prefix}lblDepartment")
    number = extract_meta(html, f"{prefix}lblNumber")
    year = extract_meta(html, f"{prefix}lblyear")
    date_str = extract_meta(html, f"{prefix}lblDate")
    judges = extract_meta(html, f"{prefix}lblcourtJudges")

    # Clean up extracted values (remove label prefixes)
    for label in ["The Court:", "المحكمة:", "Court:"]:
        court = court.replace(label, "").strip()
    for label in ["Circuit:", "الدائرة:", "Circuit :"]:
        department = department.replace(label, "").strip()
    for label in ["Number:", "رقم:", "Number :"]:
        number = number.replace(label, "").strip()
    for label in ["Year:", "السنة:", "Year :"]:
        year = year.replace(label, "").strip()
    for label in ["Session Date:", "تاريخ الجلسة:", "Session Date :"]:
        date_str = date_str.replace(label, "").strip()

    # Parse date
    iso_date = None
    if date_str:
        for fmt in ["%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d"]:
            try:
                iso_date = datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
                break
            except ValueError:
                continue

    return {
        "ruling_id": ruling_id,
        "title": title,
        "court": court,
        "circuit": department,
        "ruling_number": number,
        "year": year,
        "date": iso_date or date_str,
        "judges": judges,
        "language": language,
    }


def fetch_ruling_text(ruling_id: int, language: str = "ar") -> str:
    """Fetch full ruling text from the RulingView page."""
    url = f"{BASE_URL}/RulingView.aspx?opt&RulID={ruling_id}&language={language}"
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Failed to fetch text for ID {ruling_id}: {e}")
        return ""

    html = resp.text
    prefix = "ContentPlaceHolder12_"

    # Extract principles and ruling text
    principals = extract_block(html, f"{prefix}principals")
    ruling_text = extract_block(html, f"{prefix}RulingText")

    parts = []
    if principals:
        parts.append(principals)
    if ruling_text:
        parts.append(ruling_text)

    return "\n\n".join(parts)


def fetch_ruling(ruling_id: int) -> Optional[dict]:
    """Fetch a complete ruling with metadata and full text."""
    # Try Arabic first (more complete coverage)
    meta = fetch_ruling_metadata(ruling_id, "ar")
    if not meta:
        return None

    text_ar = fetch_ruling_text(ruling_id, "ar")
    time.sleep(0.5)

    # Try English translation
    meta_en = fetch_ruling_metadata(ruling_id, "en")
    text_en = ""
    if meta_en:
        text_en = fetch_ruling_text(ruling_id, "en")
        time.sleep(0.5)

    # Use English title/text if available, otherwise Arabic
    text = text_en if text_en else text_ar
    title = meta_en["title"] if meta_en else meta["title"]

    if not text:
        logger.warning(f"ID {ruling_id}: no full text found")
        return None

    return {
        "_id": f"QA-almeezan-ruling-{ruling_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "text_ar": text_ar if text_en else None,
        "date": meta["date"],
        "url": f"{BASE_URL}/RulingPage.aspx?id={ruling_id}&language=ar",
        "court": meta["court"],
        "circuit": meta["circuit"],
        "ruling_number": meta["ruling_number"],
        "year": meta["year"],
        "judges": meta["judges"],
        "language": "en" if text_en else "ar",
    }


def fetch_all() -> Generator[dict, None, None]:
    """Yield all rulings by enumerating IDs."""
    empty_streak = 0
    for ruling_id in range(1, MAX_ID + 1):
        ruling = fetch_ruling(ruling_id)
        if ruling:
            empty_streak = 0
            yield ruling
        else:
            empty_streak += 1
            # Stop if we hit 100 consecutive empty IDs (past the end)
            if empty_streak > 100 and ruling_id > 1500:
                logger.info(f"Stopping at ID {ruling_id} after {empty_streak} consecutive empty IDs")
                break
        time.sleep(1)


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """No incremental update support - re-fetch all."""
    yield from fetch_all()


def normalize(raw: dict) -> dict:
    """Already normalized during fetch."""
    return raw


def test_api():
    """Test API access with a known ruling ID."""
    print("Testing QA/AlMeezanCaseLaw...")
    test_ids = [100, 500, 1000, 1500]
    for rid in test_ids:
        meta = fetch_ruling_metadata(rid, "ar")
        if meta:
            print(f"  ID {rid}: {meta['title'][:80]}")
            text = fetch_ruling_text(rid, "ar")
            print(f"    Text length: {len(text)} chars")
            print(f"    Text preview: {text[:200]}...")
        else:
            print(f"  ID {rid}: no content")
        time.sleep(1)
    print("Test complete.")


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    if sample:
        # Fetch a sample of rulings from different parts of the ID range
        sample_ids = list(range(100, 120)) + list(range(500, 510)) + list(range(1000, 1010)) + list(range(1500, 1510))
        count = 0
        for ruling_id in sample_ids:
            if count >= 20:
                break
            ruling = fetch_ruling(ruling_id)
            if ruling:
                count += 1
                out_file = SAMPLE_DIR / f"{ruling['_id']}.json"
                out_file.write_text(json.dumps(ruling, ensure_ascii=False, indent=2), encoding="utf-8")
                logger.info(f"[{count}] Saved {ruling['_id']} ({len(ruling['text'])} chars)")
            time.sleep(1.5)
        logger.info(f"Sample complete: {count} rulings saved to {SAMPLE_DIR}")
    else:
        count = 0
        for ruling in fetch_all():
            count += 1
            out_file = SAMPLE_DIR / f"{ruling['_id']}.json"
            out_file.write_text(json.dumps(ruling, ensure_ascii=False, indent=2), encoding="utf-8")
            if count % 50 == 0:
                logger.info(f"Progress: {count} rulings fetched")
        logger.info(f"Bootstrap complete: {count} rulings saved to {SAMPLE_DIR}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="QA/AlMeezanCaseLaw fetcher")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("test-api", help="Test API access")

    boot = sub.add_parser("bootstrap", help="Run bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "test-api":
        test_api()
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample)
    else:
        parser.print_help()
