#!/usr/bin/env python3
"""
TR/TBMM - Turkish Parliament (Grand National Assembly) Session Transcripts

Fetches full-text session transcripts from the TBMM website.
Coverage: Terms 20-28 (1996-present), ~3,500 session transcripts.
Source: cdn.tbmm.gov.tr (HTML format, no auth required)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

SOURCE_ID = "TR/TBMM"
SAMPLE_DIR = Path(__file__).parent / "sample"
BASE_URL = "https://www.tbmm.gov.tr"
LISTING_URL = BASE_URL + "/Tutanaklar/DoneminTutanakMetinleri"
DETAIL_URL = BASE_URL + "/Tutanaklar/Tutanak"


def http_get(url: str, retries: int = 3) -> str:
    """Fetch URL content as string with retries. Handles Turkish encoding."""
    for attempt in range(retries):
        try:
            req = Request(url, headers={
                "User-Agent": "LegalDataHunter/1.0",
                "Accept": "text/html,application/xhtml+xml,*/*",
                "Accept-Language": "tr,en;q=0.5",
            })
            with urlopen(req, timeout=60) as resp:
                raw = resp.read()
                ct = resp.headers.get("Content-Type", "")
                # Try charset from Content-Type
                charset_match = re.search(r'charset=([^\s;]+)', ct, re.IGNORECASE)
                if charset_match:
                    try:
                        return raw.decode(charset_match.group(1))
                    except (UnicodeDecodeError, LookupError):
                        pass
                # Check HTML meta charset
                head = raw[:2000]
                meta_match = re.search(rb'charset=["\']?([^"\'\s;>]+)', head, re.IGNORECASE)
                if meta_match:
                    try:
                        return raw.decode(meta_match.group(1).decode('ascii'))
                    except (UnicodeDecodeError, LookupError):
                        pass
                # Try Turkish encodings
                for enc in ("utf-8", "windows-1254", "iso-8859-9"):
                    try:
                        return raw.decode(enc)
                    except UnicodeDecodeError:
                        continue
                return raw.decode("utf-8", errors="replace")
        except (HTTPError, URLError, TimeoutError) as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"Failed to fetch {url}: {e}")


def strip_html(html: str) -> str:
    """Strip HTML tags and clean up whitespace."""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<p[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    # Decode HTML entities
    import html as html_mod
    text = html_mod.unescape(text)
    # Collapse whitespace but keep newlines
    lines = [re.sub(r'[ \t]+', ' ', line).strip() for line in text.split('\n')]
    text = '\n'.join(line for line in lines if line)
    return text.strip()


def get_transcript_ids(term: int, year: int) -> list[dict]:
    """Get transcript IDs and metadata from a listing page."""
    url = f"{LISTING_URL}?Donem={term}&YasamaYili={year}"
    try:
        html = http_get(url)
    except Exception as e:
        print(f"    Error listing term {term} year {year}: {e}")
        return []

    # Extract transcript links: Tutanak?Id={UUID}
    pattern = r'href="[^"]*Tutanak\?Id=([a-f0-9\-]+)"'
    ids = re.findall(pattern, html, re.IGNORECASE)

    # Also try to extract dates from the listing
    results = []
    seen = set()
    for tid in ids:
        if tid not in seen:
            seen.add(tid)
            results.append({"id": tid, "term": term, "year": year})

    return results


def get_cdn_url(transcript_id: str) -> Optional[str]:
    """Get the CDN HTML URL for a transcript."""
    url = f"{DETAIL_URL}?Id={transcript_id}"
    try:
        html = http_get(url)
    except Exception:
        return None

    # Look for cdn.tbmm.gov.tr .html link
    match = re.search(r'(https?://cdn\.tbmm\.gov\.tr[^"\']+\.html)', html)
    if match:
        return match.group(1)

    return None


def fetch_transcript(cdn_url: str) -> Optional[str]:
    """Download and extract text from a CDN HTML transcript."""
    try:
        html = http_get(cdn_url)
    except Exception:
        return None

    text = strip_html(html)
    return text if len(text) > 100 else None


def extract_date_from_text(text: str) -> Optional[str]:
    """Try to extract a session date from transcript text."""
    # Turkish date patterns: "15 Ocak 2024", "3.1.2024", etc.
    months_tr = {
        'Ocak': '01', 'Şubat': '02', 'Mart': '03', 'Nisan': '04',
        'Mayıs': '05', 'Haziran': '06', 'Temmuz': '07', 'Ağustos': '08',
        'Eylül': '09', 'Ekim': '10', 'Kasım': '11', 'Aralık': '12',
    }
    for month_name, month_num in months_tr.items():
        m = re.search(rf'(\d{{1,2}})\s+{month_name}\s+(\d{{4}})', text[:2000])
        if m:
            day = m.group(1).zfill(2)
            year = m.group(2)
            return f"{year}-{month_num}-{day}"

    # Try numeric date: DD.MM.YYYY
    m = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', text[:2000])
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"

    return None


def normalize(transcript_id: str, text: str, term: int, year: int) -> Optional[dict]:
    """Normalize a transcript into standard schema."""
    if not text or len(text) < 100:
        return None

    session_date = extract_date_from_text(text)

    # Build title from first meaningful line
    lines = text.split('\n')
    title_parts = []
    for line in lines[:10]:
        line = line.strip()
        if line and len(line) > 5:
            title_parts.append(line)
            if len(' - '.join(title_parts)) > 60:
                break
    title = ' - '.join(title_parts[:2]) if title_parts else f"TBMM Term {term} Session"

    return {
        "_id": f"TR_TBMM_{transcript_id}",
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title[:200],
        "date": session_date,
        "term": term,
        "legislative_year": year,
        "text": text,
        "url": f"{DETAIL_URL}?Id={transcript_id}",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all session transcripts."""
    if sample:
        # Sample from recent term
        terms = [(28, 2)]
    else:
        # All terms 20-28, years 1-5
        terms = [(t, y) for t in range(20, 29) for y in range(1, 6)]

    total_yielded = 0
    for term, year in terms:
        print(f"  Listing term {term}, year {year}...")
        transcripts = get_transcript_ids(term, year)
        if not transcripts:
            continue
        print(f"    Found {len(transcripts)} transcripts")

        for t_info in transcripts:
            tid = t_info["id"]
            print(f"    Fetching {tid[:12]}...")

            cdn_url = get_cdn_url(tid)
            if not cdn_url:
                print(f"      No CDN URL found")
                continue

            time.sleep(1)

            text = fetch_transcript(cdn_url)
            if not text:
                print(f"      No text extracted")
                continue

            time.sleep(1)

            record = normalize(tid, text, term, year)
            if record:
                yield record
                total_yielded += 1
                print(f"      OK: {len(text)} chars")

                if sample and total_yielded >= 15:
                    return

    print(f"  Total transcripts with text: {total_yielded}")


def test_connection():
    """Test connectivity to TBMM."""
    print("Testing TBMM connectivity...")

    print("\n1. Checking listing page...")
    try:
        transcripts = get_transcript_ids(28, 2)
        print(f"   OK: Found {len(transcripts)} transcripts for term 28, year 2")
    except Exception as e:
        print(f"   FAIL: {e}")
        return False

    if not transcripts:
        print("   FAIL: No transcripts found")
        return False

    print("\n2. Getting CDN URL for first transcript...")
    tid = transcripts[0]["id"]
    cdn_url = get_cdn_url(tid)
    if cdn_url:
        print(f"   OK: {cdn_url[:80]}...")
    else:
        print("   FAIL: No CDN URL found")
        return False

    print("\n3. Downloading transcript text...")
    text = fetch_transcript(cdn_url)
    if text:
        print(f"   OK: {len(text)} chars")
        print(f"   Preview: {text[:150]}...")
    else:
        print("   FAIL: No text extracted")
        return False

    session_date = extract_date_from_text(text)
    print(f"   Date: {session_date}")

    print("\nAll tests passed!")
    return True


def main():
    parser = argparse.ArgumentParser(description="TR/TBMM Turkish Parliament Transcripts Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command == "test":
        success = test_connection()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        count = 0
        for record in fetch_all(sample=args.sample):
            filename = re.sub(r'[^\w\-]', '_', record["_id"])[:80] + ".json"
            filepath = SAMPLE_DIR / filename
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            print(f"  Saved: {filepath.name} ({len(record['text'])} chars)")

        print(f"\nBootstrap complete: {count} records saved to {SAMPLE_DIR}")


if __name__ == "__main__":
    main()
