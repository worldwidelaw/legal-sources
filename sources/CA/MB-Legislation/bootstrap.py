#!/usr/bin/env python3
"""
CA/MB-Legislation -- Manitoba King's Printer Legislation Fetcher

Fetches Manitoba consolidated statutes (C.C.S.M.) and regulations from
web2.gov.mb.ca/laws/. All documents are bilingual HTML with full text.

Data source: https://web2.gov.mb.ca/laws/
License: Crown Copyright (Manitoba King's Printer)

Strategy:
  - Parse alphabetical CCSM index to get act chapter IDs
  - Parse consolidated regulations index to get regulation IDs
  - Fetch each document page and extract English full text
"""

import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

BASE_URL = "https://web2.gov.mb.ca/laws"
STATUTES_INDEX = f"{BASE_URL}/statutes/index_ccsm.php"
REGS_INDEX = f"{BASE_URL}/regs/index.php"
RATE_LIMIT_DELAY = 1.5
CURL_TIMEOUT = 120

SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"


def fetch_page(url: str, retries: int = 2) -> Optional[str]:
    """Fetch a page using curl."""
    for attempt in range(retries + 1):
        try:
            result = subprocess.run(
                ["curl", "-sL", "-m", str(CURL_TIMEOUT),
                 "-w", "\n%{http_code}", url],
                capture_output=True, text=True, timeout=CURL_TIMEOUT + 10
            )
            parts = result.stdout.rsplit("\n", 1)
            if len(parts) == 2:
                body, status = parts[0], parts[1].strip()
            else:
                body, status = result.stdout, "000"

            if status == "404":
                return None
            if not status.startswith("2"):
                if attempt == retries:
                    print(f"HTTP {status} for {url}", file=sys.stderr)
                    return None
                time.sleep(3)
                continue
            if body:
                return body
            if attempt == retries:
                return None
            time.sleep(3)
        except Exception as e:
            if attempt == retries:
                print(f"Failed to fetch {url}: {e}", file=sys.stderr)
                return None
            time.sleep(3)
    return None


def strip_html(html: str) -> str:
    """Strip HTML tags and clean whitespace."""
    if not html:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
    text = text.replace('&quot;', '"').replace('&#39;', "'").replace('&nbsp;', ' ')
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def parse_statutes_index(html: str) -> list[dict]:
    """Parse the CCSM statutes index to extract act IDs and titles."""
    acts = []
    seen = set()
    # Match links like: href="ccsm/a001-5.php?lang=en">The Aboriginal Languages Recognition Act
    # Skip _info1act.php, anchor-only links, and links without ?lang=en
    pattern = re.compile(r'href="ccsm/([a-z]\d[\w-]*)\.php\?lang=en"[^>]*>(.*?)</a>', re.IGNORECASE | re.DOTALL)
    for match in pattern.finditer(html):
        chapter_id = match.group(1).strip()
        title_raw = match.group(2).strip()
        title = strip_html(title_raw).strip()
        if title and chapter_id and chapter_id not in seen:
            seen.add(chapter_id)
            acts.append({
                "chapter_id": chapter_id,
                "title": title,
                "doc_type": "statute",
                "url": f"{BASE_URL}/statutes/ccsm/{chapter_id}.php?lang=en",
            })
    return acts


def parse_regs_index(html: str) -> list[dict]:
    """Parse the consolidated regulations index to extract reg IDs and titles."""
    regs = []
    seen = set()
    # Pattern: title="Regulation Name" href="current/{id}.php?lang=en">number
    pattern = re.compile(
        r'title\s*=\s*"([^"]+)"\s+href="current/([^"?]+)\.php\?lang=en"',
        re.IGNORECASE
    )
    for match in pattern.finditer(html):
        title = match.group(1).strip()
        reg_id = match.group(2).strip()
        if title and reg_id and reg_id not in seen:
            seen.add(reg_id)
            regs.append({
                "reg_id": reg_id,
                "title": title,
                "doc_type": "regulation",
                "url": f"{BASE_URL}/regs/current/{reg_id}.php?lang=en",
            })
    return regs


def extract_english_text(html: str) -> str:
    """Extract English text from bilingual Manitoba legislation page."""
    # Try to get just the English column (class="regcol-e")
    en_parts = re.findall(r'<div[^>]*class="regcol-e"[^>]*>(.*?)</div>', html, re.DOTALL | re.IGNORECASE)
    if en_parts:
        combined = '\n'.join(en_parts)
        return strip_html(combined)

    # Fallback: try mainContent div
    main = re.search(r'<div[^>]*id="mainContent"[^>]*>(.*?)</div>\s*(?:<div[^>]*id="footer"|</body>)', html, re.DOTALL | re.IGNORECASE)
    if main:
        return strip_html(main.group(1))

    # Last resort: just strip the whole page
    return strip_html(html)


def extract_date_from_html(html: str) -> str:
    """Try to extract a date from the page metadata or content."""
    # Look for dcterms.modified or dcterms.issued meta tags
    date_match = re.search(r'<meta[^>]*name="dcterms\.(modified|issued)"[^>]*content="([^"]+)"', html, re.IGNORECASE)
    if date_match:
        return date_match.group(2)[:10]

    # Look for "Last updated: YYYY-MM-DD" or similar
    date_match = re.search(r'(?:last\s+updated|current\s+as\s+of)[:\s]*(\d{4}[-/]\d{2}[-/]\d{2})', html, re.IGNORECASE)
    if date_match:
        return date_match.group(1).replace('/', '-')

    return ""


def normalize_statute(doc: dict, html: str) -> Optional[dict]:
    """Normalize a statute record."""
    text = extract_english_text(html)
    if not text or len(text) < 100:
        return None

    date = extract_date_from_html(html)
    chapter_id = doc["chapter_id"]

    return {
        "_id": f"CA/MB-Legislation/statute/{chapter_id}",
        "_source": "CA/MB-Legislation",
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": doc["title"],
        "text": text,
        "date": date,
        "url": doc["url"],
        "doc_type": "statute",
        "chapter_id": chapter_id,
        "jurisdiction": "CA-MB",
    }


def normalize_regulation(doc: dict, html: str) -> Optional[dict]:
    """Normalize a regulation record."""
    text = extract_english_text(html)
    if not text or len(text) < 100:
        return None

    date = extract_date_from_html(html)
    reg_id = doc["reg_id"]

    return {
        "_id": f"CA/MB-Legislation/regulation/{reg_id}",
        "_source": "CA/MB-Legislation",
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": doc["title"],
        "text": text,
        "date": date,
        "url": doc["url"],
        "doc_type": "regulation",
        "reg_id": reg_id,
        "jurisdiction": "CA-MB",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all Manitoba statutes and regulations."""
    total = 0

    # Fetch statutes index
    print("Fetching CCSM statutes index...", file=sys.stderr)
    stat_html = fetch_page(STATUTES_INDEX)
    if not stat_html:
        print("ERROR: Could not fetch statutes index", file=sys.stderr)
        return

    acts = parse_statutes_index(stat_html)
    print(f"Found {len(acts)} CCSM statutes", file=sys.stderr)

    for doc in acts:
        html = fetch_page(doc["url"])
        if not html:
            continue
        record = normalize_statute(doc, html)
        if record:
            yield record
            total += 1
            if total % 25 == 0:
                print(f"  Fetched {total} records...", file=sys.stderr)
            if sample and total >= 10:
                break
        time.sleep(RATE_LIMIT_DELAY)

    if sample and total >= 10:
        # Get a few regulations too
        pass
    else:
        sample_done = False

    # Fetch regulations index
    print("Fetching consolidated regulations index...", file=sys.stderr)
    regs_html = fetch_page(REGS_INDEX)
    if not regs_html:
        print("ERROR: Could not fetch regulations index", file=sys.stderr)
    else:
        regs = parse_regs_index(regs_html)
        print(f"Found {len(regs)} consolidated regulations", file=sys.stderr)

        for doc in regs:
            html = fetch_page(doc["url"])
            if not html:
                continue
            record = normalize_regulation(doc, html)
            if record:
                yield record
                total += 1
                if total % 25 == 0:
                    print(f"  Fetched {total} records...", file=sys.stderr)
                if sample and total >= 15:
                    break
            time.sleep(RATE_LIMIT_DELAY)

    print(f"Total records: {total}", file=sys.stderr)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Manitoba legislation fetcher")
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
            out_path = SAMPLE_DIR / f"{count:04d}.json"
            out_path.write_text(json.dumps(record, ensure_ascii=False, indent=2))
            count += 1
        print(f"Done. Saved {count} records to {SAMPLE_DIR}/", file=sys.stderr)


if __name__ == "__main__":
    main()
