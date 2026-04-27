#!/usr/bin/env python3
"""
CA/NS-Legislation -- Nova Scotia Regulations Fetcher

Fetches Nova Scotia consolidated regulations from the Office of the Registrar
of Regulations at novascotia.ca/just/regulations/.

Data source: https://novascotia.ca/just/regulations/regsbyact.htm
License: Crown Copyright, Province of Nova Scotia

Strategy:
  - Parse the "Regulations by Act" index to get all regulation file links
  - Fetch each regulation page (HTML) and extract full text
  - Normalize into standard schema

Note: Statutes are hosted on nslegislature.ca which is currently unreachable.
      This source covers regulations only (~1,500 consolidated regulations).
"""

import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

BASE_URL = "https://novascotia.ca/just/regulations"
REGS_INDEX = f"{BASE_URL}/regsbyact.htm"
RATE_LIMIT_DELAY = 1.5
CURL_TIMEOUT = 60

SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"


def fetch_page(url: str, retries: int = 2) -> Optional[str]:
    """Fetch a page using curl."""
    for attempt in range(retries + 1):
        try:
            result = subprocess.run(
                ["curl", "-sL", "-m", str(CURL_TIMEOUT),
                 "-w", "\n%{http_code}", url],
                capture_output=True, timeout=CURL_TIMEOUT + 10
            )
            # Pages may be iso-8859-1 encoded
            try:
                stdout = result.stdout.decode('utf-8')
            except UnicodeDecodeError:
                stdout = result.stdout.decode('iso-8859-1', errors='replace')
            parts = stdout.rsplit("\n", 1)
            if len(parts) == 2:
                body, status = parts[0], parts[1].strip()
            else:
                body, status = stdout, "000"

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
    """Strip HTML tags and clean whitespace from Word-generated HTML."""
    if not html:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<(?:p|div|h[1-6]|li|tr)[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    # Decode common entities
    text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
    text = text.replace('&quot;', '"').replace('&#39;', "'").replace('&nbsp;', ' ')
    text = text.replace('&#160;', ' ').replace('&#8220;', '"').replace('&#8221;', '"')
    text = text.replace('&#8216;', "'").replace('&#8217;', "'").replace('&#169;', '(c)')
    text = re.sub(r'&#\d+;', '', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_text_from_regulation(html: str) -> str:
    """Extract regulation text, removing boilerplate header/footer."""
    text = strip_html(html)
    # Remove the standard boilerplate at the top
    boilerplate_end = text.find("for the purposes of resale")
    if boilerplate_end > 0:
        # Find the next non-empty line after boilerplate
        text = text[boilerplate_end:]
        nl = text.find('\n')
        if nl > 0:
            text = text[nl:].strip()

    # Remove editorial notes/superseded tables at the end
    for marker in ["Editorial Notes and Corrections", "Repealed and Superseded"]:
        idx = text.rfind(marker)
        if idx > 0 and idx > len(text) * 0.7:
            text = text[:idx].strip()

    return text


def parse_regs_index(html: str) -> list[dict]:
    """Parse the regulations-by-act index to extract regulation links and titles."""
    regs = []
    seen = set()

    # Find all regulation links with their titles
    # Pattern: <a href="regs/XXXX.htm">Title</a> or similar
    pattern = re.compile(
        r'href="regs/([^"]+\.htm)"[^>]*>\s*(.*?)\s*</a>',
        re.IGNORECASE | re.DOTALL
    )
    for match in pattern.finditer(html):
        filename = match.group(1).strip()
        title_raw = match.group(2).strip()
        title = re.sub(r'<[^>]+>', '', title_raw).strip()
        # Decode HTML entities in titles
        title = title.replace('&amp;', '&').replace('&#233;', 'é').replace('&#39;', "'")
        title = re.sub(r'&#\d+;', '', title)

        if not title or not filename or filename in seen:
            continue
        seen.add(filename)

        reg_id = filename.replace('.htm', '')
        regs.append({
            "reg_id": reg_id,
            "filename": filename,
            "title": title,
            "url": f"{BASE_URL}/regs/{filename}",
        })

    return regs


def extract_title(html: str) -> str:
    """Extract title from HTML page."""
    match = re.search(r'<title>(.*?)</title>', html, re.I | re.DOTALL)
    if match:
        title = re.sub(r'<[^>]+>', '', match.group(1)).strip()
        # Remove " - Act Name (Nova Scotia)" suffix
        title = re.sub(r'\s*-\s*.*?\(Nova Scotia\)\s*$', '', title)
        return title
    return ""


def extract_consolidation_date(html: str) -> str:
    """Extract consolidation date from meta description."""
    match = re.search(
        r'<meta\s+name="description"\s+content="[^"]*?(?:to|through)\s+N\.S\.\s+Reg\.\s+(\d+/\d{4})',
        html, re.I
    )
    if match:
        return ""  # Reg number, not a date

    match = re.search(
        r'(?:effective|in force)\s+(\w+ \d{1,2},?\s+\d{4})',
        html, re.I
    )
    if match:
        try:
            from datetime import datetime as dt
            d = dt.strptime(match.group(1).replace(',', ''), '%B %d %Y')
            return d.strftime('%Y-%m-%d')
        except ValueError:
            pass

    return ""


def normalize(doc: dict, html: str) -> Optional[dict]:
    """Normalize a regulation record."""
    text = extract_text_from_regulation(html)
    if not text or len(text) < 50:
        return None

    title = doc.get("title") or extract_title(html)
    date = extract_consolidation_date(html)

    return {
        "_id": f"CA/NS-Legislation/regulation/{doc['reg_id']}",
        "_source": "CA/NS-Legislation",
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date,
        "url": doc["url"],
        "doc_type": "regulation",
        "reg_id": doc["reg_id"],
        "jurisdiction": "CA-NS",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all Nova Scotia regulations."""
    total = 0

    print("Fetching regulations index...", file=sys.stderr)
    index_html = fetch_page(REGS_INDEX)
    if not index_html:
        print("ERROR: Could not fetch regulations index", file=sys.stderr)
        return

    regs = parse_regs_index(index_html)
    print(f"Found {len(regs)} regulations", file=sys.stderr)

    limit = 15 if sample else len(regs)

    for doc in regs:
        if total >= limit:
            break

        html = fetch_page(doc["url"])
        if not html:
            continue

        record = normalize(doc, html)
        if record:
            yield record
            total += 1
            if total % 25 == 0:
                print(f"  Fetched {total} records...", file=sys.stderr)
        time.sleep(RATE_LIMIT_DELAY)

    print(f"Total records: {total}", file=sys.stderr)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Nova Scotia regulations fetcher")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Fetch documents")
    boot.add_argument("--sample", action="store_true", help="Fetch ~15 sample records")
    boot.add_argument("--full", action="store_true", help="Fetch all records")

    sub.add_parser("test-api", help="Test connectivity")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "test-api":
        html = fetch_page(REGS_INDEX)
        if html and "regs/" in html:
            print("OK: Regulations index accessible")
        else:
            print("FAIL: Cannot access regulations index")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
if __name__ == "__main__":
    main()
