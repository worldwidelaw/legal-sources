#!/usr/bin/env python3
"""
INTL/WIPO-UDRP -- WIPO Domain Name Dispute Decisions (UDRP)

Fetches panel decisions from the WIPO Arbitration and Mediation Center.
60,000+ UDRP decisions since 1999 with full text.

Strategy:
  - Use case.jsp for structured metadata (parties, domain, date, result)
  - Follow text.jsp redirect to decision full text (HTML for 1999-2021)
  - Generate case numbers sequentially per year
  - Rate-limit to 2 seconds between requests

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import json
import re
import ssl
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://www.wipo.int"
CASE_URL = f"{BASE_URL}/amc/en/domains/search/case.jsp"
TEXT_URL = f"{BASE_URL}/amc/en/domains/search/text.jsp"
RATE_LIMIT = 2.0  # seconds between requests

# Year range for UDRP decisions
FIRST_YEAR = 2000
CURRENT_YEAR = datetime.now().year

# Cases per year (approximate upper bounds for sequential numbering)
# In full bootstrap we probe to find the actual max; for sample we use known-good numbers.
SAMPLE_CASES = [
    ("D", 2000, 1), ("D", 2000, 5), ("D", 2003, 1),
    ("D", 2005, 1), ("D", 2005, 10), ("D", 2007, 1),
    ("D", 2010, 5), ("D", 2012, 1), ("D", 2013, 1),
    ("D", 2015, 1), ("D", 2017, 1), ("D", 2018, 1),
    ("D", 2019, 5), ("D", 2020, 1), ("D", 2020, 5),
    ("D", 2020, 10), ("D", 2021, 1), ("D", 2021, 2),
    ("D", 2021, 3), ("D", 2021, 5), ("D", 2021, 10),
]

# SSL context that accepts certificates
_ssl_ctx = ssl.create_default_context()


def _fetch(url: str, timeout: int = 30, follow_redirects: bool = True) -> Optional[tuple]:
    """Fetch a URL, return (final_url, content_type, body_bytes) or None on error."""
    req = Request(url)
    req.add_header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
    req.add_header("Accept-Language", "en-US,en;q=0.5")
    try:
        resp = urlopen(req, timeout=timeout, context=_ssl_ctx)
        body = resp.read()
        final_url = resp.url
        content_type = resp.headers.get("Content-Type", "")
        return (final_url, content_type, body)
    except HTTPError as e:
        if e.code == 404:
            return None
        # For redirects to PDF, urllib follows them automatically
        print(f"  HTTP {e.code} for {url}", file=sys.stderr)
        return None
    except (URLError, OSError) as e:
        print(f"  Network error for {url}: {e}", file=sys.stderr)
        return None


def _fetch_text(url: str, timeout: int = 30) -> Optional[str]:
    """Fetch URL and return decoded text, or None."""
    result = _fetch(url, timeout)
    if result is None:
        return None
    final_url, content_type, body = result
    # Skip PDFs
    if "application/pdf" in content_type or final_url.endswith(".pdf"):
        return None
    # Detect encoding
    enc = "utf-8"
    m = re.search(r'charset=([^\s;]+)', content_type)
    if m:
        enc = m.group(1)
    try:
        return body.decode(enc, errors="replace")
    except (LookupError, UnicodeDecodeError):
        return body.decode("utf-8", errors="replace")


def fetch_case_metadata(prefix: str, year: int, seq: int) -> Optional[dict]:
    """Fetch case metadata from case.jsp. Returns dict or None if not found."""
    params = urlencode({
        "case_prefix": prefix,
        "case_year": str(year),
        "case_seq": f"{seq:04d}",
    })
    url = f"{CASE_URL}?{params}"
    html = _fetch_text(url)
    if html is None:
        return None

    # Check for "no case found" pattern
    if "no matching case" in html.lower() or "Case Details" not in html:
        return None

    # Parse metadata table
    rows = re.findall(
        r'<tr><td[^>]*><b>(.*?)</b></td><td[^>]*>(.*?)</td></tr>',
        html, re.S
    )
    meta = {}
    for label, value in rows:
        label = label.strip()
        value = re.sub(r'<[^>]+>', '', value).strip()
        value = unescape(value).strip()
        if label == "WIPO Case Number":
            meta["case_number"] = value
        elif label == "Domain name(s)":
            meta["domain_names"] = value
        elif label == "Complainant":
            meta["complainant"] = value
        elif label == "Respondent":
            meta["respondent"] = value
        elif label == "Panelist":
            meta["panelist"] = value
        elif label == "Decision Date":
            meta["decision_date_raw"] = value
        elif label == "Decision":
            meta["decision_result"] = value

    if "case_number" not in meta:
        return None

    # Check if there is a decision link (only decided cases)
    if re.search(r'text\.jsp\?case=', html) or re.search(r'/decisions/', html):
        meta["has_decision"] = True
    else:
        meta["has_decision"] = False

    return meta


def parse_decision_date(raw: str) -> str:
    """Convert DD-MM-YYYY to ISO 8601 YYYY-MM-DD."""
    if not raw:
        return ""
    m = re.match(r'(\d{2})-(\d{2})-(\d{4})', raw)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return raw


def fetch_decision_text(case_number: str) -> Optional[str]:
    """Fetch the full text of a decision. Returns cleaned text or None."""
    url = f"{TEXT_URL}?case={case_number}"
    result = _fetch(url, timeout=60)
    if result is None:
        return None

    final_url, content_type, body = result

    # Skip PDFs (2022+ decisions)
    if "application/pdf" in content_type or final_url.endswith(".pdf"):
        return None

    # Decode HTML
    enc = "utf-8"
    m = re.search(r'charset=([^\s;]+)', content_type)
    if m:
        enc = m.group(1)
    try:
        html = body.decode(enc, errors="replace")
    except (LookupError, UnicodeDecodeError):
        html = body.decode("utf-8", errors="replace")

    return extract_text_from_html(html)


def extract_text_from_html(html: str) -> str:
    """Extract clean decision text from an HTML decision page."""
    soup = BeautifulSoup(html, "html.parser")

    # Remove script, style, nav, header, footer elements
    for tag in soup(["script", "style", "nav", "header", "footer",
                     "wipo-navbar", "wipo-footer"]):
        tag.decompose()

    # Remove hidden navigation divs
    for div in soup.find_all("div", style=re.compile(r"display:\s*none", re.I)):
        div.decompose()

    # Try to find the main content area
    # Look for the content div or the body
    content = None

    # Method 1: Look for h1/h2 containing "ADMINISTRATIVE PANEL DECISION"
    for tag in soup.find_all(["h1", "h2", "p", "b"]):
        if tag.string and "ADMINISTRATIVE PANEL DECISION" in tag.get_text():
            # Walk up to find the enclosing content container
            parent = tag.parent
            while parent and parent.name not in ("div", "body", "html"):
                parent = parent.parent
            if parent:
                content = parent
            break

    # Method 2: Look for wrap-inner or content div
    if not content:
        content = soup.find("div", class_="wrap-inner")
    if not content:
        content = soup.find("div", class_="content")

    # Method 3: Just use body
    if not content:
        content = soup.find("body")

    if not content:
        content = soup

    # Get text with paragraph separation
    text = content.get_text(separator="\n")

    # Clean up whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
    text = text.strip()

    # Remove common boilerplate at the start (navigation text)
    # Find where the actual decision content starts
    markers = [
        "ADMINISTRATIVE PANEL DECISION",
        "WIPO Arbitration and Mediation Center",
        "WIPO Case No.",
    ]
    for marker in markers:
        idx = text.find(marker)
        if idx != -1 and idx < 500:
            text = text[idx:]
            break

    # Remove trailing boilerplate
    for end_marker in ["WIPO Pay", "WIPO Assemblies", "footer", "Contact Us"]:
        idx = text.rfind(end_marker)
        if idx != -1 and idx > len(text) - 2000:
            text = text[:idx].strip()

    return text


def extract_title_from_html(html: str) -> str:
    """Extract the title from an HTML decision page."""
    soup = BeautifulSoup(html, "html.parser")
    title_tag = soup.find("title")
    if title_tag:
        t = title_tag.get_text(strip=True)
        if t and "WIPO" not in t:
            return t
    # Try to build title from the heading
    for h in soup.find_all(["h2"]):
        text = h.get_text(strip=True)
        if " v. " in text or " v " in text:
            return text
    return ""


def normalize_record(meta: dict, text: str) -> dict:
    """Build a normalized record from metadata and full text."""
    case_number = meta.get("case_number", "")
    date = parse_decision_date(meta.get("decision_date_raw", ""))
    complainant = meta.get("complainant", "")
    respondent = meta.get("respondent", "")
    domain_names = meta.get("domain_names", "")
    decision_result = meta.get("decision_result", "")

    title = f"{complainant} v. {respondent}" if complainant and respondent else ""
    if not title:
        title = f"WIPO UDRP Decision {case_number}"

    return {
        "_id": case_number,
        "_source": "INTL/WIPO-UDRP",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date if date else None,
        "url": f"{BASE_URL}/amc/en/domains/search/text.jsp?case={case_number}",
        "case_number": case_number,
        "complainant": complainant,
        "respondent": respondent,
        "domain_names": domain_names,
        "decision_result": decision_result,
    }


def bootstrap_sample() -> Generator[dict, None, None]:
    """Fetch 15 sample records from known-good case numbers across years."""
    count = 0
    for prefix, year, seq in SAMPLE_CASES:
        if count >= 15:
            break

        case_id = f"{prefix}{year}-{seq:04d}"
        print(f"Fetching metadata for {case_id}...", file=sys.stderr)

        meta = fetch_case_metadata(prefix, year, seq)
        time.sleep(RATE_LIMIT)
        if meta is None or not meta.get("has_decision"):
            print(f"  Skipping {case_id} (no decision)", file=sys.stderr)
            continue

        case_number = meta["case_number"]
        print(f"  Fetching decision text for {case_number}...", file=sys.stderr)
        text = fetch_decision_text(case_number)
        time.sleep(RATE_LIMIT)

        if not text or len(text) < 200:
            print(f"  Skipping {case_number} (no/short text: {len(text) if text else 0} chars)",
                  file=sys.stderr)
            continue

        record = normalize_record(meta, text)
        print(f"  Got {case_number}: {len(text)} chars", file=sys.stderr)
        yield record
        count += 1

    print(f"Sample complete: {count} records", file=sys.stderr)


def bootstrap_full() -> Generator[dict, None, None]:
    """Fetch all UDRP decisions with full text (HTML years only: 2000-2021)."""
    count = 0
    consecutive_misses = 0
    max_consecutive_misses = 20  # Stop probing after this many misses in a row

    for year in range(FIRST_YEAR, CURRENT_YEAR + 1):
        seq = 1
        consecutive_misses = 0
        print(f"Processing year {year}...", file=sys.stderr)

        while consecutive_misses < max_consecutive_misses:
            case_id = f"D{year}-{seq:04d}"

            meta = fetch_case_metadata("D", year, seq)
            time.sleep(RATE_LIMIT)

            if meta is None:
                consecutive_misses += 1
                seq += 1
                continue

            consecutive_misses = 0

            if not meta.get("has_decision"):
                seq += 1
                continue

            case_number = meta["case_number"]
            text = fetch_decision_text(case_number)
            time.sleep(RATE_LIMIT)

            if not text or len(text) < 200:
                seq += 1
                continue

            record = normalize_record(meta, text)
            yield record
            count += 1

            if count % 100 == 0:
                print(f"  Progress: {count} records fetched", file=sys.stderr)

            seq += 1

        print(f"  Year {year}: probed up to seq {seq}, stopping after "
              f"{max_consecutive_misses} consecutive misses", file=sys.stderr)

    print(f"Full bootstrap complete: {count} records", file=sys.stderr)


def test_connectivity() -> bool:
    """Quick test that the WIPO case lookup works."""
    print("Testing WIPO UDRP connectivity...", file=sys.stderr)
    meta = fetch_case_metadata("D", 2020, 1)
    if meta and meta.get("case_number") == "D2020-0001":
        print(f"  Case lookup OK: {meta['case_number']}", file=sys.stderr)
        print(f"  Domain: {meta.get('domain_names', 'N/A')}", file=sys.stderr)
        print(f"  Decision: {meta.get('decision_result', 'N/A')}", file=sys.stderr)

        text = fetch_decision_text("D2020-0001")
        if text and len(text) > 500:
            print(f"  Decision text OK: {len(text)} chars", file=sys.stderr)
            print("PASS", file=sys.stderr)
            return True
        else:
            print(f"  Decision text FAILED (got {len(text) if text else 0} chars)",
                  file=sys.stderr)
            return False
    else:
        print("  Case lookup FAILED", file=sys.stderr)
        return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    sample = "--sample" in sys.argv

    if cmd == "test":
        ok = test_connectivity()
        sys.exit(0 if ok else 1)

    elif cmd == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        gen = bootstrap_sample() if sample else bootstrap_full()
        count = 0
        for record in gen:
            out_path = sample_dir / f"{record['_id']}.json"
            out_path.write_text(json.dumps(record, ensure_ascii=False, indent=2))
            count += 1
            print(f"  Saved {record['_id']} ({len(record.get('text', ''))} chars)",
                  file=sys.stderr)
        print(f"Done: {count} records saved to {sample_dir}", file=sys.stderr)

    elif cmd == "update":
        since = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith("-") else "2026-01-01"
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        # For update, just fetch current year
        for record in bootstrap_full():
            if record.get("date") and record["date"] >= since:
                out_path = sample_dir / f"{record['_id']}.json"
                out_path.write_text(json.dumps(record, ensure_ascii=False, indent=2))
                count += 1
                print(f"  Updated: {record['_id']}", file=sys.stderr)
        print(f"Update complete: {count} records since {since}", file=sys.stderr)

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
