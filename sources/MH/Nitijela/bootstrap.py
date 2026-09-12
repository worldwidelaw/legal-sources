#!/usr/bin/env python3
"""
MH/Nitijela -- Marshall Islands Parliament (Nitijela) Legislation

Fetches ~305 principal acts from rmiparliament.org.
Full text is extracted from PDFs using pypdf.

Usage:
    python bootstrap.py bootstrap --sample
    python bootstrap.py bootstrap --full
    python bootstrap.py updates --since YYYY-MM-DD
"""

import argparse
import io
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Generator, List, Optional

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
ROOT_DIR = SCRIPT_DIR.parent.parent.parent

sys.path.insert(0, str(ROOT_DIR))

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SOURCE_ID = "MH/Nitijela"
BASE_URL = "https://rmiparliament.org"
BY_TITLE_URL = f"{BASE_URL}/cms/legislation/acts-of-nitijela/by-title.html"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 LegalDataHunter/1.0"
)
REQUEST_DELAY = 10  # robots.txt crawl-delay: 10


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def _get(session: requests.Session, url: str, retries: int = 3) -> Optional[requests.Response]:
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=60)
            if resp.status_code == 429:
                time.sleep(2 ** (attempt + 2))
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt < retries - 1:
                time.sleep(5)
                continue
            print(f"  Error fetching {url}: {e}")
            return None
    return None


# ---------------------------------------------------------------------------
# Parse the by-title page
# ---------------------------------------------------------------------------

def _parse_date(raw: str) -> str:
    """Parse dates like 'Monday, 01 January 2024' into ISO format."""
    raw = raw.strip()
    if not raw:
        return ""
    # Try common formats
    for fmt in (
        "%A, %d %B %Y",   # Monday, 01 January 2024
        "%d %B %Y",        # 01 January 2024
        "%d/%m/%Y",        # 01/01/2024
        "%Y-%m-%d",        # 2024-01-01
    ):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw


def parse_acts(html: str) -> List[Dict]:
    """Parse the by-title page and extract act metadata."""
    soup = BeautifulSoup(html, "html.parser")
    acts = []

    for tr in soup.find_all("tr"):
        # Find the principal act link (class npWrap with PDF href)
        link = tr.find("a", class_="npWrap", href=re.compile(r"\.pdf$"))
        if not link:
            continue

        # Skip amendment rows (td with class 'amd') and subordinate rows ('sub')
        amd_td = tr.find("td", class_="amd")
        sub_td = tr.find("td", class_="sub")
        if amd_td or sub_td:
            continue

        # Extract title (text of the link, minus the version bracket)
        title_parts = []
        for child in link.children:
            if hasattr(child, "get") and child.get("title") == "Version":
                continue
            if hasattr(child, "name") and child.name == "i":
                continue
            text = child.string if hasattr(child, "string") else str(child)
            if text:
                title_parts.append(text.strip())
        title = " ".join(t for t in title_parts if t).strip()

        # PDF URL
        href = link["href"]
        pdf_url = href if href.startswith("http") else f"{BASE_URL}{href}"

        # Version number from the [N] bracket
        version_el = link.find("div", title="Version")
        version = ""
        if version_el:
            version = version_el.get_text().strip().strip("[]").strip()

        # Legislation number and long title from the notes popover
        notes_el = tr.find("div", class_="hasPopover", attrs={"title": "Legislation Notes"})
        leg_number = ""
        long_title = ""
        if notes_el:
            content = notes_el.get("data-bs-content", "")
            # Parse "Legislation Number: YYYY-NNNN<hr class='notes'>Long title..."
            num_match = re.search(r"Legislation Number:\s*([\w\-]+)", content)
            if num_match:
                leg_number = num_match.group(1)
            # Long title after the <hr>
            hr_split = re.split(r"<hr[^>]*>", content, maxsplit=1)
            if len(hr_split) > 1:
                long_title = BeautifulSoup(hr_split[1], "html.parser").get_text().strip()

        # Commencement date from calendar popover
        date_el = tr.find("div", class_="hasPopover", attrs={"title": "Date Commenced"})
        date_str = ""
        if date_el:
            date_str = _parse_date(date_el.get("data-bs-content", ""))

        # Derive ID from legislation number or PDF filename
        if leg_number:
            doc_id = f"MH-Nitijela-{leg_number}"
        else:
            # Fallback: extract from PDF path
            slug = Path(href).stem
            doc_id = f"MH-Nitijela-{slug}"

        acts.append({
            "doc_id": doc_id,
            "title": title,
            "long_title": long_title,
            "legislation_number": leg_number,
            "version": version,
            "date": date_str,
            "pdf_url": pdf_url,
        })

    return acts


# ---------------------------------------------------------------------------
# PDF text extraction
# ---------------------------------------------------------------------------

def extract_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pypdf."""
    try:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(pdf_bytes))
        pages = [p.extract_text() or "" for p in reader.pages]
        text = "\n\n".join(p for p in pages if p.strip())
        return text.strip()
    except Exception as e:
        print(f"  pypdf failed: {e}")

    try:
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages = [doc[i].get_text() for i in range(len(doc))]
        doc.close()
        text = "\n\n".join(p for p in pages if p.strip())
        return text.strip()
    except Exception as e:
        print(f"  fitz failed: {e}")

    return ""


# ---------------------------------------------------------------------------
# Normalize
# ---------------------------------------------------------------------------

def normalize(act: Dict, text: str) -> Dict:
    return {
        "_id": act["doc_id"],
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": act["title"],
        "long_title": act.get("long_title", ""),
        "text": text,
        "date": act.get("date", ""),
        "url": act["pdf_url"],
        "legislation_number": act.get("legislation_number", ""),
        "version": act.get("version", ""),
    }


# ---------------------------------------------------------------------------
# Fetch logic
# ---------------------------------------------------------------------------

def _fetch_acts_list(session: requests.Session) -> List[Dict]:
    """Fetch and parse the full acts listing."""
    print("Fetching acts listing page...")
    resp = _get(session, BY_TITLE_URL)
    if not resp:
        print("ERROR: Could not fetch acts listing page", file=sys.stderr)
        return []
    acts = parse_acts(resp.text)
    print(f"Found {len(acts)} principal acts")
    return acts


def fetch_sample(count: int = 15) -> List[Dict]:
    """Fetch a sample of acts spread across different periods."""
    session = _session()
    acts = _fetch_acts_list(session)
    if not acts:
        return []

    # Sample from beginning, middle, and end
    indices = set()
    n = len(acts)
    step = max(1, n // count)
    for i in range(0, n, step):
        indices.add(i)
        if len(indices) >= count + 5:  # grab extras in case some fail
            break
    # Also add first and last
    indices.add(0)
    indices.add(n - 1)

    records = []
    for idx in sorted(indices):
        if len(records) >= count:
            break
        act = acts[idx]
        print(f"  [{len(records)+1}/{count}] {act['title'][:70]}...")

        time.sleep(REQUEST_DELAY)
        resp = _get(session, act["pdf_url"])
        if not resp:
            print(f"       Skipped: download failed")
            continue

        ct = resp.headers.get("Content-Type", "")
        if b"%PDF" not in resp.content[:20] and "pdf" not in ct:
            print(f"       Skipped: not a PDF")
            continue

        text = extract_text(resp.content)
        if len(text) < 100:
            print(f"       Skipped: text too short ({len(text)} chars)")
            continue

        record = normalize(act, text)
        records.append(record)
        print(f"       OK: {len(text):,} chars")

    return records


def fetch_all(since: Optional[str] = None) -> Generator[Dict, None, None]:
    """Fetch all acts."""
    session = _session()
    acts = _fetch_acts_list(session)
    if not acts:
        return

    yielded = 0
    skipped = 0

    for act in acts:
        # Filter by date if since is provided
        if since and act.get("date") and act["date"] < since:
            continue

        time.sleep(REQUEST_DELAY)
        resp = _get(session, act["pdf_url"])
        if not resp:
            skipped += 1
            continue

        if b"%PDF" not in resp.content[:20]:
            skipped += 1
            continue

        text = extract_text(resp.content)
        if len(text) < 100:
            skipped += 1
            continue

        record = normalize(act, text)
        yielded += 1

        if yielded % 25 == 0:
            print(f"  Progress: {yielded} fetched, {skipped} skipped")

        yield record

    print(f"\nTotal: {yielded} fetched, {skipped} skipped")


# ---------------------------------------------------------------------------
# Save / validate
# ---------------------------------------------------------------------------

def save_samples(records: List[Dict]) -> None:
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    for i, record in enumerate(records):
        path = SAMPLE_DIR / f"record_{i:04d}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
    all_path = SAMPLE_DIR / "all_samples.json"
    with open(all_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"\nSaved {len(records)} samples to {SAMPLE_DIR}")


def validate_samples() -> bool:
    samples = sorted(SAMPLE_DIR.glob("record_*.json"))
    if len(samples) < 10:
        print(f"FAIL: Only {len(samples)} samples, need >= 10")
        return False

    ok = True
    text_lengths = []
    for path in samples:
        with open(path, "r", encoding="utf-8") as f:
            rec = json.load(f)
        text = rec.get("text", "")
        text_lengths.append(len(text))
        if not text:
            print(f"FAIL: {path.name} missing text")
            ok = False
        for field in ("_id", "_source", "_type", "title"):
            if not rec.get(field):
                print(f"WARN: {path.name} missing {field}")
        if text and re.search(r"<[a-z]+[^>]*>", text, re.IGNORECASE):
            print(f"WARN: {path.name} may contain HTML tags")

    avg = sum(text_lengths) / len(text_lengths) if text_lengths else 0
    print(f"\nValidation:")
    print(f"  Samples: {len(samples)}")
    print(f"  Avg text: {avg:,.0f} chars")
    print(f"  Min text: {min(text_lengths):,} chars")
    print(f"  Max text: {max(text_lengths):,} chars")
    print(f"  Valid: {ok}")
    return ok


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="MH/Nitijela fetcher")
    sub = parser.add_subparsers(dest="command")

    bp = sub.add_parser("bootstrap", help="Initial data fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample only")
    bp.add_argument("--full", action="store_true", help="Full fetch")

    up = sub.add_parser("updates", help="Fetch updates")
    up.add_argument("--since", required=True, help="YYYY-MM-DD")

    sub.add_parser("validate", help="Validate samples")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "validate":
        valid = validate_samples()
        sys.exit(0 if valid else 1)

    if args.command == "bootstrap":
        if args.sample:
            print("Fetching sample Marshall Islands acts...")
            records = fetch_sample()
            if records:
                save_samples(records)
                validate_samples()
                sys.exit(0 if len(records) >= 10 else 1)
            else:
                print("No records fetched!", file=sys.stderr)
                sys.exit(1)
        elif args.full:
            count = 0
            for rec in fetch_all():
                count += 1
            print(f"Fetched {count} acts")
        else:
            parser.print_help()
            sys.exit(1)

    elif args.command == "updates":
        count = 0
        for rec in fetch_all(since=args.since):
            count += 1
        print(f"Fetched {count} updates since {args.since}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
