#!/usr/bin/env python3
"""
US/OLCOpinions -- DOJ Office of Legal Counsel Opinions

Fetches ~1,400 published OLC opinions (1934-present) from justice.gov.
Full text is extracted from PDFs using common/pdf_extract.

Year-filtered listing pages require a session (cookies from main page).

Usage:
    python bootstrap.py bootstrap --sample
    python bootstrap.py bootstrap --full
    python bootstrap.py updates --since YYYY-MM-DD
"""

import argparse
import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Generator, List, Optional, Tuple

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
SOURCE_ID = "US/OLCOpinions"
BASE_URL = "https://www.justice.gov"
OPINIONS_URL = f"{BASE_URL}/olc/opinions"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 LegalDataHunter/1.0"
)
REQUEST_DELAY = 1.5


class OLCClient:
    """Client for fetching OLC opinions from justice.gov."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        # Initialize session with cookies from main page
        self.session.get(OPINIONS_URL, timeout=30)
        time.sleep(REQUEST_DELAY)

    def get_year_urls(self) -> List[Tuple[int, str]]:
        """Get all year filter URLs from the opinions page sidebar."""
        resp = self.session.get(OPINIONS_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        years = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "opinions_list_date" in href:
                m = re.search(r"opinions_list_date(?:%3A|:)(\d{4})", href)
                if m:
                    year = int(m.group(1))
                    full_url = f"{OPINIONS_URL}?f[0]=opinions_list_date:{year}"
                    years.append((year, full_url))

        return sorted(set(years), key=lambda x: -x[0])

    def _refresh_session(self):
        """Re-initialize session cookies by visiting the main page."""
        self.session.get(OPINIONS_URL, timeout=30)
        time.sleep(REQUEST_DELAY)

    def list_opinions_for_year(self, year: int) -> List[Dict]:
        """List all opinions for a given year."""
        # Refresh session before each year to avoid 403s
        self._refresh_session()
        url = f"{OPINIONS_URL}?f[0]=opinions_list_date:{year}"
        opinions = []
        page = 0

        while True:
            page_url = f"{url}&page={page}" if page > 0 else url
            try:
                resp = self.session.get(page_url, timeout=30)
                if resp.status_code == 403:
                    self._refresh_session()
                    resp = self.session.get(page_url, timeout=30)
                resp.raise_for_status()
            except requests.RequestException as exc:
                print(f"  Error fetching {year} page {page}: {exc}")
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            found = 0

            # Strategy 1: Recent opinions use h2 > a[href*=/media/]
            for h2 in soup.find_all("h2"):
                a = h2.find("a", href=True)
                if not a or "/media/" not in a.get("href", ""):
                    continue

                title = a.get_text().strip()
                href = a["href"]
                media_id = re.search(r"/media/(\d+)/", href)
                media_id = media_id.group(1) if media_id else ""

                time_el = h2.find_next("time")
                date_str = ""
                if time_el:
                    dt = time_el.get("datetime", "")
                    date_str = dt[:10] if dt else ""

                if not media_id:
                    continue

                pdf_url = f"{BASE_URL}{href}" if href.startswith("/") else href
                opinions.append({
                    "media_id": media_id,
                    "title": title,
                    "date": date_str,
                    "year": year,
                    "pdf_url": pdf_url,
                })
                found += 1

            # Strategy 2: Older opinions use a[href*=/olc/file/]
            if found == 0:
                main = soup.find("main") or soup
                for a in main.find_all("a", href=True):
                    href = a["href"]
                    if "/olc/file/" not in href or "/dl" not in href:
                        continue
                    title = a.get_text().strip()
                    if not title or len(title) < 10:
                        continue
                    # Extract date/slug from path: /olc/file/YYYY-MM-DD-slug/dl
                    slug = href.replace("/olc/file/", "").replace("/dl", "")
                    date_match = re.match(r"(\d{4}-\d{2}-\d{2})", slug)
                    date_str = date_match.group(1) if date_match else f"{year}-01-01"
                    # Use slug hash as stable ID
                    media_id = hashlib.sha256(slug.encode()).hexdigest()[:12]

                    pdf_url = f"{BASE_URL}{href}" if href.startswith("/") else href
                    opinions.append({
                        "media_id": media_id,
                        "title": title,
                        "date": date_str,
                        "year": year,
                        "pdf_url": pdf_url,
                    })
                    found += 1

            if found == 0:
                break

            page += 1
            time.sleep(REQUEST_DELAY)

        return opinions

    def download_pdf(self, pdf_url: str, retries: int = 3) -> Optional[bytes]:
        """Download PDF and return raw bytes."""
        for attempt in range(retries):
            try:
                resp = self.session.get(pdf_url, timeout=120)
                if resp.status_code == 429:
                    time.sleep(2 ** (attempt + 2))
                    continue
                if resp.status_code in (403, 404):
                    return None
                resp.raise_for_status()
                ct = resp.headers.get("Content-Type", "")
                if "pdf" in ct or b"%PDF" in resp.content[:20]:
                    return resp.content
                return None
            except requests.RequestException:
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                return None
        return None


# ---------------------------------------------------------------------------
# Text extraction
# ---------------------------------------------------------------------------

def _extract_text(pdf_bytes: bytes, media_id: str) -> str:
    """Extract text from PDF using common/pdf_extract or fallbacks."""
    try:
        from common.pdf_extract import extract_pdf_markdown
        text = extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=media_id,
            pdf_bytes=pdf_bytes,
            table="doctrine",
            force=True,
        )
        if text:
            return text
    except (ImportError, TypeError):
        pass

    try:
        import pdfplumber
        import io
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = [p.extract_text() or "" for p in pdf.pages]
            return "\n\n".join(p for p in pages if p)
    except ImportError:
        pass

    try:
        from pypdf import PdfReader
        import io
        reader = PdfReader(io.BytesIO(pdf_bytes))
        pages = [p.extract_text() or "" for p in reader.pages]
        return "\n\n".join(p for p in pages if p)
    except ImportError:
        pass

    return ""


# ---------------------------------------------------------------------------
# Normalize
# ---------------------------------------------------------------------------

def normalize(opinion: Dict, text: str) -> Dict:
    """Transform into standard schema."""
    media_id = opinion["media_id"]
    doc_id = f"US-OLC-{media_id}"

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": opinion["title"],
        "text": text,
        "date": opinion.get("date", ""),
        "url": opinion["pdf_url"],
        "media_id": media_id,
        "year": opinion.get("year"),
    }


# ---------------------------------------------------------------------------
# Fetch logic
# ---------------------------------------------------------------------------

def fetch_sample(count: int = 15) -> List[Dict]:
    """Fetch a diverse sample of OLC opinions."""
    client = OLCClient()
    records: List[Dict] = []

    # Get year URLs and sample from different periods
    year_urls = client.get_year_urls()
    total_years = len(year_urls)
    print(f"Found {total_years} years with opinions")

    # Pick a spread of years
    target_years = []
    if total_years > 0:
        step = max(1, total_years // 5)
        for i in range(0, total_years, step):
            target_years.append(year_urls[i])
            if len(target_years) >= 5:
                break

    per_year = (count // len(target_years)) + 2 if target_years else count

    for year, url in target_years:
        if len(records) >= count:
            break

        print(f"\nFetching opinions for {year}...")
        opinions = client.list_opinions_for_year(year)
        time.sleep(REQUEST_DELAY)
        print(f"  Found {len(opinions)} opinions")

        for op in opinions[:per_year]:
            if len(records) >= count:
                break

            print(f"  [{len(records)+1}] {op['title'][:65]}...")

            pdf_bytes = client.download_pdf(op["pdf_url"])
            time.sleep(REQUEST_DELAY)

            if not pdf_bytes:
                print(f"       Skipping: PDF download failed")
                continue

            text = _extract_text(pdf_bytes, op["media_id"])
            if len(text) < 200:
                print(f"       Skipping: text too short ({len(text)} chars)")
                continue

            record = normalize(op, text)
            records.append(record)
            print(f"       OK: {len(text):,} chars")

    return records


def fetch_all(since: Optional[str] = None) -> Generator[Dict, None, None]:
    """Fetch all OLC opinions by iterating through year filters."""
    client = OLCClient()

    year_urls = client.get_year_urls()
    print(f"Found {len(year_urls)} years with opinions")

    yielded = 0
    skipped = 0

    for year, url in year_urls:
        # Skip years before 'since' date
        if since and year < int(since[:4]):
            continue

        print(f"\nProcessing year {year}...")
        opinions = client.list_opinions_for_year(year)
        time.sleep(REQUEST_DELAY)
        print(f"  Found {len(opinions)} opinions")

        for op in opinions:
            # Filter by exact date if since provided
            if since and op.get("date", "") and op["date"] < since:
                continue

            pdf_bytes = client.download_pdf(op["pdf_url"])
            time.sleep(REQUEST_DELAY)

            if not pdf_bytes:
                skipped += 1
                continue

            text = _extract_text(pdf_bytes, op["media_id"])
            if len(text) < 200:
                skipped += 1
                continue

            record = normalize(op, text)
            yielded += 1

            if yielded % 50 == 0:
                print(f"  Progress: {yielded:,} fetched, {skipped} skipped")

            yield record

    print(f"\nTotal: {yielded:,} fetched, {skipped} skipped")


# ---------------------------------------------------------------------------
# Save / validate
# ---------------------------------------------------------------------------

def save_samples(records: List[Dict]) -> None:
    """Save sample records to sample/."""
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
    """Validate sample records."""
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
    parser = argparse.ArgumentParser(description="US/OLCOpinions fetcher")
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
            print("Fetching sample OLC opinions...")
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
            print(f"Fetched {count} OLC opinions")
        else:
            parser.print_help()
            sys.exit(1)

    elif args.command == "updates":
        count = 0
        for rec in fetch_all(since=args.since):
            count += 1
        print(f"Fetched {count} updates since {args.since}")


if __name__ == "__main__":
    main()
