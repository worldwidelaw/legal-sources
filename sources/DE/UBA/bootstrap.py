#!/usr/bin/env python3
"""
DE/UBA -- German Federal Environment Agency (Umweltbundesamt)

Fetches 6200+ official publications from the UBA publications portal:
  - TEXTE series (research reports)
  - Climate Change reports
  - Fact sheets and position papers
  - Environmental policy guidance

Source: https://www.umweltbundesamt.de
Discovery: Paginated HTML listing at /publikationen
Content: PDF documents extracted via common/pdf_extract

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
from typing import Dict, Generator, List, Optional
from urllib.parse import urljoin

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
SOURCE_ID = "DE/UBA"
BASE_URL = "https://www.umweltbundesamt.de"
PUB_URL = f"{BASE_URL}/publikationen"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 LegalDataHunter/1.0"
)
REQUEST_DELAY = 1.5
ITEMS_PER_PAGE = 12

MONTH_MAP = {
    "januar": "01", "february": "02", "februar": "02", "march": "03",
    "märz": "03", "april": "04", "may": "05", "mai": "05", "june": "06",
    "juni": "06", "july": "07", "juli": "07", "august": "08",
    "september": "09", "october": "10", "oktober": "10", "november": "11",
    "december": "12", "dezember": "12",
}


class UBAClient:
    """Client for fetching UBA publications."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
        })

    def list_documents(self, max_pages: int = 999) -> List[Dict]:
        """Paginate through publication listing and collect all doc metadata."""
        all_docs = []
        seen_urls = set()

        for page_num in range(0, max_pages):
            url = f"{PUB_URL}?page={page_num}"

            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
            except requests.RequestException as exc:
                print(f"  Error fetching page {page_num}: {exc}")
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            docs = self._parse_listing(soup)

            if not docs:
                break

            new_count = 0
            for doc in docs:
                if doc["pdf_url"] not in seen_urls:
                    seen_urls.add(doc["pdf_url"])
                    all_docs.append(doc)
                    new_count += 1

            print(f"  Page {page_num}: {new_count} new docs (total: {len(all_docs)})")

            if new_count == 0:
                break

            time.sleep(REQUEST_DELAY)

        return all_docs

    def _parse_listing(self, soup: BeautifulSoup) -> List[Dict]:
        """Parse publication items from a listing page."""
        docs = []
        items = soup.select(".views-row")

        for item in items:
            doc = self._extract_doc(item)
            if doc:
                docs.append(doc)

        return docs

    def _extract_doc(self, item) -> Optional[Dict]:
        """Extract document metadata from a views-row item."""
        # Title and landing page URL
        title_el = item.select_one("h2 a, h3 a")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        if not title or len(title) < 5:
            return None
        landing_url = title_el.get("href", "")
        if landing_url and not landing_url.startswith("http"):
            landing_url = f"{BASE_URL}{landing_url}"

        # PDF link
        pdf_link = item.find("a", href=lambda x: x and ".pdf" in x.lower())
        if not pdf_link:
            return None
        pdf_href = pdf_link["href"]
        if pdf_href.startswith("/"):
            pdf_url = f"{BASE_URL}{pdf_href}"
        elif pdf_href.startswith("http"):
            pdf_url = pdf_href
        else:
            pdf_url = f"{BASE_URL}/{pdf_href}"

        # Date
        date_str = ""
        time_el = item.find("time")
        if time_el:
            raw = time_el.get_text(strip=True).lower()
            # Format: "April 2026" or "März 2026"
            for month_name, month_num in MONTH_MAP.items():
                if month_name in raw:
                    year_match = re.search(r"(\d{4})", raw)
                    if year_match:
                        date_str = f"{year_match.group(1)}-{month_num}-01"
                    break

        # Stable ID from PDF URL
        clean_url = pdf_url.split("?")[0]
        doc_id = hashlib.sha256(clean_url.encode()).hexdigest()[:16]

        return {
            "id": doc_id,
            "title": title,
            "url": landing_url,
            "pdf_url": pdf_url,
            "date": date_str,
        }

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

def _extract_text(pdf_bytes: bytes, doc_id: str) -> str:
    """Extract text from PDF using common/pdf_extract or fallbacks."""
    try:
        from common.pdf_extract import extract_pdf_markdown
        text = extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=doc_id,
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

def normalize(doc: Dict, text: str) -> Dict:
    """Transform into standard schema."""
    return {
        "_id": f"DE-UBA-{doc['id']}",
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": doc["title"],
        "text": text,
        "date": doc.get("date", ""),
        "url": doc["url"],
    }


# ---------------------------------------------------------------------------
# Fetch logic
# ---------------------------------------------------------------------------

def fetch_sample(count: int = 15) -> List[Dict]:
    """Fetch a sample of UBA publications."""
    client = UBAClient()
    records = []

    print("Discovering UBA publications...")
    docs = client.list_documents(max_pages=5)
    print(f"Found {len(docs)} documents in first pages")

    for doc in docs:
        if len(records) >= count:
            break

        print(f"\n  [{len(records)+1}/{count}] {doc['title'][:65]}...")

        time.sleep(REQUEST_DELAY)
        pdf_bytes = client.download_pdf(doc["pdf_url"])

        if not pdf_bytes:
            print(f"       Skipping: PDF download failed")
            continue

        text = _extract_text(pdf_bytes, doc["id"])
        if len(text) < 200:
            print(f"       Skipping: text too short ({len(text)} chars)")
            continue

        record = normalize(doc, text)
        records.append(record)
        print(f"       OK: {len(text):,} chars")

    return records


def fetch_all(since: Optional[str] = None) -> Generator[Dict, None, None]:
    """Fetch all UBA publications."""
    client = UBAClient()

    print("Discovering all UBA publications...")
    docs = client.list_documents()
    print(f"Total documents found: {len(docs)}")

    yielded = 0
    skipped = 0

    for doc in docs:
        if since and doc.get("date") and doc["date"] < since:
            continue

        time.sleep(REQUEST_DELAY)
        pdf_bytes = client.download_pdf(doc["pdf_url"])

        if not pdf_bytes:
            skipped += 1
            continue

        text = _extract_text(pdf_bytes, doc["id"])
        if len(text) < 200:
            skipped += 1
            continue

        record = normalize(doc, text)
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
    parser = argparse.ArgumentParser(description="DE/UBA fetcher")
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
            print("Fetching sample UBA publications...")
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
            print(f"Fetched {count} UBA documents")
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
