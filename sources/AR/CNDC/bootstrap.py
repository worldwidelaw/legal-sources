#!/usr/bin/env python3
"""
AR/CNDC -- Argentina National Competition Commission decisions

Scrapes competition/antitrust decisions from cndc.produccion.gob.ar.
Each decision is a PDF; full text extracted via common/pdf_extract.

NO AUTH REQUIRED.

Usage:
    python bootstrap.py bootstrap --sample   # Fetch 15 sample records
    python bootstrap.py bootstrap --full     # Full incremental fetch
    python bootstrap.py updates --since YYYY-MM-DD
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional
from urllib.parse import urljoin, unquote

import requests
from bs4 import BeautifulSoup

SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
ROOT_DIR = SCRIPT_DIR.parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

SOURCE_ID = "AR/CNDC"
BASE_URL = "https://cndc.produccion.gob.ar"
SEARCH_URL = f"{BASE_URL}/buscador"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
REQUEST_DELAY = 1.5


class CNDCClient:
    """Client for CNDC decision search."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    def get_page(self, page: int = 0, retries: int = 3) -> Optional[str]:
        url = f"{SEARCH_URL}?page={page}"
        for attempt in range(retries):
            try:
                resp = self.session.get(url, timeout=30)
                if resp.status_code == 429:
                    time.sleep(2 ** (attempt + 2))
                    continue
                resp.raise_for_status()
                return resp.text
            except requests.RequestException as exc:
                if attempt < retries - 1:
                    time.sleep(2 ** (attempt + 1))
                    continue
                print(f"  Page {page} failed: {exc}")
                return None
        return None

    def download_pdf(self, pdf_url: str, retries: int = 3) -> Optional[bytes]:
        for attempt in range(retries):
            try:
                resp = self.session.get(pdf_url, timeout=120)
                if resp.status_code == 429:
                    time.sleep(2 ** (attempt + 2))
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                if b"%PDF" in resp.content[:20]:
                    return resp.content
                return None
            except requests.RequestException:
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                return None
        return None


def parse_listing(html: str) -> List[Dict[str, Any]]:
    """Parse a search results page into a list of case metadata."""
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("table tbody tr")
    items = []

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 6:
            continue

        title = cells[0].get_text(strip=True)
        pdf_link_tag = cells[1].find("a", href=True)
        pdf_url = pdf_link_tag["href"] if pdf_link_tag else ""
        if pdf_url and not pdf_url.startswith("http"):
            pdf_url = urljoin(BASE_URL, pdf_url)

        node_link = cells[0].find("a", href=True)
        node_path = node_link["href"] if node_link else ""
        node_id = node_path.replace("/node/", "") if "/node/" in node_path else ""

        boletin = cells[2].get_text(strip=True)
        dictamen = cells[3].get_text(strip=True)
        date_str = cells[4].get_text(strip=True)
        case_type = cells[5].get_text(strip=True)

        # Parse date from DD-MM-YYYY to ISO
        iso_date = ""
        if date_str:
            try:
                dt = datetime.strptime(date_str, "%d-%m-%Y")
                iso_date = dt.strftime("%Y-%m-%d")
            except ValueError:
                iso_date = date_str

        # Extract case number from title (e.g., "(CONC 2077)")
        case_number = ""
        m = re.match(r"\(([^)]+)\)", title)
        if m:
            case_number = m.group(1).strip()

        items.append({
            "title": title,
            "pdf_url": pdf_url,
            "node_id": node_id,
            "boletin": boletin,
            "dictamen": dictamen,
            "date": iso_date,
            "case_type": case_type,
            "case_number": case_number,
        })

    return items


def _extract_text(pdf_bytes: bytes, doc_id: str = "") -> str:
    """Extract text from PDF bytes."""
    try:
        from common.pdf_extract import extract_pdf_markdown
        text = extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=doc_id or "temp",
            pdf_bytes=pdf_bytes,
            table="case_law",
        )
        if text:
            return text
    except ImportError:
        pass

    try:
        import pdfplumber
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(pdf_bytes)
            tmp_path = tmp.name
        try:
            with pdfplumber.open(tmp_path) as pdf:
                pages = []
                for page in pdf.pages:
                    t = page.extract_text()
                    if t:
                        pages.append(t)
                return "\n\n".join(pages)
        finally:
            os.unlink(tmp_path)
    except ImportError:
        pass

    print("  WARNING: No PDF extraction library available")
    return ""


def normalize(item: Dict, text: str) -> Dict:
    doc_id = item.get("case_number") or item.get("node_id") or "unknown"
    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": item.get("title", ""),
        "text": text,
        "date": item.get("date", ""),
        "url": f"{BASE_URL}/node/{item.get('node_id', '')}" if item.get("node_id") else item.get("pdf_url", ""),
        "case_number": item.get("case_number", ""),
        "case_type": item.get("case_type", ""),
        "boletin_number": item.get("boletin", ""),
        "dictamen_number": item.get("dictamen", ""),
        "pdf_url": item.get("pdf_url", ""),
    }


def fetch_sample(count: int = 15) -> List[Dict]:
    """Fetch sample decisions from recent pages."""
    client = CNDCClient()
    records: List[Dict] = []

    for page in range(5):
        if len(records) >= count:
            break

        print(f"\nFetching page {page}...")
        html = client.get_page(page)
        time.sleep(REQUEST_DELAY)

        if not html:
            continue

        items = parse_listing(html)
        print(f"  Found {len(items)} items")

        for item in items:
            if len(records) >= count:
                break

            pdf_url = item.get("pdf_url", "")
            if not pdf_url:
                print(f"  Skipping {item['title'][:50]}: no PDF")
                continue

            print(f"  [{len(records)+1}] {item['title'][:70]}...")
            print(f"       Downloading PDF...")
            pdf_bytes = client.download_pdf(pdf_url)
            time.sleep(REQUEST_DELAY)

            if not pdf_bytes:
                print(f"       Skipping: PDF download failed")
                continue

            text = _extract_text(pdf_bytes, doc_id=item.get("case_number", ""))
            if len(text) < 100:
                print(f"       Skipping: text too short ({len(text)} chars)")
                continue

            record = normalize(item, text)
            records.append(record)
            print(f"       OK: {len(text):,} chars")

    return records


def fetch_all(sample: bool = False, since: Optional[str] = None) -> Generator[Dict, None, None]:
    if sample:
        yield from fetch_sample()
        return

    client = CNDCClient()
    total_yielded = 0
    page = 0
    max_pages = 500  # safety limit

    while page < max_pages:
        html = client.get_page(page)
        time.sleep(REQUEST_DELAY)

        if not html:
            break

        items = parse_listing(html)
        if not items:
            break

        for item in items:
            if since and item.get("date", "") and item["date"] < since:
                continue

            pdf_url = item.get("pdf_url", "")
            if not pdf_url:
                continue

            pdf_bytes = client.download_pdf(pdf_url)
            time.sleep(REQUEST_DELAY)
            if not pdf_bytes:
                continue

            text = _extract_text(pdf_bytes, doc_id=item.get("case_number", ""))
            if len(text) < 100:
                continue

            record = normalize(item, text)
            total_yielded += 1
            if total_yielded % 50 == 0:
                print(f"  Fetched {total_yielded} records (page {page})...")
            yield record

        page += 1

    print(f"  Total fetched: {total_yielded}")


def save_samples(records: List[Dict]) -> None:
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    for i, rec in enumerate(records):
        with open(SAMPLE_DIR / f"record_{i:04d}.json", "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False, indent=2)
    with open(SAMPLE_DIR / "all_samples.json", "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"\nSaved {len(records)} samples to {SAMPLE_DIR}")


def validate_samples() -> bool:
    samples = sorted(SAMPLE_DIR.glob("record_*.json"))
    if len(samples) < 10:
        print(f"FAIL: Only {len(samples)} samples, need >= 10")
        return False

    ok = True
    text_lengths = []
    for p in samples:
        with open(p, "r", encoding="utf-8") as f:
            rec = json.load(f)
        text = rec.get("text", "")
        text_lengths.append(len(text))
        if not text:
            print(f"FAIL: {p.name} missing text")
            ok = False
        for field in ("_id", "_source", "_type", "title"):
            if not rec.get(field):
                print(f"WARN: {p.name} missing {field}")

    avg = sum(text_lengths) / len(text_lengths) if text_lengths else 0
    print(f"\nValidation:")
    print(f"  Samples: {len(samples)}")
    print(f"  Avg text: {avg:,.0f} chars")
    print(f"  Min text: {min(text_lengths):,} chars")
    print(f"  Max text: {max(text_lengths):,} chars")
    print(f"  Valid: {ok}")
    return ok


def main():
    parser = argparse.ArgumentParser(description="AR/CNDC fetcher")
    sub = parser.add_subparsers(dest="command")
    bp = sub.add_parser("bootstrap")
    bp.add_argument("--sample", action="store_true")
    bp.add_argument("--full", action="store_true")
    up = sub.add_parser("updates")
    up.add_argument("--since", required=True)
    sub.add_parser("validate")
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "validate":
        sys.exit(0 if validate_samples() else 1)

    if args.command == "bootstrap":
        if args.sample:
            print("Fetching sample CNDC competition decisions...")
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
            for _ in fetch_all():
                count += 1
            print(f"Fetched {count} decisions")
        else:
            parser.print_help()
            sys.exit(1)

    elif args.command == "updates":
        count = 0
        for _ in fetch_all(since=args.since):
            count += 1
        print(f"Fetched {count} updates since {args.since}")


if __name__ == "__main__":
    main()
