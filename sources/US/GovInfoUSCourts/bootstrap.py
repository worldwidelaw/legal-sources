#!/usr/bin/env python3
"""
US/GovInfoUSCourts -- GovInfo Federal Court Opinions (USCOURTS)

Fetches US federal court opinions from all 130+ courts via GovInfo's
public wssearch API.  Full text is extracted from PDFs using the
centralized common/pdf_extract pipeline.

NO API KEY REQUIRED — uses the public wssearch/search and direct
content download endpoints.

Usage:
    python bootstrap.py bootstrap --sample   # Fetch 15 sample records
    python bootstrap.py bootstrap --full     # Full incremental fetch
    python bootstrap.py updates --since YYYY-MM-DD
"""

import argparse
import html as html_mod
import json
import os
import re
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Set

import requests

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
ROOT_DIR = SCRIPT_DIR.parent.parent.parent  # repo root

# Add repo root so we can import common modules
sys.path.insert(0, str(ROOT_DIR))

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SOURCE_ID = "US/GovInfoUSCourts"
SEARCH_URL = "https://www.govinfo.gov/wssearch/search"
DETAIL_URL = "https://www.govinfo.gov/wssearch/getContentDetail"
CONTENT_BASE = "https://www.govinfo.gov/content/pkg"
MODS_BASE = "https://www.govinfo.gov/metadata/granule"
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research; contact@legaldatahunter.com)"
REQUEST_DELAY = 1.5  # seconds between requests


class GovInfoUSCourts:
    """Client for fetching USCOURTS opinions from GovInfo."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })

    # ------------------------------------------------------------------
    # Search / list
    # ------------------------------------------------------------------
    def search_opinions(
        self,
        query: str = "collection:USCOURTS",
        offset: int = 0,
        page_size: int = 25,
        retries: int = 3,
    ) -> Dict:
        """Search USCOURTS opinions via wssearch."""
        payload = {
            "query": query,
            "offset": offset,
            "pageSize": page_size,
        }
        for attempt in range(retries):
            try:
                resp = self.session.post(
                    SEARCH_URL,
                    json=payload,
                    timeout=60,
                )
                if resp.status_code == 429:
                    wait = 2 ** (attempt + 2)
                    print(f"  Rate-limited, waiting {wait}s...")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, json.JSONDecodeError) as exc:
                if attempt < retries - 1:
                    time.sleep(2 ** (attempt + 1))
                    continue
                print(f"  Search failed: {exc}")
                return {}
        return {}

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------
    def get_detail(self, package_id: str, granule_id: str, retries: int = 3) -> Dict:
        """Get content detail (metadata) for a granule."""
        params = {"packageId": package_id}
        if granule_id:
            params["granuleId"] = granule_id
        for attempt in range(retries):
            try:
                resp = self.session.get(DETAIL_URL, params=params, timeout=30)
                if resp.status_code == 429:
                    time.sleep(2 ** (attempt + 2))
                    continue
                if resp.status_code == 404:
                    return {}
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, json.JSONDecodeError):
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                return {}
        return {}

    # ------------------------------------------------------------------
    # PDF download
    # ------------------------------------------------------------------
    def download_pdf(self, pdf_url: str, retries: int = 3) -> Optional[bytes]:
        """Download a PDF and return raw bytes."""
        if pdf_url.startswith("//"):
            pdf_url = "https:" + pdf_url
        elif not pdf_url.startswith("http"):
            pdf_url = f"{CONTENT_BASE}/{pdf_url}"

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


# ---------------------------------------------------------------------------
# Metadata extraction helpers
# ---------------------------------------------------------------------------

def _parse_detail_metadata(detail: Dict) -> Dict:
    """Extract structured metadata from a getContentDetail response."""
    meta: Dict[str, Any] = {}
    meta["title"] = detail.get("title", "")

    # DC metadata
    dc = detail.get("dcMD", {})
    meta["date"] = dc.get("origDateIssued", "")
    meta["publisher"] = dc.get("publisher", "")
    authors = dc.get("governmentAuthors", [])
    if authors:
        meta["court_name"] = authors[0] if authors[0] else ""

    # Column metadata
    for col in detail.get("metadata", {}).get("columnnamevalueset", []):
        name = col.get("colname", "")
        value = col.get("colvalue", "")
        if not value:
            continue
        value = re.sub(r"<[^>]+>", " ", value).strip()  # strip HTML
        value = html_mod.unescape(value)
        if name == "Court Type":
            meta["court_type"] = value.lower()
        elif name == "Court Name":
            meta["court_name"] = value
        elif name == "Circuit":
            meta["circuit"] = value
        elif name == "State":
            meta["state"] = value
        elif name == "Party Names":
            meta["parties"] = value
        elif name == "Docket Text":
            meta["docket_text"] = value
        elif name == "Nature of Suit":
            meta["nature_of_suit"] = value
        elif name == "Case Number":
            meta["case_number"] = value
        elif name == "Opinion Filed Date":
            meta["opinion_date"] = value

    return meta


def _extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using common/pdf_extract or fallback."""
    # Try common/pdf_extract first
    try:
        from common.pdf_extract import extract_pdf_markdown
        # Write to temp file, extract, delete
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(pdf_bytes)
            tmp_path = tmp.name
        try:
            text = extract_pdf_markdown(
                source=SOURCE_ID,
                source_id="temp",
                pdf_path=tmp_path,
            )
            if text:
                return text
        except TypeError:
            # extract_pdf_markdown might not accept pdf_path; try differently
            pass
        finally:
            os.unlink(tmp_path)
    except ImportError:
        pass

    # Fallback: pdfplumber
    try:
        import pdfplumber
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(pdf_bytes)
            tmp_path = tmp.name
        try:
            with pdfplumber.open(tmp_path) as pdf:
                pages = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages.append(text)
                return "\n\n".join(pages)
        finally:
            os.unlink(tmp_path)
    except ImportError:
        pass

    # Fallback: pypdf
    try:
        from pypdf import PdfReader
        import io
        reader = PdfReader(io.BytesIO(pdf_bytes))
        pages = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                pages.append(text)
        return "\n\n".join(pages)
    except ImportError:
        pass

    print("  WARNING: No PDF extraction library available")
    return ""


# ---------------------------------------------------------------------------
# Normalize
# ---------------------------------------------------------------------------

def normalize(granule_id: str, package_id: str, text: str, meta: Dict) -> Dict:
    """Transform into standard schema."""
    # Parse court code from package_id (e.g. USCOURTS-ca10-25-01001 -> ca10)
    court_code = ""
    parts = package_id.replace("USCOURTS-", "").split("-", 1)
    if parts:
        court_code = parts[0]

    date = meta.get("date", "")
    if not date and meta.get("opinion_date"):
        # Try parsing "April 9, 2026" format
        try:
            dt = datetime.strptime(meta["opinion_date"], "%B %d, %Y")
            date = dt.strftime("%Y-%m-%d")
        except (ValueError, KeyError):
            pass

    return {
        "_id": granule_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": meta.get("title", ""),
        "text": text,
        "date": date,
        "url": f"https://www.govinfo.gov/app/details/{package_id}/{granule_id}",
        "package_id": package_id,
        "granule_id": granule_id,
        "court_name": meta.get("court_name", ""),
        "court_type": meta.get("court_type", ""),
        "court_code": court_code,
        "circuit": meta.get("circuit", ""),
        "state": meta.get("state", ""),
        "case_number": meta.get("case_number", ""),
        "parties": meta.get("parties", ""),
        "docket_text": meta.get("docket_text", ""),
        "nature_of_suit": meta.get("nature_of_suit", ""),
        "publisher": meta.get("publisher", ""),
    }


# ---------------------------------------------------------------------------
# Fetch logic
# ---------------------------------------------------------------------------

def fetch_sample(count: int = 15) -> List[Dict]:
    """Fetch a sample of court opinions across different court types."""
    client = GovInfoUSCourts()
    records: List[Dict] = []

    # Sample from appellate, district, bankruptcy
    queries = [
        ("collection:USCOURTS AND courttype:appellate", 6),
        ("collection:USCOURTS AND courttype:district", 6),
        ("collection:USCOURTS AND courttype:bankruptcy", 3),
    ]

    for query, target in queries:
        if len(records) >= count:
            break

        print(f"\nSearching: {query} (target {target})...")
        result = client.search_opinions(query=query, offset=0, page_size=target + 5)
        time.sleep(REQUEST_DELAY)

        result_set = result.get("resultSet", [])
        print(f"  Got {len(result_set)} results")

        for item in result_set:
            if len(records) >= count:
                break

            fm = item.get("fieldMap", {})
            package_id = fm.get("packageid", "")
            granule_id = fm.get("granuleid", "")
            title = fm.get("title", "")
            pdf_url = fm.get("url", "")

            if not package_id or not granule_id or not pdf_url:
                continue

            print(f"  [{len(records)+1}] {title[:70]}...")

            # Get detailed metadata
            detail = client.get_detail(package_id, granule_id)
            time.sleep(REQUEST_DELAY)
            meta = _parse_detail_metadata(detail) if detail else {"title": title}

            # Download PDF and extract text
            print(f"       Downloading PDF...")
            pdf_bytes = client.download_pdf(pdf_url)
            time.sleep(REQUEST_DELAY)

            if not pdf_bytes:
                print(f"       Skipping: PDF download failed")
                continue

            text = _extract_text_from_pdf(pdf_bytes)
            if len(text) < 200:
                print(f"       Skipping: extracted text too short ({len(text)} chars)")
                continue

            record = normalize(granule_id, package_id, text, meta)
            records.append(record)
            print(f"       OK: {len(text):,} chars")

    return records


def fetch_all(sample: bool = False, since: Optional[str] = None) -> Generator[Dict, None, None]:
    """
    Fetch court opinions.

    Args:
        sample: If True, fetch only a small sample
        since: ISO date string; if set, only fetch opinions after this date
    """
    if sample:
        for record in fetch_sample():
            yield record
        return

    client = GovInfoUSCourts()

    # Build query
    query = "collection:USCOURTS"
    if since:
        query += f" AND publishdate:>={since}"

    offset = 0
    page_size = 100
    total_yielded = 0

    while True:
        result = client.search_opinions(query=query, offset=offset, page_size=page_size)
        time.sleep(REQUEST_DELAY)

        result_set = result.get("resultSet", [])
        total = result.get("iTotalCount", 0)

        if not result_set:
            break

        for item in result_set:
            fm = item.get("fieldMap", {})
            package_id = fm.get("packageid", "")
            granule_id = fm.get("granuleid", "")
            title = fm.get("title", "")
            pdf_url = fm.get("url", "")

            if not package_id or not granule_id or not pdf_url:
                continue

            # Get metadata
            detail = client.get_detail(package_id, granule_id)
            time.sleep(REQUEST_DELAY)
            meta = _parse_detail_metadata(detail) if detail else {"title": title}

            # Download PDF and extract text
            pdf_bytes = client.download_pdf(pdf_url)
            time.sleep(REQUEST_DELAY)

            if not pdf_bytes:
                continue

            text = _extract_text_from_pdf(pdf_bytes)
            if len(text) < 200:
                continue

            record = normalize(granule_id, package_id, text, meta)
            total_yielded += 1

            if total_yielded % 50 == 0:
                print(f"  Fetched {total_yielded} / {total} opinions...")

            yield record

        offset += page_size
        if offset >= total:
            break

    print(f"  Total opinions fetched: {total_yielded}")


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
    parser = argparse.ArgumentParser(description="US/GovInfoUSCourts fetcher")
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
            print("Fetching sample court opinions from GovInfo USCOURTS...")
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
            print(f"Fetched {count} opinions")
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
