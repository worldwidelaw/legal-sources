#!/usr/bin/env python3
"""
US/GAOReports -- GAO Reports & Comptroller General Decisions

Fetches all 16,000+ GAO reports from GovInfo's public wssearch API.
Full text is extracted from HTML content pages (preferred) with PDF fallback.

NO API KEY REQUIRED — uses the public wssearch/search endpoint.

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
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import requests

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
SOURCE_ID = "US/GAOReports"
SEARCH_URL = "https://www.govinfo.gov/wssearch/search"
DETAIL_URL = "https://www.govinfo.gov/wssearch/getContentDetail"
CONTENT_BASE = "https://www.govinfo.gov/content/pkg"
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research; contact@legaldatahunter.com)"
REQUEST_DELAY = 1.5


class GovInfoGAO:
    """Client for fetching GAO reports from GovInfo."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })

    def search(
        self,
        query: str = "collection:GAOREPORTS",
        offset: int = 0,
        page_size: int = 100,
        retries: int = 3,
    ) -> Dict:
        """Search GAOREPORTS via wssearch."""
        payload = {
            "query": query,
            "offset": offset,
            "pageSize": page_size,
        }
        for attempt in range(retries):
            try:
                resp = self.session.post(SEARCH_URL, json=payload, timeout=60)
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

    def get_detail(self, package_id: str, retries: int = 3) -> Dict:
        """Get content detail (metadata) for a package."""
        for attempt in range(retries):
            try:
                resp = self.session.get(
                    DETAIL_URL,
                    params={"packageId": package_id},
                    timeout=30,
                )
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

    def download_html(self, package_id: str, retries: int = 3) -> Optional[str]:
        """Download HTML content and return cleaned text."""
        url = f"{CONTENT_BASE}/{package_id}/html/{package_id}.htm"
        for attempt in range(retries):
            try:
                resp = self.session.get(url, timeout=60)
                if resp.status_code == 429:
                    time.sleep(2 ** (attempt + 2))
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return _clean_html(resp.text)
            except requests.RequestException:
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                return None
        return None

    def download_pdf_bytes(self, package_id: str, retries: int = 3) -> Optional[bytes]:
        """Download PDF and return raw bytes."""
        url = f"{CONTENT_BASE}/{package_id}/pdf/{package_id}.pdf"
        for attempt in range(retries):
            try:
                resp = self.session.get(url, timeout=120)
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
# Text extraction helpers
# ---------------------------------------------------------------------------

def _clean_html(raw_html: str) -> str:
    """Strip HTML tags and clean whitespace."""
    text = re.sub(r"<script[^>]*>.*?</script>", " ", raw_html, flags=re.S)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_mod.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _extract_text_from_pdf(pdf_bytes: bytes, package_id: str) -> str:
    """Extract text from PDF using common/pdf_extract or fallback."""
    try:
        from common.pdf_extract import extract_pdf_markdown
        text = extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=package_id,
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
# Metadata extraction
# ---------------------------------------------------------------------------

def _parse_detail_metadata(detail: Dict) -> Dict:
    """Extract structured metadata from a getContentDetail response."""
    meta: Dict[str, Any] = {}
    meta["title"] = detail.get("title", "")

    dc = detail.get("dcMD", {})
    meta["date"] = dc.get("origDateIssued", "")

    for col in detail.get("metadata", {}).get("columnnamevalueset", []):
        name = col.get("colname", "")
        value = col.get("colvalue", "")
        if not value:
            continue
        value = re.sub(r"<[^>]+>", " ", value).strip()
        value = html_mod.unescape(value)
        if name == "Report Number":
            meta["report_number"] = value
        elif name == "Document Type":
            meta["document_type"] = value
        elif name == "Subject":
            meta["subject"] = value
        elif name == "Date Issued":
            meta["date_issued"] = value
        elif name == "SuDoc Class Number":
            meta["sudoc"] = value
        elif name == "Category":
            meta["category"] = value

    return meta


# ---------------------------------------------------------------------------
# Normalize
# ---------------------------------------------------------------------------

def normalize(package_id: str, text: str, meta: Dict) -> Dict:
    """Transform into standard schema."""
    # Extract report number from package_id (e.g. GAOREPORTS-GAO-08-1126T -> GAO-08-1126T)
    report_number = meta.get("report_number", "")
    if not report_number:
        report_number = package_id.replace("GAOREPORTS-", "")

    date = meta.get("date", "")
    if not date and meta.get("date_issued"):
        try:
            dt = datetime.strptime(meta["date_issued"], "%B %d, %Y")
            date = dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    return {
        "_id": package_id,
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": meta.get("title", ""),
        "text": text,
        "date": date,
        "url": f"https://www.govinfo.gov/app/details/{package_id}",
        "package_id": package_id,
        "report_number": report_number,
        "document_type": meta.get("document_type", ""),
        "subject": meta.get("subject", ""),
        "category": meta.get("category", ""),
        "sudoc": meta.get("sudoc", ""),
    }


# ---------------------------------------------------------------------------
# Fetch logic
# ---------------------------------------------------------------------------

def fetch_text(client: GovInfoGAO, package_id: str) -> str:
    """Fetch full text: try HTML first, fall back to PDF."""
    text = client.download_html(package_id)
    if text and len(text) >= 200:
        return text

    pdf_bytes = client.download_pdf_bytes(package_id)
    if pdf_bytes:
        time.sleep(REQUEST_DELAY)
        return _extract_text_from_pdf(pdf_bytes, package_id)

    return ""


def fetch_sample(count: int = 15) -> List[Dict]:
    """Fetch a diverse sample of GAO reports from different offsets."""
    client = GovInfoGAO()
    records: List[Dict] = []

    # Get total count
    initial = client.search(query="collection:GAOREPORTS", offset=0, page_size=1)
    total = initial.get("iTotalCount", 0)
    print(f"Total GAO reports available: {total:,}")
    time.sleep(REQUEST_DELAY)

    # GovInfo caps offsets at ~4999 for GAOREPORTS; use spread within that range
    max_offset = min(total, 4000)
    offsets = [0, max_offset // 3, 2 * max_offset // 3]
    per_offset = (count // len(offsets)) + 2

    for start_offset in offsets:
        if len(records) >= count:
            break

        print(f"\nFetching from offset {start_offset:,} (target {per_offset})...")
        result = client.search(
            query="collection:GAOREPORTS",
            offset=max(0, start_offset),
            page_size=per_offset + 3,
        )
        time.sleep(REQUEST_DELAY)

        result_set = result.get("resultSet", [])
        print(f"  Got {len(result_set)} results")

        for item in result_set:
            if len(records) >= count:
                break

            fm = item.get("fieldMap", {})
            package_id = fm.get("packageid", "")
            title = fm.get("title", "")

            if not package_id:
                continue

            print(f"  [{len(records)+1}] {title[:70]}...")

            # Get metadata
            detail = client.get_detail(package_id)
            time.sleep(REQUEST_DELAY)
            meta = _parse_detail_metadata(detail) if detail else {"title": title}

            # Get full text (HTML preferred, PDF fallback)
            text = fetch_text(client, package_id)
            time.sleep(REQUEST_DELAY)

            if len(text) < 200:
                print(f"       Skipping: text too short ({len(text)} chars)")
                continue

            record = normalize(package_id, text, meta)
            records.append(record)
            print(f"       OK: {len(text):,} chars ({meta.get('document_type', 'unknown')})")

    return records


def _get_all_package_ids() -> List[str]:
    """Get all package IDs from GovInfo sitemaps (bypasses 2000-result wssearch limit)."""
    import xml.etree.ElementTree as ET
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    # Fetch sitemap index
    resp = session.get(
        "https://www.govinfo.gov/sitemap/GAOREPORTS_sitemap_index.xml",
        timeout=30,
    )
    resp.raise_for_status()
    root = ET.fromstring(resp.text)
    sitemap_urls = [el.text for el in root.iter() if el.tag.endswith("loc") and el.text]
    print(f"Found {len(sitemap_urls)} yearly sitemaps")

    package_ids: List[str] = []
    for sm_url in sorted(sitemap_urls):
        time.sleep(REQUEST_DELAY)
        try:
            resp = session.get(sm_url, timeout=30)
            resp.raise_for_status()
            sm_root = ET.fromstring(resp.text)
            urls = [el.text for el in sm_root.iter() if el.tag.endswith("loc") and el.text]
            # Extract package_id from URL: .../app/details/GAOREPORTS-GAO-08-1126T
            for url in urls:
                pkg = url.rsplit("/", 1)[-1] if "/details/" in url else ""
                if pkg.startswith("GAOREPORTS-"):
                    package_ids.append(pkg)
            year = sm_url.split("_")[-2] if "_" in sm_url else "?"
            print(f"  {year}: {len(urls)} reports")
        except Exception as exc:
            print(f"  Error fetching {sm_url}: {exc}")

    print(f"Total package IDs from sitemaps: {len(package_ids):,}")
    return package_ids


def fetch_all(since: Optional[str] = None) -> Generator[Dict, None, None]:
    """Fetch all GAO reports using sitemap enumeration + HTML content."""
    client = GovInfoGAO()

    package_ids = _get_all_package_ids()
    total = len(package_ids)
    print(f"\nFetching full text for {total:,} reports...")

    yielded = 0
    skipped = 0

    for i, package_id in enumerate(package_ids):
        # Get metadata
        detail = client.get_detail(package_id)
        time.sleep(REQUEST_DELAY)
        meta = _parse_detail_metadata(detail) if detail else {}

        # Filter by date if --since provided
        if since and meta.get("date", ""):
            if meta["date"] < since:
                continue

        # Get full text
        text = fetch_text(client, package_id)
        time.sleep(REQUEST_DELAY)

        if len(text) < 200:
            skipped += 1
            continue

        record = normalize(package_id, text, meta)
        yielded += 1

        if yielded % 100 == 0:
            print(f"  Progress: {yielded:,} fetched, {skipped} skipped ({i+1}/{total})")

        yield record

    print(f"  Total: {yielded:,} fetched, {skipped} skipped")


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
    parser = argparse.ArgumentParser(description="US/GAOReports fetcher")
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
            print("Fetching sample GAO reports from GovInfo...")
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
            print(f"Fetched {count} GAO reports")
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
