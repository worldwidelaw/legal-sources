#!/usr/bin/env python3
"""
US/GovInfoUSReports -- GovInfo Supreme Court Opinions (US Reports)

Fetches US Supreme Court opinions from the USREPORTS collection via
GovInfo's public wssearch API.  Full text extracted from PDFs.

NO API KEY REQUIRED.

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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import requests

# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
ROOT_DIR = SCRIPT_DIR.parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

SOURCE_ID = "US/GovInfoUSReports"
SEARCH_URL = "https://www.govinfo.gov/wssearch/search"
DETAIL_URL = "https://www.govinfo.gov/wssearch/getContentDetail"
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research; contact@legaldatahunter.com)"
REQUEST_DELAY = 1.5


class USReportsClient:
    """Client for USREPORTS collection on GovInfo."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })

    def search(self, query: str, offset: int = 0, page_size: int = 25,
               retries: int = 3) -> Dict:
        payload = {"query": query, "offset": offset, "pageSize": page_size}
        for attempt in range(retries):
            try:
                resp = self.session.post(SEARCH_URL, json=payload, timeout=60)
                if resp.status_code == 429:
                    time.sleep(2 ** (attempt + 2))
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

    def get_detail(self, package_id: str, granule_id: str = "",
                   retries: int = 3) -> Dict:
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

    def download_pdf(self, pdf_url: str, retries: int = 3) -> Optional[bytes]:
        if pdf_url.startswith("//"):
            pdf_url = "https:" + pdf_url
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


def _parse_detail(detail: Dict) -> Dict:
    """Extract metadata from getContentDetail response."""
    meta: Dict[str, Any] = {}
    meta["title"] = detail.get("title", "")

    dc = detail.get("dcMD", {})
    meta["date"] = dc.get("origDateIssued", "")
    meta["publisher"] = dc.get("publisher", "")

    for col in detail.get("metadata", {}).get("columnnamevalueset", []):
        name = col.get("colname", "")
        value = col.get("colvalue", "")
        if not value:
            continue
        value = re.sub(r"<[^>]+>", " ", value).strip()
        value = html_mod.unescape(value)
        if name == "Volume":
            meta["volume"] = value
        elif name == "Page":
            meta["page"] = value
        elif name == "Citation":
            meta["citation"] = value
        elif name == "Term":
            meta["term"] = value

    return meta


def _extract_text_from_pdf(pdf_bytes: bytes, doc_id: str = "") -> str:
    """Extract text from PDF bytes via centralized extractor."""
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


def normalize(granule_id: str, package_id: str, text: str, meta: Dict) -> Dict:
    # Extract volume/page from granule_id (e.g. USREPORTS-582-449)
    volume, page = "", ""
    parts = granule_id.replace("USREPORTS-", "").split("-", 1)
    if len(parts) == 2:
        volume, page = parts[0], parts[1]

    return {
        "_id": granule_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": meta.get("title", ""),
        "text": text,
        "date": meta.get("date", ""),
        "url": f"https://www.govinfo.gov/app/details/{package_id}/{granule_id}",
        "package_id": package_id,
        "granule_id": granule_id,
        "court_name": "Supreme Court of the United States",
        "volume": meta.get("volume", volume),
        "page": meta.get("page", page),
        "citation": meta.get("citation", ""),
        "term": meta.get("term", ""),
        "publisher": meta.get("publisher", ""),
    }


def fetch_sample(count: int = 15) -> List[Dict]:
    """Fetch sample opinions from recent volumes."""
    client = USReportsClient()
    records: List[Dict] = []

    # Fetch from several recent volumes
    volumes = [582, 581, 580, 579, 578]

    for vol in volumes:
        if len(records) >= count:
            break

        query = f"collection:USREPORTS AND volume:{vol}"
        print(f"\nSearching volume {vol}...")
        result = client.search(query=query, offset=0, page_size=5)
        time.sleep(REQUEST_DELAY)

        result_set = result.get("resultSet", [])
        print(f"  Got {len(result_set)} results")

        for item in result_set:
            if len(records) >= count:
                break

            fm = item.get("fieldMap", {})
            granule_id = fm.get("granuleid", "")
            package_id = fm.get("packageid", "")
            title = fm.get("title", "")

            if not granule_id:
                continue

            # Skip whole-volume PDFs (no page granule)
            if granule_id == package_id:
                continue

            print(f"  [{len(records)+1}] {title[:70]}...")

            # Get metadata + correct PDF link from detail endpoint
            detail = client.get_detail(package_id, granule_id)
            time.sleep(REQUEST_DELAY)
            meta = _parse_detail(detail) if detail else {"title": title}

            pdf_url = (detail.get("download", {}).get("pdflink", "")
                       if detail else "")
            if not pdf_url:
                print(f"       Skipping: no PDF link in detail")
                continue

            # Skip very large PDFs
            size_str = (detail.get("download", {}).get("pdfSize", "")
                        if detail else "")
            if "MB" in size_str:
                try:
                    size_mb = float(size_str.replace(" MB", "").replace(",", ""))
                    if size_mb > 2.0:
                        print(f"       Skipping: too large ({size_str})")
                        continue
                except ValueError:
                    pass

            # Download and extract
            print(f"       Downloading PDF...")
            pdf_bytes = client.download_pdf(pdf_url)
            time.sleep(REQUEST_DELAY)

            if not pdf_bytes:
                print(f"       Skipping: PDF download failed")
                continue

            text = _extract_text_from_pdf(pdf_bytes, doc_id=granule_id)
            if len(text) < 200:
                print(f"       Skipping: text too short ({len(text)} chars)")
                continue

            record = normalize(granule_id, package_id, text, meta)
            records.append(record)
            print(f"       OK: {len(text):,} chars")

    return records


def fetch_all(sample: bool = False, since: Optional[str] = None) -> Generator[Dict, None, None]:
    if sample:
        for r in fetch_sample():
            yield r
        return

    client = USReportsClient()
    query = "collection:USREPORTS"
    if since:
        query += f" AND publishdate:>={since}"

    offset = 0
    page_size = 100
    total_yielded = 0

    while True:
        result = client.search(query=query, offset=offset, page_size=page_size)
        time.sleep(REQUEST_DELAY)

        result_set = result.get("resultSet", [])
        total = result.get("iTotalCount", 0)

        if not result_set:
            break

        for item in result_set:
            fm = item.get("fieldMap", {})
            granule_id = fm.get("granuleid", "")
            package_id = fm.get("packageid", "")
            title = fm.get("title", "")

            if not granule_id:
                continue
            if granule_id == package_id:
                continue

            detail = client.get_detail(package_id, granule_id)
            time.sleep(REQUEST_DELAY)
            meta = _parse_detail(detail) if detail else {"title": title}

            pdf_url = (detail.get("download", {}).get("pdflink", "")
                       if detail else "")
            if not pdf_url:
                continue

            # Skip huge PDFs
            size_str = (detail.get("download", {}).get("pdfSize", "")
                        if detail else "")
            if "MB" in size_str:
                try:
                    if float(size_str.replace(" MB", "").replace(",", "")) > 5.0:
                        continue
                except ValueError:
                    pass

            pdf_bytes = client.download_pdf(pdf_url)
            time.sleep(REQUEST_DELAY)
            if not pdf_bytes:
                continue

            text = _extract_text_from_pdf(pdf_bytes, doc_id=granule_id)
            if len(text) < 200:
                continue

            record = normalize(granule_id, package_id, text, meta)
            total_yielded += 1
            if total_yielded % 50 == 0:
                print(f"  Fetched {total_yielded} / {total}...")
            yield record

        offset += page_size
        if offset >= total:
            break

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
    parser = argparse.ArgumentParser(description="US/GovInfoUSReports fetcher")
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
            print("Fetching sample Supreme Court opinions from GovInfo USREPORTS...")
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
            print(f"Fetched {count} opinions")
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
