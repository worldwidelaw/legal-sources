#!/usr/bin/env python3
"""
TZ/OAG-MIS - Tanzania Attorney General's Office Management Information System

Fetches legislation from the OAG MIS portal (https://oagmis.oag.go.tz).
Uses JSON/DataTables AJAX endpoints for metadata + PDF download for full text.

Data coverage:
- ~314 parliament acts
- ~4,479 subsidiary legislation items
- ~542 revised acts
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, Optional

import requests

try:
    from pdfminer.high_level import extract_text as pdf_extract_text
except ImportError:
    pdf_extract_text = None

BASE_URL = "https://oagmis.oag.go.tz"
RATE_LIMIT_DELAY = 1.5

ENDPOINTS = {
    "acts": "/portal/acts-ajax",
    "subsidiary": "/portal/legislation-ajax",
    "revised": "/portal/revised-acts-ajax",
}

DOWNLOAD_ENDPOINTS = {
    "acts": "/portal/acts/{id}/download",
    "subsidiary": "/portal/legislation/{id}/download",
    "revised": "/portal/revised-acts/revised/{id}/download",
}

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
    "Accept": "application/json",
})


def fetch_metadata(doc_type: str, start: int = 0, length: int = 100) -> Dict[str, Any]:
    """Fetch metadata page from AJAX endpoint."""
    url = f"{BASE_URL}{ENDPOINTS[doc_type]}"
    params = {"draw": 1, "start": start, "length": length}
    resp = SESSION.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def download_pdf_text(doc_type: str, doc_id: int, storage_path: str = None) -> str:
    """Download PDF and extract text."""
    if pdf_extract_text is None:
        return ""
    if storage_path:
        url = f"{BASE_URL}/storage/{storage_path}"
    else:
        url = f"{BASE_URL}{DOWNLOAD_ENDPOINTS[doc_type].format(id=doc_id)}"
    tmp_path = f"/tmp/tz_oag_{doc_type}_{doc_id}.pdf"
    try:
        resp = SESSION.get(url, timeout=60)
        resp.raise_for_status()
        if len(resp.content) < 100:
            return ""
        with open(tmp_path, "wb") as f:
            f.write(resp.content)
        text = pdf_extract_text(tmp_path)
        return text.strip() if text else ""
    except Exception as e:
        print(f"  PDF extraction failed for {doc_type}/{doc_id}: {e}", file=sys.stderr)
        return ""
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def normalize_act(raw: Dict[str, Any], text: str) -> Dict[str, Any]:
    """Normalize a parliament act record."""
    doc_id = raw.get("id", "")
    title = raw.get("shortTitle") or raw.get("longTitle") or ""
    long_title = raw.get("longTitle") or ""
    date = raw.get("publicationDate") or raw.get("enactmentDate") or ""
    enactment_no = raw.get("enactmentNo") or ""
    chapter = raw.get("chapterNumber") or ""

    return {
        "_id": f"TZ-OAG-act-{doc_id}",
        "_source": "TZ/OAG-MIS",
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title.strip(),
        "long_title": long_title.strip(),
        "text": text,
        "date": date[:10] if date else None,
        "enactment_number": enactment_no,
        "chapter_number": chapter,
        "doc_type": "parliament_act",
        "url": f"{BASE_URL}/portal/acts/{doc_id}/download",
    }


def normalize_subsidiary(raw: Dict[str, Any], text: str) -> Dict[str, Any]:
    """Normalize a subsidiary legislation record."""
    doc_id = raw.get("id", "")
    citation = raw.get("citation") or ""
    gn = raw.get("gn") or ""
    date = raw.get("publicationDate") or ""

    return {
        "_id": f"TZ-OAG-sub-{doc_id}",
        "_source": "TZ/OAG-MIS",
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": citation.strip(),
        "text": text,
        "date": date[:10] if date else None,
        "gazette_number": gn,
        "doc_type": "subsidiary_legislation",
        "url": f"{BASE_URL}/portal/legislation/{doc_id}/download",
    }


def normalize_revised(raw: Dict[str, Any], text: str) -> Dict[str, Any]:
    """Normalize a revised act record."""
    doc_id = raw.get("id", "")
    title = raw.get("shortTitle") or ""
    chapter = raw.get("chapterNumber") or ""
    date = raw.get("publicationDate") or ""

    return {
        "_id": f"TZ-OAG-rev-{doc_id}",
        "_source": "TZ/OAG-MIS",
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title.strip(),
        "text": text,
        "date": date[:10] if date else None,
        "chapter_number": chapter,
        "doc_type": "revised_act",
        "url": f"{BASE_URL}/portal/revised-acts/revised/{doc_id}/download",
    }


def fetch_all() -> Generator[Dict[str, Any], None, None]:
    """Yield all documents with full text."""
    for doc_type in ENDPOINTS:
        print(f"\nFetching {doc_type}...")
        start = 0
        page_size = 100
        total = None

        while True:
            data = fetch_metadata(doc_type, start=start, length=page_size)
            if total is None:
                total = data.get("recordsTotal", 0)
                print(f"  Total {doc_type}: {total}")

            records = data.get("data", [])
            if not records:
                break

            for raw in records:
                doc_id = raw.get("id")
                if not doc_id:
                    continue
                storage_path = raw.get("actDocPath") if doc_type == "revised" else None
                text = download_pdf_text(doc_type, doc_id, storage_path=storage_path)
                if doc_type == "acts":
                    yield normalize_act(raw, text)
                elif doc_type == "subsidiary":
                    yield normalize_subsidiary(raw, text)
                else:
                    yield normalize_revised(raw, text)
                time.sleep(RATE_LIMIT_DELAY)

            start += page_size
            if start >= total:
                break


def fetch_updates(since: str) -> Generator[Dict[str, Any], None, None]:
    """Yield documents modified since a date."""
    for doc in fetch_all():
        doc_date = doc.get("date") or ""
        if doc_date >= since:
            yield doc


def bootstrap_sample(sample_dir: Path, count: int = 15):
    """Fetch sample records for testing."""
    sample_dir.mkdir(parents=True, exist_ok=True)
    saved = 0

    for doc_type in ["acts", "revised", "subsidiary"]:
        if saved >= count:
            break
        data = fetch_metadata(doc_type, start=0, length=min(5, count - saved))
        records = data.get("data", [])
        print(f"Fetching {len(records)} {doc_type} samples...")

        for raw in records:
            if saved >= count:
                break
            doc_id = raw.get("id")
            if not doc_id:
                continue

            storage_path = raw.get("actDocPath") if doc_type == "revised" else None
            text = download_pdf_text(doc_type, doc_id, storage_path=storage_path)
            if doc_type == "acts":
                record = normalize_act(raw, text)
            elif doc_type == "subsidiary":
                record = normalize_subsidiary(raw, text)
            else:
                record = normalize_revised(raw, text)

            out_path = sample_dir / f"{record['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            text_len = len(record.get("text", ""))
            print(f"  Saved {record['_id']}: {record['title'][:60]}... ({text_len} chars)")
            saved += 1
            time.sleep(RATE_LIMIT_DELAY)

    print(f"\nTotal samples saved: {saved}")
    return saved


def main():
    parser = argparse.ArgumentParser(description="TZ/OAG-MIS - Tanzania legislation fetcher")
    parser.add_argument("command", choices=["bootstrap", "updates"],
                        help="bootstrap: fetch all; updates: fetch since date")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records only")
    parser.add_argument("--since", type=str, default=None,
                        help="Fetch updates since this date (YYYY-MM-DD)")
    parser.add_argument("--output", type=str, default=None,
                        help="Output directory")
    args = parser.parse_args()

    source_dir = Path(__file__).parent
    sample_dir = source_dir / "sample"
    output_dir = Path(args.output) if args.output else source_dir / "data"

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(sample_dir, count=15)
        else:
            output_dir.mkdir(parents=True, exist_ok=True)
            count = 0
            for record in fetch_all():
                out_path = output_dir / f"{record['_id']}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
                if count % 100 == 0:
                    print(f"  Saved {count} records...")
            print(f"Total: {count} records saved to {output_dir}")
    elif args.command == "updates":
        since = args.since or "2024-01-01"
        count = 0
        for record in fetch_updates(since):
            out_path = output_dir / f"{record['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        print(f"Updates since {since}: {count} records")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
