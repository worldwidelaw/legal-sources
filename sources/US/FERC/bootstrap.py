#!/usr/bin/env python3
"""
US/FERC -- Federal Energy Regulatory Commission Orders

Fetches FERC regulatory documents (rules, proposed rules, notices) from the
Federal Register API. Full text is retrieved from the Federal Register's
plain-text endpoint for each document.

Coverage: ~10,000+ FERC documents from 1994 to present.
Document types: RULE (final orders), PRORULE (NPRMs), NOTICE (public notices)

Data access:
  - Federal Register API v1 (no auth required)
  - Search by agency + document type
  - Full text via /documents/full_text/text/ endpoint

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental (recent docs)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urlencode

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.FERC")

API_BASE = "https://www.federalregister.gov/api/v1"
DELAY = 1.0
PER_PAGE = 100
DOC_TYPES = ["RULE", "PRORULE", "NOTICE"]

FIELDS = [
    "title", "document_number", "publication_date", "type",
    "abstract", "html_url", "raw_text_url", "docket_ids",
    "citation", "action", "agencies",
]


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "LegalDataHunter/1.0 (legal research; https://github.com/ZachLaik/LegalDataHunter)",
        "Accept": "application/json",
    })
    return session


def clean_text(raw: str) -> str:
    """Strip HTML tags and clean Federal Register text."""
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', raw)
    # Decode common HTML entities
    text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
    text = text.replace('&quot;', '"').replace('&#39;', "'")
    text = text.replace('&nbsp;', ' ')
    # Remove [[Page XXXXX]] markers
    text = re.sub(r'\[\[Page \d+\]\]', '', text)
    # Collapse excessive whitespace but preserve paragraph breaks
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def fetch_full_text(session: requests.Session, raw_text_url: str) -> Optional[str]:
    """Download full text from the Federal Register text endpoint."""
    if not raw_text_url:
        return None
    try:
        resp = session.get(raw_text_url, timeout=60)
        resp.raise_for_status()
        text = clean_text(resp.text)
        if len(text) > 100:
            return text
        return None
    except Exception as e:
        logger.warning(f"Full text fetch failed for {raw_text_url}: {e}")
        return None


def search_documents(session: requests.Session, doc_type: str,
                     page: int = 1, per_page: int = PER_PAGE,
                     date_start: str = None, date_end: str = None) -> Optional[Dict]:
    """Search Federal Register for FERC documents of a given type."""
    params = {
        "conditions[agencies][]": "federal-energy-regulatory-commission",
        "conditions[type][]": doc_type,
        "per_page": per_page,
        "page": page,
        "order": "newest",
    }
    for field in FIELDS:
        params[f"fields[]"] = field  # Will be overwritten -- use list below

    # Build URL manually to support repeated keys
    parts = [
        ("conditions[agencies][]", "federal-energy-regulatory-commission"),
        ("conditions[type][]", doc_type),
        ("per_page", str(per_page)),
        ("page", str(page)),
        ("order", "newest"),
    ]
    if date_start:
        parts.append(("conditions[publication_date][gte]", date_start))
    if date_end:
        parts.append(("conditions[publication_date][lte]", date_end))
    for field in FIELDS:
        parts.append(("fields[]", field))

    url = f"{API_BASE}/documents.json?" + urlencode(parts)

    try:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"Search failed (type={doc_type}, page={page}): {e}")
        return None


def normalize(doc: Dict, full_text: str) -> Dict[str, Any]:
    """Normalize a Federal Register document into standard schema."""
    doc_number = doc.get("document_number", "")
    title = doc.get("title", "")
    pub_date = doc.get("publication_date")  # Already YYYY-MM-DD
    doc_type = doc.get("type", "")
    abstract = doc.get("abstract", "") or ""
    docket_ids = doc.get("docket_ids", []) or []
    citation = doc.get("citation", "")
    action = doc.get("action", "")

    # Build text: full text first, then abstract as fallback context
    text_parts = []
    if full_text:
        text_parts.append(full_text)
    elif abstract:
        text_parts.append(abstract)

    text = "\n\n".join(text_parts)

    return {
        "_id": f"US-FERC-{doc_number}",
        "_source": "US/FERC",
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": pub_date,
        "url": doc.get("html_url", ""),
        "document_number": doc_number,
        "document_type": doc_type,
        "action": action,
        "citation": citation,
        "docket_ids": docket_ids,
        "abstract": abstract,
    }


def fetch_all(sample: bool = False) -> Generator[Dict[str, Any], None, None]:
    """Fetch all FERC documents from the Federal Register API."""
    session = get_session()
    total_yielded = 0
    sample_limit = 15

    for doc_type in DOC_TYPES:
        logger.info(f"Fetching {doc_type} documents...")
        page = 1

        while True:
            time.sleep(DELAY)
            data = search_documents(session, doc_type, page=page)
            if not data:
                break

            results = data.get("results", [])
            if not results:
                break

            total_count = data.get("count", 0)
            total_pages = data.get("total_pages", 0)
            logger.info(f"  {doc_type} page {page}/{total_pages} ({total_count} total)")

            for doc in results:
                raw_text_url = doc.get("raw_text_url")

                time.sleep(DELAY)
                full_text = fetch_full_text(session, raw_text_url)

                record = normalize(doc, full_text or "")

                # Skip if text is too short
                if len(record.get("text", "")) < 200:
                    logger.warning(f"  Skipping {doc.get('document_number')}: text too short")
                    continue

                yield record
                total_yielded += 1
                logger.info(f"  Record {total_yielded}: {record['document_number']} "
                            f"({len(record['text'])} chars)")

                if sample and total_yielded >= sample_limit:
                    logger.info(f"Sample complete: {total_yielded} records")
                    return

            page += 1

            # Federal Register API caps at ~200 pages
            if page > total_pages:
                break

        if sample and total_yielded >= sample_limit:
            return

    logger.info(f"Total records: {total_yielded}")


def fetch_updates(since: str) -> Generator[Dict[str, Any], None, None]:
    """Fetch documents published since a given date."""
    session = get_session()
    total_yielded = 0
    today = datetime.now().strftime("%Y-%m-%d")

    for doc_type in DOC_TYPES:
        logger.info(f"Fetching {doc_type} updates since {since}...")
        page = 1

        while True:
            time.sleep(DELAY)
            data = search_documents(session, doc_type, page=page,
                                    date_start=since, date_end=today)
            if not data:
                break

            results = data.get("results", [])
            if not results:
                break

            for doc in results:
                time.sleep(DELAY)
                full_text = fetch_full_text(session, doc.get("raw_text_url"))
                record = normalize(doc, full_text or "")
                if len(record.get("text", "")) < 200:
                    continue
                yield record
                total_yielded += 1

            page += 1
            if page > data.get("total_pages", 0):
                break

    logger.info(f"Update complete: {total_yielded} records since {since}")


def test_connectivity() -> bool:
    """Quick connectivity test."""
    session = get_session()
    try:
        data = search_documents(session, "RULE", page=1, per_page=1)
        if not data:
            logger.error("Search returned no data")
            return False

        count = data.get("count", 0)
        results = data.get("results", [])
        logger.info(f"Search OK: {count} FCC RULE documents available")

        if results:
            doc = results[0]
            raw_url = doc.get("raw_text_url")
            logger.info(f"First doc: {doc.get('title', '')[:80]}")

            time.sleep(1)
            text = fetch_full_text(session, raw_url)
            if text:
                logger.info(f"Full text OK: {len(text)} chars")
            else:
                logger.warning("Full text retrieval failed")
                return False

        return True
    except Exception as e:
        logger.error(f"Connectivity test failed: {e}")
        return False


# ── CLI ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "test"
    sample = "--sample" in sys.argv

    if cmd == "test":
        ok = test_connectivity()
        sys.exit(0 if ok else 1)

    elif cmd == "bootstrap":
        out_dir = Path(__file__).parent / "sample"
        out_dir.mkdir(exist_ok=True)
        count = 0
        for record in fetch_all(sample=sample):
            fname = out_dir / f"{record['_id']}.json"
            fname.write_text(json.dumps(record, ensure_ascii=False, indent=2))
            count += 1
            logger.info(f"Saved {fname.name}")
        logger.info(f"Bootstrap complete: {count} records in {out_dir}")

    elif cmd == "update":
        since = sys.argv[2] if len(sys.argv) > 2 else "2024-01-01"
        out_dir = Path(__file__).parent / "sample"
        out_dir.mkdir(exist_ok=True)
        count = 0
        for record in fetch_updates(since):
            fname = out_dir / f"{record['_id']}.json"
            fname.write_text(json.dumps(record, ensure_ascii=False, indent=2))
            count += 1
        logger.info(f"Update complete: {count} records since {since}")

    else:
        print(f"Usage: {sys.argv[0]} [test|bootstrap|update] [--sample]")
        sys.exit(1)
