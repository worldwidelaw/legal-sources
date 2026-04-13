#!/usr/bin/env python3
"""
US/OCC -- Office of the Comptroller of the Currency Enforcement Actions

Fetches OCC enforcement actions via JSON export API + PDF full text.
~6,000 enforcement actions (C&D orders, CMPs, formal agreements, etc.)

Data access:
  - JSON index: https://apps.occ.gov/EASearch/Search/ExportToJSON
  - PDFs: https://www.occ.treas.gov/static/enforcement-actions/ea{doc}.pdf

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental (newest first)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import tempfile
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, Set

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.OCC")

JSON_URL = "https://apps.occ.gov/EASearch/Search/ExportToJSON"
PDF_BASE = "https://www.occ.treas.gov/static/enforcement-actions/ea"
DELAY = 2.0


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/html,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return session


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r'\r\n', '\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="US/OCC",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""

def fetch_pdf_text(session: requests.Session, doc_number: str) -> str:
    url = f"{PDF_BASE}{doc_number}.pdf"
    try:
        resp = session.get(url, timeout=60)
        if resp.status_code == 404:
            logger.debug(f"PDF not found: {url}")
            return ""
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "")
        if "pdf" not in content_type and "octet" not in content_type:
            logger.debug(f"Not a PDF response for {doc_number}: {content_type}")
            return ""
        return extract_pdf_text(resp.content)
    except requests.RequestException as e:
        logger.warning(f"Failed to fetch PDF {doc_number}: {e}")
        return ""


def fetch_index(session: requests.Session) -> list:
    logger.info(f"Fetching enforcement actions index from {JSON_URL}")
    resp = session.get(JSON_URL, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    logger.info(f"Index contains {len(data)} entries")
    return data


def parse_date(date_str: str) -> Optional[str]:
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%m/%d/%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def build_entity_name(entry: Dict[str, Any]) -> str:
    parts = []
    if entry.get("Institution"):
        parts.append(entry["Institution"])
    if entry.get("Company"):
        parts.append(entry["Company"])
    if entry.get("Individual"):
        parts.append(entry["Individual"])
    return "; ".join(parts) if parts else "Unknown"


def normalize(raw: Dict[str, Any]) -> Dict[str, Any]:
    doc_num = raw.get("doc_number", "")
    docket = raw.get("DocketNumber", "")
    _id = f"US-OCC-{doc_num}" if doc_num else f"US-OCC-docket-{docket}"

    entity = build_entity_name(raw)
    action_type = raw.get("TypeDescription", raw.get("TypeCode", ""))
    title = f"OCC {action_type}: {entity}"
    if len(title) > 200:
        title = title[:197] + "..."

    return {
        "_id": _id,
        "_source": "US/OCC",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": raw.get("text", ""),
        "date": parse_date(raw.get("StartDate", "")),
        "termination_date": parse_date(raw.get("TerminationDate", "")),
        "url": f"{PDF_BASE}{doc_num}.pdf" if doc_num else "",
        "action_type": action_type,
        "type_code": raw.get("TypeCode", ""),
        "docket_number": docket,
        "entity_name": entity,
        "institution": raw.get("Institution", ""),
        "company": raw.get("Company", ""),
        "individual": raw.get("Individual", ""),
        "location": raw.get("Location", ""),
        "amount": raw.get("Amount", ""),
        "charter_number": raw.get("CharterNumber", ""),
        "subject_matters": raw.get("SubjectMatters", []),
    }


def fetch_all(sample: bool = False) -> Generator[Dict[str, Any], None, None]:
    session = get_session()
    index = fetch_index(session)

    # Sort by date descending (newest first)
    index.sort(key=lambda x: x.get("StartDate", ""), reverse=True)

    # Deduplicate by document number — each PDF is one enforcement action
    seen_docs: Set[str] = set()
    total_yielded = 0
    max_records = 15 if sample else len(index)

    for entry in index:
        docs = entry.get("StartDocuments", [])
        if not docs:
            continue

        for doc_number in docs:
            if doc_number in seen_docs:
                continue
            seen_docs.add(doc_number)

            time.sleep(DELAY)
            logger.info(f"Fetching PDF for {doc_number}")
            text = fetch_pdf_text(session, doc_number)
            if not text or len(text) < 100:
                logger.warning(f"  Insufficient text for {doc_number} ({len(text)} chars)")
                continue

            entry_copy = dict(entry)
            entry_copy["doc_number"] = doc_number
            entry_copy["text"] = text
            record = normalize(entry_copy)
            yield record
            total_yielded += 1
            logger.info(f"  Record {total_yielded}: {doc_number} ({len(text)} chars)")

            if total_yielded >= max_records:
                logger.info(f"{'Sample' if sample else 'Full'} complete: {total_yielded} records")
                return

    logger.info(f"Total records: {total_yielded}")


def fetch_updates(since: str) -> Generator[Dict[str, Any], None, None]:
    since_date = datetime.fromisoformat(since).date()
    for record in fetch_all():
        if record.get("date"):
            try:
                rec_date = datetime.fromisoformat(record["date"]).date()
                if rec_date < since_date:
                    return
            except ValueError:
                pass
        yield record


def test_connectivity() -> bool:
    session = get_session()
    try:
        resp = session.get(JSON_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        logger.info(f"JSON index OK: {len(data)} entries")

        if data:
            docs = data[0].get("StartDocuments", [])
            if docs:
                text = fetch_pdf_text(session, docs[0])
                logger.info(f"PDF test: {len(text)} chars extracted")
                return len(text) > 0
        return True
    except Exception as e:
        logger.error(f"Failed: {e}")
        return False


def main():
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        ok = test_connectivity()
        sys.exit(0 if ok else 1)

    if command == "bootstrap":
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in fetch_all(sample=sample):
            fname = re.sub(r'[^\w\-]', '_', record["_id"])[:80] + ".json"
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            print(f"  [{count}] {record['_id']} — {record['title'][:60]}")
        print(f"\nDone: {count} records saved to {sample_dir}/")

    elif command == "update":
        since = sys.argv[2] if len(sys.argv) > 2 else "2024-01-01"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in fetch_updates(since):
            fname = re.sub(r'[^\w\-]', '_', record["_id"])[:80] + ".json"
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        print(f"Updated: {count} records since {since}")


if __name__ == "__main__":
    main()
