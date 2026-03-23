#!/usr/bin/env python3
"""
CY/CYLAW Bootstrap - Cyprus Legislation Database

Fetches legislation from CyLaw (cylaw.org) via their XML export.
The export contains consolidated Cypriot legislation in structured XML format.

Data source: http://www.cylaw.org/nomoi/enop/backup/cybarlegis/zips/export/
License: Open Government Data
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional
import xml.etree.ElementTree as ET
import html

import requests
import yaml


# Configuration
BASE_URL = "http://www.cylaw.org/nomoi/enop/backup/cybarlegis/zips/export/full20240628-0951/db/cybar/legislation/"
CONTENTS_URL = f"{BASE_URL}__contents__.xml"
SOURCE_ID = "CY/CYLAW"
RATE_LIMIT = 1.0  # seconds between requests
TIMEOUT = 30


def load_config() -> dict:
    """Load configuration from config.yaml."""
    config_path = Path(__file__).parent / "config.yaml"
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    return {}


def get_session() -> requests.Session:
    """Create a requests session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "LegalDataHunter/1.0 (EU Legal Data Collection; +https://github.com/)",
        "Accept": "application/xml, text/xml, */*",
        "Accept-Language": "en-US,en;q=0.9,el;q=0.8",
    })
    return session


def fetch_document_list(session: requests.Session) -> list[dict]:
    """Fetch list of all documents from the contents XML."""
    print(f"Fetching document index from {CONTENTS_URL}")

    try:
        resp = session.get(CONTENTS_URL, timeout=TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching contents: {e}")
        return []

    # Parse XML
    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as e:
        print(f"Error parsing contents XML: {e}")
        return []

    documents = []

    # Handle namespace (eXist-db uses a namespace)
    ns = {"exist": "http://exist.sourceforge.net/NS/exist"}

    # Find all resource elements with namespace
    for resource in root.findall(".//exist:resource", ns):
        name = resource.get("name", "")
        if name.endswith(".xml") and name != "__contents__.xml":
            doc_id = name.replace(".xml", "")
            created = resource.get("created", "")
            modified = resource.get("modified", "")
            documents.append({
                "doc_id": doc_id,
                "filename": name,
                "url": f"{BASE_URL}{name}",
                "created": created,
                "modified": modified,
            })

    # Also try without namespace (fallback)
    if not documents:
        for resource in root.findall(".//resource"):
            name = resource.get("name", "")
            if name.endswith(".xml") and name != "__contents__.xml":
                doc_id = name.replace(".xml", "")
                created = resource.get("created", "")
                modified = resource.get("modified", "")
                documents.append({
                    "doc_id": doc_id,
                    "filename": name,
                    "url": f"{BASE_URL}{name}",
                    "created": created,
                    "modified": modified,
                })

    print(f"Found {len(documents)} legislation documents")
    return documents


def extract_text_content(element: ET.Element) -> str:
    """Recursively extract text content from an XML element, stripping HTML tags."""
    if element is None:
        return ""

    # Get direct text
    text_parts = []
    if element.text:
        text_parts.append(element.text)

    # Process children
    for child in element:
        # Handle specific elements
        tag = child.tag.lower() if child.tag else ""

        if tag in ("p", "text", "header", "enum"):
            child_text = extract_text_content(child)
            if child_text:
                text_parts.append(child_text)
        elif tag in ("division", "section", "subsection", "paragraph"):
            child_text = extract_text_content(child)
            if child_text:
                text_parts.append("\n\n" + child_text)
        elif tag == "table":
            # Skip tables or add placeholder
            text_parts.append("\n[Table content]\n")
        else:
            child_text = extract_text_content(child)
            if child_text:
                text_parts.append(child_text)

        # Add tail text
        if child.tail:
            text_parts.append(child.tail)

    result = " ".join(text_parts)
    # Clean up HTML entities and extra whitespace
    result = html.unescape(result)
    result = re.sub(r'\s+', ' ', result)
    result = re.sub(r'\n\s*\n', '\n\n', result)
    return result.strip()


def parse_legislation_xml(content: bytes, doc_id: str, url: str) -> Optional[dict]:
    """Parse a legislation XML document and extract relevant fields."""
    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        print(f"  Error parsing XML for {doc_id}: {e}")
        return None

    # Get attributes from root
    lang = root.get("lang", "EL")
    enforced_date = root.get("enforced-date", "")
    instrument_type = root.get("instrument-type", "")

    # Extract form/metadata
    form = root.find(".//form")
    legis_type = ""
    legis_num = ""
    official_title = ""
    short_title = ""

    if form is not None:
        legis_type_el = form.find("legis-type")
        if legis_type_el is not None and legis_type_el.text:
            legis_type = legis_type_el.text.strip()

        legis_num_el = form.find("legis-num")
        if legis_num_el is not None and legis_num_el.text:
            legis_num = legis_num_el.text.strip()

        official_title_el = form.find("official-title")
        if official_title_el is not None:
            official_title = extract_text_content(official_title_el)

        short_title_el = form.find("short-title")
        if short_title_el is not None:
            short_title = extract_text_content(short_title_el)

    # Extract body text
    body = root.find(".//legis-body")
    full_text = ""
    if body is not None:
        full_text = extract_text_content(body)

    # Also check for appendix
    appendix = root.find(".//appendix")
    if appendix is not None:
        appendix_text = extract_text_content(appendix)
        if appendix_text:
            full_text += "\n\n[Appendix]\n" + appendix_text

    # Use official title or short title as the main title
    title = official_title or short_title or f"Law {legis_num}" if legis_num else f"Document {doc_id}"

    # Parse date from enforced_date or legis_num
    date = None
    if enforced_date:
        try:
            date = enforced_date[:10]  # YYYY-MM-DD format
        except:
            pass

    if not date and legis_num:
        # Try to extract year from legis_num like "103(I)/2002"
        year_match = re.search(r'/(\d{4})$', legis_num)
        if year_match:
            date = f"{year_match.group(1)}-01-01"

    return {
        "doc_id": doc_id,
        "legis_type": legis_type,
        "legis_num": legis_num,
        "official_title": official_title,
        "short_title": short_title,
        "instrument_type": instrument_type,
        "language": lang,
        "enforced_date": enforced_date,
        "full_text": full_text,
        "title": title,
        "date": date,
        "url": url,
    }


def normalize(raw: dict) -> dict:
    """Transform raw data into standard schema."""
    doc_id = raw.get("doc_id", "")

    return {
        "_id": f"{SOURCE_ID}/{doc_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw.get("title", ""),
        "text": raw.get("full_text", ""),
        "date": raw.get("date"),
        "url": raw.get("url", ""),
        "doc_id": doc_id,
        "legis_num": raw.get("legis_num", ""),
        "legis_type": raw.get("legis_type", ""),
        "official_title": raw.get("official_title", ""),
        "short_title": raw.get("short_title", ""),
        "instrument_type": raw.get("instrument_type", ""),
        "language": raw.get("language", "EL"),
        "enforced_date": raw.get("enforced_date", ""),
        "_raw": raw,
    }


def fetch_all(session: requests.Session, max_docs: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch all legislation documents."""
    documents = fetch_document_list(session)

    if not documents:
        print("No documents found")
        return

    if max_docs:
        documents = documents[:max_docs]

    for i, doc in enumerate(documents):
        doc_id = doc["doc_id"]
        url = doc["url"]

        print(f"[{i+1}/{len(documents)}] Fetching {doc_id}")

        try:
            resp = session.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  Error fetching {doc_id}: {e}")
            time.sleep(RATE_LIMIT)
            continue

        raw = parse_legislation_xml(resp.content, doc_id, url)
        if raw and raw.get("full_text"):
            record = normalize(raw)
            yield record
        else:
            print(f"  Skipping {doc_id}: no text content")

        time.sleep(RATE_LIMIT)


def fetch_updates(session: requests.Session, since: str) -> Generator[dict, None, None]:
    """Fetch documents modified since a given date."""
    documents = fetch_document_list(session)

    if not documents:
        print("No documents found")
        return

    # Filter by modified date
    since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
    filtered = []

    for doc in documents:
        modified = doc.get("modified", "")
        if modified:
            try:
                mod_dt = datetime.fromisoformat(modified.replace("Z", "+00:00"))
                if mod_dt >= since_dt:
                    filtered.append(doc)
            except:
                pass

    print(f"Found {len(filtered)} documents modified since {since}")

    for i, doc in enumerate(filtered):
        doc_id = doc["doc_id"]
        url = doc["url"]

        print(f"[{i+1}/{len(filtered)}] Fetching {doc_id}")

        try:
            resp = session.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  Error fetching {doc_id}: {e}")
            time.sleep(RATE_LIMIT)
            continue

        raw = parse_legislation_xml(resp.content, doc_id, url)
        if raw and raw.get("full_text"):
            record = normalize(raw)
            yield record
        else:
            print(f"  Skipping {doc_id}: no text content")

        time.sleep(RATE_LIMIT)


def save_sample(records: list[dict], sample_dir: Path) -> None:
    """Save sample records to the sample directory."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    # Save individual records
    for i, record in enumerate(records):
        record_path = sample_dir / f"record_{i:04d}.json"
        with open(record_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    # Save all samples in one file
    all_samples_path = sample_dir / "all_samples.json"
    with open(all_samples_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(records)} sample records to {sample_dir}")


def update_status(status_path: Path, stats: dict) -> None:
    """Update the status.yaml file with run statistics."""
    status = {}
    if status_path.exists():
        with open(status_path, "r", encoding="utf-8") as f:
            status = yaml.safe_load(f) or {}

    # Update with new run
    run_history = status.get("run_history", [])
    run_history.append(stats)

    # Keep only last 10 runs
    status["run_history"] = run_history[-10:]
    status["last_run"] = stats.get("finished_at")
    status["last_bootstrap"] = stats.get("finished_at") if stats.get("sample_records_saved", 0) > 0 else status.get("last_bootstrap")
    status["total_records"] = stats.get("records_fetched", 0)

    if stats.get("error_message"):
        status["last_error"] = stats["error_message"]

    with open(status_path, "w", encoding="utf-8") as f:
        yaml.dump(status, f, default_flow_style=False)


def main():
    parser = argparse.ArgumentParser(description="CY/CYLAW Bootstrap - Cyprus Legislation")
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records only (12 documents)")
    parser.add_argument("--since", type=str,
                        help="Fetch updates since date (ISO 8601)")
    parser.add_argument("--max", type=int,
                        help="Maximum number of documents to fetch")

    args = parser.parse_args()

    source_dir = Path(__file__).parent
    sample_dir = source_dir / "sample"
    status_path = source_dir / "status.yaml"

    session = get_session()
    stats = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "records_fetched": 0,
        "records_new": 0,
        "records_updated": 0,
        "records_skipped": 0,
        "sample_records_saved": 0,
        "errors": 0,
        "error_message": None,
    }

    try:
        if args.command == "bootstrap":
            max_docs = 12 if args.sample else (args.max or 100)
            print(f"Bootstrap mode: fetching up to {max_docs} documents")

            records = []
            for record in fetch_all(session, max_docs=max_docs):
                records.append(record)
                stats["records_fetched"] += 1

                if args.sample and len(records) >= 12:
                    break

            if records:
                save_sample(records, sample_dir)
                stats["sample_records_saved"] = len(records)

                # Print summary
                text_lengths = [len(r.get("text", "")) for r in records]
                avg_text = sum(text_lengths) // len(text_lengths) if text_lengths else 0
                print(f"\nBootstrap complete:")
                print(f"  Records fetched: {len(records)}")
                print(f"  Average text length: {avg_text} chars")
                print(f"  Sample saved to: {sample_dir}")

        elif args.command == "fetch":
            max_docs = args.max
            print(f"Fetch mode: fetching {'all' if not max_docs else max_docs} documents")

            for record in fetch_all(session, max_docs=max_docs):
                stats["records_fetched"] += 1
                # In production, would save to database here
                print(f"  Fetched: {record['_id']}")

        elif args.command == "updates":
            if not args.since:
                print("Error: --since is required for updates command")
                sys.exit(1)

            print(f"Updates mode: fetching documents modified since {args.since}")

            for record in fetch_updates(session, args.since):
                stats["records_fetched"] += 1
                stats["records_updated"] += 1
                print(f"  Updated: {record['_id']}")

    except Exception as e:
        stats["error_message"] = str(e)
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        stats["finished_at"] = datetime.now(timezone.utc).isoformat()
        update_status(status_path, stats)


if __name__ == "__main__":
    main()
