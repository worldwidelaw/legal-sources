#!/usr/bin/env python3
"""
LK/LawNet - Sri Lanka Legal Data (Acts + Court Judgments)

Fetches Sri Lankan legal documents from open GitHub datasets by nuuuwan:
- Acts of Parliament (1981-2026): ~2,865 acts from parliament.lk
- Supreme Court Judgments (2009-2026): ~2,641 decisions from supremecourt.lk
- Appeal Court Judgments (2012-2026): ~10,574 decisions from courtofappeal.lk

Data is CC BY 4.0 licensed. Full text extracted from official government PDFs.

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records (5 per type)
  python bootstrap.py bootstrap             # Full extraction
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

SOURCE_ID = "LK/LawNet"
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
DATA_DIR = SCRIPT_DIR / "data"

# GitHub raw content base URLs
ACTS_INDEX_URL = "https://raw.githubusercontent.com/nuuuwan/lk_acts_data/main/data/hf/acts.json"
ACTS_DATA_BASE = "https://raw.githubusercontent.com/nuuuwan/lk_acts_data/main/data/acts"

SC_INDEX_URL = "https://raw.githubusercontent.com/nuuuwan/lk_supreme_court_judgements/data/data/lk_supreme_court_judgements/summary.json"
SC_DATA_BASE = "https://raw.githubusercontent.com/nuuuwan/lk_supreme_court_judgements/data/data/lk_supreme_court_judgements"

CA_INDEX_URL = "https://raw.githubusercontent.com/nuuuwan/lk_appeal_court_judgements/data/data/lk_appeal_court_judgements/docs_all.tsv"
CA_DATA_BASE = "https://raw.githubusercontent.com/nuuuwan/lk_appeal_court_judgements/data/data/lk_appeal_court_judgements"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "LegalDataHunter/1.0 (research)"})


def get_decade(year: int) -> str:
    """Get decade folder name from year."""
    decade = (year // 10) * 10
    return f"{decade}s"


def fetch_json(url: str) -> dict:
    """Fetch JSON from a URL with retries."""
    for attempt in range(3):
        try:
            resp = SESSION.get(url, timeout=30)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt < 2:
                time.sleep(1)
            else:
                print(f"  Failed to fetch {url}: {e}")
                return None


def fetch_text(url: str) -> str:
    """Fetch text content from a URL with retries."""
    for attempt in range(3):
        try:
            resp = SESSION.get(url, timeout=30)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            if attempt < 2:
                time.sleep(1)
            else:
                print(f"  Failed to fetch {url}: {e}")
                return None


def normalize_act(act_meta: dict, full_text: str) -> dict:
    """Normalize an act record."""
    act_id = act_meta.get("act_id", "")
    title = act_meta.get("act_description") or act_meta.get("description", "")
    date = act_meta.get("act_date") or act_meta.get("date", "")
    source_url = act_meta.get("act_source_url") or act_meta.get("url_pdf_en", "")

    return {
        "_id": f"LK-ACT-{act_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": date if date else None,
        "url": source_url if source_url else "https://www.lawnet.gov.lk/",
        "language": "en",
        "act_id": act_id,
        "act_type": act_meta.get("act_type", ""),
    }


def normalize_judgment(doc_meta: dict, full_text: str, court: str) -> dict:
    """Normalize a court judgment record."""
    doc_id = doc_meta.get("doc_id", "")
    title = doc_meta.get("description", "")
    date = doc_meta.get("date_str", "")
    url = doc_meta.get("url_pdf", "") or doc_meta.get("url_metadata", "")
    court_label = "Supreme Court" if court == "sc" else "Appeal Court"

    return {
        "_id": f"LK-{court.upper()}-{doc_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": f"[{court_label}] {title}" if title else f"[{court_label}] {doc_id}",
        "text": full_text,
        "date": date if date else None,
        "url": url if url else f"https://{'supremecourt' if court == 'sc' else 'courtofappeal'}.lk/",
        "language": "en",
        "case_number": doc_meta.get("num", ""),
        "court": court_label,
        "parties": doc_meta.get("parties", ""),
        "judge": doc_meta.get("judgement_by", ""),
    }


def iter_acts(limit: int = 0):
    """Iterate over acts from the index."""
    print("Fetching acts index...")
    index = fetch_json(ACTS_INDEX_URL)
    if not index:
        print("  Could not fetch acts index")
        return

    print(f"  Found {len(index)} acts in index")
    count = 0

    for entry in index:
        act_id = entry.get("act_id", "")
        if not act_id:
            continue

        year_str = entry.get("act_year") or act_id.split("-")[0]
        try:
            year = int(year_str)
        except (ValueError, TypeError):
            continue

        decade = get_decade(year)
        text_url = f"{ACTS_DATA_BASE}/{decade}/{year}/{act_id}/en.txt"
        meta_url = f"{ACTS_DATA_BASE}/{decade}/{year}/{act_id}/metadata.json"

        # Fetch full text
        full_text = fetch_text(text_url)
        if not full_text or len(full_text.strip()) < 50:
            continue

        # Fetch metadata (use index entry as fallback)
        meta = fetch_json(meta_url)
        if meta:
            entry.update(meta)

        # Clean "==== BLOCKS ====" prefix if present
        text = full_text.strip()
        if text.startswith("==== BLOCKS ===="):
            text = text[len("==== BLOCKS ===="):].strip()

        record = normalize_act(entry, text)
        yield record
        count += 1
        time.sleep(0.2)

        if limit and count >= limit:
            break

    print(f"  Processed {count} acts")


def parse_tsv_index(tsv_text: str) -> list:
    """Parse a TSV index into list of dicts with doc_id and metadata."""
    lines = tsv_text.strip().split("\n")
    if len(lines) < 2:
        return []
    headers = lines[0].split("\t")
    results = []
    for line in lines[1:]:
        if not line.strip():
            continue
        parts = line.split("\t")
        entry = {}
        for i, h in enumerate(headers):
            entry[h.strip()] = parts[i].strip() if i < len(parts) else ""
        if entry.get("doc_id"):
            results.append(entry)
    return results


def iter_sc_judgments(limit: int = 0):
    """Iterate over Supreme Court judgments."""
    print("Fetching Supreme Court index...")
    tsv_url = f"{SC_DATA_BASE}/docs_all.tsv"
    tsv_text = fetch_text(tsv_url)
    if not tsv_text:
        print("  Could not fetch SC index")
        return

    entries = parse_tsv_index(tsv_text)
    print(f"  Found {len(entries)} SC judgment entries")
    count = 0
    failures = 0

    for entry in entries:
        doc_id = entry["doc_id"]

        # Extract year from doc_id (format: YYYY-MM-DD-...)
        try:
            year = int(doc_id[:4])
        except (ValueError, IndexError):
            continue

        decade = get_decade(year)
        text_url = f"{SC_DATA_BASE}/{decade}/{year}/{doc_id}/doc.txt"

        # Fetch full text
        full_text = fetch_text(text_url)
        if not full_text or len(full_text.strip()) < 50:
            failures += 1
            if failures > 10 and count == 0:
                print("  Too many failures without success, stopping SC")
                return
            continue

        # Clean "==== BLOCKS ====" prefix if present
        text = full_text.strip()
        if text.startswith("==== BLOCKS ===="):
            text = text[len("==== BLOCKS ===="):].strip()

        record = normalize_judgment(entry, text, "sc")
        yield record
        count += 1
        failures = 0
        time.sleep(0.2)

        if limit and count >= limit:
            break

    print(f"  Processed {count} SC judgments")


def iter_ca_judgments(limit: int = 0):
    """Iterate over Appeal Court judgments."""
    print("Fetching Appeal Court index...")
    tsv_text = fetch_text(CA_INDEX_URL)
    if not tsv_text:
        print("  Could not fetch CA index")
        return

    entries = parse_tsv_index(tsv_text)
    print(f"  Found {len(entries)} CA judgment entries")
    count = 0
    failures = 0

    for entry in entries:
        doc_id = entry["doc_id"]

        # Extract year from doc_id
        try:
            year = int(doc_id[:4])
        except (ValueError, IndexError):
            continue

        decade = get_decade(year)
        text_url = f"{CA_DATA_BASE}/{decade}/{year}/{doc_id}/doc.txt"

        # Fetch full text
        full_text = fetch_text(text_url)
        if not full_text or len(full_text.strip()) < 50:
            failures += 1
            if failures > 10 and count == 0:
                print("  Too many failures without success, stopping CA")
                return
            continue

        # Clean "==== BLOCKS ====" prefix if present
        text = full_text.strip()
        if text.startswith("==== BLOCKS ===="):
            text = text[len("==== BLOCKS ===="):].strip()

        record = normalize_judgment(entry, text, "ca")
        yield record
        count += 1
        failures = 0
        time.sleep(0.2)

        if limit and count >= limit:
            break

    print(f"  Processed {count} CA judgments")


def test_connectivity():
    """Test connectivity to all three data sources."""
    print("Testing connectivity to nuuuwan datasets...")

    for name, url in [
        ("Acts index", ACTS_INDEX_URL),
        ("SC summary", SC_INDEX_URL),
        ("CA index", CA_INDEX_URL),
    ]:
        try:
            resp = SESSION.head(url, timeout=15, allow_redirects=True)
            status = resp.status_code
            print(f"  {name}: HTTP {status} {'OK' if status == 200 else 'FAIL'}")
        except Exception as e:
            print(f"  {name}: FAILED ({e})")

    # Test fetching a sample text file
    print("\nTesting sample act text fetch...")
    text = fetch_text(f"{ACTS_DATA_BASE}/2020s/2025/2025-001/en.txt")
    if text:
        print(f"  Sample act text: {len(text)} chars, starts with: {text[:80]}...")
    else:
        print("  Could not fetch sample act text")

    print("\nConnectivity test complete")


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    all_records = []
    saved = 0

    if sample:
        # Fetch 5 of each type for sample
        iterators = [
            ("acts", iter_acts(limit=5)),
            ("sc", iter_sc_judgments(limit=5)),
            ("ca", iter_ca_judgments(limit=5)),
        ]
    else:
        iterators = [
            ("acts", iter_acts()),
            ("sc", iter_sc_judgments()),
            ("ca", iter_ca_judgments()),
        ]

    for label, iterator in iterators:
        print(f"\n--- Fetching {label} ---")
        for record in iterator:
            out_path = SAMPLE_DIR / f"record_{saved:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            all_records.append(record)
            saved += 1

    # Save combined samples
    all_path = SAMPLE_DIR / "all_samples.json"
    with open(all_path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)

    print(f"\nBootstrap complete: {saved} records saved to {SAMPLE_DIR}")

    # Validation
    text_count = sum(1 for r in all_records if r.get("text") and len(r["text"]) > 100)
    leg_count = sum(1 for r in all_records if r["_type"] == "legislation")
    case_count = sum(1 for r in all_records if r["_type"] == "case_law")

    print(f"  Legislation records: {leg_count}")
    print(f"  Case law records: {case_count}")
    print(f"  Records with substantial text: {text_count}/{saved}")

    if saved > 0 and text_count < saved * 0.5:
        print("WARNING: Less than 50% of records have substantial text")


def main():
    parser = argparse.ArgumentParser(description="LK/LawNet Sri Lanka Legal Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (15 records)")
    args = parser.parse_args()

    if args.command == "test":
        test_connectivity()
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample)


if __name__ == "__main__":
    main()
