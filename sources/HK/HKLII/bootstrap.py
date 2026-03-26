#!/usr/bin/env python3
"""
HK/HKLII -- Hong Kong Legal Information Institute

Fetches case law and legislation from HKLII's internal JSON API.
Courts: Court of Final Appeal, Court of Appeal, Court of First Instance,
        District Court, Family Court, Lands Tribunal, Competition Tribunal.
Legislation: Ordinances, Regulations, Constitutional Instruments.

API response formats (discovered via reverse engineering):
  getcasefiles -> {totalfiles, judgments: [{neutral, path, date, cases: [{title, act}]}]}
  getjudgment  -> {date, db, neutral, content (HTML), doc (URL), cases, ...}
  getlegisfiles -> {totalfiles, files: [{num, title}]}
  getcapversions -> [{id, title, date, cap}]
  getcapversiontoc -> [{id, subpath, title, content (XML/HTML), section_type, ...}]

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py bootstrap --full     # Full fetch
"""

import argparse
import html
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import requests

SOURCE_ID = "HK/HKLII"
BASE_URL = "https://www.hklii.hk/api"
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"
REQUEST_DELAY = 1.0

COURTS = [
    ("hkcfa", "Court of Final Appeal"),
    ("hkca", "Court of Appeal"),
    ("hkcfi", "Court of First Instance"),
    ("hkdc", "District Court"),
    ("hkfc", "Family Court"),
    ("hkldt", "Lands Tribunal"),
    ("hkct", "Competition Tribunal"),
]

LEGIS_TYPES = [
    ("ord", "Ordinances"),
    ("reg", "Regulations"),
    ("instrument", "Constitutional Instruments"),
]

SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
DATA_DIR = SCRIPT_DIR / "data"


class HKLIIAPI:
    """Client for HKLII's internal JSON API."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })

    def _get(self, endpoint: str, params: Optional[Dict] = None,
             retries: int = 3) -> Any:
        url = f"{BASE_URL}/{endpoint}"
        for attempt in range(retries):
            try:
                resp = self.session.get(url, params=params, timeout=60)
                if resp.status_code == 429 and attempt < retries - 1:
                    wait = 2 ** (attempt + 1)
                    print(f"  Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.RequestException as e:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                raise

    def get_case_list(self, court: str, page: int = 1,
                      items_per_page: int = 500) -> Dict:
        """Returns {totalfiles, judgments: [{neutral, path, date, cases}]}"""
        return self._get("getcasefiles", {
            "caseDb": court, "lang": "en",
            "itemsPerPage": items_per_page, "page": page,
        })

    def get_judgment(self, abbr: str, year: int, num: int) -> Dict:
        """Returns {date, db, neutral, content, doc, cases, ...}"""
        return self._get("getjudgment", {
            "lang": "en", "abbr": abbr, "year": year, "num": num,
        })

    def get_legis_list(self, cap_type: str, page: int = 1,
                       items_per_page: int = 500) -> Dict:
        """Returns {totalfiles, files: [{num, title}]}"""
        return self._get("getlegisfiles", {
            "capType": cap_type, "lang": "en",
            "itemsPerPage": items_per_page, "page": page,
        })

    def get_cap_versions(self, cap_num: str) -> List:
        """Returns [{id, title, date, cap}] sorted newest first."""
        return self._get("getcapversions", {"lang": "en", "cap": cap_num})

    def get_cap_version_toc(self, version_id: int) -> List:
        """Returns [{id, subpath, title, content, section_type, ...}]"""
        return self._get("getcapversiontoc", {"id": version_id})


def clean_html(text: str) -> str:
    """Strip HTML/XML tags and clean whitespace."""
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<p[^>]*>', '\n\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    return text.strip()


def parse_case_path(path: str):
    """Parse '/en/cases/hkcfa/2024/1' -> (abbr, year, num)."""
    m = re.search(r'/cases/(\w+)/(\d{4})/(\d+)', path)
    if m:
        return m.group(1), int(m.group(2)), int(m.group(3))
    return None, None, None


def normalize_case(listing_item: Dict, judgment: Dict,
                   court_abbr: str, court_name: str) -> Dict:
    """Normalize a case law record from listing + judgment data."""
    path = listing_item.get("path", "")
    _, year, num = parse_case_path(path)

    citation = judgment.get("neutral", "") or listing_item.get("neutral", "")

    # Title from cases array
    cases = listing_item.get("cases", []) or judgment.get("cases", [])
    title = cases[0].get("title", "") if cases else ""
    if not title:
        title = citation or f"{court_name} judgment"

    date_str = judgment.get("date", "") or listing_item.get("date", "")
    # Normalize date to ISO date only
    if date_str and "T" in date_str:
        date_str = date_str.split("T")[0]

    text = clean_html(judgment.get("content", ""))

    doc_id = citation or (f"hklii-{court_abbr}-{year}-{num}" if year else path)

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_str,
        "url": f"https://www.hklii.hk{path}" if path else "",
        "court": court_name,
        "court_abbr": court_abbr,
        "neutral_citation": citation,
    }


def normalize_legislation(cap_title: str, cap_num: str, cap_type: str,
                          sections: List[Dict]) -> Dict:
    """Normalize a legislation record from TOC sections."""
    parts = []
    for sec in sections:
        sec_title = clean_html(sec.get("title", ""))
        sec_content = clean_html(sec.get("content", ""))
        if sec_content:
            if sec_title:
                parts.append(f"{sec_title}\n{sec_content}")
            else:
                parts.append(sec_content)

    full_text = "\n\n".join(parts)

    type_label = {"ord": "Ordinance", "reg": "Regulation",
                  "instrument": "Constitutional Instrument"}.get(cap_type, cap_type)

    return {
        "_id": f"hklii-{cap_type}-cap{cap_num}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": cap_title or f"Cap. {cap_num}",
        "text": full_text,
        "date": None,
        "url": f"https://www.hklii.hk/en/legis/{cap_type}/{cap_num}",
        "cap_number": cap_num,
        "cap_type": type_label,
    }


def fetch_case_sample(api: HKLIIAPI, count_per_court: int = 3) -> List[Dict]:
    """Fetch sample case law records with full text."""
    records = []
    for abbr, name in COURTS:
        if len(records) >= 12:
            break
        print(f"  Fetching cases from {name} ({abbr})...")
        try:
            data = api.get_case_list(abbr, page=1, items_per_page=10)
        except Exception as e:
            print(f"    Error listing {abbr}: {e}")
            continue

        if not data:
            continue

        items = data.get("judgments", [])
        fetched_for_court = 0

        for item in items:
            if fetched_for_court >= count_per_court:
                break
            path = item.get("path", "")
            _, year, num = parse_case_path(path)
            if not year or not num:
                continue

            time.sleep(REQUEST_DELAY)
            try:
                judgment = api.get_judgment(abbr, year, num)
            except Exception as e:
                print(f"    Error fetching {path}: {e}")
                continue

            if not judgment or not judgment.get("content"):
                # Skip judgments without inline content
                continue

            record = normalize_case(item, judgment, abbr, name)
            if record["text"]:
                records.append(record)
                fetched_for_court += 1
                print(f"    Got: {record['_id']} ({len(record['text'])} chars)")

    return records


def fetch_legis_sample(api: HKLIIAPI, count: int = 5) -> List[Dict]:
    """Fetch sample legislation records with full text."""
    records = []
    for cap_type, type_name in LEGIS_TYPES:
        if len(records) >= count:
            break
        print(f"  Fetching {type_name}...")
        try:
            data = api.get_legis_list(cap_type, page=1, items_per_page=5)
        except Exception as e:
            print(f"    Error listing {type_name}: {e}")
            continue

        if not data:
            continue

        items = data.get("files", [])

        for item in items[:3]:
            if len(records) >= count:
                break
            cap_num = item.get("num")
            cap_title = item.get("title", "")
            if not cap_num:
                continue

            time.sleep(REQUEST_DELAY)
            try:
                versions = api.get_cap_versions(cap_num)
            except Exception as e:
                print(f"    Error getting versions for cap {cap_num}: {e}")
                continue

            if not versions or not isinstance(versions, list) or len(versions) == 0:
                continue

            version_id = versions[0].get("id")
            if not version_id:
                continue

            time.sleep(REQUEST_DELAY)
            try:
                toc = api.get_cap_version_toc(version_id)
            except Exception as e:
                print(f"    Error getting TOC for cap {cap_num}: {e}")
                continue

            if not toc or not isinstance(toc, list):
                continue

            record = normalize_legislation(cap_title, cap_num, cap_type, toc)
            if record["text"]:
                records.append(record)
                print(f"    Got: Cap. {cap_num} - {cap_title} ({len(record['text'])} chars)")

    return records


def fetch_all_cases(api: HKLIIAPI) -> Generator[Dict, None, None]:
    """Yield all case law records with full text."""
    for abbr, name in COURTS:
        print(f"\nFetching all cases from {name} ({abbr})...")
        page = 1
        fetched = 0
        while True:
            data = api.get_case_list(abbr, page=page, items_per_page=500)
            if not data:
                break
            items = data.get("judgments", [])
            if not items:
                break

            for item in items:
                path = item.get("path", "")
                _, year, num = parse_case_path(path)
                if not year or not num:
                    continue

                time.sleep(REQUEST_DELAY)
                try:
                    judgment = api.get_judgment(abbr, year, num)
                except Exception as e:
                    print(f"  Error: {path}: {e}")
                    continue

                if not judgment or not judgment.get("content"):
                    continue

                record = normalize_case(item, judgment, abbr, name)
                if record["text"]:
                    fetched += 1
                    if fetched % 100 == 0:
                        print(f"  {fetched} cases fetched...")
                    yield record

            page += 1
            time.sleep(REQUEST_DELAY)


def fetch_all_legislation(api: HKLIIAPI) -> Generator[Dict, None, None]:
    """Yield all legislation records with full text."""
    for cap_type, type_name in LEGIS_TYPES:
        print(f"\nFetching all {type_name}...")
        page = 1
        fetched = 0
        while True:
            data = api.get_legis_list(cap_type, page=page, items_per_page=500)
            if not data:
                break
            items = data.get("files", [])
            if not items:
                break

            for item in items:
                cap_num = item.get("num")
                cap_title = item.get("title", "")
                if not cap_num:
                    continue

                time.sleep(REQUEST_DELAY)
                try:
                    versions = api.get_cap_versions(cap_num)
                except Exception:
                    continue

                if not versions or not isinstance(versions, list) or not versions:
                    continue

                version_id = versions[0].get("id")
                if not version_id:
                    continue

                time.sleep(REQUEST_DELAY)
                try:
                    toc = api.get_cap_version_toc(version_id)
                except Exception:
                    continue

                if not toc or not isinstance(toc, list):
                    continue

                record = normalize_legislation(cap_title, cap_num, cap_type, toc)
                if record["text"]:
                    fetched += 1
                    if fetched % 50 == 0:
                        print(f"  {fetched} pieces fetched...")
                    yield record

            page += 1
            time.sleep(REQUEST_DELAY)


def save_records(records: List[Dict], output_dir: Path):
    """Save records as individual JSON files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for record in records:
        safe_id = re.sub(r'[^\w\-]', '_', str(record["_id"]))[:100]
        filepath = output_dir / f"{safe_id}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(records)} records to {output_dir}")


def bootstrap_sample():
    """Fetch sample records for validation."""
    print(f"=== {SOURCE_ID} Bootstrap (Sample) ===\n")
    api = HKLIIAPI()

    print("Fetching case law samples...")
    cases = fetch_case_sample(api, count_per_court=3)

    print(f"\nFetching legislation samples...")
    legis = fetch_legis_sample(api, count=5)

    all_records = cases + legis
    if all_records:
        save_records(all_records, SAMPLE_DIR)

    print(f"\n=== Validation ===")
    print(f"Total records: {len(all_records)}")
    print(f"  Case law: {len(cases)}")
    print(f"  Legislation: {len(legis)}")

    with_text = sum(1 for r in all_records if r.get("text"))
    print(f"  With full text: {with_text}/{len(all_records)}")

    if all_records:
        avg_len = sum(len(r.get("text", "")) for r in all_records) // len(all_records)
        print(f"  Average text length: {avg_len} chars")

    if with_text >= 10:
        print("\nSample validation PASSED")
    else:
        print(f"\nWARNING: Only {with_text} records with text (need 10+)")

    return all_records


def bootstrap_full():
    """Full fetch of all records."""
    print(f"=== {SOURCE_ID} Bootstrap (Full) ===\n")
    api = HKLIIAPI()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    count = 0

    for record in fetch_all_cases(api):
        safe_id = re.sub(r'[^\w\-]', '_', str(record["_id"]))[:100]
        with open(DATA_DIR / f"{safe_id}.json", "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1

    for record in fetch_all_legislation(api):
        safe_id = re.sub(r'[^\w\-]', '_', str(record["_id"]))[:100]
        with open(DATA_DIR / f"{safe_id}.json", "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1

    print(f"\nTotal: {count} records saved to {DATA_DIR}")


def main():
    parser = argparse.ArgumentParser(description="HK/HKLII Legal Data Fetcher")
    subparsers = parser.add_subparsers(dest="command")

    boot = subparsers.add_parser("bootstrap", help="Fetch data")
    boot.add_argument("--sample", action="store_true", help="Sample mode")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample()
        elif args.full:
            bootstrap_full()
        else:
            parser.print_help()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
