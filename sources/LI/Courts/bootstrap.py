#!/usr/bin/env python3
"""
LI/Courts - Liechtenstein Courts Decisions Fetcher

Fetches court decisions from all Liechtenstein courts via gerichtsentscheidungen.li.
Uses the getAkten JSON API for enumeration and the detail page for full text extraction.

Courts covered: StGH, VGH, OGH, OG.

Data source: https://www.gerichtsentscheidungen.li/
License: Open Government Data
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.gerichtsentscheidungen.li"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "LI/Courts"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LegalDataHunter/1.0",
}
AJAX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LegalDataHunter/1.0",
    "Content-Type": "application/json",
}
REQUEST_DELAY = 1.5

COURTS = {
    "StGH": "Staatsgerichtshof (Constitutional Court)",
    "VGH": "Verwaltungsgerichtshof (Administrative Court)",
    "OGH": "Oberster Gerichtshof (Supreme Court)",
    "OG": "Obergericht (Higher Court)",
}

MIN_YEAR = 1997
MAX_YEAR = datetime.now().year


def get_akten(prefix: str) -> list[dict]:
    """Call getAkten API to enumerate decisions matching a prefix.

    Returns list of {case_number, listing_url} dicts.
    """
    url = f"{BASE_URL}/methods.aspx/getAkten"
    try:
        resp = requests.post(
            url,
            json={"s": prefix},
            headers=AJAX_HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        results = []
        for item in data.get("d", []):
            if isinstance(item, list) and len(item) >= 2:
                case_num = item[0]
                listing_url = f"{BASE_URL}/{item[1]}"
                results.append({"case_number": case_num, "listing_url": listing_url})
        return results
    except Exception as e:
        print(f"  getAkten error for '{prefix}': {e}")
        return []


def enumerate_all_decisions() -> list[dict]:
    """Enumerate all decisions across all courts using getAkten."""
    all_decisions = []
    seen = set()

    for court_prefix in COURTS:
        print(f"  Enumerating {court_prefix}...")

        if court_prefix in ("OGH", "OG"):
            # OGH and OG can exceed 500-result cap, enumerate by year
            for year in range(MAX_YEAR, MIN_YEAR - 1, -1):
                query = f"{court_prefix}.{year}"
                results = get_akten(query)
                for r in results:
                    if r["case_number"] not in seen:
                        seen.add(r["case_number"])
                        all_decisions.append(r)
                time.sleep(0.5)
            count = sum(1 for d in all_decisions if d["case_number"].startswith(court_prefix))
            print(f"    {court_prefix} total: {count}")
        else:
            results = get_akten(court_prefix)
            for r in results:
                if r["case_number"] not in seen:
                    seen.add(r["case_number"])
                    all_decisions.append(r)
            print(f"    {court_prefix}: {len(results)} decisions")

        time.sleep(REQUEST_DELAY)

    print(f"  Total enumerated: {len(all_decisions)}")
    return all_decisions


def extract_detail_url(listing_html: str) -> Optional[str]:
    """Extract the detail page URL from a listing page's hit row onclick."""
    match = re.search(
        r"class=\"pointer hit[^\"]*\"[^>]*>\s*"
        r"<td class=\"hitCol1",
        listing_html,
    )
    # Look for onclick with detail URL in hit rows
    hit_match = re.search(
        r'<tr[^>]*onclick="window\.location=\'(default\.aspx\?z=[^\']+)\'[^>]*class="pointer hit',
        listing_html,
    )
    if hit_match:
        return f"{BASE_URL}/{hit_match.group(1)}"

    # Alternative: any hit row onclick
    hit_match2 = re.search(
        r'<tr id="\d+"[^>]*onclick="window\.location=\'(default\.aspx\?z=[^\']+)\'',
        listing_html,
    )
    if hit_match2:
        return f"{BASE_URL}/{hit_match2.group(1)}"

    return None


def extract_metadata_from_detail(html: str) -> dict:
    """Extract metadata from a decision detail page."""
    soup = BeautifulSoup(html, "html.parser")

    case_number = None
    aktz = soup.find("div", class_="aktenzeichen")
    if aktz:
        case_number = aktz.get_text(strip=True)
    if not case_number:
        match = re.search(r"((?:StGH|VGH|OGH|OG|LG|KG|HG)\s*[\d./]+)", html)
        if match:
            case_number = match.group(1).strip()

    date = None
    date_match = re.search(r"<div class='fL hEIItem'>(\d{1,2}\.\d{1,2}\.\d{4})</div>", html)
    if date_match:
        parts = date_match.group(1).split(".")
        if len(parts) == 3:
            date = f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"

    court = None
    court_match = re.search(
        r"<div class='fL hEIItem'>(StGH|VGH|OGH|OG|LG|KG|HG|RPG|CO|CG)</div>", html
    )
    if court_match:
        court = court_match.group(1)

    decision_type = None
    type_match = re.search(
        r"<div class='fL hEIItem'>(Urteil|Beschluss|Entscheidung|Gutachten)</div>", html
    )
    if type_match:
        decision_type = type_match.group(1)

    return {
        "case_number": case_number,
        "date": date,
        "court": court,
        "decision_type": decision_type,
    }


def extract_full_text(html: str) -> Optional[str]:
    """Extract full text from the eintrag div on a decision detail page."""
    soup = BeautifulSoup(html, "html.parser")
    content = soup.find("div", class_="eintrag")
    if not content:
        # Fallback: try scrolldiv
        content = soup.find("div", id="scrolldiv")
    if not content:
        return None

    for tag in content(["script", "style"]):
        tag.decompose()
    text = content.get_text(separator="\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r" {2,}", " ", text)
    return text.strip()


def normalize(raw: dict) -> dict:
    """Normalize a raw record into the standard schema."""
    court_names = {
        "StGH": "Staatsgerichtshof (Constitutional Court)",
        "VGH": "Verwaltungsgerichtshof (Administrative Court)",
        "OGH": "Oberster Gerichtshof (Supreme Court)",
        "OG": "Obergericht (Higher Court)",
        "LG": "Landgericht (District Court)",
        "KG": "Kriminalgericht (Criminal Court)",
        "HG": "Handelsgericht (Commercial Court)",
        "RPG": "Rechtspflegegericht",
        "CO": "Court Other",
        "CG": "Civil Court",
    }

    case_number = raw.get("case_number", "")
    court_code = raw.get("court", "")
    decision_type = raw.get("decision_type", "")

    title_parts = [p for p in [case_number, decision_type] if p]
    title = " - ".join(title_parts) if title_parts else "Court Decision"

    return {
        "_id": case_number or "unknown",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.utcnow().isoformat() + "Z",
        "title": title,
        "text": raw["text"],
        "date": raw.get("date"),
        "url": raw.get("detail_url", ""),
        "language": "deu",
        "court": court_names.get(court_code, court_code or "Unknown"),
        "case_number": case_number,
        "decision_type": decision_type,
        "country": "LI",
    }


def fetch_all(max_records: int = None) -> Generator[dict, None, None]:
    """Fetch all court decisions with full text."""
    print("Enumerating decisions via getAkten API...")
    decisions = enumerate_all_decisions()

    if max_records:
        decisions = decisions[:max_records * 3]

    count = 0
    for i, dec in enumerate(decisions):
        if max_records and count >= max_records:
            break

        case_num = dec["case_number"]
        listing_url = dec["listing_url"]
        print(f"  [{i+1}/{len(decisions)}] {case_num}...")

        try:
            # Step 1: Fetch the listing page to get the detail URL
            resp = requests.get(listing_url, headers=HEADERS, timeout=30)
            resp.raise_for_status()

            detail_url = extract_detail_url(resp.text)
            if not detail_url:
                print(f"    No detail URL found, skipping")
                time.sleep(REQUEST_DELAY)
                continue

            time.sleep(1.0)

            # Step 2: Fetch the detail page for full text
            resp2 = requests.get(detail_url, headers=HEADERS, timeout=30)
            resp2.raise_for_status()

            meta = extract_metadata_from_detail(resp2.text)
            if not meta.get("case_number"):
                meta["case_number"] = case_num

            # Infer court from case number if not found in metadata
            if not meta.get("court"):
                for prefix in COURTS:
                    if case_num.startswith(prefix):
                        meta["court"] = prefix
                        break

            text = extract_full_text(resp2.text)
            if not text or len(text) < 100:
                print(f"    Insufficient text ({len(text) if text else 0} chars), skipping")
                time.sleep(REQUEST_DELAY)
                continue

            meta["text"] = text
            meta["detail_url"] = detail_url
            record = normalize(meta)
            yield record
            count += 1

        except Exception as e:
            print(f"    Error: {e}")

        time.sleep(REQUEST_DELAY)

    print(f"Total records: {count}")


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch documents updated since a given date."""
    for record in fetch_all():
        if record.get("date"):
            try:
                doc_date = datetime.fromisoformat(record["date"])
                if doc_date >= since:
                    yield record
            except (ValueError, TypeError):
                yield record


def bootstrap_sample(sample_count: int = 15):
    """Fetch sample records and save to sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample records from LI/Courts...")
    print("=" * 60)

    records = []
    for i, record in enumerate(fetch_all(max_records=sample_count)):
        records.append(record)
        filename = SAMPLE_DIR / f"record_{i+1:03d}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        text_len = len(record.get("text", ""))
        print(f"  [{i+1:02d}] {record['_id']}: {record['title'][:50]} ({text_len:,} chars)")

    print("=" * 60)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")

    if records:
        avg_text_len = sum(len(r.get("text", "")) for r in records) / len(records)
        print(f"Average text length: {avg_text_len:,.0f} chars/doc")

    if len(records) < 10:
        print("WARNING: Fewer than 10 records fetched!")
        return False

    empty_text = sum(1 for r in records if not r.get("text"))
    if empty_text > 0:
        print(f"WARNING: {empty_text} records have empty text!")
        return False

    print("VALIDATION PASSED: All records have full text content.")
    return True


def main():
    parser = argparse.ArgumentParser(description="LI/Courts case law fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "fetch", "info"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample records only")
    parser.add_argument("--count", type=int, default=15, help="Number of sample records")

    args = parser.parse_args()

    if args.command == "info":
        print(f"LI/Courts - Liechtenstein Courts Decisions")
        print(f"Source URL: {BASE_URL}")
        print(f"Courts: {', '.join(COURTS.keys())}")

    elif args.command == "bootstrap":
        success = bootstrap_sample(args.count)
        sys.exit(0 if success else 1)

    elif args.command == "fetch":
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
