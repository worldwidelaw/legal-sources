#!/usr/bin/env python3
"""
CoE/VeniceCommission - CODICES Constitutional Case Law Database

Fetches constitutional case law precis from the Venice Commission's CODICES
database (https://codices.coe.int).

Data coverage:
- ~12,500 precis (case law summaries from 90+ constitutional courts)
- Each precis contains: headnote, summary, text, additional information
- Bilingual content (English/French)
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import requests

# Configuration
BASE_URL = "https://codices.coe.int/api"
SEARCH_URL = f"{BASE_URL}/search"
PRECIS_URL = f"{BASE_URL}/precis"
RATE_LIMIT_DELAY = 1.0
SEARCH_PAGE_SIZE = 100


def clean_html(text: str) -> str:
    """Remove HTML tags and normalize whitespace."""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&quot;", '"').replace("&#39;", "'").replace("&nbsp;", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def search_precis(
    page: int = 0,
    size: int = SEARCH_PAGE_SIZE,
    country: str = "",
    contains: str = "",
) -> Dict[str, Any]:
    """Search CODICES for precis records. Returns IDs for detail fetching."""
    body = {
        "treePathList": ["PRECIS"],
        "countryFilterList": [country] if country else [],
        "thesaurusFilterList": [],
        "contains": contains,
        "alphaIndexText": "",
        "languageCode": "eng",
        "page": page,
        "size": size,
    }

    response = requests.post(SEARCH_URL, json=body, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_precis_detail(precis_id: str) -> Optional[Dict[str, Any]]:
    """Fetch full precis data by ID."""
    url = f"{PRECIS_URL}/{precis_id}"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"  Warning: Failed to fetch precis {precis_id}: {e}")
        return None


def normalize(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a CODICES precis record to standard schema."""
    precis_id = raw.get("id", "")
    ref_code = raw.get("referenceCode", "")
    country = raw.get("country", "")
    decision_date = raw.get("decisionDate", "")
    is_published = raw.get("isPublished", False)

    # Get English translation (prefer English, fall back to French)
    translations = raw.get("precisTranslations", {})
    lang = "eng"
    tr = translations.get("eng", {})
    if not tr:
        tr = translations.get("fra", {})
        lang = "fra"

    title = tr.get("title", "")
    court_name = tr.get("courtName", "")
    country_name = tr.get("countryName", "")
    chamber = tr.get("chamber", "")
    decision_number = tr.get("decisionNumber", "")
    headnote = clean_html(tr.get("headNote", ""))
    summary = clean_html(tr.get("summary", ""))
    additional_info = clean_html(tr.get("additionnalInformation", ""))
    cross_ref = clean_html(tr.get("crossReference", ""))
    official_pub = clean_html(tr.get("officialPublication", ""))

    # Build full text from all available content sections
    text_parts = []
    if headnote:
        text_parts.append(f"HEADNOTE:\n{headnote}")
    if summary:
        text_parts.append(f"SUMMARY:\n{summary}")
    if additional_info:
        text_parts.append(f"ADDITIONAL INFORMATION:\n{additional_info}")
    if cross_ref:
        text_parts.append(f"CROSS-REFERENCES:\n{cross_ref}")

    text = "\n\n".join(text_parts)

    # Parse date
    date_iso = None
    if decision_date:
        try:
            dt = datetime.fromisoformat(decision_date.replace("Z", "+00:00"))
            date_iso = dt.strftime("%Y-%m-%d")
        except ValueError:
            date_iso = decision_date[:10] if len(decision_date) >= 10 else decision_date

    # Alpha index keywords
    alpha_index = []
    for item in raw.get("alphaIndex", []):
        for taxon in item.get("taxon", []):
            word = taxon.get("word", "")
            if word:
                alpha_index.append(word)

    return {
        "_id": precis_id,
        "_source": "CoE/VeniceCommission",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title or ref_code,
        "text": text,
        "date": date_iso,
        "url": f"https://codices.coe.int/codices/detail/{precis_id}",
        "reference_code": ref_code,
        "country": country,
        "country_name": country_name,
        "court_name": court_name,
        "chamber": chamber,
        "decision_number": decision_number,
        "language": lang,
        "official_publication": official_pub,
        "keywords": alpha_index,
        "is_published": is_published,
    }


def discover_precis_ids(
    max_records: Optional[int] = None,
    country: str = "",
) -> Generator[str, None, None]:
    """Discover precis IDs via search pagination."""
    page = 0
    yielded = 0

    while True:
        if max_records and yielded >= max_records:
            return

        result = search_precis(page=page, size=SEARCH_PAGE_SIZE, country=country)
        items = result.get("searchResult", [])

        if not items:
            break

        for item in items:
            if max_records and yielded >= max_records:
                return
            yield item["id"]
            yielded += 1

        if not result.get("hasMoreChildren", False):
            break

        page += 1
        time.sleep(0.5)


def fetch_all(
    max_records: Optional[int] = None,
    country: str = "",
) -> Generator[Dict[str, Any], None, None]:
    """Fetch all precis from CODICES.

    Args:
        max_records: Optional limit on records
        country: Optional 3-letter country filter (e.g., "fra", "ger")
    """
    fetched = 0

    for precis_id in discover_precis_ids(max_records=max_records, country=country):
        if max_records and fetched >= max_records:
            return

        time.sleep(RATE_LIMIT_DELAY)
        raw = fetch_precis_detail(precis_id)
        if not raw:
            continue

        record = normalize(raw)
        if record.get("text"):
            yield record
            fetched += 1


def fetch_updates(since: datetime) -> Generator[Dict[str, Any], None, None]:
    """Fetch precis updated since a given date.

    Note: CODICES search doesn't support date filtering natively,
    so we paginate and filter by lastModifiedDate.
    """
    since_str = since.isoformat()

    for precis_id in discover_precis_ids():
        time.sleep(RATE_LIMIT_DELAY)
        raw = fetch_precis_detail(precis_id)
        if not raw:
            continue

        modified = raw.get("lastModifiedDate", "")
        if modified and modified >= since_str:
            record = normalize(raw)
            if record.get("text"):
                yield record
        elif modified and modified < since_str:
            # Results are not sorted by date, so we can't break early
            continue


def bootstrap_sample(sample_dir: Path, count: int = 15) -> int:
    """Fetch sample records from multiple countries."""
    sample_dir.mkdir(parents=True, exist_ok=True)
    saved = 0

    # Sample from diverse countries
    countries = ["fra", "ger", "ita", "esp", "aut"]
    per_country = max(count // len(countries), 3)

    for country in countries:
        remaining = count - saved
        if remaining <= 0:
            break
        to_fetch = min(per_country, remaining)

        print(f"\nFetching {to_fetch} precis for {country.upper()}...")

        for record in fetch_all(max_records=to_fetch, country=country):
            doc_id = str(record["_id"]).replace("/", "_")
            filename = f"{doc_id}.json"
            filepath = sample_dir / filename

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            text_len = len(record.get("text", ""))
            date = record.get("date", "N/A")
            ref = record.get("reference_code", "")
            print(f"  Saved {filename} ({ref}, date={date}, {text_len:,} chars)")
            saved += 1

    return saved


def validate_samples(sample_dir: Path) -> bool:
    """Validate sample records."""
    samples = list(sample_dir.glob("*.json"))

    if len(samples) < 10:
        print(f"FAIL: Only {len(samples)} samples, need at least 10")
        return False

    total_text_len = 0
    all_valid = True

    for sample_path in samples:
        with open(sample_path, "r", encoding="utf-8") as f:
            record = json.load(f)

        text = record.get("text", "")
        if not text:
            print(f"FAIL: {sample_path.name} has no text")
            all_valid = False
        elif len(text) < 50:
            print(f"WARN: {sample_path.name} has short text ({len(text)} chars)")

        total_text_len += len(text)

        for field in ["_id", "_source", "_type", "title", "date"]:
            if not record.get(field):
                print(f"WARN: {sample_path.name} missing {field}")

    avg_len = total_text_len // len(samples) if samples else 0
    print(f"\nValidation summary:")
    print(f"  Samples: {len(samples)}")
    print(f"  Average text length: {avg_len:,} chars")
    print(f"  All valid: {all_valid}")

    return all_valid and len(samples) >= 10


def main():
    parser = argparse.ArgumentParser(description="CoE/VeniceCommission CODICES fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "validate", "fetch"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample records")
    parser.add_argument("--count", type=int, default=15, help="Number of records to fetch")
    parser.add_argument("--country", type=str, default="", help="Country filter (3-letter code)")
    parser.add_argument("--since", type=str, help="Fetch records since date (YYYY-MM-DD)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            saved = bootstrap_sample(sample_dir, args.count)
            print(f"\nSaved {saved} sample records to {sample_dir}")
            valid = validate_samples(sample_dir)
            sys.exit(0 if saved >= 10 and valid else 1)
        else:
            print("Use --sample for bootstrap mode")
            sys.exit(1)

    elif args.command == "validate":
        valid = validate_samples(sample_dir)
        sys.exit(0 if valid else 1)

    elif args.command == "fetch":
        if args.since:
            since = datetime.fromisoformat(args.since)
            for record in fetch_updates(since):
                print(json.dumps(record, ensure_ascii=False))
        else:
            for record in fetch_all(max_records=args.count, country=args.country):
                print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
