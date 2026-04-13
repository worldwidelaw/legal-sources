#!/usr/bin/env python3
from __future__ import annotations
"""
US/OK-Legislation -- Oklahoma Statutes

Fetches Oklahoma Statutes with full text from oklegislature.gov.

Strategy:
  1. Probe title PDFs at predictable URLs:
     https://www.oklegislature.gov/OK_Statutes/CompleteTitles/os{N}.pdf
  2. Download each title PDF and extract text via common/pdf_extract
  3. Normalize into standard schema

Titles 1-85 exist (80 titles total, some numbers are skipped/reserved).
Each PDF contains the full text of all sections within that title.

Data: Public domain (Oklahoma government works). No auth required.
Rate limit: 2 sec between downloads.

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample titles
  python bootstrap.py bootstrap            # Full pull (all 80 titles)
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.OK-Legislation")

SOURCE_ID = "US/OK-Legislation"
BASE_URL = "https://www.oklegislature.gov/OK_Statutes/CompleteTitles"
SAMPLE_DIR = Path(__file__).parent / "sample"

# Title names (Oklahoma Statutes Titles 1-85)
TITLE_NAMES = {
    1: "Abstracting", 2: "Agriculture", 3: "Aircraft and Airports",
    4: "Amusements and Sports", 5: "Attorneys and the State Bar",
    6: "Banks and Trust Companies", 7: "Bills of Lading",
    8: "Bonds - State and Other", 9: "Cemeteries",
    10: "Children", 11: "Cities and Towns", 12: "Civil Procedure",
    13: "Common Carriers", 14: "Corporations", 15: "Contracts",
    16: "Conveyances", 17: "Credit", 18: "Corporation Commission",
    19: "Counties and County Officers", 20: "Courts",
    21: "Crimes and Punishments", 22: "Criminal Procedure",
    23: "Damages", 24: "Debtor and Creditor", 25: "Definitions and General Provisions",
    26: "Elections", 27: "Eminent Domain", 28: "Ethics and Lobbying",
    29: "Game and Fish", 30: "Guardian and Ward",
    31: "Habeas Corpus", 32: "Homestead and Exemption",
    33: "Initiative and Referendum", 34: "Interest and Usury",
    36: "Insurance", 37: "Intoxicating Liquors",
    38: "Jails and Reformatories", 39: "Judgments and Decrees",
    40: "Labor", 41: "Landlord and Tenant", 42: "Liens",
    43: "Marriage and Family", 44: "Militia - Military Code",
    45: "Mining", 46: "Mortgages", 47: "Motor Vehicles",
    49: "Navigable Streams and Waters", 50: "Notaries Public",
    51: "Officers", 52: "Oil and Gas", 53: "Partnership",
    54: "Pawnbrokers and Consumer Finance", 56: "Poor Persons",
    57: "Prisons and Reformatories", 58: "Probate Procedures",
    59: "Professions and Occupations", 60: "Property",
    61: "Public Buildings and Public Works", 62: "Public Finance",
    63: "Public Health and Safety", 64: "Public Lands",
    65: "Public Printing", 66: "Railroads", 67: "Records",
    68: "Revenue and Taxation", 69: "Roads, Bridges and Ferries",
    70: "Schools", 71: "Soldiers and Sailors",
    72: "State Department of Health", 73: "State Employment",
    74: "State Government", 75: "Statutes and Reports",
    76: "Suretyship", 78: "Trademarks",
    79: "Trespasses and Malicious Mischief",
    80: "Veterinarians", 82: "Waters and Water Rights",
    83: "Waterways", 84: "Wills and Succession",
    85: "Workers' Compensation",
}

SAMPLE_TITLES = [1, 5, 12, 21, 25, 36, 47, 59, 63, 68, 70, 74]

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
    "Accept": "application/pdf,*/*",
}


def build_pdf_url(title_num: int) -> str:
    """Build PDF download URL for a statute title."""
    return f"{BASE_URL}/os{title_num}.pdf"


def download_title_pdf(title_num: int) -> bytes | None:
    """Download a title PDF. Returns bytes or None on failure."""
    url = build_pdf_url(title_num)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=120)
        if resp.status_code == 200:
            return resp.content
        logger.warning("Title %d: HTTP %d", title_num, resp.status_code)
        return None
    except Exception as e:
        logger.warning("Title %d download failed: %s", title_num, e)
        return None


def fetch_title(title_num: int) -> dict | None:
    """Fetch and extract text for a single title."""
    title_name = TITLE_NAMES.get(title_num, f"Title {title_num}")
    doc_id = f"OK_TITLE_{title_num}"

    logger.info("Fetching Title %d: %s", title_num, title_name)

    pdf_bytes = download_title_pdf(title_num)
    if not pdf_bytes:
        return None

    text = extract_pdf_markdown(
        source=SOURCE_ID,
        source_id=doc_id,
        pdf_bytes=pdf_bytes,
        table="legislation",
        force=True,
    )

    if not text or len(text.strip()) < 200:
        logger.warning("Title %d: insufficient text (%d chars)",
                       title_num, len(text) if text else 0)
        return None

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": f"Oklahoma Statutes - Title {title_num}. {title_name}",
        "text": text,
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "url": build_pdf_url(title_num),
        "title_number": title_num,
        "title_name": title_name,
        "jurisdiction": "US-OK",
        "language": "en",
        "text_length": len(text),
    }


def normalize(raw: dict) -> dict:
    """Data is already normalized in fetch_title."""
    return raw


def fetch_all(
    title_nums: list[int] | None = None,
    limit: int | None = None,
) -> Generator[dict, None, None]:
    """Fetch Oklahoma Statutes titles."""
    targets = title_nums or sorted(TITLE_NAMES.keys())
    count = 0

    for title_num in targets:
        if limit and count >= limit:
            break

        record = fetch_title(title_num)
        if record:
            yield record
            count += 1
            logger.info("[%d] Title %d: %s (%d chars)",
                        count, title_num,
                        record["title_name"], record["text_length"])

        time.sleep(2.0)

    logger.info("Total records: %d", count)


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch all titles (statutes don't have per-document timestamps)."""
    yield from fetch_all()


def bootstrap_sample(sample_dir: Path, limit: int = 12):
    """Create sample data for testing."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    records = []
    for record in fetch_all(title_nums=SAMPLE_TITLES, limit=limit):
        records.append(record)

        filename = f"{record['_id']}.json"
        with open(sample_dir / filename, "w", encoding="utf-8") as f:
            json.dump(record, f, indent=2, ensure_ascii=False)

        print(f"  [{len(records):02d}] Title {record['title_number']}: "
              f"{record['title_name']} ({record['text_length']:,} chars)")

    if records:
        with open(sample_dir / "all_samples.json", "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, ensure_ascii=False)

        total_chars = sum(r["text_length"] for r in records)
        avg_chars = total_chars // len(records)
        print(f"\nSaved {len(records)} samples to {sample_dir}")
        print(f"Total text: {total_chars:,} chars")
        print(f"Average: {avg_chars:,} chars/title")

    return len(records) >= 10


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Oklahoma Statutes Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"],
                        help="Command to execute")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample data only")
    parser.add_argument("--limit", type=int, help="Max records to fetch")

    args = parser.parse_args()

    if args.command == "test-api":
        print("Testing Oklahoma Legislature PDF access...")
        for t in [1, 21, 70]:
            url = build_pdf_url(t)
            try:
                resp = requests.head(url, headers=HEADERS, timeout=10)
                size = int(resp.headers.get("Content-Length", 0))
                print(f"  Title {t}: {resp.status_code} ({size:,} bytes)")
            except Exception as e:
                print(f"  Title {t}: ERROR {e}")

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        if args.sample:
            success = bootstrap_sample(sample_dir, limit=args.limit or 12)
            sys.exit(0 if success else 1)
        else:
            limit = args.limit
            for record in fetch_all(limit=limit):
                pass


if __name__ == "__main__":
    main()
