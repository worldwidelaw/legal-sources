#!/usr/bin/env python3
"""
AE/eLaws -- UAE Ministry of Justice Legal Portal Data Fetcher

Fetches UAE federal laws, court decisions, and treaties from the Ministry
of Justice Legal Portal (elaws.moj.gov.ae).

Strategy:
  - POST /api/Laws/Search to paginate through 16 databases
  - Fetch full text HTML from document Link paths
  - Clean HTML to extract plain text

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import quote

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "AE/eLaws"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AE.eLaws")

BASE_URL = "https://elaws.moj.gov.ae"
API_URL = f"{BASE_URL}/api/Laws"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# Databases to scrape (key -> (name, data_type, language))
DATABASES = {
    "AL1": ("UAE Legislation (Arabic)", "legislation", "ar"),
    "EL1": ("UAE Legislation (English)", "legislation", "en"),
    "AI1": ("International Treaties", "legislation", "ar"),
    "UAE-CC-ArAC1": ("Civil Court Decisions (Arabic)", "case_law", "ar"),
    "UAE-UC-Ar": ("Federal Supreme Court (Arabic)", "case_law", "ar"),
    "UAE-UC-En": ("Federal Supreme Court (English)", "case_law", "en"),
    "UAE-KaitAA1": ("Federal Laws Analysis (Arabic)", "legislation", "ar"),
    "UAE-KaitEL1": ("Federal Laws Analysis (English)", "legislation", "en"),
    "UAE-FokehAA1": ("Journal of Judicial Studies", "doctrine", "ar"),
}

# Priority databases for sampling (English first, then major Arabic)
SAMPLE_DATABASES = ["EL1", "UAE-UC-En", "AL1", "UAE-CC-ArAC1"]


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</tr>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</td>', ' | ', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def link_to_url(link: str) -> Optional[str]:
    """Convert a Link field value to a fetchable URL."""
    if not link:
        return None
    # Remove fragment
    link = link.split('#')[0]
    # Replace backslashes
    link = link.replace('\\', '/')
    # URL-encode each path segment (preserving /)
    parts = link.split('/')
    encoded_parts = [quote(p, safe='') for p in parts]
    encoded_path = '/'.join(encoded_parts)
    return f"{BASE_URL}/{encoded_path}"


def fetch_full_text(url: str) -> str:
    """Fetch and extract full text from an HTML document."""
    try:
        response = requests.get(url, headers={
            "User-Agent": HEADERS["User-Agent"],
            "Accept": "text/html",
        }, timeout=30)
        response.raise_for_status()
        html = response.text
        # Try to extract body
        body_match = re.search(r'<body[^>]*>(.*)</body>', html, re.DOTALL | re.IGNORECASE)
        if body_match:
            html = body_match.group(1)
        return clean_html(html)
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            logger.debug(f"404 for {url}")
        else:
            logger.warning(f"HTTP error fetching {url}: {e}")
        return ""
    except Exception as e:
        logger.warning(f"Failed to fetch {url}: {e}")
        return ""


def search_database(db_key: str, page: int = 1, count_per_page: int = 100) -> dict:
    """Search a database via the API."""
    payload = {
        "Keyword": None,
        "Page": page,
        "CountPerPage": count_per_page,
        "Key": db_key,
        "LawYears": None,
        "LawTypes": None,
        "MainClassifications": None,
        "SecondaryClassifications": None,
    }
    response = requests.post(
        f"{API_URL}/Search",
        json=payload,
        headers=HEADERS,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def normalize(record: dict, text: str, db_key: str) -> dict:
    """Transform to standard schema."""
    db_name, data_type, lang = DATABASES.get(db_key, (db_key, "legislation", "ar"))

    reference = record.get("Reference", "") or record.get("Id", "")
    title = record.get("FinalTitle", "") or record.get("DisplayName", "")
    law_number = record.get("LawNumber", "")
    law_year = record.get("LawYear", "")
    law_date = record.get("LawDate", "")
    link = record.get("Link", "")

    # Build date - LawDate format varies: "/11/2023", "27/11/2023", etc.
    date = None
    if law_date:
        # Try DD/MM/YYYY
        m = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', law_date)
        if m:
            day, month, year = m.groups()
            date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        else:
            # Try /MM/YYYY (missing day)
            m = re.match(r'/(\d{1,2})/(\d{4})', law_date)
            if m:
                month, year = m.groups()
                date = f"{year}-{month.zfill(2)}-01"
    if not date and law_year:
        date = f"{law_year}-01-01"

    # Build URL
    url = link_to_url(link) if link else f"{BASE_URL}/"

    doc_id = f"{db_key}_{reference}" if reference else f"{db_key}_{record.get('Id', '')}"

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": data_type,
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date,
        "url": url,
        "reference": reference,
        "law_number": law_number,
        "law_year": law_year,
        "law_type": record.get("LawType", ""),
        "database": db_key,
        "database_name": db_name,
        "language": lang,
    }


def fetch_sample(count: int = 15) -> list:
    """Fetch sample documents with full text from multiple databases."""
    records = []
    per_db = max(4, count // len(SAMPLE_DATABASES))

    for db_key in SAMPLE_DATABASES:
        if len(records) >= count:
            break

        db_name = DATABASES[db_key][0]
        logger.info(f"Searching {db_key} ({db_name})...")

        try:
            result = search_database(db_key, page=1, count_per_page=per_db + 5)
        except Exception as e:
            logger.warning(f"  Search failed for {db_key}: {e}")
            continue

        items = result.get("results", [])
        total = result.get("totalCount", 0)
        logger.info(f"  Total in {db_key}: {total}, got {len(items)} results")

        fetched_this_db = 0
        for item in items:
            if fetched_this_db >= per_db or len(records) >= count:
                break

            link = item.get("Link", "")
            if not link:
                continue

            url = link_to_url(link)
            if not url:
                continue

            title = item.get("FinalTitle", "") or item.get("DisplayName", "")
            logger.info(f"  Fetching: {title[:60]}...")

            text = fetch_full_text(url)
            time.sleep(0.5)

            if text and len(text) > 100:
                normalized = normalize(item, text, db_key)
                records.append(normalized)
                fetched_this_db += 1
                logger.info(f"  [{len(records)}/{count}] {len(text)} chars")
            else:
                logger.warning(f"  Skipped - no/short text ({len(text)} chars)")

    return records


def fetch_all() -> Generator[dict, None, None]:
    """Fetch all documents from all searchable databases."""
    for db_key, (db_name, _, _) in DATABASES.items():
        logger.info(f"Processing {db_key} ({db_name})...")
        page = 1
        total = None
        yielded = 0

        while True:
            try:
                result = search_database(db_key, page=page, count_per_page=100)
            except Exception as e:
                logger.warning(f"Search failed for {db_key} page {page}: {e}")
                break

            items = result.get("results", [])
            if total is None:
                total = result.get("totalCount", 0)
                logger.info(f"  Total: {total}")

            if not items:
                break

            for item in items:
                link = item.get("Link", "")
                if not link:
                    continue

                url = link_to_url(link)
                if not url:
                    continue

                text = fetch_full_text(url)
                time.sleep(0.5)

                if text and len(text) > 100:
                    normalized = normalize(item, text, db_key)
                    yielded += 1
                    if yielded % 50 == 0:
                        logger.info(f"  {db_key}: {yielded}/{total} processed")
                    yield normalized

            page += 1
            if total and (page - 1) * 100 >= total:
                break

        logger.info(f"  {db_key}: done ({yielded} documents)")


def test_api():
    """Test API connectivity and full text access."""
    logger.info("Testing UAE eLaws API...")

    # Test search API
    try:
        result = search_database("EL1", page=1, count_per_page=3)
        items = result.get("results", [])
        total = result.get("totalCount", 0)
        logger.info(f"Search API OK - EL1 has {total} documents, got {len(items)} results")
    except Exception as e:
        logger.error(f"Search API failed: {e}")
        return False

    if not items:
        logger.error("No results from search")
        return False

    # Test full text
    item = items[0]
    link = item.get("Link", "")
    title = item.get("FinalTitle", "")
    logger.info(f"Testing full text: {title[:80]}")

    url = link_to_url(link)
    if url:
        text = fetch_full_text(url)
        if text and len(text) > 100:
            logger.info(f"Full text OK - {len(text)} characters")
            logger.info(f"Preview: {text[:200]}...")
            return True
        else:
            logger.error(f"Full text extraction failed ({len(text)} chars)")
            return False
    else:
        logger.error(f"Could not construct URL from link: {link}")
        return False


def bootstrap_sample():
    """Fetch and save sample records."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = fetch_sample(count=15)

    if not records:
        logger.error("No records fetched!")
        return False

    for i, record in enumerate(records, 1):
        safe_id = re.sub(r'[^\w\-]', '_', record["_id"])[:80]
        filename = f"sample_{i:02d}_{safe_id}.json"
        filepath = SAMPLE_DIR / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    logger.info(f"\nSaved {len(records)} sample records to {SAMPLE_DIR}")

    text_lengths = [len(r.get("text", "")) for r in records]
    avg_text = sum(text_lengths) / len(text_lengths) if text_lengths else 0

    logger.info(f"Validation:")
    logger.info(f"  - Records with text: {sum(1 for t in text_lengths if t > 0)}/{len(records)}")
    logger.info(f"  - Avg text length: {avg_text:.0f} chars")
    logger.info(f"  - Min text length: {min(text_lengths) if text_lengths else 0}")
    logger.info(f"  - Max text length: {max(text_lengths) if text_lengths else 0}")

    dbs = set(r.get("database", "") for r in records)
    logger.info(f"  - Databases sampled: {', '.join(sorted(dbs))}")

    langs = set(r.get("language", "") for r in records)
    logger.info(f"  - Languages: {', '.join(sorted(langs))}")

    types = set(r.get("_type", "") for r in records)
    logger.info(f"  - Data types: {', '.join(sorted(types))}")

    return len(records) >= 10 and avg_text > 200


def main():
    parser = argparse.ArgumentParser(description="AE/eLaws Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true")

    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        if args.sample:
            success = bootstrap_sample()
            sys.exit(0 if success else 1)
        else:
            logger.info("Full bootstrap mode")
            count = 0
            SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
            for record in fetch_all():
                count += 1
                safe_id = re.sub(r'[^\w\-]', '_', record["_id"])[:80]
                filepath = SAMPLE_DIR / f"record_{safe_id}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"Processed {count} records")
            sys.exit(0)


if __name__ == "__main__":
    main()
