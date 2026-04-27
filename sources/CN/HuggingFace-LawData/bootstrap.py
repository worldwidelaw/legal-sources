#!/usr/bin/env python3
"""
CN/HuggingFace-LawData -- Chinese Law and Regulations HuggingFace Dataset

Fetches 22,552 Chinese laws and regulations with full text from the
HuggingFace datasets API (twang2218/chinese-law-and-regulations).

Strategy:
  - GET /rows?dataset=...&offset={offset}&length={length}
  - Paginate through all rows in batches of 100
  - Full text in 'content' field
  - Rich metadata: type, status, office, dates

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
  python bootstrap.py test-api
"""

import argparse
import hashlib
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "CN/HuggingFace-LawData"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CN.HuggingFace-LawData")

DATASET = "twang2218/chinese-law-and-regulations"
API_BASE = "https://datasets-server.huggingface.co"
ROWS_URL = f"{API_BASE}/rows"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
    "Accept": "application/json",
}

BATCH_SIZE = 100


def _request_with_retry(url, params=None, retries=3, backoff=5):
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            return resp
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < retries - 1:
                wait = backoff * (attempt + 1)
                logger.warning(f"Retry {attempt+1}/{retries}... waiting {wait}s")
                time.sleep(wait)
            else:
                raise


def _make_doc_id(title: str, publish_date: str) -> str:
    """Generate a stable unique ID from title + date."""
    raw = f"{title}|{publish_date}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]


def _clean_content(text: str) -> str:
    """Clean markdown-style content from HuggingFace dataset."""
    if not text:
        return ""
    # Remove markdown blockquote markers
    text = re.sub(r'^>\s*', '', text, flags=re.MULTILINE)
    # Collapse multiple blank lines
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _parse_date(date_str) -> str:
    """Parse date to ISO format."""
    if not date_str:
        return ""
    if isinstance(date_str, str):
        # Handle ISO format timestamps
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return date_str[:10] if len(date_str) >= 10 else date_str
    return ""


def fetch_rows(offset: int = 0, length: int = BATCH_SIZE) -> tuple:
    """Fetch rows from HuggingFace API. Returns (rows, total_count)."""
    params = {
        "dataset": DATASET,
        "config": "default",
        "split": "train",
        "offset": offset,
        "length": length,
    }
    resp = _request_with_retry(ROWS_URL, params=params)
    data = resp.json()
    total = data.get("num_rows_total", 0)
    rows = [r.get("row", {}) for r in data.get("rows", [])]
    return rows, total


def normalize(row: dict) -> dict:
    title = row.get("title", "").strip()
    content = _clean_content(row.get("content", ""))
    publish_date = _parse_date(row.get("publish_date", ""))
    effective_date = _parse_date(row.get("effective_date", ""))
    doc_id = _make_doc_id(title, publish_date)

    return {
        "_id": f"HF-LawData-{doc_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": content,
        "date": publish_date,
        "effective_date": effective_date,
        "url": f"https://huggingface.co/datasets/{DATASET}",
        "doc_type": row.get("type", ""),
        "status": row.get("status", ""),
        "office": row.get("office", ""),
        "office_level": row.get("office_level", ""),
        "office_category": row.get("office_category", ""),
    }


def fetch_sample(count: int = 15) -> list:
    """Fetch sample documents."""
    records = []
    rows, total = fetch_rows(offset=0, length=count)
    logger.info(f"Total rows in dataset: {total:,}")

    for row in rows:
        normalized = normalize(row)
        text_len = len(normalized.get("text", ""))
        if text_len > 50:
            records.append(normalized)
            logger.info(f"  [{len(records)}] {normalized['title'][:50]}... ({text_len} chars)")

    return records


def fetch_all() -> Generator[dict, None, None]:
    """Fetch all documents."""
    offset = 0
    total_yielded = 0

    # Get total count
    _, total = fetch_rows(offset=0, length=1)
    logger.info(f"Total rows: {total:,}")

    while offset < total:
        try:
            rows, _ = fetch_rows(offset=offset, length=BATCH_SIZE)
        except Exception as e:
            logger.error(f"Failed at offset {offset}: {e}")
            break

        if not rows:
            break

        for row in rows:
            normalized = normalize(row)
            if len(normalized.get("text", "")) > 50:
                total_yielded += 1
                if total_yielded % 500 == 0:
                    logger.info(f"  Processed {total_yielded}/{total}")
                yield normalized

        offset += len(rows)
        time.sleep(0.5)

    logger.info(f"Total yielded: {total_yielded}")


def test_api():
    """Test API connectivity."""
    logger.info(f"Testing HuggingFace dataset API for {DATASET}...")
    try:
        rows, total = fetch_rows(offset=0, length=3)
        logger.info(f"  Total rows: {total:,}")
        for i, row in enumerate(rows):
            title = row.get("title", "")[:50]
            content = row.get("content", "")
            logger.info(f"  [{i+1}] '{title}' ({len(content)} chars)")
        return len(rows) > 0
    except Exception as e:
        logger.error(f"  FAILED: {e}")
        return False


def bootstrap_sample():
    """Fetch and save sample records."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = fetch_sample(count=15)

    if not records:
        logger.error("No records fetched!")
        return False

    for i, record in enumerate(records, 1):
        safe_title = re.sub(r'[^\w-]', '_', record["title"][:30])
        filename = f"sample_{i:02d}_{safe_title}.json"
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

    types = {}
    for r in records:
        t = r.get("doc_type", "unknown")
        types[t] = types.get(t, 0) + 1
    logger.info(f"  - Types: {types}")

    return len(records) >= 10 and avg_text > 100


def main():
    parser = argparse.ArgumentParser(description="CN/HuggingFace-LawData Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")

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
                safe_title = re.sub(r'[^\w-]', '_', record["title"][:30])
                filepath = SAMPLE_DIR / f"record_{safe_title}_{count}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"Processed {count} records")


if __name__ == "__main__":
    main()
