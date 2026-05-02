#!/usr/bin/env python3
"""
CN/HuggingFace-CAIL2018 -- Chinese Court Judgments Dataset (CAIL 2018)

Fetches 2.17M Chinese criminal case records with fact descriptions,
applicable law articles, charges, and sentences from the HuggingFace
datasets API (china-ai-law-challenge/cail2018).

Strategy:
  - GET /rows?dataset=...&config=default&split={split}&offset={offset}&length={length}
  - Paginate through all rows in batches of 100
  - Full text in 'fact' field (case fact descriptions)
  - Rich metadata: relevant_articles, accusation, imprisonment, penalties

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

SOURCE_ID = "CN/HuggingFace-CAIL2018"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CN.HuggingFace-CAIL2018")

DATASET = "china-ai-law-challenge/cail2018"
API_BASE = "https://datasets-server.huggingface.co"
ROWS_URL = f"{API_BASE}/rows"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
    "Accept": "application/json",
}

BATCH_SIZE = 100

SPLITS = [
    ("first_stage_train", 1710856),
    ("first_stage_test", 217016),
    ("exercise_contest_train", 154592),
    ("exercise_contest_valid", 17131),
    ("exercise_contest_test", 32508),
    ("final_test", 35922),
]


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


def _make_doc_id(split: str, row_idx: int) -> str:
    """Generate a stable unique ID from split + row index."""
    raw = f"{split}|{row_idx}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]


def fetch_rows(split: str, offset: int = 0, length: int = BATCH_SIZE) -> tuple:
    """Fetch rows from HuggingFace API. Returns (rows, total_count)."""
    params = {
        "dataset": DATASET,
        "config": "default",
        "split": split,
        "offset": offset,
        "length": length,
    }
    resp = _request_with_retry(ROWS_URL, params=params)
    data = resp.json()
    total = data.get("num_rows_total", 0)
    rows = []
    for r in data.get("rows", []):
        row = r.get("row", {})
        row["_row_idx"] = r.get("row_idx", offset)
        rows.append(row)
    return rows, total


def normalize(row: dict, split: str) -> dict:
    fact = (row.get("fact") or "").strip()
    row_idx = row.get("_row_idx", 0)
    doc_id = _make_doc_id(split, row_idx)

    relevant_articles = row.get("relevant_articles") or []
    accusations = row.get("accusation") or []
    criminals = row.get("criminals") or []
    imprisonment = row.get("imprisonment")
    punish_of_money = row.get("punish_of_money")
    death_penalty = row.get("death_penalty", False)
    life_imprisonment = row.get("life_imprisonment", False)

    # Build a title from accusation + criminals
    acc_str = "、".join(accusations) if accusations else "刑事案件"
    crim_str = "、".join(criminals) if criminals else "被告人"
    title = f"{crim_str}{acc_str}案"

    # Build sentence summary
    sentence_parts = []
    if death_penalty:
        sentence_parts.append("死刑")
    if life_imprisonment:
        sentence_parts.append("无期徒刑")
    if imprisonment and imprisonment > 0 and not death_penalty and not life_imprisonment:
        sentence_parts.append(f"有期徒刑{imprisonment}个月")
    if punish_of_money and punish_of_money > 0:
        sentence_parts.append(f"罚金{punish_of_money}元")
    sentence_summary = "；".join(sentence_parts) if sentence_parts else ""

    return {
        "_id": f"CAIL2018-{split}-{doc_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": fact,
        "date": "2018-01-01",  # Dataset year; individual dates not available
        "url": f"https://huggingface.co/datasets/{DATASET}",
        "split": split,
        "relevant_articles": relevant_articles,
        "accusation": accusations,
        "criminals": criminals,
        "imprisonment_months": imprisonment,
        "punish_of_money": punish_of_money,
        "death_penalty": death_penalty,
        "life_imprisonment": life_imprisonment,
        "sentence_summary": sentence_summary,
    }


def fetch_sample(count: int = 15) -> list:
    """Fetch sample documents from multiple splits."""
    records = []
    # Get samples from the largest split
    rows, total = fetch_rows("first_stage_train", offset=0, length=count)
    logger.info(f"first_stage_train: {total:,} total rows")

    for row in rows:
        normalized = normalize(row, "first_stage_train")
        text_len = len(normalized.get("text", ""))
        if text_len > 20:
            records.append(normalized)
            logger.info(f"  [{len(records)}] {normalized['title'][:60]}... ({text_len} chars)")

    return records


def fetch_all() -> Generator[dict, None, None]:
    """Fetch all documents across all splits."""
    total_yielded = 0

    for split_name, expected_count in SPLITS:
        logger.info(f"Processing split: {split_name} ({expected_count:,} expected)")
        offset = 0
        split_count = 0

        _, total = fetch_rows(split_name, offset=0, length=1)
        logger.info(f"  Actual rows: {total:,}")

        while offset < total:
            try:
                rows, _ = fetch_rows(split_name, offset=offset, length=BATCH_SIZE)
            except Exception as e:
                logger.error(f"Failed at {split_name} offset {offset}: {e}")
                break

            if not rows:
                break

            for row in rows:
                normalized = normalize(row, split_name)
                if len(normalized.get("text", "")) > 20:
                    total_yielded += 1
                    split_count += 1
                    if total_yielded % 5000 == 0:
                        logger.info(f"  Processed {total_yielded} total ({split_name}: {split_count})")
                    yield normalized

            offset += len(rows)
            time.sleep(0.3)

        logger.info(f"  {split_name}: {split_count} records yielded")

    logger.info(f"Total yielded: {total_yielded}")


def test_api():
    """Test API connectivity."""
    logger.info(f"Testing HuggingFace dataset API for {DATASET}...")
    try:
        for split_name, _ in SPLITS[:2]:
            rows, total = fetch_rows(split_name, offset=0, length=2)
            logger.info(f"  {split_name}: {total:,} rows")
            for i, row in enumerate(rows):
                fact = (row.get("fact") or "")[:80]
                logger.info(f"    [{i+1}] '{fact}...' ({len(row.get('fact',''))} chars)")
        return True
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
        filename = f"sample_{i:02d}.json"
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

    accusations = {}
    for r in records:
        for a in r.get("accusation", []):
            accusations[a] = accusations.get(a, 0) + 1
    logger.info(f"  - Accusations: {accusations}")

    return len(records) >= 10 and avg_text > 50


def main():
    parser = argparse.ArgumentParser(description="CN/HuggingFace-CAIL2018 Fetcher")
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
                filepath = SAMPLE_DIR / f"record_{count:07d}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"Processed {count} records")


if __name__ == "__main__":
    main()
