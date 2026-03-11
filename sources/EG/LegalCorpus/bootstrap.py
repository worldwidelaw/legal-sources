#!/usr/bin/env python3
"""
EG/LegalCorpus -- Egyptian Legal Corpus Data Fetcher

Fetches Egyptian legislation from the Hugging Face dataflare/egypt-legal-corpus dataset.

Data source: https://huggingface.co/datasets/dataflare/egypt-legal-corpus
License: MIT
Language: Arabic

Dataset contains:
  - 2,434 Egyptian laws and legal documents
  - 25M+ tokens of full text content
  - Hierarchical legal taxonomy categories

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py bootstrap            # Full bootstrap (all records)
  python bootstrap.py test-api             # Quick API connectivity test
"""

import argparse
import hashlib
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

try:
    from datasets import load_dataset
except ImportError:
    print("ERROR: datasets library not installed. Run: pip3 install datasets")
    sys.exit(1)

# Setup
SOURCE_ID = "EG/LegalCorpus"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EG.LegalCorpus")

# Dataset configuration
DATASET_NAME = "dataflare/egypt-legal-corpus"


def generate_id(law_name: str, text: str) -> str:
    """Generate a stable unique ID from law name and text hash."""
    # Create hash from text content for uniqueness
    text_hash = hashlib.md5(text.encode('utf-8')).hexdigest()[:8]
    # Clean law name for ID
    clean_name = law_name.replace(" ", "_").replace("/", "_")[:50]
    return f"{clean_name}_{text_hash}"


def normalize(raw: dict) -> dict:
    """
    Transform raw Hugging Face record to standard schema.

    Args:
        raw: Raw dataset row with keys: text, categories, law_name, tokens

    Returns:
        Normalized record with standard fields
    """
    text = raw.get("text", "")
    law_name = raw.get("law_name", "")
    categories = raw.get("categories", [])
    tokens = raw.get("tokens", 0)

    # Generate unique ID
    doc_id = generate_id(law_name, text)

    # Build title from law_name (replace underscores with spaces)
    title = law_name.replace("_", " ").strip()
    if not title:
        title = f"Egyptian Law ({doc_id[:20]})"

    # Build category string for metadata
    category_str = " > ".join(categories) if categories else ""

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "law_name": law_name,
        "title": title,
        "text": text,
        "date": None,  # Dataset doesn't include dates
        "url": f"https://huggingface.co/datasets/dataflare/egypt-legal-corpus",
        "categories": categories,
        "category_hierarchy": category_str,
        "tokens": tokens,
        "language": "ar",
    }


def fetch_all() -> Generator[dict, None, None]:
    """
    Fetch all documents from the Hugging Face dataset.

    Yields:
        Normalized document records
    """
    logger.info(f"Loading dataset from Hugging Face: {DATASET_NAME}")

    dataset = load_dataset(DATASET_NAME, split="train")

    logger.info(f"Dataset loaded: {len(dataset)} records")

    for i, row in enumerate(dataset):
        normalized = normalize(row)
        if i % 500 == 0:
            logger.info(f"  Processed {i}/{len(dataset)} records...")
        yield normalized


def fetch_sample(count: int = 15) -> list:
    """
    Fetch sample documents from the dataset.

    Args:
        count: Number of sample documents to fetch

    Returns:
        List of normalized document records
    """
    logger.info(f"Loading dataset from Hugging Face: {DATASET_NAME}")

    dataset = load_dataset(DATASET_NAME, split="train")

    logger.info(f"Dataset loaded: {len(dataset)} total records")
    logger.info(f"Fetching {count} sample records...")

    records = []
    for i, row in enumerate(dataset):
        if len(records) >= count:
            break

        normalized = normalize(row)

        # Validate - must have meaningful text content
        if len(normalized.get("text", "")) > 100:
            records.append(normalized)
            logger.info(f"  [{len(records)}/{count}] {normalized['title'][:50]}... ({len(normalized['text'])} chars)")

    return records


def test_api():
    """Test dataset accessibility from Hugging Face."""
    logger.info(f"Testing Hugging Face dataset availability: {DATASET_NAME}")

    try:
        # Load dataset - this validates accessibility
        dataset = load_dataset(DATASET_NAME, split="train")

        logger.info(f"Dataset loaded successfully:")
        logger.info(f"  - Total records: {len(dataset)}")
        logger.info(f"  - Features: {list(dataset.features.keys())}")

        # Check first record
        if len(dataset) > 0:
            sample = dataset[0]
            logger.info(f"  - Sample law_name: {sample.get('law_name', 'N/A')[:50]}")
            logger.info(f"  - Sample text length: {len(sample.get('text', ''))} chars")
            logger.info(f"  - Sample tokens: {sample.get('tokens', 0)}")
            logger.info(f"  - Sample categories: {sample.get('categories', [])}")

        return True

    except Exception as e:
        logger.error(f"Dataset test failed: {e}")
        return False


def bootstrap_sample():
    """Fetch and save sample records for validation."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = fetch_sample(count=15)

    if not records:
        logger.error("No records fetched!")
        return False

    # Save individual files
    for i, record in enumerate(records, 1):
        filename = f"sample_{i:02d}_{record['_id'][:30]}.json"
        filepath = SAMPLE_DIR / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    # Print summary
    logger.info(f"\nSaved {len(records)} sample records to {SAMPLE_DIR}")

    # Validate
    text_lengths = [len(r.get("text", "")) for r in records]
    avg_text = sum(text_lengths) / len(text_lengths) if text_lengths else 0

    logger.info(f"Validation:")
    logger.info(f"  - Records with text: {sum(1 for t in text_lengths if t > 0)}/{len(records)}")
    logger.info(f"  - Avg text length: {avg_text:.0f} chars")
    logger.info(f"  - Min text length: {min(text_lengths) if text_lengths else 0} chars")
    logger.info(f"  - Max text length: {max(text_lengths) if text_lengths else 0} chars")

    # Check categories
    all_categories = set()
    for r in records:
        for cat in r.get("categories", []):
            all_categories.add(cat)
    logger.info(f"  - Unique categories: {len(all_categories)}")
    if all_categories:
        logger.info(f"  - Sample categories: {', '.join(list(all_categories)[:5])}")

    return len(records) >= 10 and avg_text > 100


def main():
    parser = argparse.ArgumentParser(description="EG/LegalCorpus Egyptian Legislation Fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run"
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Fetch sample records only (for bootstrap)"
    )

    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        if args.sample:
            success = bootstrap_sample()
            sys.exit(0 if success else 1)
        else:
            logger.info("Full bootstrap mode - processing all records")
            SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
            count = 0
            for record in fetch_all():
                count += 1
                filepath = SAMPLE_DIR / f"record_{record['_id'][:40]}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"Processed {count} records")
            sys.exit(0)


if __name__ == "__main__":
    main()
