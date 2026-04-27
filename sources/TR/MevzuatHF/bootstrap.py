#!/usr/bin/env python3
"""
TR/MevzuatHF -- Turkish Legislation via Hugging Face Dataset

Fetches Turkish legislation from the Hugging Face muhammetakkurt/mevzuat-gov-dataset.
Alternative to TR/Mevzuat which is VPS-blocked.

Data source: https://huggingface.co/datasets/muhammetakkurt/mevzuat-gov-dataset
License: MIT
Language: Turkish

Dataset contains:
  - 907 Turkish laws with full text articles
  - Sourced from official mevzuat.gov.tr
  - Includes law number, dates, official gazette info

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
from typing import Any, Dict, Generator, List, Optional

try:
    from datasets import load_dataset
except ImportError:
    print("ERROR: datasets library not installed. Run: pip3 install datasets")
    sys.exit(1)

# Setup
SOURCE_ID = "TR/MevzuatHF"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TR.MevzuatHF")

# Dataset configuration
DATASET_NAME = "muhammetakkurt/mevzuat-gov-dataset"


def parse_date(date_str: Optional[str]) -> Optional[str]:
    """Parse Turkish date format (DD.MM.YYYY or DD/MM/YYYY) to ISO format."""
    if not date_str or not isinstance(date_str, str):
        return None
    date_str = date_str.strip()
    if not date_str:
        return None

    # Try different formats
    for fmt in ["%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d"]:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def extract_full_text(maddeler: List[Dict[str, Any]]) -> str:
    """
    Extract full text from the maddeler (articles) array.

    Args:
        maddeler: List of article dicts with 'madde_numarasi' and 'text' keys

    Returns:
        Combined full text of all articles
    """
    if not maddeler:
        return ""

    parts = []
    for madde in maddeler:
        if not isinstance(madde, dict):
            continue
        madde_no = madde.get("madde_numarasi", "")
        text = madde.get("text", "")

        if text:
            if madde_no:
                parts.append(f"Madde {madde_no}\n{text}")
            else:
                parts.append(text)

    return "\n\n".join(parts)


def normalize(raw: dict) -> dict:
    """
    Transform raw Hugging Face record to standard schema.

    Args:
        raw: Raw dataset row from muhammetakkurt/mevzuat-gov-dataset

    Returns:
        Normalized record with standard fields
    """
    # Extract fields
    url = raw.get("url", "")
    title = raw.get("Kanun Adı", "") or raw.get("kanun_adi", "")
    law_number = raw.get("kanun_numarasi", "")
    accept_date = raw.get("kabul_tarihi", "")

    # Official gazette info
    resmi_gazete = raw.get("resmi_gazete", {}) or {}
    gazette_number = resmi_gazete.get("sayi", "")
    gazette_date = resmi_gazete.get("tarih", "")

    # Dustur (legal compendium) info
    dustur = raw.get("dustur", {}) or {}
    dustur_volume = dustur.get("cilt", "")
    dustur_page = dustur.get("sayfa", "")

    # Extract full text from articles
    maddeler = raw.get("maddeler", []) or []
    full_text = extract_full_text(maddeler)

    # Parse dates
    accept_date_iso = parse_date(accept_date)
    gazette_date_iso = parse_date(gazette_date)

    # Use gazette date as primary, fallback to accept date
    primary_date = gazette_date_iso or accept_date_iso

    # Generate unique ID
    if law_number:
        doc_id = f"TR-kanun-{law_number}"
    else:
        # Fallback: hash the title
        title_hash = hashlib.md5(title.encode("utf-8")).hexdigest()[:8]
        doc_id = f"TR-kanun-{title_hash}"

    # Build clean title
    if not title:
        title = f"Turkish Law No. {law_number}" if law_number else "Turkish Law"

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": primary_date,
        "url": url or f"https://www.mevzuat.gov.tr",
        # Additional metadata
        "kanun_numarasi": law_number,
        "kabul_tarihi": accept_date_iso,
        "resmi_gazete_tarihi": gazette_date_iso,
        "resmi_gazete_sayisi": gazette_number,
        "dustur_cilt": dustur_volume,
        "dustur_sayfa": dustur_page,
        "madde_count": len(maddeler),
        "language": "tr",
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

        # Skip records with no text
        if not normalized.get("text"):
            logger.warning(f"  Skipping record {i}: no text content")
            continue

        if i % 100 == 0:
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
        text_len = len(normalized.get("text", ""))
        if text_len > 100:
            records.append(normalized)
            logger.info(f"  [{len(records)}/{count}] {normalized['title'][:50]}... ({text_len} chars)")

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
            logger.info(f"  - Sample title: {sample.get('Kanun Adı', 'N/A')[:50]}")
            maddeler = sample.get("maddeler", [])
            logger.info(f"  - Sample articles: {len(maddeler)}")

            # Test text extraction
            full_text = extract_full_text(maddeler)
            logger.info(f"  - Sample full text length: {len(full_text)} chars")

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
        # Clean filename
        safe_id = record['_id'].replace("/", "_").replace(":", "_")
        filename = f"sample_{i:02d}_{safe_id}.json"
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

    # Check article counts
    article_counts = [r.get("madde_count", 0) for r in records]
    logger.info(f"  - Avg articles per law: {sum(article_counts) / len(article_counts):.1f}")

    # Check dates
    dates_present = sum(1 for r in records if r.get("date"))
    logger.info(f"  - Records with dates: {dates_present}/{len(records)}")

    return len(records) >= 10 and avg_text > 100


def main():
    parser = argparse.ArgumentParser(description="TR/MevzuatHF Turkish Legislation Fetcher")
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
    parser.add_argument("--full", action="store_true", help="Fetch all records")

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
                safe_id = record['_id'].replace("/", "_").replace(":", "_")
                filepath = SAMPLE_DIR / f"record_{safe_id}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"Processed {count} records")
            sys.exit(0)


if __name__ == "__main__":
    main()
