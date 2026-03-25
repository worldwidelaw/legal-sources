#!/usr/bin/env python3
"""
TH/OpenLawData - Open Law Data Thailand (OCS Krisdika) Fetcher

Fetches Thai legislation from the OCS Krisdika dataset on Hugging Face.
Data source: Office of the Council of State (Krisdika), Thailand's official law drafter.
Coverage: Acts, Royal Decrees, Ministerial Regulations from 1877 to present.

Dataset: open-law-data-thailand/ocs-krisdika (Hugging Face)
License: CC-BY 4.0
Format: JSONL files organized by year/month

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

from huggingface_hub import HfApi, hf_hub_download

SOURCE_ID = "TH/OpenLawData"
SAMPLE_DIR = Path(__file__).parent / "sample"
REPO_ID = "open-law-data-thailand/ocs-krisdika"


def extract_text(sections: list) -> str:
    """Extract full text from sections array."""
    if not sections:
        return ""
    parts = []
    for section in sections:
        content = section.get("content", "")
        if content:
            parts.append(content.strip())
    return "\n\n".join(parts)


def normalize(raw: dict) -> Optional[dict]:
    """Normalize a raw record into standard schema."""
    sections = raw.get("sections", [])
    text = extract_text(sections)

    if not text or len(text) < 50:
        return None

    title = raw.get("title", "")
    law_code = raw.get("law_code", "")
    filename = raw.get("filename", "")

    # Create a stable ID from law_code or filename
    doc_id = law_code or filename.replace(".json", "")
    if not doc_id:
        return None

    return {
        "_id": f"TH_{doc_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "law_code": law_code,
        "date": raw.get("publish_date"),
        "is_latest": raw.get("is_latest", False),
        "reference_url": raw.get("reference_url", ""),
        "text": text,
        "url": f"https://huggingface.co/datasets/{REPO_ID}",
    }


def list_data_files() -> list[str]:
    """List all JSONL data files in the dataset."""
    api = HfApi()
    files = []
    # List year directories
    tree = list(api.list_repo_tree(REPO_ID, repo_type="dataset", path_in_repo="data"))
    year_dirs = sorted([item.path for item in tree if hasattr(item, 'path') and '.' not in item.path.split('/')[-1]])

    for year_dir in year_dirs:
        year_files = list(api.list_repo_tree(REPO_ID, repo_type="dataset", path_in_repo=year_dir))
        for f in year_files:
            if hasattr(f, 'path') and f.path.endswith('.jsonl'):
                files.append(f.path)

    return sorted(files)


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all legislation records from the dataset."""
    if sample:
        # For sample, download a few recent months
        target_files = ["data/2024/2024-03.jsonl", "data/2024/2024-06.jsonl", "data/2024/2024-09.jsonl"]
    else:
        print("  Listing all data files...")
        target_files = list_data_files()
        print(f"  Found {len(target_files)} files")

    total_yielded = 0
    for filepath in target_files:
        print(f"  Processing: {filepath}")
        try:
            local_path = hf_hub_download(
                repo_id=REPO_ID,
                filename=filepath,
                repo_type="dataset",
            )
        except Exception as e:
            print(f"    Error downloading {filepath}: {e}")
            continue

        with open(local_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError:
                    continue

                record = normalize(raw)
                if record:
                    yield record
                    total_yielded += 1

                    if sample and total_yielded >= 15:
                        return

    print(f"  Total records with text: {total_yielded}")


def test_connection():
    """Test connectivity to the Hugging Face dataset."""
    print("Testing Open Law Data Thailand connectivity...")

    print("\n1. Checking dataset availability...")
    api = HfApi()
    try:
        info = api.dataset_info(REPO_ID)
        print(f"   OK: Dataset found - {info.id}")
        print(f"   Downloads: {info.downloads}")
    except Exception as e:
        print(f"   FAIL: {e}")
        return False

    print("\n2. Downloading sample file...")
    try:
        local_path = hf_hub_download(
            repo_id=REPO_ID,
            filename="data/2024/2024-03.jsonl",
            repo_type="dataset",
        )
        print(f"   OK: Downloaded to {local_path}")
    except Exception as e:
        print(f"   FAIL: {e}")
        return False

    print("\n3. Checking data quality...")
    total = 0
    with_text = 0
    with open(local_path, "r", encoding="utf-8") as f:
        for line in f:
            raw = json.loads(line.strip())
            total += 1
            record = normalize(raw)
            if record:
                with_text += 1
                if with_text == 1:
                    print(f"   Sample record title: {record['title'][:80]}")
                    print(f"   Text length: {len(record['text'])} chars")
                    print(f"   Text preview: {record['text'][:150]}...")

    print(f"   Records: {total} total, {with_text} with text ({with_text/total*100:.0f}%)")

    print("\nAll tests passed!")
    return True


def main():
    parser = argparse.ArgumentParser(description="TH/OpenLawData Thailand Legislation Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    args = parser.parse_args()

    if args.command == "test":
        success = test_connection()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        count = 0
        for record in fetch_all(sample=args.sample):
            filename = re.sub(r'[^\w\-]', '_', record["_id"]) + ".json"
            filepath = SAMPLE_DIR / filename
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            print(f"  Saved: {filepath.name} ({len(record['text'])} chars)")

        print(f"\nBootstrap complete: {count} records saved to {SAMPLE_DIR}")


if __name__ == "__main__":
    main()
