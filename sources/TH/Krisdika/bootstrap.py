#!/usr/bin/env python3
"""
TH/Krisdika - Office of the Council of State (Krisdika) Thai Laws Fetcher

Fetches Thai legislation from the pythainlp/thailaw dataset on HuggingFace.
Coverage: 42,755 Acts, Royal Decrees, Ministerial Regulations, and ordinances.
Source: Office of the Council of State (Krisdika), Thailand.

Dataset: pythainlp/thailaw (HuggingFace)
License: CC0 1.0 (public domain)
Format: Parquet files downloaded directly from HuggingFace CDN

Previous approach used the datasets-server API (/rows endpoint) which is
unreliable from VPS IPs (502 errors, rate limiting). This version downloads
parquet files directly, which is served from CDN and much more reliable.

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import io
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TH.Krisdika")

DATASET = "pythainlp/thailaw"
PARQUET_INDEX_URL = f"https://huggingface.co/api/datasets/{DATASET}/parquet"

_client = HttpClient(
    base_url="https://huggingface.co",
    headers={"User-Agent": "LegalDataHunter/1.0"},
    timeout=120,
    max_retries=5,
    backoff_factor=2.0,
)


def get_parquet_urls() -> list[str]:
    """Get the list of parquet file URLs for the dataset."""
    resp = _client.get(PARQUET_INDEX_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    # Structure: {"default": {"train": ["url1", "url2", ...]}}
    return data.get("default", {}).get("train", [])


def iter_parquet_rows(urls: list[str], limit: int = 0) -> Generator[dict, None, None]:
    """
    Download parquet files and yield rows as dicts.

    Args:
        urls: List of parquet file URLs to download
        limit: Maximum number of rows to yield (0 = no limit)
    """
    try:
        import pyarrow.parquet as pq
    except ImportError:
        raise ImportError("Install pyarrow: pip install pyarrow")

    count = 0
    for i, url in enumerate(urls):
        logger.info(f"Downloading parquet file {i + 1}/{len(urls)}...")
        try:
            resp = _client.get(url, timeout=300)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to download parquet file {i + 1}: {e}")
            continue

        logger.info(
            f"Downloaded {len(resp.content) / (1024 * 1024):.1f} MB, "
            f"parsing parquet..."
        )

        try:
            table = pq.read_table(io.BytesIO(resp.content))
        except Exception as e:
            logger.error(f"Failed to parse parquet file {i + 1}: {e}")
            continue

        logger.info(f"Parquet file {i + 1}: {len(table)} rows")

        # Convert to dict-of-lists (fast batch conversion), then yield
        # rows one at a time. This is much faster than per-row .as_py().
        columns = table.to_pydict()
        col_names = list(columns.keys())
        n_rows = len(table)
        del table  # free arrow memory early

        for row_idx in range(n_rows):
            row_dict = {col: columns[col][row_idx] for col in col_names}
            yield row_dict
            count += 1
            if limit and count >= limit:
                return

        # Free memory after processing each file
        del columns, resp


class KrisdikaScraper(BaseScraper):
    """Scraper for TH/Krisdika - Thai Laws from HuggingFace dataset."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Normalize a raw record into standard schema."""
        text = raw.get("txt", "")
        if not text or len(text) < 50:
            return None

        title = raw.get("title", "")
        sysid = str(raw.get("sysid", ""))
        if not sysid:
            return None

        return {
            "_id": f"TH_KR_{sysid}",
            "_source": "TH/Krisdika",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "sysid": sysid,
            "date": None,
            "text": text,
            "url": f"https://huggingface.co/datasets/{DATASET}",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all legislation records from the dataset via parquet files."""
        logger.info("Fetching parquet file index...")
        urls = get_parquet_urls()
        if not urls:
            logger.error("No parquet files found for dataset")
            return

        logger.info(f"Found {len(urls)} parquet files to process")
        yield from iter_parquet_rows(urls)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        yield from self.fetch_all()


if __name__ == "__main__":
    scraper = KrisdikaScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        print("Testing Krisdika/thailaw connectivity...")
        try:
            urls = get_parquet_urls()
            print(f"  OK: {len(urls)} parquet files found")
            for i, url in enumerate(urls):
                print(f"  [{i}] {url.split('/')[-1]}")

            # Test: download a small portion and check
            print("  Downloading first parquet file for validation...")
            rows = list(iter_parquet_rows(urls[:1], limit=5))
            print(f"  OK: Got {len(rows)} sample rows from first file")

            for i, raw in enumerate(rows[:3]):
                record = scraper.normalize(raw)
                if record:
                    print(
                        f"  [{i}] sysid={record['sysid']}, "
                        f"title={record['title'][:60]}"
                    )
                    print(f"       text_length={len(record['text'])} chars")
            print("Test PASSED")
        except Exception as e:
            print(f"  FAIL: {e}")
            import traceback

            traceback.print_exc()
            sys.exit(1)

    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
