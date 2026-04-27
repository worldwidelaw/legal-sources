#!/usr/bin/env python3
"""
INTL/PileOfLaw -- Pile of Law NLP Corpus (Selected Subsets)

Fetches selected subsets from the Pile of Law dataset on HuggingFace.
Full dataset is 256GB; we download only small, non-overlapping subsets.

Selected subsets:
  - constitutions (3.2MB): 139 world constitutions
  - olc_memos (5.7MB): US Office of Legal Counsel opinions
  - un_debates (22.6MB): UN General Assembly debates

Strategy:
  - Download .jsonl.xz files directly from HuggingFace
  - Decompress with lzma and parse JSONL
  - Each record has: text, created_timestamp, downloaded_timestamp, url

Usage:
  python bootstrap.py bootstrap          # Full initial pull (selected subsets)
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import io
import json
import lzma
import re
import sys
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urlparse

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.PileOfLaw")

HF_BASE = "https://huggingface.co/datasets/pile-of-law/pile-of-law/resolve/main/data"

# Subsets to download: (config_name, data_type, description)
SUBSETS = [
    ("constitutions", "legislation", "World constitutions from constituteproject.org"),
    ("olc_memos", "doctrine", "US Office of Legal Counsel memoranda"),
    ("un_debates", "doctrine", "UN General Assembly debate transcripts"),
]


def clean_text(text: str) -> str:
    """Clean text: normalize whitespace, remove excess blank lines."""
    if not text:
        return ""
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_title_from_text(text: str, subset: str) -> str:
    """Extract a title from the first meaningful line of text."""
    lines = text.strip().split('\n')
    for line in lines[:5]:
        line = line.strip()
        if line and len(line) > 3 and len(line) < 200:
            return line
    return f"Document ({subset})"


class PileOfLawScraper(BaseScraper):
    """
    Scraper for INTL/PileOfLaw -- Pile of Law NLP Corpus.
    Country: INTL
    URL: https://pile-of-law.github.io/

    Data types: legislation, case_law, doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (research project)",
        })

    def _download_subset(self, subset_name: str) -> Generator[dict, None, None]:
        """Download and parse a JSONL.XZ subset from HuggingFace."""
        url = f"{HF_BASE}/train.{subset_name}.jsonl.xz"
        logger.info(f"Downloading subset '{subset_name}' from {url}...")

        r = self.session.get(url, timeout=120, stream=True)
        r.raise_for_status()

        content = r.content
        logger.info(f"Downloaded {len(content)} bytes for '{subset_name}'")

        count = 0
        with lzma.open(io.BytesIO(content), 'rt', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                rec['_subset'] = subset_name
                count += 1
                yield rec

        logger.info(f"Parsed {count} records from '{subset_name}'")

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw JSONL record into standard schema."""
        text = clean_text(raw.get("text", ""))
        if not text or len(text) < 200:
            return None

        subset = raw.get("_subset", "unknown")
        url = raw.get("url", "")
        created = raw.get("created_timestamp", "")

        # Find matching subset config
        data_type = "doctrine"
        for name, dtype, _ in SUBSETS:
            if name == subset:
                data_type = dtype
                break

        title = extract_title_from_text(text, subset)

        # Generate stable ID from URL or text hash
        if url:
            doc_id = f"POL-{subset}-{abs(hash(url)) % 10**8}"
        else:
            doc_id = f"POL-{subset}-{abs(hash(text[:500])) % 10**8}"

        # Parse date
        date = None
        if created:
            try:
                date = created[:10]  # Take YYYY-MM-DD portion
            except Exception:
                pass

        return {
            "_id": doc_id,
            "_source": "INTL/PileOfLaw",
            "_type": data_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url if url else "https://pile-of-law.github.io/",
            "subset": subset,
            "license": "CC-BY-NC-SA-4.0",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all records from selected subsets."""
        total = 0
        for subset_name, _, desc in SUBSETS:
            logger.info(f"Processing subset: {subset_name} ({desc})")
            try:
                for rec in self._download_subset(subset_name):
                    text = rec.get("text", "")
                    if len(text) < 200:
                        continue
                    yield rec
                    total += 1
            except Exception as e:
                logger.warning(f"Failed to process subset '{subset_name}': {e}")
                continue
            time.sleep(2)

        logger.info(f"Total records fetched: {total}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Static dataset — re-fetch all."""
        yield from self.fetch_all()


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/PileOfLaw data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = PileOfLawScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            url = f"{HF_BASE}/train.constitutions.jsonl.xz"
            r = scraper.session.head(url, timeout=30, allow_redirects=True)
            r.raise_for_status()
            logger.info(f"OK: HuggingFace reachable, status {r.status_code}")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
