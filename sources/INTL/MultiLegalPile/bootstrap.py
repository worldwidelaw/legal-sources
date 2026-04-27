#!/usr/bin/env python3
"""
INTL/MultiLegalPile -- Multi Legal Pile (Multilingual Legal Corpus)

Fetches multilingual legal documents from the Multi Legal Pile dataset
hosted on HuggingFace. Downloads individual JSONL.xz files per
language/type/jurisdiction combination.

Strategy:
  - Enumerate files via HuggingFace API tree endpoints
  - Download JSONL.xz files under MAX_FILE_SIZE (100 MB)
  - Decompress with Python lzma module
  - Each line is a JSON record with {type, language, jurisdiction, text}

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Same as bootstrap (no update mechanism)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import lzma
import time
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.MultiLegalPile")

HF_API_BASE = "https://huggingface.co/api/datasets/joelniklaus/Multi_Legal_Pile/tree/main/data"
HF_RESOLVE_BASE = "https://huggingface.co/datasets/joelniklaus/Multi_Legal_Pile/resolve/main/data"
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 MB

# Map file type names to our standard types
TYPE_MAP = {
    "legislation": "legislation",
    "caselaw": "case_law",
}


def enumerate_files(session: requests.Session) -> list[dict]:
    """Enumerate all downloadable JSONL.xz files from the dataset."""
    files = []

    # Get language directories
    r = session.get(HF_API_BASE, timeout=30)
    r.raise_for_status()
    langs = [d["path"].split("/")[-1] for d in r.json() if d.get("size", -1) == 0]

    for lang in langs:
        # Get type directories (caselaw, legislation)
        r = session.get(f"{HF_API_BASE}/{lang}", timeout=30)
        if r.status_code != 200:
            continue
        types = [d["path"].split("/")[-1] for d in r.json() if d.get("size", -1) == 0]

        for dtype in types:
            # Get actual files
            r = session.get(f"{HF_API_BASE}/{lang}/{dtype}", timeout=30)
            if r.status_code != 200:
                continue
            for f in r.json():
                fpath = f["path"]
                size = f.get("size", 0)
                if not fpath.endswith(".jsonl.xz"):
                    continue
                files.append({
                    "path": fpath,
                    "size": size,
                    "language": lang,
                    "data_type": dtype,
                    "filename": fpath.split("/")[-1],
                })

    return files


class MultiLegalPileScraper(BaseScraper):
    """
    Scraper for INTL/MultiLegalPile -- Multi Legal Pile.
    Country: INTL
    URL: https://huggingface.co/datasets/joelniklaus/Multi_Legal_Pile

    Data types: legislation, case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        })

    def _extract_title(self, text: str, jurisdiction: str, idx: int) -> str:
        """Extract a title from the document text."""
        # Try first non-empty line as title
        for line in text.split("\n"):
            line = line.strip()
            if line and len(line) > 5 and len(line) < 300:
                return line
        return f"{jurisdiction} document #{idx}"

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw record into standard schema."""
        text = raw.get("text", "").strip()
        if not text or len(text) < 100:
            return None

        jurisdiction = raw.get("jurisdiction", "unknown")
        language = raw.get("language", "unknown")
        data_type = raw.get("type", "legislation")
        file_idx = raw.get("_file_idx", 0)
        filename = raw.get("_filename", "")

        title = self._extract_title(text, jurisdiction, file_idx)

        # Generate stable ID from text hash
        text_hash = hashlib.md5(text[:1000].encode()).hexdigest()[:12]
        doc_id = f"MLP-{language}-{jurisdiction[:20]}-{text_hash}"

        our_type = TYPE_MAP.get(data_type, "legislation")

        return {
            "_id": doc_id,
            "_source": "INTL/MultiLegalPile",
            "_type": our_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": None,
            "url": f"https://huggingface.co/datasets/joelniklaus/Multi_Legal_Pile",
            "jurisdiction": jurisdiction,
            "language": language,
            "source_file": filename,
        }

    def _download_and_parse(self, file_info: dict) -> Generator[dict, None, None]:
        """Download a JSONL.xz file and yield records."""
        rel_path = file_info["path"].replace("data/", "")
        url = f"{HF_RESOLVE_BASE}/{rel_path}"
        filename = file_info["filename"]

        logger.info(f"Downloading {filename} ({file_info['size'] / (1024*1024):.1f} MB)")

        try:
            r = self.session.get(url, timeout=300, stream=True)
            r.raise_for_status()
            compressed = r.content
        except Exception as e:
            logger.warning(f"Failed to download {filename}: {e}")
            return

        try:
            decompressed = lzma.decompress(compressed)
            lines = decompressed.decode("utf-8").split("\n")
        except Exception as e:
            logger.warning(f"Failed to decompress {filename}: {e}")
            return

        for idx, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                record["_file_idx"] = idx
                record["_filename"] = filename
                yield record
            except json.JSONDecodeError:
                continue

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all documents from files under MAX_FILE_SIZE."""
        all_files = enumerate_files(self.session)
        logger.info(f"Found {len(all_files)} data files")

        eligible = [f for f in all_files if f["size"] <= MAX_FILE_SIZE]
        skipped = [f for f in all_files if f["size"] > MAX_FILE_SIZE]

        logger.info(f"Eligible files (<100 MB): {len(eligible)}")
        logger.info(f"Skipped files (>100 MB): {len(skipped)}")
        for s in skipped:
            logger.info(f"  Skipped: {s['filename']} ({s['size']/(1024*1024):.0f} MB)")

        total = 0
        for file_info in eligible:
            for record in self._download_and_parse(file_info):
                yield record
                total += 1

            time.sleep(1)

        logger.info(f"Total records yielded: {total}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No incremental updates — re-fetch all."""
        yield from self.fetch_all()


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/MultiLegalPile data fetcher")
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

    scraper = MultiLegalPileScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            all_files = enumerate_files(scraper.session)
            eligible = [f for f in all_files if f["size"] <= MAX_FILE_SIZE]
            logger.info(f"Found {len(all_files)} files, {len(eligible)} eligible (<100 MB)")

            # Download smallest file as test
            smallest = min(eligible, key=lambda f: f["size"])
            records = list(scraper._download_and_parse(smallest))
            logger.info(f"Test file {smallest['filename']}: {len(records)} records")
            if records:
                r = records[0]
                logger.info(f"  Keys: {list(r.keys())}")
                logger.info(f"  Text length: {len(r.get('text', ''))}")
                logger.info(f"  Jurisdiction: {r.get('jurisdiction', '')}")
            logger.info("Connectivity test passed!")
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
