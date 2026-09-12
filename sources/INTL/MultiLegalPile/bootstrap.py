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

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.MultiLegalPile")

DATASET = "joelniklaus/Multi_Legal_Pile"
HF_DATASET_API = f"https://huggingface.co/api/datasets/{DATASET}"
HF_API_BASE = f"{HF_DATASET_API}/tree/main/data"
HF_RESOLVE_BASE = f"https://huggingface.co/datasets/{DATASET}/resolve/main/data"
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
                    # git blob sha of this file's contents. Identical oid means
                    # identical bytes, which is what the incremental refresh
                    # compares against instead of re-downloading (#1502).
                    "oid": f.get("oid", ""),
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

        # Baseline for the incremental refresh: record each file's blob oid as
        # it is consumed, so the first refresh after a bootstrap has something
        # to diff against rather than re-downloading the whole corpus.
        current_sha, _ = self._dataset_revision()
        seen_oids = dict((self._load_checkpoint().get("file_oids") or {}))

        total = 0
        for file_info in eligible:
            for record in self._download_and_parse(file_info):
                yield record
                total += 1

            seen_oids[file_info["path"]] = file_info.get("oid", "")
            self._save_checkpoint(current_sha or "", seen_oids)
            time.sleep(1)

        logger.info(f"Total records yielded: {total}")

    # ── Incremental refresh (#1502) ───────────────────────────────────

    def _checkpoint_path(self) -> Path:
        return self.source_dir / "data" / "hf_checkpoint.json"

    def _load_checkpoint(self) -> dict:
        try:
            with open(self._checkpoint_path(), encoding="utf-8") as f:
                return json.load(f)
        except (OSError, ValueError):
            return {}

    def _save_checkpoint(self, sha: str, file_oids: dict) -> None:
        path = self._checkpoint_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"sha": sha, "file_oids": file_oids,
                       "updated_at": datetime.now(timezone.utc).isoformat()}, f, indent=2)
        tmp.replace(path)  # atomic: a half-written checkpoint would skip real files

    def _dataset_revision(self) -> tuple:
        """(commit sha, lastModified) of the dataset, or (None, None)."""
        try:
            r = self.session.get(HF_DATASET_API, timeout=60)
            r.raise_for_status()
            info = r.json()
            return info.get("sha"), info.get("lastModified")
        except Exception as e:
            logger.warning(f"Could not read dataset revision: {e}")
            return None, None

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield only records from data files whose contents changed.

        This is a static HuggingFace corpus with no per-document date — every
        record normalises to `date: None` — so a date cutoff has nothing to
        compare against. "What changed" is a revision question instead, at two
        levels, both read from the API without downloading anything:

        * the dataset commit `sha` — unchanged means nothing to do at all, one
          request instead of re-downloading every eligible `.jsonl.xz`
        * each file's git blob `oid` — identical oid means identical bytes, so
          only files that actually moved are downloaded and parsed

        `since` is the fallback: if the checkpoint is empty (first refresh after
        this fix) it is compared against the dataset's `lastModified`, so a
        corpus untouched since before the cutoff still costs zero downloads
        rather than a full re-fetch. The previous implementation was
        `yield from self.fetch_all()`, which re-downloaded the whole corpus on
        every refresh slot (#1502).
        """
        checkpoint = self._load_checkpoint()
        prev_sha = checkpoint.get("sha")
        prev_oids = checkpoint.get("file_oids") or {}

        current_sha, last_modified = self._dataset_revision()

        if current_sha and prev_sha and current_sha == prev_sha:
            logger.info(
                f"Dataset revision unchanged ({current_sha[:12]}) since last run — "
                f"no new documents. Skipped re-downloading {len(prev_oids)} data file(s)."
            )
            return

        # No checkpoint yet: fall back to the date cutoff against the dataset's
        # own last-modified stamp rather than re-downloading blindly.
        cutoff = as_date_str(since)
        if not prev_oids and cutoff and last_modified:
            modified_day = as_date_str(last_modified)
            if modified_day and modified_day < cutoff:
                logger.info(
                    f"Dataset last modified {modified_day}, before the {cutoff} cutoff "
                    f"— no new documents."
                )
                return

        all_files = enumerate_files(self.session)
        eligible = [f for f in all_files if f["size"] <= MAX_FILE_SIZE]
        changed = [f for f in eligible if f.get("oid") != prev_oids.get(f["path"])]

        logger.info(
            f"Revision {str(prev_sha)[:12]} -> {str(current_sha)[:12]}: "
            f"{len(changed)} of {len(eligible)} eligible file(s) changed"
        )

        seen_oids = dict(prev_oids)
        total = 0
        for file_info in changed:
            for record in self._download_and_parse(file_info):
                yield record
                total += 1
            # Recorded only after the file is fully consumed, so an interrupted
            # run re-reads it next time instead of skipping its tail.
            seen_oids[file_info["path"]] = file_info.get("oid", "")
            self._save_checkpoint(current_sha or prev_sha or "", seen_oids)
            time.sleep(1)

        self._save_checkpoint(current_sha or prev_sha or "", seen_oids)
        logger.info(f"Incremental refresh yielded {total} record(s)")


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
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
