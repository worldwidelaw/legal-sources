"""
Storage manager for World Wide Law.

Handles reading/writing individual records as JSON-lines files,
organized by source. Designed for simplicity and portability —
the actual database (PostgreSQL, etc.) is built later from these files.

Storage layout:
  data/
    records.jsonl          # all records, one JSON object per line
    index.json             # dedup_key -> line_number index for fast lookups
"""

import json
import os
import logging
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone

logger = logging.getLogger("legal-data-hunter")


class StorageManager:
    """
    Simple file-based storage using JSON Lines format.

    Why JSONL and not a database?
    - Zero dependencies (no PostgreSQL/SQLite setup needed in local)
    - Git-friendly (text-based, diffable)
    - Easily convertible to any database later
    - Works on any system without configuration

    The index provides O(1) lookups by dedup_key for idempotency checks.
    """

    def __init__(self, data_dir):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.records_path = self.data_dir / "records.jsonl"
        self.index_path = self.data_dir / "index.json"
        self._index = self._load_index()
        # O(1) line tracking: avoids re-scanning the entire file on every write
        self._line_count = (max(self._index.values()) + 1) if self._index else 0
        self._writes_since_flush = 0
        self._flush_interval = 100  # save index every N writes

    def _load_index(self) -> dict:
        """Load or build the dedup index."""
        if self.index_path.exists():
            with open(self.index_path, "r") as f:
                return json.load(f)

        # Build index from existing records if any
        index = {}
        if self.records_path.exists():
            with open(self.records_path, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        key = record.get("_dedup_key")
                        if key:
                            index[key] = line_num
                    except json.JSONDecodeError:
                        logger.warning(f"Corrupt line {line_num} in {self.records_path}")
        return index

    def _save_index(self):
        """Persist the dedup index."""
        with open(self.index_path, "w") as f:
            json.dump(self._index, f)

    def exists(self, dedup_key: str) -> bool:
        """Check if a record with this dedup key already exists."""
        return dedup_key in self._index

    def read(self, dedup_key: str) -> Optional[dict]:
        """Read a record by its dedup key. Returns None if not found."""
        if dedup_key not in self._index:
            return None

        line_num = self._index[dedup_key]
        try:
            with open(self.records_path, "r", encoding="utf-8") as f:
                for i, line in enumerate(f):
                    if i == line_num:
                        return json.loads(line.strip())
        except (json.JSONDecodeError, FileNotFoundError):
            return None

        return None

    def write(self, dedup_key: str, record: dict) -> None:
        """
        Write or overwrite a record.

        For new records: append to JSONL.
        For existing records (upsert): we append the new version and update the index.
        Old versions remain in the file (acts as an audit trail).
        Periodic compaction can clean this up.
        """
        record["_dedup_key"] = dedup_key
        record["_stored_at"] = datetime.now(timezone.utc).isoformat()

        with open(self.records_path, "a", encoding="utf-8") as f:
            line = json.dumps(record, ensure_ascii=False, default=str)
            f.write(line + "\n")

        # O(1) index update using in-memory line counter
        self._index[dedup_key] = self._line_count
        self._line_count += 1

        # Periodic index flush (every N writes) instead of every write
        self._writes_since_flush += 1
        if self._writes_since_flush >= self._flush_interval:
            self._save_index()
            self._writes_since_flush = 0

    def write_batch(self, records: list) -> int:
        """
        Write multiple records in one shot, flushing the index once at the end.

        Args:
            records: list of (dedup_key, record_dict) tuples

        Returns:
            Number of records written
        """
        if not records:
            return 0

        now = datetime.now(timezone.utc).isoformat()
        written = 0

        with open(self.records_path, "a", encoding="utf-8") as f:
            for dedup_key, record in records:
                record["_dedup_key"] = dedup_key
                record["_stored_at"] = now
                line = json.dumps(record, ensure_ascii=False, default=str)
                f.write(line + "\n")
                self._index[dedup_key] = self._line_count
                self._line_count += 1
                written += 1

        self._save_index()
        self._writes_since_flush = 0
        return written

    def flush(self):
        """Force-save the index to disk. Call this when done writing."""
        if self._writes_since_flush > 0:
            self._save_index()
            self._writes_since_flush = 0

    def close(self):
        """Ensure index is persisted. Call when completely done with this storage."""
        self.flush()

    def count(self) -> int:
        """Return the number of unique records (by dedup key)."""
        return len(self._index)

    def compact(self) -> int:
        """
        Remove superseded records, keeping only the latest version of each.
        Returns the number of records removed.

        This should be run periodically to keep file sizes manageable.
        """
        if not self.records_path.exists():
            return 0

        # Read all records, keeping only the latest for each dedup_key
        latest = {}
        with open(self.records_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    key = record.get("_dedup_key")
                    if key:
                        latest[key] = record
                except json.JSONDecodeError:
                    continue

        original_lines = sum(1 for _ in open(self.records_path, "r"))

        # Rewrite file with only latest records
        with open(self.records_path, "w", encoding="utf-8") as f:
            for record in latest.values():
                f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

        # Rebuild index
        self._index = {}
        with open(self.records_path, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f):
                record = json.loads(line.strip())
                key = record.get("_dedup_key")
                if key:
                    self._index[key] = line_num
        self._save_index()

        removed = original_lines - len(latest)
        logger.info(f"Compacted: removed {removed} superseded records")
        return removed

    def export_all(self) -> list[dict]:
        """Export all current (latest) records as a list."""
        records = []
        seen_keys = set()

        if not self.records_path.exists():
            return records

        # Read all, then filter to latest by dedup_key
        all_records = []
        with open(self.records_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        all_records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue

        # Reverse to get latest first, then deduplicate
        for record in reversed(all_records):
            key = record.get("_dedup_key")
            if key and key not in seen_keys:
                seen_keys.add(key)
                records.append(record)

        records.reverse()  # Restore original order
        return records
