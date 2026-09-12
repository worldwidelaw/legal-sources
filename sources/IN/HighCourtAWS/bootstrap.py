#!/usr/bin/env python3
"""IN/HighCourtAWS: stream one exact raw-PDF tar shard from AWS Open Data."""

from __future__ import annotations

import json
import logging
import re
import sys
import tarfile
import time
from collections.abc import Generator
from concurrent.futures import Future, ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown
from common.storage import StorageManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IN.HighCourtAWS")

S3_BASE = "https://s3.ap-south-1.amazonaws.com/indian-high-court-judgments"
DATA_TAR_PREFIX = "data/tar/"
METADATA_TAR_PREFIX = "metadata/tar/"
PDF_PREFIX = "data/pdf/"
SOURCE_ID = "IN/HighCourtAWS"
_SHARD_SEGMENT_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_ARCHIVE_PART_RE = re.compile(r"^[A-Za-z0-9_-]+\.tar(?:\.gz)?$")
_PDF_MEMBER_RE = re.compile(r"^[A-Za-z0-9_-]+\.pdf$")
_METADATA_MEMBER_RE = re.compile(r"^[A-Za-z0-9_-]+\.json$")


class IndianHighCourtAWSScraper(BaseScraper):
    """Process one isolated `(year, court, bench)` shard per job."""

    def __init__(
        self,
        year_range: tuple[int, int] | None = None,
        court: str | None = None,
        bench: str | None = None,
        max_pdfs: int | None = None,
        extract_workers: int = 1,
        max_pdf_bytes: int = 100_000_000,
        max_metadata_member_bytes: int = 5_000_000,
        max_metadata_entries: int = 250_000,
        max_metadata_bytes: int = 512_000_000,
        max_index_bytes: int = 20_000_000,
        output_dir: str | Path | None = None,
    ):
        super().__init__()
        if not 1 <= extract_workers <= 8:
            raise ValueError("extract_workers must be between 1 and 8")
        resource_limits = (
            max_pdf_bytes,
            max_metadata_member_bytes,
            max_metadata_entries,
            max_metadata_bytes,
            max_index_bytes,
        )
        if any(not isinstance(limit, int) or limit <= 0 for limit in resource_limits):
            raise ValueError("resource limits must be positive integers")
        if max_pdfs is not None and max_pdfs <= 0:
            raise ValueError("max_pdfs must be positive")

        self._explicit_output_dir = output_dir is not None
        self._published_data_dir = self.source_dir / "data"
        if output_dir is not None:
            output_root = Path(output_dir)
            if output_root.is_symlink():
                raise ValueError("--output-dir must not be a symlink")
            if output_root.exists():
                if not output_root.is_dir() or any(output_root.iterdir()):
                    raise ValueError("--output-dir must be a fresh empty output directory")
            else:
                output_root.mkdir(parents=True)
            staging_dir = output_root / ".in-progress-data"
            staging_dir.mkdir()
            self.source_dir = output_root
            self.status = self._load_status()
            self._published_data_dir = output_root / "data"
            self.storage = StorageManager(staging_dir)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "LegalDataHunter/1.0"})
        self.year_range = year_range
        self.court = court
        self.bench = bench
        self.max_pdfs = max_pdfs
        self.extract_workers = extract_workers
        self.max_pdf_bytes = max_pdf_bytes
        self.max_metadata_member_bytes = max_metadata_member_bytes
        self.max_metadata_entries = max_metadata_entries
        self.max_metadata_bytes = max_metadata_bytes
        self.max_index_bytes = max_index_bytes
        self._sample_mode = False
        self._pdfs_seen = 0
        self._expected_records = 0
        self._attempted = False

    def _s3_get(
        self,
        url: str,
        *,
        timeout: int = 120,
        retries: int = 3,
        stream: bool = True,
    ) -> requests.Response:
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=timeout, stream=stream)
                response.raise_for_status()
                return response
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                if attempt == retries - 1:
                    raise
                time.sleep(5 * (attempt + 1))
        raise RuntimeError(f"S3 request exhausted retries: {url}")

    def _selected_shard(self) -> dict[str, Any]:
        year = self.year_range[0] if self.year_range else None
        if (
            not self.year_range
            or self.year_range[0] != self.year_range[1]
            or not isinstance(year, int)
            or not 1900 <= year <= 2100
            or not self.court
            or not _SHARD_SEGMENT_RE.fullmatch(self.court)
            or not self.bench
            or not _SHARD_SEGMENT_RE.fullmatch(self.bench)
        ):
            raise RuntimeError(
                "Raw-PDF ingestion requires one exact natural shard: "
                "--year-range YYYY --court X_Y --bench NAME"
            )
        prefix = f"{DATA_TAR_PREFIX}year={year}/court={self.court}/bench={self.bench}/"
        return {
            "year": year,
            "court": self.court,
            "bench": self.bench,
            "data_index_key": f"{prefix}data.index.json",
        }

    @staticmethod
    def _shard_prefix(root: str, shard: dict[str, Any]) -> str:
        return (
            f"{root}year={shard['year']}/court={shard['court']}/"
            f"bench={shard['bench']}/"
        )

    @staticmethod
    def _canonical_member(name: str) -> str:
        while name.startswith("./"):
            name = name[2:]
        if not name or name.startswith("/") or ".." in Path(name).parts:
            raise RuntimeError(f"Unsafe archive member path: {name!r}")
        return name

    def _read_index(self, key: str) -> dict[str, Any]:
        url = f"{S3_BASE}/{key}"
        with self._s3_get(url, stream=True) as response:
            length = int(response.headers.get("Content-Length") or 0)
            if length > self.max_index_bytes:
                raise RuntimeError(f"S3 index exceeds {self.max_index_bytes} bytes: {key}")
            body = response.raw.read(self.max_index_bytes + 1)
        if len(body) > self.max_index_bytes:
            raise RuntimeError(f"S3 index exceeds {self.max_index_bytes} bytes: {key}")
        try:
            index = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Invalid S3 index JSON: {key}") from exc
        self._validate_index(index, key)
        return index

    @staticmethod
    def _validate_index(index: dict[str, Any], key: str) -> None:
        parts = index.get("parts")
        if not isinstance(parts, list):
            raise RuntimeError(f"Invalid S3 index parts: {key}")  # noqa: TRY004
        index_name = Path(key).name
        member_pattern = {
            "data.index.json": _PDF_MEMBER_RE,
            "metadata.index.json": _METADATA_MEMBER_RE,
        }.get(index_name)
        combined: list[str] = []
        for part in parts:
            files = part.get("files")
            part_name = part.get("name")
            if (
                not isinstance(part_name, str)
                or not _ARCHIVE_PART_RE.fullmatch(part_name)
                or not isinstance(files, list)
            ):
                raise RuntimeError(f"Invalid S3 index inventory: {key}")
            if any(
                not isinstance(name, str)
                or Path(name).name != name
                or (member_pattern is not None and not member_pattern.fullmatch(name))
                for name in files
            ):
                raise RuntimeError(f"Invalid S3 index inventory: {key}")
            if len(files) != len(set(files)) or int(part.get("file_count") or 0) != len(files):
                raise RuntimeError(f"Invalid S3 index inventory: {key}")
            if int(part.get("size") or 0) <= 0 and files:
                raise RuntimeError(f"Invalid S3 index part size: {key}")
            combined.extend(files)
        if len(combined) != len(set(combined)) or int(
            index.get("file_count") or 0
        ) != len(combined):
            raise RuntimeError(f"Invalid S3 index inventory: {key}")
        if "files" in index and index["files"] != combined:
            raise RuntimeError(f"Invalid S3 index inventory: {key}")

    def _archive_members(
        self,
        key: str,
        part: dict[str, Any],
        *,
        label: str,
        member_cap: int,
    ) -> Generator[tuple[str, bytes], None, None]:
        expected = set(part["files"])
        seen: set[str] = set()
        url = f"{S3_BASE}/{key}"
        with self._s3_get(url, timeout=300, stream=True) as response:
            length = int(response.headers.get("Content-Length") or 0)
            expected_size = int(part["size"])
            if length != expected_size:
                raise RuntimeError(
                    f"{label} archive size mismatch for {key}: {length} != {expected_size}"
                )
            mode = "r|gz" if key.endswith(".gz") else "r|"
            with tarfile.open(fileobj=response.raw, mode=mode) as archive:
                for member in archive:
                    if not member.isfile():
                        continue
                    name = self._canonical_member(member.name)
                    if name not in expected or name in seen:
                        raise RuntimeError(f"{label} archive inventory mismatch: {name}")
                    if member.size > member_cap:
                        raise RuntimeError(
                            f"{label} member {name} exceeds {member_cap} bytes"
                        )
                    extracted = archive.extractfile(member)
                    body = extracted.read(member_cap + 1) if extracted is not None else b""
                    if len(body) != member.size:
                        raise RuntimeError(f"Incomplete {label} member: {name}")
                    seen.add(name)
                    yield name, body
        if seen != expected:
            missing = sorted(expected - seen)[:3]
            raise RuntimeError(f"{label} archive inventory mismatch; missing={missing}")

    def _load_metadata(self, shard: dict[str, Any]) -> dict[str, dict[str, Any]]:
        prefix = self._shard_prefix(METADATA_TAR_PREFIX, shard)
        index = self._read_index(f"{prefix}metadata.index.json")
        if index["file_count"] > self.max_metadata_entries:
            raise RuntimeError("Metadata index exceeds entry cap")
        metadata: dict[str, dict[str, Any]] = {}
        total_bytes = 0
        for part in index["parts"]:
            for name, body in self._archive_members(
                f"{prefix}{part['name']}",
                part,
                label="Metadata",
                member_cap=self.max_metadata_member_bytes,
            ):
                total_bytes += len(body)
                if total_bytes > self.max_metadata_bytes:
                    raise RuntimeError("Metadata shard exceeds byte cap")
                try:
                    metadata[Path(name).stem] = json.loads(body)
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise RuntimeError(f"Invalid metadata JSON member: {name}") from exc
        if len(metadata) != index["file_count"]:
            raise RuntimeError("Metadata archive contains duplicate document names")
        return metadata

    @staticmethod
    def _usable_text(text: str | None) -> bool:
        return bool(text and len(text.strip()) >= 50)
    @staticmethod
    def _extract_pdf_text(pdf_content: bytes, source_id: str = "") -> str:
        return extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=source_id,
            pdf_bytes=pdf_content,
            table="case_law",
            force=True,
        ) or ""

    def _raw_record(
        self,
        shard: dict[str, Any],
        filename: str,
        metadata: dict[str, Any],
        text: str,
    ) -> dict[str, Any]:
        stem = Path(filename).stem
        return {
            "filename": filename,
            "pdf_key": (
                f"{PDF_PREFIX}year={shard['year']}/court={shard['court']}/"
                f"bench={shard['bench']}/{filename}"
            ),
            "metadata": metadata,
            "text": text,
            "court": shard["court"],
            "cnr_number": stem.split("_", 1)[0],
            "date": self._date_from_filename(filename),
        }

    def _iter_raw_tar_records(
        self, shard: dict[str, Any]
    ) -> Generator[dict[str, Any], None, None]:
        data_index = self._read_index(shard["data_index_key"])
        metadata = self._load_metadata(shard)
        pdf_stems = [
            Path(filename).stem
            for part in data_index["parts"]
            for filename in part["files"]
        ]
        if len(set(pdf_stems)) != data_index["file_count"] or set(pdf_stems) != set(
            metadata
        ):
            raise RuntimeError(
                "S3 data/metadata inventory mismatch: "
                f"data={len(set(pdf_stems))} metadata={len(metadata)}"
            )
        cnrs = [stem.split("_", 1)[0] for stem in pdf_stems]
        if len(set(cnrs)) != data_index["file_count"]:
            raise RuntimeError("S3 data inventory contains duplicate CNR identities")
        self._expected_records = data_index["file_count"]
        prefix = self._shard_prefix(DATA_TAR_PREFIX, shard)
        failures: list[str] = []

        with ThreadPoolExecutor(max_workers=self.extract_workers) as executor:
            pending: list[tuple[Future[str], str]] = []

            def resolve(item: tuple[Future[str], str]) -> dict[str, Any] | None:
                future, filename = item
                stem = Path(filename).stem
                try:
                    text = future.result()
                except Exception:  # noqa: BLE001
                    text = ""
                raw = self._raw_record(shard, filename, metadata.get(stem, {}), text)
                normalized = self._normalize_raw(raw)
                if not normalized or stem not in metadata:
                    failures.append(filename)
                    return None
                raw["_normalized_record"] = normalized
                return raw

            for part in data_index["parts"]:
                for filename, pdf in self._archive_members(
                    f"{prefix}{part['name']}",
                    part,
                    label="PDF",
                    member_cap=self.max_pdf_bytes,
                ):
                    self._pdfs_seen += 1
                    source_id = Path(filename).stem.split("_", 1)[0]
                    pending.append(
                        (executor.submit(self._extract_pdf_text, pdf, source_id), filename)
                    )
                    if len(pending) >= self.extract_workers * 2:
                        resolved = resolve(pending.pop(0))
                        if resolved:
                            yield resolved
                    if self.max_pdfs and self._pdfs_seen >= self.max_pdfs:
                        for item in pending:
                            resolved = resolve(item)
                            if resolved:
                                yield resolved
                        if failures:
                            raise RuntimeError(
                                f"Refusing partial sample; {len(failures)} judgment(s) failed"
                            )
                        return

            for item in pending:
                resolved = resolve(item)
                if resolved:
                    yield resolved

        if failures:
            raise RuntimeError(
                f"Refusing to complete whole shard; {len(failures)} judgment(s) failed"
            )
        if self.max_pdfs and self._pdfs_seen < self.max_pdfs:
            raise RuntimeError(
                f"Refusing partial sample; expected {self.max_pdfs}, got {self._pdfs_seen}"
            )

    def fetch_all(self) -> Generator[dict, None, None]:
        shard = self._selected_shard()
        if not self._explicit_output_dir:
            raise RuntimeError("--output-dir is required for shard ingestion")
        if self.max_pdfs and not self._sample_mode:
            raise RuntimeError("--max-pdfs is sample-only")
        if not self._sample_mode and (
            self._published_data_dir.exists()
            or any(
                path.exists()
                for path in (self.storage.records_path, self.storage.index_path)
            )
        ):
            raise RuntimeError("Production job data directory must start empty")
        yield from self._iter_raw_tar_records(shard)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        del since
        yield from self.fetch_all()

    def update(self) -> dict:
        """Exact-shard refreshes use the same failure handling as bootstrap."""
        return self.bootstrap()

    def _quarantine_failed_output(self) -> None:
        paths = [self.storage.records_path, self.storage.index_path]
        if not any(path.exists() for path in paths):
            return
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        destination = self.source_dir / "failed-runs" / run_id
        destination.mkdir(parents=True, exist_ok=False)
        for path in paths:
            if path.exists():
                path.replace(destination / path.name)

    def _publish_output(self) -> None:
        if self._published_data_dir.exists():
            raise RuntimeError("Production job data directory must start empty")
        if self.storage.count() != self._expected_records:
            raise RuntimeError(
                "Completed shard cardinality mismatch: "
                f"stored={self.storage.count()} expected={self._expected_records}"
            )
        self.storage.flush()
        self.storage.data_dir.replace(self._published_data_dir)

    def bootstrap(self, sample_mode: bool = False, sample_size: int = 10) -> dict:
        if sample_size <= 0:
            raise ValueError("sample_size must be positive")
        if self._attempted:
            raise RuntimeError("Each scraper instance supports one attempt")
        self._attempted = True
        status_before = deepcopy(self.status)
        previous_limit = self.max_pdfs
        previous_sample_mode = self._sample_mode
        self._pdfs_seen = 0
        self._sample_mode = sample_mode
        if sample_mode:
            self.max_pdfs = min(previous_limit or sample_size, sample_size)
        try:
            stats = super().bootstrap(sample_mode=sample_mode, sample_size=sample_size)
            if not sample_mode and not stats.get("error_message"):
                try:
                    self._publish_output()
                except (OSError, RuntimeError) as exc:
                    stats["error_message"] = f"Failed to publish completed shard: {exc}"
            if stats.get("error_message") and not sample_mode:
                self._quarantine_failed_output()
                error_message = stats["error_message"]
                self.status = status_before
                self.status["last_error"] = error_message
                self.status.setdefault("run_history", []).append(stats)
                self._save_status()
            return stats
        finally:
            self.max_pdfs = previous_limit
            self._sample_mode = previous_sample_mode

    def bootstrap_fast(
        self,
        max_workers: int | None = None,
        batch_size: int = 100,
        adaptive: bool = True,
    ) -> dict:
        """Fleet-compatible alias; whole-shard retries make batching unnecessary."""
        del max_workers, batch_size, adaptive
        return self.bootstrap()

    def _date_from_filename(self, filename: str) -> str:
        match = re.search(r"(\d{4}-\d{2}-\d{2})(?:\.pdf)?$", filename or "")
        return match.group(1) if match else ""

    def _parse_raw_html(self, raw_html: str) -> dict[str, str]:
        info = {
            "case_number": "",
            "parties": "",
            "judges": "",
            "cnr_number": "",
            "registration_date": "",
            "decision_date": "",
            "disposal_nature": "",
        }
        if not raw_html:
            return info
        html = unescape(raw_html)
        button = re.search(r'aria-label="([^"]+)"', html)
        if button:
            label = re.sub(r"\s+pdf$", "", button.group(1))
            label = re.sub(r"\.Array\[\d+\]\.?\s*", " Vs ", label)
            parts = re.split(r"\s+of\s+", label, maxsplit=1)
            if len(parts) == 2:
                info["case_number"], info["parties"] = map(str.strip, parts)
            else:
                info["parties"] = label.strip()
        else:
            button_text = re.search(r"'>([^<]+)</button>", html)
            if button_text:
                text = re.sub(
                    r"\.Array\[\d+\]\.?\s*", " Vs ", button_text.group(1).strip()
                )
                parts = re.split(r"\s+of\s+", text, maxsplit=1)
                if len(parts) == 2:
                    info["case_number"], info["parties"] = map(str.strip, parts)
                else:
                    info["parties"] = text
        patterns = {
            "judges": r"Judge\s*:\s*([^<]+)",
            "cnr_number": r"CNR\s*:\s*</span><font[^>]*>\s*(\w+)",
            "registration_date": r"Date of registration\s*:\s*</span><font[^>]*>\s*([\d-]+)",
            "decision_date": r"Decision Date\s*:\s*</span><font[^>]*>\s*([\d-]+)",
            "disposal_nature": r"Disposal Nature\s*:\s*</span><font[^>]*>\s*([^<]+)",
        }
        for field, pattern in patterns.items():
            match = re.search(pattern, html)
            if match:
                info[field] = match.group(1).strip()
        return info

    @staticmethod
    def _normalize_date(value: str) -> str:
        for date_format in ("%Y-%m-%d", "%d-%m-%Y"):
            try:
                return datetime.strptime(value or "", date_format).date().isoformat()  # noqa: DTZ007
            except ValueError:
                continue
        return ""

    def normalize(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, raw: dict
    ) -> Optional[dict]:
        precomputed = raw.get("_normalized_record")
        return precomputed if isinstance(precomputed, dict) else self._normalize_raw(raw)

    def _normalize_raw(self, raw: dict) -> Optional[dict]:
        metadata = raw.get("metadata", {}) or {}
        if not isinstance(metadata, dict):
            return None
        parsed = self._parse_raw_html(metadata.get("raw_html", ""))
        text = raw.get("text") or ""
        if not self._usable_text(text):
            return None
        filename = raw.get("filename", "")
        court_name_value = metadata.get("court_name")
        court_code_value = metadata.get("court_code")
        raw_court = raw.get("court")
        if (
            not isinstance(court_name_value, str)
            or not isinstance(court_code_value, str)
            or not isinstance(raw_court, str)
        ):
            return None
        court_name = court_name_value.strip()
        court_code = court_code_value.strip()
        raw_cnr = raw.get("cnr_number") or filename.split("_", 1)[0]
        parsed_cnr = parsed.get("cnr_number") or ""
        raw_date_value = raw.get("date") or ""
        parsed_date_value = parsed.get("decision_date") or ""
        if not isinstance(raw_date_value, str) or not isinstance(parsed_date_value, str):
            return None
        raw_date = self._normalize_date(raw_date_value)
        parsed_date = self._normalize_date(parsed_date_value)
        if (
            not court_name
            or court_code.replace("~", "_") != raw_court
            or (raw_cnr and parsed_cnr and raw_cnr != parsed_cnr)
            or (raw_date_value and not raw_date)
            or (parsed_date_value and not parsed_date)
        ):
            return None
        cnr = raw_cnr or parsed_cnr
        decision_date = parsed_date or raw_date
        if not cnr or not decision_date:
            return None
        title = parsed.get("parties") or parsed.get("case_number") or cnr
        if parsed.get("case_number") and parsed.get("parties"):
            title = f"{parsed['case_number']} - {parsed['parties']}"
        pdf_key = raw.get("pdf_key", "")
        return {
            "_id": cnr,
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": decision_date,
            "url": f"{S3_BASE}/{pdf_key}" if pdf_key else "",
            "cnr_number": cnr,
            "court_name": court_name,
            "court_code": court_code,
            "case_number": parsed.get("case_number", ""),
            "parties": parsed.get("parties", ""),
            "judges": parsed.get("judges", ""),
            "registration_date": self._normalize_date(parsed.get("registration_date", "")) or None,
            "disposal_nature": parsed.get("disposal_nature", ""),
        }


def parse_year_range(value: str | None) -> tuple[int, int] | None:
    if not value:
        return None
    left, separator, right = value.partition("-")
    start = int(left)
    end = int(right) if separator else start
    if start > end:
        raise ValueError("--year-range start must be <= end")
    return start, end


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="IN/HighCourtAWS raw-tar fetcher")
    commands = parser.add_subparsers(dest="command")

    def add_scope(command):
        command.add_argument("--year-range")
        command.add_argument("--court")
        command.add_argument("--bench")
        command.add_argument("--workers", type=int, default=1)
        command.add_argument("--output-dir")
        command.add_argument("--max-pdfs", type=int, help=argparse.SUPPRESS)
        command.add_argument("--full", action="store_true", help=argparse.SUPPRESS)
        command.add_argument(
            "--batch", "--batch-size", dest="batch_size", type=int, help=argparse.SUPPRESS
        )

    for name in ("bootstrap", "bootstrap-fast"):
        command = commands.add_parser(name)
        command.add_argument("--sample", action="store_true")
        command.add_argument("--sample-size", type=int, default=15)
        add_scope(command)
    add_scope(commands.add_parser("update"))
    commands.add_parser("test")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        raise SystemExit(1)

    sample_mode = args.command in {"bootstrap", "bootstrap-fast"} and args.sample
    year_range = parse_year_range(getattr(args, "year_range", None))
    court = getattr(args, "court", None)
    bench = getattr(args, "bench", None)
    if sample_mode or args.command == "test":
        year_range = year_range or (2024, 2024)
        court = court or "11_24"
        bench = bench or "sikkimhc_pg"

    scraper = IndianHighCourtAWSScraper(
        year_range=year_range,
        court=court,
        bench=bench,
        max_pdfs=getattr(args, "max_pdfs", None),
        extract_workers=getattr(args, "workers", 1),
        output_dir=getattr(args, "output_dir", None),
    )

    if args.command == "test":
        shard = scraper._selected_shard()
        data = scraper._read_index(shard["data_index_key"])
        metadata_key = f"{scraper._shard_prefix(METADATA_TAR_PREFIX, shard)}metadata.index.json"
        metadata = scraper._read_index(metadata_key)
        logger.info(
            "Current raw-tar layout OK: data=%d metadata=%d",
            data["file_count"],
            metadata["file_count"],
        )
        return

    if args.command in {"bootstrap", "bootstrap-fast"}:
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=args.sample_size)
    else:
        stats = scraper.update()
    logger.info("Run complete: %s", json.dumps(stats, indent=2, default=str))
    if stats.get("error_message"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
