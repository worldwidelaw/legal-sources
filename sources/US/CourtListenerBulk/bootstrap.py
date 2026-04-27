#!/usr/bin/env python3
"""
US/CourtListenerBulk — Free Law Project bulk dumps via public S3.

Streams quarterly CSV dumps from `s3://com-courtlistener-storage/bulk-data/`
(public, anonymous access), joins courts/dockets/opinion-clusters/opinions
in memory, and emits LDH-shaped case-law documents.

Implements the BaseScraper contract (fetch_all / fetch_updates / normalize)
plus a custom run_sample() override that scans only the heads of the
supporting tables — sample mode would otherwise pull GBs of clusters and
dockets just to emit 10 records.

Usage:
    python bootstrap.py bootstrap --sample --sample-size 10
    python bootstrap.py bootstrap                   # full ingest
    python bootstrap.py bootstrap-fast --workers 1  # via runner.py fast
    python bootstrap.py update                      # incremental (re-runs full pass; storage dedups)
"""

import bz2
import csv
import io
import json
import logging
import os
import re
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, Iterator, Optional, Set

# Add project root so we can import from common/
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import boto3
from botocore import UNSIGNED
from botocore.config import Config

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter")

SOURCE_ID = "US/CourtListenerBulk"

# Allow CSV fields to exceed Python's default 128 KB limit. Some
# html_with_citations columns approach 1–2 MB.
csv.field_size_limit(sys.maxsize)

# Text fallback order — first non-empty column wins.
TEXT_COLUMN_PRIORITY = (
    "html_with_citations",
    "html_columbia",
    "html_lawbox",
    "html_anon_2020",
    "xml_harvard",
    "html",
    "plain_text",
)

DUMP_DATE_RE = re.compile(r"-(\d{4}-\d{2}-\d{2})\.csv\.bz2$")


class _TeeBody:
    """
    Wraps a botocore StreamingBody (or any object with .read(n)) so that
    every byte pulled is also written to a local cache file. The cache
    file is renamed into place atomically only on a clean finalize() —
    crash mid-download leaves no half-cached file to confuse a later run.
    """

    def __init__(self, body, dest_path: Path):
        self._body = body
        self._dest = dest_path
        self._tmp = dest_path.with_suffix(dest_path.suffix + ".part")
        self._fp = open(self._tmp, "wb")
        self._saw_eof = False

    def read(self, n: int = -1) -> bytes:
        chunk = self._body.read(n)
        if chunk:
            self._fp.write(chunk)
        else:
            self._saw_eof = True
        return chunk

    def finalize(self) -> None:
        """
        Promote the .part file to its final name only if the body was
        fully consumed (read returned an empty chunk at least once).
        Otherwise discard — a half-downloaded file would confuse the
        next run, which checks for `local_path.exists()`.
        """
        try:
            self._fp.close()
        except Exception:
            pass
        if not self._saw_eof:
            try:
                self._tmp.unlink()
            except Exception:
                pass
            return
        try:
            self._tmp.replace(self._dest)
            logger.info(f"Cached → {self._dest}")
        except Exception as exc:
            logger.warning(f"Cache finalize failed for {self._dest}: {exc}")
            try:
                self._tmp.unlink()
            except Exception:
                pass


class _BZ2DecompressingStream(io.RawIOBase):
    """
    Binary-stream adapter over a botocore StreamingBody that decompresses
    bz2 on the fly. Plays nicely with io.BufferedReader + io.TextIOWrapper,
    which is what we want under csv.reader so multi-line quoted records
    parse correctly.

    `max_bytes`, when set, caps the number of *compressed* bytes pulled
    from the underlying stream — used by sample-mode scans to bound I/O
    on the multi-GB tables.
    """

    PROGRESS_INTERVAL = 25 * 1024 * 1024  # log every 25 MB compressed

    def __init__(self, body, max_bytes: Optional[int] = None, on_cap=None):
        self._body = body
        self._decomp = bz2.BZ2Decompressor()
        self._buffer = bytearray()
        self._raw_read = 0
        self._next_progress_at = self.PROGRESS_INTERVAL
        self._max = max_bytes
        self._on_cap = on_cap
        self._cap_logged = False
        self._eof = False

    def readable(self) -> bool:
        return True

    def _fill(self, target: int) -> None:
        while not self._eof and len(self._buffer) < target:
            if self._max is not None and self._raw_read >= self._max:
                if not self._cap_logged and self._on_cap is not None:
                    self._on_cap(self._raw_read)
                    self._cap_logged = True
                self._eof = True
                break
            chunk = self._body.read(1024 * 1024)
            if not chunk:
                self._eof = True
                break
            self._raw_read += len(chunk)
            if self._raw_read >= self._next_progress_at:
                logger.info(f"  … read {self._raw_read // (1024*1024)} MB compressed")
                self._next_progress_at += self.PROGRESS_INTERVAL
            try:
                self._buffer.extend(self._decomp.decompress(chunk))
            except OSError as exc:
                logger.error(f"bz2 decompress error: {exc}")
                self._eof = True
                break

    def readinto(self, buf) -> int:
        n = len(buf)
        if n == 0:
            return 0
        self._fill(n)
        if not self._buffer:
            return 0
        take = min(n, len(self._buffer))
        buf[:take] = self._buffer[:take]
        del self._buffer[:take]
        return take


def _csv_bool(v: Any) -> bool:
    """CourtListener exports booleans as 't' / 'f' strings."""
    return str(v).strip().lower() == "t"


class _HTMLTextExtractor(HTMLParser):
    """Extract plain text from HTML, discarding all tags."""

    def __init__(self):
        super().__init__()
        self._pieces: list = []

    def handle_data(self, data):
        self._pieces.append(data)

    def get_text(self):
        return "".join(self._pieces)


def _strip_html(html: str) -> str:
    """Remove HTML tags and collapse excessive whitespace."""
    extractor = _HTMLTextExtractor()
    extractor.feed(html)
    text = extractor.get_text()
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _empty_to_none(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _normalize_date(v: Any) -> Optional[str]:
    """Coerce a CourtListener date column into a YYYY-MM-DD string."""
    s = _empty_to_none(v)
    if not s:
        return None
    # Already YYYY-MM-DD
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    # Timestamps like '2022-07-29 03:26:30.717838+00'
    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        return s[:10]
    return s


class CourtListenerBulkScraper(BaseScraper):
    """
    Streams CourtListener bulk CSV dumps from public S3 and emits joined
    case-law documents.
    """

    def __init__(self):
        super().__init__(Path(__file__).parent)

        s3_cfg = self.config.get("s3", {})
        self.bucket = s3_cfg.get("bucket", "com-courtlistener-storage")
        self.region = s3_cfg.get("region", "us-west-2")
        self.prefix = s3_cfg.get("prefix", "bulk-data/")
        self.file_patterns = self.config.get("files", {})

        sample_cfg = self.config.get("sample", {})
        self.sample_streaming_max_bytes = int(
            sample_cfg.get("streaming_max_bytes", 500 * 1024 * 1024)
        )

        # Optional on-disk cache. When set (CLBULK_CACHE_DIR env var),
        # _stream_csv() downloads each S3 object to this directory once,
        # then streams from disk on subsequent calls. Sample-mode reruns
        # become near-instant. Hetzner can use this too — point it at a
        # large scratch volume.
        cache_dir_env = os.environ.get("CLBULK_CACHE_DIR")
        self._cache_dir = Path(cache_dir_env) if cache_dir_env else None
        if self._cache_dir is not None:
            self._cache_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"S3 cache dir: {self._cache_dir}")

        self._s3 = boto3.client(
            "s3",
            region_name=self.region,
            config=Config(signature_version=UNSIGNED),
        )

        # Resolved at first use; populated by _resolve_dump_date().
        self._dump_date: Optional[str] = None

        # Populated by _load_indexes() for full ingest, or by run_sample()
        # for sample mode.
        self._courts: Optional[Dict[str, Dict[str, Any]]] = None
        self._dockets: Optional[Dict[str, Dict[str, Any]]] = None
        self._clusters: Optional[Dict[str, Dict[str, Any]]] = None

        # SQLite-backed index for full-mode ingest (avoids OOM on 4GB VPS
        # when dockets table has 16M+ rows).
        self._db: Optional[sqlite3.Connection] = None
        self._db_path: Optional[Path] = None

    # ── S3 streaming ──────────────────────────────────────────────────

    def _resolve_dump_date(self) -> str:
        """Discover the most recent date stamp shared by all required files."""
        if self._dump_date:
            return self._dump_date

        # Find the latest dump that has all four required tables
        # (citation_map is optional and ignored for the join).
        required = ["courts", "dockets", "opinion_clusters", "opinions"]
        date_sets = []
        for table in required:
            pattern = self.file_patterns.get(table, "")
            stem = pattern.split("{date}")[0]  # e.g., "courts-"
            dates: Set[str] = set()
            paginator = self._s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(
                Bucket=self.bucket, Prefix=f"{self.prefix}{stem}"
            ):
                for obj in page.get("Contents", []) or []:
                    m = DUMP_DATE_RE.search(obj["Key"])
                    if m and obj.get("Size", 0) > 1024:  # skip 14-byte placeholders
                        dates.add(m.group(1))
            date_sets.append(dates)

        common = set.intersection(*date_sets) if date_sets else set()
        if not common:
            raise RuntimeError(
                f"No dump date is shared across all required tables in s3://{self.bucket}/{self.prefix}"
            )

        latest = max(common)
        logger.info(f"Using bulk dump date: {latest}")
        self._dump_date = latest
        return latest

    def _key_for(self, table: str) -> str:
        date = self._resolve_dump_date()
        pattern = self.file_patterns[table]
        return f"{self.prefix}{pattern.format(date=date)}"

    def _stream_csv(
        self,
        key: str,
        max_bytes: Optional[int] = None,
    ) -> Iterator[Dict[str, str]]:
        """
        Yield CSV rows as dicts from a `.csv.bz2` object on S3.

        Builds a real binary stream over (S3 body → bz2 → BufferedReader →
        TextIOWrapper) and hands it to csv.reader, so multi-line quoted fields
        (CourtListener's `html_with_citations`, `xml_harvard`, etc. routinely
        contain raw newlines inside quotes) parse correctly.

        Stops early when `max_bytes` of compressed bytes have been read
        (used by sample-mode scans to cap I/O on the multi-GB tables).
        """
        # Cache hit → stream from disk; otherwise pull from S3 and
        # (optionally) tee into the cache as we go.
        local_path = None
        if self._cache_dir is not None:
            local_path = self._cache_dir / key.replace("/", "_")
            if local_path.exists() and local_path.stat().st_size > 1024:
                logger.info(f"Streaming cached {local_path}")
                source = open(local_path, "rb")
                binary = _BZ2DecompressingStream(
                    source, max_bytes=max_bytes, on_cap=self._log_cap
                )
                text = io.TextIOWrapper(
                    io.BufferedReader(binary, buffer_size=1024 * 1024),
                    encoding="utf-8",
                    errors="replace",
                    newline="",
                )
                yield from self._iter_csv(text)
                return

        logger.info(f"Streaming s3://{self.bucket}/{key}")
        obj = self._s3.get_object(Bucket=self.bucket, Key=key)
        body = obj["Body"]
        if local_path is not None:
            # Wrap the body so each chunk pulled is also written to disk —
            # next run can read straight from local file.
            body = _TeeBody(body, local_path)

        binary = _BZ2DecompressingStream(body, max_bytes=max_bytes, on_cap=self._log_cap)
        text = io.TextIOWrapper(
            io.BufferedReader(binary, buffer_size=1024 * 1024),
            encoding="utf-8",
            errors="replace",
            newline="",  # let csv.reader handle row delimiting
        )
        try:
            yield from self._iter_csv(text)
        finally:
            if local_path is not None and isinstance(body, _TeeBody):
                body.finalize()

    @staticmethod
    def _iter_csv(text: io.TextIOWrapper) -> Iterator[Dict[str, str]]:
        """
        Yield CSV rows as dicts.

        CourtListener exports use PostgreSQL COPY ... CSV with backslash-
        escaped quotes (e.g. <opinion type=\\"majority\\">) inside double-
        quoted fields, NOT the standard CSV "" doubling. Configure
        csv.reader to match: doublequote=False, escapechar='\\'.
        """
        reader = csv.reader(text, doublequote=False, escapechar="\\")
        try:
            header = next(reader)
        except StopIteration:
            return

        for row in reader:
            if len(row) != len(header):
                # Mid-record truncation when the cap fires can produce a
                # partial final row. Drop it.
                continue
            yield dict(zip(header, row))

    @staticmethod
    def _log_cap(bytes_read: int) -> None:
        logger.info(f"  stopped early at {bytes_read:,} compressed bytes (cap reached)")

    # ── Index loading (full mode) ────────────────────────────────────

    def _load_indexes(self) -> None:
        """Load courts into memory; stream dockets and clusters into a
        temporary on-disk SQLite database so the 4GB VPS doesn't OOM."""
        if self._courts is not None:
            return

        logger.info("Loading courts table…")
        self._courts = {row["id"]: row for row in self._stream_csv(self._key_for("courts"))}
        logger.info(f"  → {len(self._courts):,} courts loaded")

        # Dockets (16M+ rows) and clusters (10M+ rows) are too large for
        # in-memory dicts on a 4GB VPS.  Stream them into a temporary
        # SQLite file — disk is cheap, RAM is not.
        db_fd, db_path = tempfile.mkstemp(suffix=".db", prefix="clbulk_")
        os.close(db_fd)
        self._db_path = Path(db_path)
        self._db = sqlite3.connect(str(self._db_path))
        self._db.execute("PRAGMA journal_mode=OFF")
        self._db.execute("PRAGMA synchronous=OFF")
        self._db.execute("PRAGMA cache_size=-65536")  # 64 MB page cache

        # --- dockets (only the columns we join on) ---
        self._db.execute(
            "CREATE TABLE dockets ("
            "  id TEXT PRIMARY KEY,"
            "  court_id TEXT,"
            "  case_name TEXT,"
            "  docket_number TEXT,"
            "  date_filed TEXT,"
            "  blocked INTEGER DEFAULT 0"
            ")"
        )
        logger.info("Loading dockets table into SQLite (this is the big one)…")
        batch: list = []
        count = 0
        for row in self._stream_csv(self._key_for("dockets")):
            batch.append((
                row.get("id", ""),
                row.get("court_id", ""),
                row.get("case_name", ""),
                row.get("docket_number", ""),
                row.get("date_filed", ""),
                1 if _csv_bool(row.get("blocked", "")) else 0,
            ))
            count += 1
            if len(batch) >= 50_000:
                self._db.executemany(
                    "INSERT OR IGNORE INTO dockets VALUES (?,?,?,?,?,?)", batch
                )
                batch.clear()
            if count % 1_000_000 == 0:
                logger.info(f"  … {count:,} dockets")
        if batch:
            self._db.executemany(
                "INSERT OR IGNORE INTO dockets VALUES (?,?,?,?,?,?)", batch
            )
        self._db.commit()
        logger.info(f"  → {count:,} dockets loaded into SQLite")

        # --- opinion_clusters ---
        self._db.execute(
            "CREATE TABLE clusters ("
            "  id TEXT PRIMARY KEY,"
            "  docket_id TEXT,"
            "  case_name TEXT,"
            "  case_name_short TEXT,"
            "  case_name_full TEXT,"
            "  slug TEXT,"
            "  date_filed TEXT,"
            "  precedential_status TEXT,"
            "  blocked INTEGER DEFAULT 0"
            ")"
        )
        logger.info("Loading opinion_clusters table into SQLite…")
        batch = []
        count = 0
        for row in self._stream_csv(self._key_for("opinion_clusters")):
            batch.append((
                row.get("id", ""),
                row.get("docket_id", ""),
                row.get("case_name", ""),
                row.get("case_name_short", ""),
                row.get("case_name_full", ""),
                row.get("slug", ""),
                row.get("date_filed", ""),
                row.get("precedential_status", ""),
                1 if _csv_bool(row.get("blocked", "")) else 0,
            ))
            count += 1
            if len(batch) >= 50_000:
                self._db.executemany(
                    "INSERT OR IGNORE INTO clusters VALUES (?,?,?,?,?,?,?,?,?)",
                    batch,
                )
                batch.clear()
            if count % 500_000 == 0:
                logger.info(f"  … {count:,} clusters")
        if batch:
            self._db.executemany(
                "INSERT OR IGNORE INTO clusters VALUES (?,?,?,?,?,?,?,?,?)",
                batch,
            )
        self._db.commit()
        logger.info(f"  → {count:,} clusters loaded into SQLite")

    # ── Lookups (dict for sample mode, SQLite for full mode) ────────

    def _lookup_cluster(self, cluster_id: str) -> Optional[Dict[str, Any]]:
        """Look up a cluster by ID — in-memory dict or SQLite."""
        if self._clusters is not None:
            return self._clusters.get(cluster_id)
        if self._db is not None:
            row = self._db.execute(
                "SELECT docket_id, case_name, case_name_short, case_name_full, "
                "slug, date_filed, precedential_status, blocked "
                "FROM clusters WHERE id = ?",
                (cluster_id,),
            ).fetchone()
            if row is None:
                return None
            return {
                "id": cluster_id, "docket_id": row[0],
                "case_name": row[1], "case_name_short": row[2],
                "case_name_full": row[3], "slug": row[4],
                "date_filed": row[5], "precedential_status": row[6],
                "blocked": "t" if row[7] else "f",
            }
        return None

    def _lookup_docket(self, docket_id: str) -> Optional[Dict[str, Any]]:
        """Look up a docket by ID — in-memory dict or SQLite."""
        if self._dockets is not None:
            return self._dockets.get(docket_id)
        if self._db is not None:
            row = self._db.execute(
                "SELECT court_id, case_name, docket_number, date_filed, blocked "
                "FROM dockets WHERE id = ?",
                (docket_id,),
            ).fetchone()
            if row is None:
                return None
            return {
                "id": docket_id, "court_id": row[0],
                "case_name": row[1], "docket_number": row[2],
                "date_filed": row[3], "blocked": "t" if row[4] else "f",
            }
        return None

    def _cleanup_db(self) -> None:
        """Close and remove the temporary SQLite database."""
        if self._db is not None:
            try:
                self._db.close()
            except Exception:
                pass
            self._db = None
        if self._db_path is not None:
            try:
                self._db_path.unlink(missing_ok=True)
            except Exception:
                pass
            self._db_path = None

    # ── Joining ─────────────────────────────────────────────────────

    def _join(self, opinion: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """
        Build a joined record for one opinion row, or return None if the
        opinion should be skipped (missing cluster, blocked privacy flag, etc.).
        """
        cluster_id = _empty_to_none(opinion.get("cluster_id"))
        if not cluster_id:
            return None

        cluster = self._lookup_cluster(cluster_id)
        docket = None
        court = None
        if cluster:
            if _csv_bool(cluster.get("blocked", "")):
                # Privacy redaction — skip entirely, do not emit.
                return None
            docket_id = _empty_to_none(cluster.get("docket_id"))
            if docket_id:
                docket = self._lookup_docket(docket_id)
                if docket and _csv_bool(docket.get("blocked", "")):
                    return None
                if docket:
                    court_id = _empty_to_none(docket.get("court_id"))
                    if court_id:
                        court = (self._courts or {}).get(court_id)

        return {
            "opinion": opinion,
            "cluster": cluster,
            "docket": docket,
            "court": court,
        }

    # ── BaseScraper required methods ─────────────────────────────────

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Full pass: load join tables, stream opinions, yield joined records."""
        self._load_indexes()
        logger.info("Streaming opinions table and joining…")
        try:
            for opinion in self._stream_csv(self._key_for("opinions")):
                joined = self._join(opinion)
                if joined is None:
                    continue
                yield joined
        finally:
            self._cleanup_db()

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """
        Bulk dumps are quarterly snapshots, so 'updates' = a fresh full pass.
        The storage layer deduplicates by `_id`, so re-emitting unchanged
        opinions is a cheap no-op.
        """
        del since  # unused; the bulk format doesn't expose per-row mtimes
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Project a joined record into the LDH document shape."""
        opinion = raw.get("opinion") or {}
        cluster = raw.get("cluster") or {}
        docket = raw.get("docket") or {}
        court = raw.get("court") or {}

        opinion_id = _empty_to_none(opinion.get("id"))
        cluster_id = _empty_to_none(opinion.get("cluster_id"))
        if not opinion_id or not cluster_id:
            return None

        # Pick the richest available text representation and strip HTML.
        text = ""
        for column in TEXT_COLUMN_PRIORITY:
            value = opinion.get(column) or ""
            if value and value.strip():
                text = _strip_html(value)
                break

        title = (
            _empty_to_none(cluster.get("case_name"))
            or _empty_to_none(cluster.get("case_name_short"))
            or _empty_to_none(cluster.get("case_name_full"))
            or _empty_to_none(docket.get("case_name"))
            or f"CourtListener opinion {opinion_id}"
        )

        slug = _empty_to_none(cluster.get("slug")) or "opinion"
        url = f"https://www.courtlistener.com/opinion/{cluster_id}/{slug}/"

        date = _normalize_date(cluster.get("date_filed")) or _normalize_date(
            docket.get("date_filed")
        )

        return {
            "_id": f"cl-opinion-{opinion_id}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "court": _empty_to_none(court.get("full_name"))
            or _empty_to_none(court.get("short_name")),
            "court_id": _empty_to_none(docket.get("court_id")),
            "court_jurisdiction": _empty_to_none(court.get("jurisdiction")),
            "case_number": _empty_to_none(docket.get("docket_number")),
            "case_name_full": _empty_to_none(cluster.get("case_name_full")),
            "author": _empty_to_none(opinion.get("author_str")),
            "per_curiam": _csv_bool(opinion.get("per_curiam", "")),
            "opinion_type": _empty_to_none(opinion.get("type")),
            "precedential_status": _empty_to_none(cluster.get("precedential_status")),
            "opinion_id": opinion_id,
            "cluster_id": cluster_id,
            "docket_id": _empty_to_none(cluster.get("docket_id")),
            "page_count": _empty_to_none(opinion.get("page_count")),
            "extracted_by_ocr": _csv_bool(opinion.get("extracted_by_ocr", "")),
            "license": "Public domain (US government works); no known copyright restrictions",
            "original_source": "CourtListener / Free Law Project",
        }

    # ── Sample mode override ─────────────────────────────────────────

    def run_sample(self, n: int = 10) -> Dict[str, Any]:
        """
        Opinion-first sample.

        The bulk CSVs are not sorted by the same key, so a cluster-first
        approach (read clusters, find dockets, then scan opinions) fails
        because the cluster IDs from the head of the clusters file don't
        appear in the first portion of the opinions file.

        Instead we work opinion-first:

          1. Stream `opinions` — read a large pool of raw opinions that
             have non-empty text. These are our candidates; we already
             have the opinion text at this point.
          2. Stream `opinion-clusters` looking for cluster_ids that match
             our opinion pool. Build an index of docket_ids needed.
          3. Stream `dockets` looking for the docket_ids collected in
             step 2. Stop early once we have enough fully-joined records.
          4. Load courts (tiny — full).
          5. Join and emit.

        Hetzner runs `bootstrap()` / `bootstrap_fast()`, which fully load
        all three tables and stream opinions exhaustively — no caps.
        """
        started_at = datetime.now(timezone.utc).isoformat()
        cap = self.sample_streaming_max_bytes
        logger.info(
            f"Sample mode (n={n}, max_bytes per scan={cap:,}) — "
            f"resolving latest dump…"
        )
        self._resolve_dump_date()

        # 1. Stream opinions. Collect a pool of opinions that have text.
        # We need enough candidates that the subsequent cluster/docket
        # lookups find matches within their byte caps.
        opinion_pool: Dict[str, Dict[str, Any]] = {}  # cluster_id -> opinion
        opinion_pool_cap = 5000
        rows_seen = 0
        for opinion in self._stream_csv(self._key_for("opinions"), max_bytes=cap):
            rows_seen += 1
            if rows_seen % 10_000 == 0:
                logger.info(
                    f"    opinion scan: {rows_seen:,} rows, "
                    f"{len(opinion_pool):,}/{opinion_pool_cap} with text"
                )
            # Only keep opinions that have actual text content.
            has_text = False
            for col in TEXT_COLUMN_PRIORITY:
                val = opinion.get(col, "")
                if val and val.strip():
                    has_text = True
                    break
            if not has_text:
                continue
            cid = _empty_to_none(opinion.get("cluster_id"))
            if not cid or cid in opinion_pool:
                continue
            opinion_pool[cid] = opinion
            if len(opinion_pool) >= opinion_pool_cap:
                break
        logger.info(f"  → collected {len(opinion_pool)} opinions with text")

        target_cluster_ids = set(opinion_pool.keys())

        # 2. Stream clusters looking for IDs that match our opinion pool.
        clusters: Dict[str, Dict[str, Any]] = {}
        needed_docket_ids: Dict[str, str] = {}  # docket_id -> cluster_id
        rows_seen = 0
        for cluster in self._stream_csv(
            self._key_for("opinion_clusters"), max_bytes=cap
        ):
            rows_seen += 1
            if rows_seen % 250_000 == 0:
                logger.info(
                    f"    cluster scan: {rows_seen:,} rows, "
                    f"{len(clusters):,} matched"
                )
            cid = cluster.get("id")
            if cid not in target_cluster_ids:
                continue
            if _csv_bool(cluster.get("blocked", "")):
                continue
            did = _empty_to_none(cluster.get("docket_id"))
            if not did:
                continue
            clusters[cid] = cluster
            needed_docket_ids[did] = cid
            # Stop once we have plenty of matches
            if len(clusters) >= n * 30:
                break
        logger.info(f"  → matched {len(clusters)} clusters")

        # 3. Stream dockets looking for IDs needed by our matched clusters.
        dockets: Dict[str, Dict[str, Any]] = {}
        rows_seen = 0
        for docket in self._stream_csv(self._key_for("dockets"), max_bytes=cap):
            rows_seen += 1
            if rows_seen % 250_000 == 0:
                logger.info(
                    f"    docket scan: {rows_seen:,} rows, "
                    f"{len(dockets):,} matched"
                )
            if _csv_bool(docket.get("blocked", "")):
                continue
            did = docket.get("id")
            if did not in needed_docket_ids:
                continue
            dockets[did] = docket
            if len(dockets) >= len(needed_docket_ids):
                break  # Found all needed dockets
        logger.info(f"  → matched {len(dockets)} dockets")

        # 4. Load courts (small file, no cap needed).
        courts: Dict[str, Dict[str, Any]] = {
            row["id"]: row for row in self._stream_csv(self._key_for("courts"))
        }
        logger.info(f"  → {len(courts)} courts")

        self._courts, self._dockets, self._clusters = courts, dockets, clusters

        # 5. Build sample records from fully-joined opinions.
        records: list = []
        for cid, opinion in opinion_pool.items():
            if cid not in clusters:
                continue
            cluster = clusters[cid]
            did = _empty_to_none(cluster.get("docket_id"))
            if did not in dockets:
                continue
            joined = self._join(opinion)
            if joined is None:
                continue
            record = self.normalize(joined)
            if record is None:
                continue
            # Skip records with empty text after normalization
            if not record.get("text", "").strip():
                continue
            records.append(record)
            if len(records) >= n:
                break

        if len(records) < n:
            logger.warning(
                f"Only produced {len(records)} samples (target {n}). "
                f"Increase sample.streaming_max_bytes in config.yaml."
            )

        self._save_samples(records)

        stats = {
            "started_at": started_at,
            "mode": "sample",
            "sample_records_saved": len(records),
            "opinions_in_pool": len(opinion_pool),
            "clusters_matched": len(clusters),
            "dockets_matched": len(dockets),
            "dump_date": self._dump_date,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        self.status.setdefault("run_history", []).append(stats)
        self.status["last_run"] = stats["finished_at"]
        self._save_status()

        return stats


# ── CLI entry point ─────────────────────────────────────────────────


def _parse_int_flag(argv: list, flag: str, default: int) -> int:
    if flag in argv:
        idx = argv.index(flag)
        if idx + 1 < len(argv):
            try:
                return int(argv[idx + 1])
            except ValueError:
                pass
    return default


def main():
    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py "
            "[bootstrap|bootstrap-fast|update] [--sample] [--sample-size N] [--workers N] [--batch-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    scraper = CourtListenerBulkScraper()

    if command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        sample_size = _parse_int_flag(sys.argv, "--sample-size", 10)
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: {stats['sample_records_saved']} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats.get('records_updated', 0)} updated, "
                f"{stats.get('records_skipped', 0)} skipped"
            )
    elif command == "bootstrap-fast":
        workers = _parse_int_flag(sys.argv, "--workers", 1)
        batch_size = _parse_int_flag(sys.argv, "--batch-size", 100)
        stats = scraper.bootstrap_fast(max_workers=workers, batch_size=batch_size)
        print(
            f"\nFast bootstrap complete: {stats['records_new']} new, "
            f"{stats.get('records_updated', 0)} updated, "
            f"{stats['errors']} errors"
        )
    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats.get('records_updated', 0)} updated"
        )
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

    print(json.dumps(stats, indent=2, default=str))


if __name__ == "__main__":
    main()
