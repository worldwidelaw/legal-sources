"""
Base scraper class that all source-specific scrapers inherit from.

Handles:
- Config loading
- Authentication
- Rate limiting
- Idempotent upsert / append-only logic
- Sample mode (fetch N documents for testing)
- Status tracking
"""

import os
import sys
import json
import time
import yaml
import hashlib
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed

from .rate_limiter import RateLimiter, AdaptiveRateLimiter
from .storage import StorageManager, scrub_surrogates
from .validators import SchemaValidator

logger = logging.getLogger("legal-data-hunter")


class BaseScraper(ABC):
    """
    Abstract base class for all Legal Data Hunter scrapers.

    Each data source implements:
      - fetch_all()       -> generator yielding all documents (for bootstrap)
      - fetch_updates()   -> generator yielding documents modified since last_run
      - normalize(raw)    -> transform raw API/HTML response into standard schema

    The base class handles everything else: config, auth, rate limiting,
    deduplication, storage, and status tracking.
    """

    # Captured at subclass-definition time; the resolved path to the module
    # (bootstrap.py) that defines each concrete scraper. See __init_subclass__.
    _module_file: Optional[str] = None

    def __init_subclass__(cls, **kwargs):
        """Record the defining module's file path when a scraper subclass is
        created.

        ``inspect.getfile(type(self))`` fails (raises ``TypeError: is a
        built-in class``) when the subclass module is loaded via
        ``importlib.util.exec_module`` **without** being registered in
        ``sys.modules`` — which is exactly what the fleet's bootstrap-fast
        wrapper does when it instantiates ``ScraperClass()`` with no args.
        When that happens, source_dir silently fell back to ``os.getcwd()``
        (a temp working dir the fleet cleans up), so every fetched record was
        written outside the source tree and lost before ingest (issue #1171).

        At class-definition time the module globals still carry ``__file__``,
        so capture it here from the class-statement frame — robust regardless
        of how the module was later (un)registered.
        """
        super().__init_subclass__(**kwargs)
        try:
            mod = sys.modules.get(cls.__module__)
            module_file = getattr(mod, "__file__", None)
            if not module_file:
                # Module isn't registered in sys.modules (importlib exec_module).
                # Walk the call stack for the frame that actually defines this
                # class — the ABCMeta machinery inserts an abc.py frame between
                # __init_subclass__ and the module body, so f_back alone is
                # wrong. Match on the module name to find the real defining frame.
                import inspect
                frame = inspect.currentframe()
                while frame is not None:
                    fg = frame.f_globals
                    if fg.get("__name__") == cls.__module__ and fg.get("__file__"):
                        module_file = fg["__file__"]
                        break
                    frame = frame.f_back
            if module_file:
                cls._module_file = str(Path(module_file).resolve())
        except Exception:
            pass

    def __init__(self, source_dir: Optional[str] = None):
        """
        Initialize the scraper from a source directory.

        Args:
            source_dir: Path to the source directory (e.g., sources/FR/legifrance/).
                If omitted, it is resolved to the directory containing the
                subclass's module file (its bootstrap.py). This lets callers
                that instantiate a scraper class by introspection — e.g. the
                VPS bootstrap-fast wrapper doing ``ScraperClass()`` — work
                without knowing the source path (see issue #857).
        """
        if source_dir is None:
            source_dir = self._resolve_source_dir()
        self.source_dir = Path(source_dir)
        self.config = self._load_config()
        self.status = self._load_status()
        self.rate_limiter = RateLimiter(
            requests_per_second=self.config.get("rate_limit", {}).get("requests_per_second", 2),
            burst=self.config.get("rate_limit", {}).get("burst", 5),
        )
        self.storage = StorageManager(self.source_dir / "data")
        self.validator = SchemaValidator(self.config.get("schema", {}))
        self._auth_headers = self._setup_auth()

    def _resolve_source_dir(self) -> str:
        """Resolve the source directory from the subclass's module file.

        Used when no source_dir is passed to __init__ (e.g. a wrapper that
        instantiates the scraper class by introspection). Falls back to the
        current working directory if the module file cannot be located.
        """
        # Prefer the path captured at subclass-definition time — it survives
        # importlib loads that never register the module in sys.modules, which
        # is what breaks inspect.getfile below (see __init_subclass__, #1171).
        if self._module_file:
            return str(Path(self._module_file).resolve().parent)

        import inspect

        try:
            module_file = inspect.getfile(type(self))
            return str(Path(module_file).resolve().parent)
        except (TypeError, OSError):
            return os.getcwd()

    def _load_config(self) -> dict:
        """Load config.yaml for this source.

        Resolution order:
          1. ``self.source_dir / config.yaml``
          2. The directory containing the subclass's module file. This rescues
             generic VPS wrappers that import/exec a scraper from a working
             directory (e.g. ``/tmp/legal-bootstrap``) where ``source_dir`` was
             mis-resolved to the CWD (see issue #863).

        If config.yaml cannot be found in any location, fall back to safe
        defaults (an empty dict) with a warning rather than crashing. Every
        config consumer in this class reads keys via ``.get(..., default)``,
        so a missing config degrades gracefully instead of aborting the run.
        """
        candidates = [self.source_dir / "config.yaml"]

        module_dir = None
        if self._module_file:
            module_dir = Path(self._module_file).resolve().parent
        else:
            import inspect
            try:
                module_dir = Path(inspect.getfile(type(self))).resolve().parent
            except (TypeError, OSError):
                module_dir = None
        if module_dir is not None:
            module_config = module_dir / "config.yaml"
            if module_config not in candidates:
                candidates.append(module_config)

        for config_path in candidates:
            if config_path.exists():
                if config_path.parent != self.source_dir:
                    # Keep storage/status alongside the real config.
                    self.source_dir = config_path.parent
                with open(config_path, "r") as f:
                    return yaml.safe_load(f) or {}

        logging.getLogger("legal-data-hunter").warning(
            "No config.yaml found in %s; falling back to default config.",
            " or ".join(str(c.parent) for c in candidates),
        )
        return {}

    def _load_status(self) -> dict:
        """Load or initialize status.yaml for this source."""
        status_path = self.source_dir / "status.yaml"
        if status_path.exists():
            with open(status_path, "r") as f:
                return yaml.safe_load(f) or {}
        return {
            "last_run": None,
            "last_bootstrap": None,
            "total_records": 0,
            "last_error": None,
            "run_history": [],
        }

    def _save_status(self):
        """Persist status.yaml."""
        status_path = self.source_dir / "status.yaml"
        with open(status_path, "w") as f:
            yaml.dump(self.status, f, default_flow_style=False, allow_unicode=True)

    def _setup_auth(self) -> dict:
        """
        Build auth headers from config + environment variables.
        Returns a dict of HTTP headers.
        """
        auth_config = self.config.get("auth", {})
        auth_type = auth_config.get("type", "none")

        if auth_type == "none":
            return {}

        if auth_type == "api_key":
            env_var = auth_config.get("env_var")
            if not env_var:
                raise ValueError(f"auth.type is api_key but no env_var specified in config")

            api_key = os.environ.get(env_var)
            if not api_key:
                # Try loading from .env file in source dir
                api_key = self._load_env_var(env_var)

            if not api_key:
                raise EnvironmentError(
                    f"API key not found. Set {env_var} in environment or in {self.source_dir}/.env"
                )

            header_name = auth_config.get("header", "Authorization")
            header_prefix = auth_config.get("prefix", "")
            value = f"{header_prefix}{api_key}" if header_prefix else api_key
            return {header_name: value}

        if auth_type == "oauth2":
            # OAuth2 client credentials flow
            env_var_id = auth_config.get("client_id_env")
            env_var_secret = auth_config.get("client_secret_env")
            client_id = os.environ.get(env_var_id) or self._load_env_var(env_var_id)
            client_secret = os.environ.get(env_var_secret) or self._load_env_var(env_var_secret)
            if not client_id or not client_secret:
                raise EnvironmentError(
                    f"OAuth2 credentials not found. Set {env_var_id} and {env_var_secret}"
                )
            # Token acquisition is delegated to subclass
            return {"_oauth2_client_id": client_id, "_oauth2_client_secret": client_secret}

        return {}

    def _load_env_var(self, var_name: str) -> Optional[str]:
        """Load a variable from .env file in source directory."""
        env_path = self.source_dir / ".env"
        if not env_path.exists():
            return None
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    if key.strip() == var_name:
                        return value.strip().strip("'\"")
        return None

    def _dedup_key(self, record: dict) -> str:
        """
        Generate a deduplication key for a record based on config.

        For legislation (upsert model): key is typically article_id + version_date
        For case_law (append_only model): key is typically case_number
        """
        dedup_fields = self.config.get("data_model", {}).get("dedup_key", [])
        if not dedup_fields:
            # Fallback: hash the entire record
            blob = json.dumps(record, sort_keys=True, default=str)
            return hashlib.sha256(blob.encode("utf-8", "surrogatepass")).hexdigest()

        key_parts = []
        for field in dedup_fields:
            val = record.get(field, "")
            key_parts.append(str(val))
        return "|".join(key_parts)

    # ── Abstract methods that each source must implement ──────────────

    @abstractmethod
    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from the source. Used for bootstrap.
        Each yielded dict is a raw document from the API/page.
        """
        pass

    @abstractmethod
    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents modified/created since the given datetime.
        Used for incremental updates.
        """
        pass

    @abstractmethod
    def normalize(self, raw: dict) -> dict:
        """
        Transform a raw document into a standardized schema.

        Must return a dict with at least:
          - _id: unique identifier
          - _source: source identifier (e.g., "FR/legifrance")
          - _type: "legislation" or "case_law"
          - _fetched_at: ISO 8601 timestamp
          - ...plus all source-specific fields
        """
        pass

    # ── Public API ────────────────────────────────────────────────────

    def bootstrap(self, sample_mode: bool = False, sample_size: int = 10) -> dict:
        """
        Full initial fetch. Idempotent — safe to run multiple times.

        Args:
            sample_mode: If True, stop after sample_size records and save to sample/
            sample_size: Number of records to fetch in sample mode

        Returns:
            dict with run statistics
        """
        stats = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "records_fetched": 0,
            "records_new": 0,
            "records_updated": 0,
            "records_skipped": 0,
            "errors": 0,
        }

        # Categorical skip counters for diagnostics
        skip_normalize_none = 0  # normalize returned None
        skip_exception = 0       # exception during normalize
        first_skips_logged = 0   # limit DEBUG logging of first few skips

        update_strategy = self.config.get("data_model", {}).get("update_strategy", "upsert")
        sample_records = []

        try:
            for raw in self.fetch_all():
                self.rate_limiter.wait()

                try:
                    record = self.normalize(raw)
                except Exception as e:
                    skip_exception += 1
                    stats["errors"] += 1
                    if first_skips_logged < 3:
                        logger.debug(f"Skip (exception): {e}")
                        first_skips_logged += 1
                    elif skip_exception == 100:
                        logger.warning(f"Normalization exceptions: {skip_exception} so far")
                    continue

                # Handle normalize returning None or non-dict (intentional skip)
                if not isinstance(record, dict):
                    skip_normalize_none += 1
                    stats["errors"] += 1
                    if first_skips_logged < 3:
                        raw_id = (raw.get("id") or raw.get("_id") or str(raw)[:50]) if isinstance(raw, dict) else str(raw)[:50]
                        logger.debug(f"Skip (normalize returned {type(record).__name__}): {raw_id}")
                        first_skips_logged += 1
                    continue

                # Validate
                is_valid, errors = self.validator.validate(record)
                if not is_valid:
                    logger.warning(f"Validation errors for {record.get('_id', '?')}: {errors}")

                stats["records_fetched"] += 1
                dedup_key = self._dedup_key(record)

                if sample_mode:
                    sample_records.append(record)
                    if len(sample_records) >= sample_size:
                        break
                    continue

                # Idempotent write
                if update_strategy == "append_only":
                    if self.storage.exists(dedup_key):
                        stats["records_skipped"] += 1
                    else:
                        self.storage.write(dedup_key, record)
                        stats["records_new"] += 1
                else:  # upsert
                    existing = self.storage.read(dedup_key)
                    if existing is None:
                        self.storage.write(dedup_key, record)
                        stats["records_new"] += 1
                    elif existing != record:
                        self.storage.write(dedup_key, record)
                        stats["records_updated"] += 1
                    else:
                        stats["records_skipped"] += 1

        except Exception as e:
            logger.error(f"Bootstrap error: {e}")
            stats["error_message"] = str(e)
            self.status["last_error"] = str(e)

        stats["finished_at"] = datetime.now(timezone.utc).isoformat()

        # Add categorical skip counts to stats
        stats["skip_normalize_none"] = skip_normalize_none
        stats["skip_exception"] = skip_exception

        # Log skip summary if there were any skips
        total_skips = skip_normalize_none + skip_exception
        if total_skips > 0:
            logger.info(
                f"Skip summary: {skip_normalize_none} normalize-returned-None, "
                f"{skip_exception} exceptions"
            )

        # Flush any pending index writes
        self.storage.flush()

        if sample_mode:
            self._save_samples(sample_records)
            stats["sample_records_saved"] = len(sample_records)
        else:
            self.status["last_bootstrap"] = stats["finished_at"]
            self.status["total_records"] = (
                self.status.get("total_records", 0) + stats["records_new"]
            )

        self.status["last_run"] = stats["finished_at"]
        self.status["run_history"].append(stats)
        self._save_status()

        return stats

    def update(self) -> dict:
        """
        Incremental update — fetch only what changed since last run.
        Falls back to bootstrap if never run before.
        """
        last_run = self.status.get("last_run") or self.status.get("last_bootstrap")
        if not last_run:
            logger.info("No previous run found, falling back to bootstrap")
            return self.bootstrap()

        since = datetime.fromisoformat(last_run)
        stats = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "records_fetched": 0,
            "records_new": 0,
            "records_updated": 0,
            "records_skipped": 0,
            "errors": 0,
        }

        # Categorical skip counters for diagnostics
        skip_normalize_none = 0  # normalize returned None
        skip_exception = 0       # exception during normalize
        first_skips_logged = 0   # limit DEBUG logging of first few skips

        update_strategy = self.config.get("data_model", {}).get("update_strategy", "upsert")

        try:
            for raw in self.fetch_updates(since):
                self.rate_limiter.wait()

                try:
                    record = self.normalize(raw)
                except Exception as e:
                    skip_exception += 1
                    stats["errors"] += 1
                    if first_skips_logged < 3:
                        logger.debug(f"Skip (exception): {e}")
                        first_skips_logged += 1
                    elif skip_exception == 100:
                        logger.warning(f"Normalization exceptions: {skip_exception} so far")
                    continue

                # Handle normalize returning None or non-dict (intentional skip)
                if not isinstance(record, dict):
                    skip_normalize_none += 1
                    stats["errors"] += 1
                    if first_skips_logged < 3:
                        raw_id = (raw.get("id") or raw.get("_id") or str(raw)[:50]) if isinstance(raw, dict) else str(raw)[:50]
                        logger.debug(f"Skip (normalize returned {type(record).__name__}): {raw_id}")
                        first_skips_logged += 1
                    continue

                stats["records_fetched"] += 1
                dedup_key = self._dedup_key(record)

                if update_strategy == "append_only":
                    if self.storage.exists(dedup_key):
                        stats["records_skipped"] += 1
                    else:
                        self.storage.write(dedup_key, record)
                        stats["records_new"] += 1
                else:  # upsert
                    existing = self.storage.read(dedup_key)
                    if existing is None:
                        self.storage.write(dedup_key, record)
                        stats["records_new"] += 1
                    elif existing != record:
                        self.storage.write(dedup_key, record)
                        stats["records_updated"] += 1
                    else:
                        stats["records_skipped"] += 1

        except Exception as e:
            logger.error(f"Update error: {e}")
            stats["error_message"] = str(e)
            self.status["last_error"] = str(e)

        stats["finished_at"] = datetime.now(timezone.utc).isoformat()

        # Add categorical skip counts to stats
        stats["skip_normalize_none"] = skip_normalize_none
        stats["skip_exception"] = skip_exception

        # Log skip summary if there were any skips
        total_skips = skip_normalize_none + skip_exception
        if total_skips > 0:
            logger.info(
                f"Skip summary: {skip_normalize_none} normalize-returned-None, "
                f"{skip_exception} exceptions"
            )

        # Flush any pending index writes
        self.storage.flush()

        self.status["last_run"] = stats["finished_at"]
        self.status["total_records"] = (
            self.status.get("total_records", 0) + stats["records_new"]
        )
        self.status["run_history"].append(stats)
        self._save_status()

        return stats

    def _save_samples(self, records: list):
        """Save sample records to the sample/ directory."""
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(parents=True, exist_ok=True)

        # Save individual records (scrub lone surrogates so the UTF-8 write can't crash)
        for i, record in enumerate(records):
            path = sample_dir / f"record_{i:04d}.json"
            with open(path, "w", encoding="utf-8") as f:
                f.write(scrub_surrogates(json.dumps(record, indent=2, ensure_ascii=False, default=str)))

        # Save combined file
        combined_path = sample_dir / "all_samples.json"
        with open(combined_path, "w", encoding="utf-8") as f:
            f.write(scrub_surrogates(json.dumps(records, indent=2, ensure_ascii=False, default=str)))

        logger.info(f"Saved {len(records)} sample records to {sample_dir}")

    def run_sample(self, n: int = 10) -> dict:
        """Convenience method: bootstrap in sample mode."""
        return self.bootstrap(sample_mode=True, sample_size=n)

    # ── Fast bootstrap with concurrent fetching ───────────────────────

    def bootstrap_fast(
        self,
        max_workers: int = None,
        batch_size: int = 100,
        adaptive: bool = True,
    ) -> dict:
        """
        High-throughput bootstrap using concurrent full-text downloads
        and batched writes.

        Architecture:
          - Main thread: iterates fetch_all() (sequential pagination, rate-limited)
          - Worker threads: normalize records concurrently (full-text downloads overlap)
          - Batched writes: storage.write_batch() flushes every `batch_size` records

        Args:
            max_workers: Concurrent download threads (default: from config or 5)
            batch_size: Records per batch write (default: 100)
            adaptive: Use AdaptiveRateLimiter to auto-discover API limits

        Returns:
            dict with run statistics
        """
        if max_workers is None:
            max_workers = self.config.get("rate_limit", {}).get("max_workers", 5)

        # Optionally swap in adaptive rate limiter
        if adaptive:
            cfg = self.config.get("rate_limit", {})
            self.rate_limiter = AdaptiveRateLimiter(
                start_rate=cfg.get("requests_per_second", 5.0),
                min_rate=cfg.get("min_rate", 0.5),
                max_rate=cfg.get("max_rate", 50.0),
                burst=cfg.get("burst", 10),
            )

        stats = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "mode": "fast",
            "max_workers": max_workers,
            "batch_size": batch_size,
            "records_fetched": 0,
            "records_new": 0,
            "records_updated": 0,
            "records_skipped": 0,
            "errors": 0,
        }

        # Categorical skip counters for diagnostics
        skip_normalize_none = 0  # normalize returned None
        skip_exception = 0       # exception during normalize
        first_skips_logged = 0   # limit DEBUG logging

        update_strategy = self.config.get("data_model", {}).get("update_strategy", "upsert")
        batch = []
        log_interval = 500  # log progress every N records

        # Sentinel to distinguish exception from intentional None return
        _EXCEPTION_SENTINEL = object()

        def _process_one(raw):
            """Normalize a single record (runs in worker thread)."""
            try:
                result = self.normalize(raw)
                # Return tuple: (record_or_None, was_exception)
                return (result, False)
            except Exception as e:
                logger.debug(f"Normalization error: {e}")
                return (None, True)  # was_exception=True

        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {}

                for raw in self.fetch_all():
                    self.rate_limiter.wait()

                    future = executor.submit(_process_one, raw)
                    futures[future] = True

                    # Drain completed futures when we have enough in flight
                    if len(futures) >= max_workers * 2:
                        done_futures = [f for f in futures if f.done()]
                        if not done_futures:
                            # Wait for at least one to complete
                            done_iter = as_completed(futures.keys())
                            done_futures = [next(done_iter)]

                        for fut in done_futures:
                            del futures[fut]
                            record, was_exception = fut.result()
                            if record is None:
                                stats["errors"] += 1
                                if was_exception:
                                    skip_exception += 1
                                else:
                                    skip_normalize_none += 1
                                continue

                            stats["records_fetched"] += 1
                            dedup_key = self._dedup_key(record)

                            if update_strategy == "append_only":
                                if self.storage.exists(dedup_key):
                                    stats["records_skipped"] += 1
                                else:
                                    batch.append((dedup_key, record))
                                    stats["records_new"] += 1
                            else:  # upsert
                                if not self.storage.exists(dedup_key):
                                    batch.append((dedup_key, record))
                                    stats["records_new"] += 1
                                else:
                                    # For upsert, just overwrite — skip the expensive
                                    # read-compare for fast mode
                                    batch.append((dedup_key, record))
                                    stats["records_updated"] += 1

                            # Flush batch
                            if len(batch) >= batch_size:
                                self.storage.write_batch(batch)
                                batch = []

                            # Progress logging
                            total = stats["records_fetched"]
                            if total % log_interval == 0:
                                rate_info = self.rate_limiter.stats()
                                logger.info(
                                    f"Progress: {total} records fetched, "
                                    f"{stats['records_new']} new, "
                                    f"{stats['errors']} errors"
                                    + (f", rate: {rate_info.get('current_rate', '?')} req/s"
                                       if 'current_rate' in rate_info else "")
                                )

                # Drain remaining futures
                for fut in as_completed(futures.keys()):
                    record, was_exception = fut.result()
                    if record is None:
                        stats["errors"] += 1
                        if was_exception:
                            skip_exception += 1
                        else:
                            skip_normalize_none += 1
                        continue

                    stats["records_fetched"] += 1
                    dedup_key = self._dedup_key(record)

                    if update_strategy == "append_only":
                        if self.storage.exists(dedup_key):
                            stats["records_skipped"] += 1
                        else:
                            batch.append((dedup_key, record))
                            stats["records_new"] += 1
                    else:
                        if not self.storage.exists(dedup_key):
                            batch.append((dedup_key, record))
                            stats["records_new"] += 1
                        else:
                            batch.append((dedup_key, record))
                            stats["records_updated"] += 1

        except Exception as e:
            logger.error(f"bootstrap_fast error: {e}")
            stats["error_message"] = str(e)
            self.status["last_error"] = str(e)

        # Write remaining batch
        if batch:
            self.storage.write_batch(batch)

        self.storage.flush()
        stats["finished_at"] = datetime.now(timezone.utc).isoformat()
        stats["rate_limiter"] = self.rate_limiter.stats()

        # Add categorical skip counts to stats
        stats["skip_normalize_none"] = skip_normalize_none
        stats["skip_exception"] = skip_exception

        # Log skip summary if there were any skips
        total_skips = skip_normalize_none + skip_exception
        if total_skips > 0:
            logger.info(
                f"Skip summary: {skip_normalize_none} normalize-returned-None, "
                f"{skip_exception} exceptions"
            )

        self.status["last_bootstrap"] = stats["finished_at"]
        self.status["last_run"] = stats["finished_at"]
        self.status["total_records"] = (
            self.status.get("total_records", 0) + stats["records_new"]
        )
        self.status["run_history"].append(stats)
        self._save_status()

        logger.info(
            f"bootstrap_fast complete: {stats['records_fetched']} fetched, "
            f"{stats['records_new']} new, {stats['errors']} errors, "
            f"rate limiter: {self.rate_limiter.stats()}"
        )

        return stats
