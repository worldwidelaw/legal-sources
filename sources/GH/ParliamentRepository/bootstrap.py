#!/usr/bin/env python3
"""
GH/ParliamentRepository -- Ghana Parliament Institutional Repository

Fetches all parliamentary materials from the official Parliament of Ghana
DSpace 9 institutional repository via REST API.

Covers: bills, official reports (Hansard), committee reports,
constitutional/executive/legislative instruments, decrees, budget estimates,
agreements, conventions, and more (~3,900+ items).

Strategy:
  - Global search across entire repository (no community scope)
  - Paginate through all items via discover/search endpoint
  - Fetch item metadata + full text from TEXT bundle bitstreams
  - DSpace pre-extracts text from PDFs — no PDF parsing needed
  - Uses curl subprocess for HTTPS (system Python SSL compatibility)
  - Every request carries a connect/transfer/stall deadline and the crawl
    checkpoints settled item UUIDs, so an interrupted run resumes instead of
    restarting at 0 (issue #1542)

Usage:
  python bootstrap.py bootstrap            # Fetch all records (resumes)
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap-fast       # Same as bootstrap
  python bootstrap.py bootstrap --restart  # Discard the checkpoint, re-crawl
  python bootstrap.py test                 # Quick connectivity test

Env:
  GH_PARLREPO_DEADLINE_HOURS  wall-clock budget for one run (default 20)
"""

import os
import sys
import json
import logging
import re
import signal
import subprocess
import tempfile
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GH.ParliamentRepository")

API_BASE = "https://repository.parliament.gh/server/api"
REPO_BASE = "https://repository.parliament.gh"
PAGE_SIZE = 20

# Every request is bounded three ways: a connect deadline, a whole-transfer
# deadline, and a stall detector (a connection that trickles under 1 B/s for
# STALL_SECONDS is aborted). curl enforces these; the parent additionally
# hard-kills the process group if curl itself ever wedges, so no single fetch
# can freeze the crawl (issue #1542: CPU 0, log frozen ~40 min at 500 records).
CONNECT_TIMEOUT = 15
MAX_TIME = 60
STALL_SECONDS = 20
# A pre-extracted DSpace .txt this large is a runaway; skip rather than buffer it.
MAX_FILESIZE = 64 * 1024 * 1024

# Wall-clock budget for one full crawl. On expiry the run stops cleanly with its
# checkpoint written, so the next fleet slot resumes instead of restarting at 0.
DEADLINE_HOURS = float(os.environ.get("GH_PARLREPO_DEADLINE_HOURS", "20"))

CHECKPOINT_FLUSH_EVERY = 25


def _curl_get(url: str, accept: str = "application/json", timeout: int = MAX_TIME) -> Optional[str]:
    """HTTP GET via curl subprocess (bypasses Python SSL limitations).

    Writes the body to a temp file instead of a pipe: nothing to drain means the
    kill path can never block, and a large bitstream never lands in the parent's
    memory as both bytes and a decoded str.
    """
    fd, tmp_path = tempfile.mkstemp(prefix="gh-parlrepo-", suffix=".body")
    os.close(fd)
    proc = None
    try:
        proc = subprocess.Popen(
            [
                "curl", "-s", "-f", "-L",
                "--connect-timeout", str(CONNECT_TIMEOUT),
                "--max-time", str(timeout),
                "--speed-limit", "1", "--speed-time", str(STALL_SECONDS),
                "--max-filesize", str(MAX_FILESIZE),
                "-o", tmp_path,
                "-H", f"Accept: {accept}",
                "-H", "User-Agent: Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
                url,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        try:
            returncode = proc.wait(timeout=timeout + 15)
        except subprocess.TimeoutExpired:
            logger.warning(f"curl exceeded its deadline, killing: {url[:100]}")
            _kill_process_group(proc)
            return None

        if returncode != 0:
            return None
        return Path(tmp_path).read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        logger.warning(f"curl failed for {url[:100]}: {e}")
        if proc is not None and proc.poll() is None:
            _kill_process_group(proc)
        return None
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def _kill_process_group(proc: subprocess.Popen) -> None:
    """SIGKILL the curl process group and reap it, bounded."""
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(proc.pid, sig)
        except (ProcessLookupError, PermissionError, OSError):
            pass
        try:
            proc.wait(timeout=5)
            return
        except subprocess.TimeoutExpired:
            continue


def _curl_json(url: str, timeout: int = 30) -> Optional[Dict]:
    """Fetch JSON from URL via curl."""
    body = _curl_get(url, accept="application/json", timeout=timeout)
    if body is None:
        return None
    try:
        return json.loads(body)
    except json.JSONDecodeError as e:
        logger.warning(f"JSON decode error: {e}")
        return None


class ParliamentRepositoryScraper(BaseScraper):
    """Scraper for GH/ParliamentRepository -- full Ghana Parliament DSpace repository."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.checkpoint_path = source_dir / "data" / "checkpoint.json"
        self.use_checkpoint = True
        self._done: set = set()
        self._since_flush = 0
        self._deadline: Optional[float] = None

    # ---- checkpoint / resume -------------------------------------------------

    def _load_checkpoint(self) -> None:
        """Load the set of item UUIDs already emitted by a previous run."""
        self._done = set()
        if not self.use_checkpoint or not self.checkpoint_path.exists():
            return
        try:
            data = json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
            self._done = set(data.get("done", []))
            logger.info(f"Checkpoint: resuming, {len(self._done)} items already done")
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Checkpoint unreadable ({e}) — starting from scratch")

    def _save_checkpoint(self, force: bool = False) -> None:
        """Atomically persist the done-set (tmp file + rename)."""
        if not self.use_checkpoint:
            return
        self._since_flush += 1
        if not force and self._since_flush < CHECKPOINT_FLUSH_EVERY:
            return
        self._since_flush = 0
        try:
            self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.checkpoint_path.with_suffix(".json.tmp")
            tmp.write_text(
                json.dumps({"version": 1, "done": sorted(self._done)}),
                encoding="utf-8",
            )
            os.replace(tmp, self.checkpoint_path)
        except OSError as e:
            logger.warning(f"Could not write checkpoint: {e}")

    def _mark_done(self, uuid: str) -> None:
        self._done.add(uuid)
        self._save_checkpoint()

    def _out_of_time(self) -> bool:
        return self._deadline is not None and time.monotonic() > self._deadline

    def _api_get(self, url: str) -> Optional[Dict]:
        """GET request to DSpace API with retry and rate limiting."""
        for attempt in range(3):
            time.sleep(1)
            data = _curl_json(url)
            if data is not None:
                return data
            logger.warning(f"Attempt {attempt+1} failed for {url[:100]}")
            if attempt < 2:
                time.sleep(5)
        return None

    def _fetch_text(self, url: str) -> Optional[str]:
        """GET plain text content (bitstream)."""
        for attempt in range(3):
            time.sleep(1)
            body = _curl_get(url, accept="text/plain, */*")
            if body is not None:
                return body
            logger.warning(f"Text fetch attempt {attempt+1} failed")
            if attempt < 2:
                time.sleep(5)
        return None

    def _get_text_bitstream_url(self, item_uuid: str) -> Optional[str]:
        """Find the TEXT bundle bitstream content URL for an item.

        Uses DSpace's ``embed=bundles/bitstreams`` so one request replaces the
        old bundles + bitstreams pair, and ``sizeBytes`` is known before we
        spend a download on a bitstream that is obviously too small or too big.
        """
        data = self._api_get(f"{API_BASE}/core/items/{item_uuid}?embed=bundles/bitstreams")
        if not data:
            return None

        bundles = (
            data.get("_embedded", {})
            .get("bundles", {})
            .get("_embedded", {})
            .get("bundles", [])
        )
        for bundle in bundles:
            if bundle.get("name") != "TEXT":
                continue
            bitstreams = (
                bundle.get("_embedded", {})
                .get("bitstreams", {})
                .get("_embedded", {})
                .get("bitstreams", [])
            )
            for bitstream in bitstreams:
                size = bitstream.get("sizeBytes")
                if isinstance(size, int) and not (50 <= size <= MAX_FILESIZE):
                    continue
                href = bitstream.get("_links", {}).get("content", {}).get("href")
                if href:
                    return href
        return None

    def _extract_metadata(self, item: Dict) -> Dict[str, str]:
        """Extract metadata fields from a DSpace item object."""
        meta = item.get("metadata", {})

        def get_val(key: str) -> str:
            vals = meta.get(key, [])
            return vals[0].get("value", "") if vals else ""

        def get_all_vals(key: str) -> str:
            vals = meta.get(key, [])
            return "; ".join(v.get("value", "") for v in vals if v.get("value"))

        title = get_val("dc.title")
        date_issued = get_val("dc.date.issued")
        uri = get_val("dc.identifier.uri")
        author = get_val("dc.contributor.author")
        subject = get_all_vals("dc.subject")
        publisher = get_val("dc.publisher")
        language = get_val("dc.language.iso")
        doc_type = get_val("dc.type")
        abstract = get_val("dc.description.abstract")
        description = get_val("dc.description")

        return {
            "uuid": item.get("uuid", ""),
            "title": title,
            "date": date_issued,
            "uri": uri,
            "author": author,
            "subject": subject,
            "publisher": publisher,
            "language": language or "en",
            "handle": item.get("handle", ""),
            "doc_type": doc_type,
            "abstract": abstract,
            "description": description,
        }

    def _search_all_items(self, max_pages: int = 250) -> Generator[Dict, None, None]:
        """Iterate all items in the repository via global discover search."""
        page = 0
        while page < max_pages:
            url = (
                f"{API_BASE}/discover/search/objects"
                f"?dsoType=ITEM&size={PAGE_SIZE}&page={page}"
                f"&sort=dc.date.accessioned,DESC"
            )
            if self._out_of_time():
                logger.warning(f"Wall-clock budget exhausted at page {page} — stopping")
                break

            data = self._api_get(url)
            if not data:
                break

            search_result = data.get("_embedded", {}).get("searchResult", {})
            objects = search_result.get("_embedded", {}).get("objects", [])
            if not objects:
                break

            for obj in objects:
                item = obj.get("_embedded", {}).get("indexableObject", {})
                if item:
                    yield item

            page_info = search_result.get("page", {})
            total_pages = page_info.get("totalPages", 0)
            if page + 1 >= total_pages:
                break
            page += 1

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        uuid = raw.get("uuid", "")
        date = raw.get("date", "")
        if date and re.match(r"^\d{4}$", date):
            date = f"{date}-01-01"

        handle = raw.get("handle", "")
        url = raw.get("uri", "")
        if not url and handle:
            url = f"{REPO_BASE}/handle/{handle}"

        return {
            "_id": f"GH-PARLREPO-{uuid}",
            "_source": "GH/ParliamentRepository",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date,
            "url": url,
            "author": raw.get("author", ""),
            "subject": raw.get("subject", ""),
            "doc_type": raw.get("doc_type", ""),
            "language": raw.get("language", "en"),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all items from the entire repository, resuming where we left off."""
        self._deadline = time.monotonic() + DEADLINE_HOURS * 3600
        self._load_checkpoint()
        try:
            yield from self._iter_new_items()
        finally:
            # Also runs when the consumer stops early or the crawl raises, so a
            # partial run still hands its progress to the next fleet slot.
            self._save_checkpoint(force=True)

    def _iter_new_items(self) -> Generator[Dict[str, Any], None, None]:
        count = 0
        skipped = 0
        resumed = 0
        failed = 0
        seen_uuids = set()

        for item in self._search_all_items():
            uuid = item.get("uuid", "")
            if not uuid or uuid in seen_uuids:
                continue
            seen_uuids.add(uuid)

            # Resume: items settled by an earlier run cost no network calls.
            if uuid in self._done:
                resumed += 1
                continue

            if self._out_of_time():
                logger.warning("Wall-clock budget exhausted — stopping, checkpoint saved")
                break

            metadata = self._extract_metadata(item)

            text_url = self._get_text_bitstream_url(uuid)
            if not text_url:
                # Deterministic outcome (no TEXT bundle) — settle it so later
                # runs skip it. A transient fetch failure is left unmarked.
                skipped += 1
                self._mark_done(uuid)
                logger.debug(f"No TEXT bundle: {metadata['title'][:60]}")
                continue

            text = self._fetch_text(text_url)
            if text is None:
                failed += 1
                logger.debug(f"Text fetch failed (will retry next run): {uuid}")
                continue
            if len(text.strip()) < 50:
                skipped += 1
                self._mark_done(uuid)
                logger.debug(f"Insufficient text: {metadata['title'][:60]}")
                continue

            text = re.sub(r"\r\n", "\n", text)
            text = re.sub(r"\n{3,}", "\n\n", text)
            text = text.strip()

            raw = {**metadata, "text": text}
            count += 1
            yield raw
            # Only settled once the consumer has written it.
            self._mark_done(uuid)

            if count % 100 == 0:
                logger.info(
                    f"Progress: {count} fetched, {skipped} skipped, "
                    f"{resumed} already done, {failed} failed"
                )

        logger.info(
            f"Completed: {count} records fetched, {skipped} skipped (no text), "
            f"{resumed} resumed from checkpoint, {failed} transient failures"
        )

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch most recently added items."""
        count = 0
        url = (
            f"{API_BASE}/discover/search/objects"
            f"?dsoType=ITEM&size=20&page=0"
            f"&sort=dc.date.accessioned,DESC"
        )
        data = self._api_get(url)
        if not data:
            return

        objects = (
            data.get("_embedded", {})
            .get("searchResult", {})
            .get("_embedded", {})
            .get("objects", [])
        )

        for obj in objects:
            item = obj.get("_embedded", {}).get("indexableObject", {})
            if not item:
                continue

            uuid = item.get("uuid", "")
            metadata = self._extract_metadata(item)

            text_url = self._get_text_bitstream_url(uuid)
            if not text_url:
                continue

            text = self._fetch_text(text_url)
            if not text or len(text.strip()) < 50:
                continue

            text = re.sub(r"\r\n", "\n", text)
            text = re.sub(r"\n{3,}", "\n\n", text)
            text = text.strip()

            raw = {**metadata, "text": text}
            count += 1
            yield raw

        logger.info(f"Updates: {count} records fetched")

    def test(self) -> bool:
        """Quick connectivity test."""
        url = f"{API_BASE}/core/communities?size=1"
        data = self._api_get(url)
        if not data:
            logger.error("Cannot reach DSpace API")
            return False

        communities = data.get("_embedded", {}).get("communities", [])
        if communities:
            logger.info(f"API OK: community '{communities[0].get('name', 'N/A')}'")

        search_url = (
            f"{API_BASE}/discover/search/objects"
            f"?dsoType=ITEM&size=1&sort=dc.date.accessioned,DESC"
        )
        search_data = self._api_get(search_url)
        if not search_data:
            logger.error("Search endpoint failed")
            return False

        page_info = (
            search_data.get("_embedded", {})
            .get("searchResult", {})
            .get("page", {})
        )
        logger.info(f"Total items: {page_info.get('totalElements', 'unknown')}")

        objects = (
            search_data.get("_embedded", {})
            .get("searchResult", {})
            .get("_embedded", {})
            .get("objects", [])
        )
        if not objects:
            logger.error("No items found")
            return False

        item = objects[0].get("_embedded", {}).get("indexableObject", {})
        meta = self._extract_metadata(item)
        logger.info(f"Item OK: {meta['title'][:60]}")

        text_url = self._get_text_bitstream_url(meta["uuid"])
        if text_url:
            text = self._fetch_text(text_url)
            if text and len(text.strip()) > 50:
                logger.info(f"Text OK: {len(text)} chars")
            else:
                logger.warning("Text fetch returned empty or short content")
        else:
            logger.warning("No TEXT bundle found for test item")

        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="GH/ParliamentRepository data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument(
        "--restart",
        action="store_true",
        help="Ignore and overwrite the resume checkpoint (re-crawl from scratch)",
    )
    args = parser.parse_args()

    scraper = ParliamentRepositoryScraper()
    # Sample runs must not consume or advance the resume checkpoint.
    scraper.use_checkpoint = not args.sample
    if args.restart and scraper.checkpoint_path.exists():
        scraper.checkpoint_path.unlink()
        logger.info("Checkpoint cleared — crawling from scratch")

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
