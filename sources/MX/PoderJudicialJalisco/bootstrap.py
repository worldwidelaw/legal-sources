#!/usr/bin/env python3
"""
MX/PoderJudicialJalisco -- Jalisco State Court Decisions

Fetches full-text sentencias from the Supremo Tribunal de Justicia del Estado
de Jalisco.

API: https://publica-sentencias-backend.stjjalisco.gob.mx/
Frontend: https://publicacionsentencias.stjjalisco.gob.mx/

Strategy (see issue #1374):
  The paginated listing endpoint /tocas is now gated behind reCAPTCHA v3
  ("403 {"error":"reCAPTCHA requerido"}"), which is why the old paginating
  crawler stopped at ~32 records. The per-document endpoints are still open:

    GET /toca/{id}        -> JSON metadata (sala, materia, magistrado, fechas)
    GET /toca/{id}/file   -> the sentencia PDF itself (born-digital)

  So the corpus is enumerated over the toca id space (1 .. ~88,200) instead of
  paginated. The PDF is the real judgment text (20K-150K chars), a large
  upgrade over the AI summaries the previous version stored.

Rate limits (Laravel throttle, per route, per minute):
    /toca/{id}       ~26/min
    /toca/{id}/file   20/min  (sends Retry-After on 429)
  The two routes are throttled independently, so a document costs one slot on
  each. Throughput is bounded by the PDF route at ~20 documents/minute, which
  means a full sweep takes tens of hours — hence the resume checkpoint.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast     # Concurrent full pull -> data/records.jsonl
  python bootstrap.py update             # Newest ids only
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import threading
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MX.PoderJudicialJalisco")

API_BASE = "https://publica-sentencias-backend.stjjalisco.gob.mx"

# Highest toca id observed 2026-08-04 (binary search: 88125 OK, 88203 gone).
# The ceiling is re-probed at run time so the crawl follows new publications.
KNOWN_MAX_ID = 88_200
PROBE_STEP = 2_000
PROBE_FINE_STEP = 100
PROBE_WINDOW = 6
PROBE_MAX = 400_000

# Requests/minute allowed per route (from X-RateLimit-Limit), minus a margin.
ROUTE_BUDGETS = {"meta": 24, "file": 18}

# Adaptive throttling (#1432). The published limits above are not always what
# the server actually enforces, so a route that keeps drawing 429s slows itself
# down instead of retrying into the limiter, then creeps back once it is clean.
PENALTIES_BEFORE_SLOWDOWN = 5
SLOWDOWN_FACTOR = 1.25
MAX_SLOWDOWN = 4.0            # never crawl slower than a quarter of the budget
SUCCESSES_BEFORE_SPEEDUP = 200
SPEEDUP_FACTOR = 0.95

# A 429 is the throttle working, not a failed request, so it must not spend the
# retry budget that exists for genuine errors -- but it still needs a ceiling.
MAX_THROTTLE_WAITS = 12

CHECKPOINT_LAG = 250  # rewind on resume to cover in-flight ids


class _RouteThrottle:
    """Minimum-interval throttle for one throttled API route.

    One instance is shared by the whole worker pool, so a slot is reserved under
    the lock and the waiting happens outside it.

    The wait has to be re-checked after each sleep rather than computed once.
    A 429 arriving while N workers are already asleep calls penalise(), but a
    worker sleeping on a deadline computed *before* that penalty would wake at
    its stale slot and fire straight into the limiter anyway -- so every penalty
    cost N more 429s instead of zero.  That is how one throttled route booked
    21,707 backoffs in a single run (#1432).
    """

    def __init__(self, per_minute: float):
        self._base_interval = 60.0 / per_minute
        self._interval = self._base_interval
        self._max_interval = self._base_interval * MAX_SLOWDOWN
        self._lock = threading.Lock()
        self._next_at = 0.0
        self._blocked_until = 0.0
        self._penalties = 0
        self._successes = 0

    def wait(self):
        with self._lock:
            slot = max(time.monotonic(), self._next_at)
            self._next_at = slot + self._interval
        while True:
            with self._lock:
                target = max(slot, self._blocked_until)
            remaining = target - time.monotonic()
            if remaining <= 0:
                return
            # Cap each nap so a long Retry-After is still re-checked promptly.
            time.sleep(min(remaining, 5.0))

    def penalise(self, seconds: float, route: str = ""):
        """Hold the whole route back after a 429, and slow its steady state.

        Repeated 429s mean the configured rate sits above the server's real
        limit, so widen the interval instead of rediscovering the same wall on
        every request.
        """
        with self._lock:
            self._blocked_until = max(self._blocked_until,
                                      time.monotonic() + seconds)
            self._next_at = max(self._next_at, self._blocked_until)
            self._successes = 0
            self._penalties += 1
            if self._penalties < PENALTIES_BEFORE_SLOWDOWN:
                return
            self._penalties = 0
            if self._interval >= self._max_interval:
                return
            self._interval = min(self._interval * SLOWDOWN_FACTOR,
                                 self._max_interval)
            rate = 60.0 / self._interval
        logger.info(f"/{route}: sustained 429s, easing to {rate:.1f} req/min")

    def note_success(self, route: str = ""):
        """Creep back toward the configured rate after a long clean stretch.

        Without this a single burst of 429s would hold the route at its floor
        for the rest of a multi-day sweep.
        """
        with self._lock:
            if self._interval <= self._base_interval:
                return
            self._successes += 1
            if self._successes < SUCCESSES_BEFORE_SPEEDUP:
                return
            self._successes = 0
            self._interval = max(self._interval * SPEEDUP_FACTOR,
                                 self._base_interval)
            rate = 60.0 / self._interval
        logger.info(f"/{route}: recovered, raising to {rate:.1f} req/min")


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from a born-digital sentencia PDF."""
    try:
        import fitz  # PyMuPDF
    except ImportError:
        fitz = None

    if fitz is not None:
        try:
            with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
                parts = [page.get_text() for page in doc]
            text = "\n".join(parts).strip()
            if len(text) >= 200:
                return text
        except Exception as e:
            logger.debug(f"fitz extraction failed: {e}")

    try:
        from pypdf import PdfReader
    except ImportError:
        try:
            from PyPDF2 import PdfReader  # type: ignore
        except ImportError:
            return ""

    try:
        import io

        reader = PdfReader(io.BytesIO(pdf_bytes))
        return "\n".join((p.extract_text() or "") for p in reader.pages).strip()
    except Exception as e:
        logger.debug(f"pypdf extraction failed: {e}")
        return ""


class JaliscoCourtScraper(BaseScraper):
    """Scraper for MX/PoderJudicialJalisco court decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "application/json",
            "Referer": "https://publicacionsentencias.stjjalisco.gob.mx/",
        })
        self._throttles = {
            route: _RouteThrottle(per_minute)
            for route, per_minute in ROUTE_BUDGETS.items()
        }
        self._checkpoint_path = source_dir / "data" / "checkpoint.json"
        self._sample_stride = 0  # set by `bootstrap --sample`

    # ── HTTP ──────────────────────────────────────────────────────────

    def _api_get(
        self, url: str, route: str, timeout: int = 60, attempts: int = 4
    ) -> Optional[requests.Response]:
        """GET with per-route throttling, 429 backoff and retries."""
        throttle = self._throttles[route]
        attempt = 0
        throttle_waits = 0
        while attempt < attempts:
            throttle.wait()
            try:
                resp = self.session.get(url, timeout=timeout)
            except requests.exceptions.RequestException as e:
                attempt += 1
                logger.debug(f"{url}: {e} (attempt {attempt})")
                time.sleep(5 * attempt)
                continue

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                try:
                    delay = float(retry_after)
                except (TypeError, ValueError):
                    delay = 60.0
                throttle.penalise(delay + 1, route)
                throttle_waits += 1
                if throttle_waits >= MAX_THROTTLE_WAITS:
                    logger.warning(
                        f"/{route}: still 429 after {throttle_waits} waits, giving up on {url}")
                    return None
                logger.debug(f"429 on /{route}; backing off {delay:.0f}s")
                continue
            if resp.status_code in (404, 410):
                return None
            if resp.status_code >= 500:
                attempt += 1
                logger.debug(f"HTTP {resp.status_code} for {url}")
                time.sleep(5 * attempt)
                continue
            if resp.status_code != 200:
                logger.warning(f"HTTP {resp.status_code} for {url}")
                return None
            throttle.note_success(route)
            return resp
        return None

    # ── ID space ──────────────────────────────────────────────────────

    def _id_exists(self, toca_id: int) -> bool:
        return self._api_get(f"{API_BASE}/toca/{toca_id}", "meta", timeout=30) is not None

    def _window_exists(self, start: int, width: int = PROBE_WINDOW) -> bool:
        """True if any id in [start, start+width) resolves.

        Single-id probes are unreliable near the ceiling because the id space
        is pitted with holes (88200 is one, 88150 is not), so probe a window.
        """
        return any(self._id_exists(i) for i in range(start, start + width))

    def _discover_max_id(self) -> int:
        """Walk past the known ceiling so newly published tocas are picked up."""
        max_id = KNOWN_MAX_ID
        for step in (PROBE_STEP, PROBE_FINE_STEP):
            while max_id < PROBE_MAX and self._window_exists(max_id + step):
                max_id += step
        logger.info(f"Highest toca id in range: ~{max_id}")
        return max_id + PROBE_FINE_STEP

    # ── Checkpoint ────────────────────────────────────────────────────

    def _read_checkpoint(self) -> int:
        try:
            data = json.loads(self._checkpoint_path.read_text())
            last_id = int(data.get("last_id", 0))
        except Exception:
            last_id = 0
        # A fleet worker may carry data/records.jsonl over from an earlier run
        # without checkpoint.json (or with a stale one). Trust whichever is
        # further along, so a resumed sweep never re-walks ids it already wrote.
        return max(1, max(last_id, self._max_id_in_records()) - CHECKPOINT_LAG)

    def _max_id_in_records(self) -> int:
        """Highest toca id already written to data/records.jsonl, else 0."""
        path = self._checkpoint_path.parent / "records.jsonl"
        max_id = 0
        try:
            with path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    try:
                        rec_id = json.loads(line).get("_id", "")
                    except ValueError:
                        continue
                    if isinstance(rec_id, str) and rec_id.startswith("MX-JAL-"):
                        try:
                            max_id = max(max_id, int(rec_id[len("MX-JAL-"):]))
                        except ValueError:
                            pass
        except OSError:
            return 0
        if max_id:
            logger.info(f"records.jsonl already covers toca ids up to {max_id}")
        return max_id

    def _write_checkpoint(self, last_id: int) -> None:
        try:
            self._checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            self._checkpoint_path.write_text(json.dumps({"last_id": last_id}))
        except Exception as e:
            logger.debug(f"Could not write checkpoint: {e}")

    # ── Fetch ─────────────────────────────────────────────────────────

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield id stubs across the whole toca id space, resuming if possible."""
        if self._sample_stride:
            # Sample mode: spread over the id space so the samples span every
            # era of the corpus. Early ids (2014) carry no relation metadata.
            max_id = self._discover_max_id()
            step = max(1, max_id // self._sample_stride)
            for toca_id in range(1, max_id + 1, step):
                yield {"id": toca_id}
            return

        start_id = self._read_checkpoint()
        max_id = self._discover_max_id()
        if start_id > 1:
            logger.info(f"Resuming from toca id {start_id}")

        for toca_id in range(start_id, max_id + 1):
            yield {"id": toca_id}
            if toca_id % 100 == 0:
                self._write_checkpoint(toca_id)
        self._write_checkpoint(max_id)

    def fetch_updates(self, since: datetime = None) -> Generator[Dict[str, Any], None, None]:
        """Yield the most recent slice of the id space."""
        max_id = self._discover_max_id()
        for toca_id in range(max(1, max_id - 2_000), max_id + 1):
            yield {"id": toca_id}

    # ── Normalize (downloads metadata + PDF) ──────────────────────────

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        toca_id = raw.get("id")
        if not toca_id:
            return None

        resp = self._api_get(f"{API_BASE}/toca/{toca_id}", "meta", timeout=30)
        if resp is None:
            return None
        try:
            toca = resp.json().get("data", {}).get("toca", {})
        except ValueError:
            return None
        if not toca:
            return None

        pdf_url = f"{API_BASE}/toca/{toca_id}/file"
        pdf_resp = self._api_get(pdf_url, "file", timeout=120)
        if pdf_resp is None:
            return None
        text = _extract_pdf_text(pdf_resp.content)
        if len(text) < 200:
            logger.debug(f"No usable text for toca {toca_id} ({len(text)} chars)")
            return None

        sala_name = (toca.get("salas_data") or {}).get("nombre", "")
        magistrado = toca.get("magistrado_data") or {}
        judge = " ".join(
            p for p in (
                magistrado.get("nombre", ""),
                magistrado.get("primer_apellido", ""),
                magistrado.get("segundo_apellido", ""),
            ) if p
        ).strip()

        numero = toca.get("numero", "")
        periodo = toca.get("periodo", "")
        case_number = f"{numero}/{periodo}" if numero and periodo else str(numero or "")
        title = f"Toca {case_number}" + (f" — {sala_name}" if sala_name else "")

        fecha_emision = toca.get("fecha_emision") or ""
        fecha_pub = toca.get("fecha_publicacion") or ""

        return {
            "_id": f"MX-JAL-{toca_id}",
            "_source": "MX/PoderJudicialJalisco",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": fecha_emision[:10] if fecha_emision else None,
            "url": pdf_url,
            "court": sala_name,
            "judge": judge,
            "subject_area": (toca.get("materia_data") or {}).get("nombre", ""),
            "case_type": (toca.get("tipo_juicio_data") or {}).get("nombre", ""),
            "ruling": (toca.get("sentido_data") or {}).get("nombre", ""),
            "case_number": case_number,
            "publication_date": fecha_pub[:10] if fecha_pub else None,
            "crime_or_action": (toca.get("delito_data") or {}).get("nombre", ""),
        }

    # ── Test ──────────────────────────────────────────────────────────

    def test(self) -> bool:
        record = None
        for toca_id in (100, 2500, 60000):
            record = self.normalize({"id": toca_id})
            if record:
                break
        if not record:
            logger.error("Could not build a record from any probe id")
            return False
        logger.info(
            f"API OK: {record['title']} — {len(record['text'])} chars, "
            f"date {record['date']}, court {record['court']}"
        )
        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MX/PoderJudicialJalisco data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Only fetch a small sample")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = JaliscoCourtScraper()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)
    elif args.command == "bootstrap":
        if args.sample:
            scraper._sample_stride = 30
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"Bootstrap-fast complete: {json.dumps(stats, indent=2)}")
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
