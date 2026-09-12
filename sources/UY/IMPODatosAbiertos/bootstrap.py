#!/usr/bin/env python3
"""
UY/IMPODatosAbiertos -- Uruguayan Decrees, Resolutions & Constitution via IMPO Open Data

Fetches full text from IMPO's open data JSON API (?json=true on any document URL).
Laws are already covered by UY/IMPO (via Parlamento); this source covers:
  - Constitution (constitucion/1967-1967)
  - Decrees (decretos/{number}-{year}) — 1964-present
  - Decree-Laws (decretos-ley/{number}-{year}) — 1973-1985, numbered in the law series
  - Executive Resolutions (resoluciones/{number}-{year}) — 1975-present

IMPO publishes no listing/index endpoint, so the corpus is enumerated by probing
the (document number, year) space. An invalid combination returns an HTML
"Acceso no válido" page instead of JSON, which is how misses are detected.

Probing is done with a small thread pool in chunks; every completed chunk is
logged and every completed (type, year) unit is checkpointed to
``data/scan_checkpoint.json`` so a relaunch resumes instead of re-walking the
whole space.

An incremental refresh narrows on ``fechaPublicacion`` (the Diario Oficial issue
that carried the norm — IMPO is the publisher, so that date is when the document
became available, not a document date lagging behind it) and starts each year's
probe just below the high-water number recorded on the last crawl, instead of
re-walking two full years from 1 (#1502).

Data: Public open data (Licencia de Datos Abiertos Uruguay).

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap-fast       # Full pull (fleet entry point)
  python bootstrap.py update               # Incremental refresh since last_run
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample documents
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import html as html_module
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UY.IMPODatosAbiertos")

IMPO_BASE = "https://www.impo.com.uy/bases"

FIRST_DECREE_YEAR = 1964
FIRST_RESOLUTION_YEAR = 1975

# Empirically probed ceilings (2026-08): decrees reach ~700-800/year in the
# 1970s-1990s, resolutions reach ~1200-1500/year. Scan generously above both.
DECREE_MAX_PER_YEAR = 900
RESOLUTION_MAX_PER_YEAR = 1600

# Numbering is sparse — resolutions in particular have gaps of 100+ — so only
# abandon a year after a long unbroken run of misses.
DECREE_MISS_TOLERANCE = 150
RESOLUTION_MISS_TOLERANCE = 300

# Decree-laws (1973-1985 de facto government) continue the national law number
# series rather than restarting each year. Anchors probed on a 100-number grid.
DECREE_LEY_MIN = 14100
DECREE_LEY_MAX = 15850
DECREE_LEY_YEARS = (1973, 1986)  # half-open
DECREE_LEY_ANCHORS = [
    (14200, 1974), (14400, 1975), (14500, 1976), (14700, 1977),
    (14800, 1978), (14900, 1979), (15000, 1980), (15200, 1981),
    (15300, 1982), (15400, 1983), (15600, 1984), (15700, 1985),
]

CHUNK = 100
MAX_WORKERS = 8
REQUEST_TIMEOUT = 30
FETCH_ATTEMPTS = 3

# An incremental refresh restarts each (type, year) below its recorded high-water
# number rather than at 1. Numbering tracks the publication order only loosely --
# decreto 5/1985 was published two weeks after decreto 50/1985 -- so drop back far
# enough that a late-numbered straggler is still re-probed.
UPDATE_BACKFILL_MARGIN = 200

# Seeding a high-water mark for a year crawled before this checkpoint existed:
# probe a coarse descending grid instead of re-walking the year from 1.
HIGHWATER_STRIDE = 25


def strip_html(html_content: str) -> str:
    """Remove HTML tags and decode entities."""
    if not html_content:
        return ""
    content = re.sub(r"<br\s*/?>", "\n", html_content, flags=re.IGNORECASE)
    content = re.sub(r"<p[^>]*>", "\n\n", content, flags=re.IGNORECASE)
    content = re.sub(r"</p>", "", content, flags=re.IGNORECASE)
    content = re.sub(r"<[^>]+>", "", content)
    content = html_module.unescape(content)
    content = re.sub(r"[ \t]+", " ", content)
    content = re.sub(r"\n[ \t]+", "\n", content)
    content = re.sub(r"\n{3,}", "\n\n", content)
    return content.strip()


def extract_text_from_articulos(articulos: list) -> str:
    """Concatenate all article texts into a single document."""
    parts = []
    for art in articulos:
        section_parts = []
        for key in ("titulosArticulo", "tituloArticulo", "textoArticulo"):
            clean = strip_html(art.get(key, ""))
            if clean:
                section_parts.append(clean)
        notes = strip_html(art.get("notasArticulo", ""))
        if notes:
            section_parts.append(f"[Nota: {notes}]")
        if section_parts:
            parts.append("\n".join(section_parts))
    return "\n\n".join(parts)


def parse_impo_date(date_str: str) -> Optional[str]:
    """Convert DD/MM/YYYY to ISO 8601."""
    if not date_str:
        return None
    try:
        parts = date_str.strip().split("/")
        if len(parts) == 3:
            return f"{parts[2]}-{parts[1]}-{parts[0]}"
    except Exception:
        pass
    return None


def decree_ley_year_candidates(number: int) -> list:
    """Years to try for a decree-law number, nearest-first from the anchor grid."""
    guess = DECREE_LEY_ANCHORS[0][1]
    for anchor_num, anchor_year in DECREE_LEY_ANCHORS:
        if number >= anchor_num:
            guess = anchor_year
        else:
            break
    lo, hi = DECREE_LEY_YEARS
    ordered = []
    for delta in (0, 1, -1, 2, -2):
        year = guess + delta
        if lo <= year < hi:
            ordered.append(year)
    return ordered


def is_valid_document(data: Optional[dict]) -> bool:
    """Check if the JSON response is a real document (not an error page)."""
    return bool(data) and bool(data.get("articulos"))


class IMPODatosAbiertosScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
            "Accept": "application/json, text/html",
        })
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS * 2
        )
        self.session.mount("https://", adapter)
        self.checkpoint_path = Path(__file__).parent / "data" / "scan_checkpoint.json"
        self._completed_units, self._high_water = self._load_checkpoint()

    # ------------------------------------------------------------------ HTTP

    def fetch_document(self, path_segment: str, number: int, year: int) -> Optional[dict]:
        """Fetch a single document from the IMPO JSON API. Never raises."""
        url = f"{IMPO_BASE}/{path_segment}/{number}-{year}?json=true"
        for attempt in range(FETCH_ATTEMPTS):
            try:
                resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
            except Exception:
                continue
            if resp.status_code != 200:
                return None
            content_type = resp.headers.get("Content-Type", "")
            if "json" not in content_type and "javascript" not in content_type:
                # Misses are served as an HTML "Acceso no válido" page.
                return None
            try:
                return resp.json()
            except Exception:
                return None
        return None

    def test_api(self):
        """Test connectivity to IMPO JSON API."""
        logger.info("Testing IMPO datos abiertos JSON API...")
        data = self.fetch_document("constitucion", 1967, 1967)
        if is_valid_document(data):
            logger.info(f"  Constitution articles: {len(data['articulos'])}")
            logger.info("Connectivity test PASSED")
            return True
        logger.error("Connectivity test FAILED")
        return False

    # ------------------------------------------------------------ checkpoint

    def _load_checkpoint(self) -> tuple:
        """Return (completed units, per-unit highest document number seen)."""
        try:
            with open(self.checkpoint_path, encoding="utf-8") as f:
                state = json.load(f)
            high_water = {k: int(v) for k, v in (state.get("high_water") or {}).items()}
            return set(state.get("completed", [])), high_water
        except Exception:
            return set(), {}

    def _save_checkpoint(self):
        try:
            self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.checkpoint_path.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({
                    "completed": sorted(self._completed_units),
                    "high_water": self._high_water,
                }, f)
            tmp.replace(self.checkpoint_path)
        except Exception as e:
            logger.warning(f"Could not persist checkpoint: {e}")

    def _mark_complete(self, unit: str):
        self._completed_units.add(unit)
        self._save_checkpoint()

    def _note_hit(self, unit: str, number: int):
        """Track the highest live document number in a unit, for later refreshes."""
        if number > self._high_water.get(unit, 0):
            self._high_water[unit] = number

    # --------------------------------------------------------------- probing

    def _probe_chunk(self, jobs: list) -> list:
        """Probe (path_segment, number, [years]) jobs concurrently, order preserved."""
        def run(job):
            path_segment, number, years = job
            for year in years:
                data = self.fetch_document(path_segment, number, year)
                if is_valid_document(data):
                    return {
                        "path_segment": path_segment,
                        "number": number,
                        "year": year,
                        "data": data,
                    }
            return None

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            return list(pool.map(run, jobs))

    def _discover_high_water(self, path_segment: str, year: int, cap: int) -> int:
        """Find roughly the highest live number in a (type, year), cheaply.

        Years crawled before the high-water checkpoint existed have no recorded
        ceiling, and re-walking one from 1 just to learn it would cost the whole
        saving. A single strided descending pass costs ``cap / HIGHWATER_STRIDE``
        probes and lands within one stride below the true maximum.
        """
        jobs = [(path_segment, n, [year])
                for n in range(cap, 0, -HIGHWATER_STRIDE)]
        for raw in self._probe_chunk(jobs):
            if raw is not None:
                return raw["number"]
        return 0

    def _scan_year(self, path_segment: str, year: int, cap: int,
                   tolerance: int, first_number: int = 1
                   ) -> Generator[dict, None, None]:
        """Walk the number space for one (type, year), yielding raw hits."""
        unit = f"{path_segment}:{year}"
        if unit in self._completed_units:
            logger.info(f"  {unit}: already done (checkpoint), skipping")
            return
        miss_run = 0
        hits = 0
        for start in range(first_number, cap + 1, CHUNK):
            jobs = [(path_segment, n, [year])
                    for n in range(start, min(start + CHUNK, cap + 1))]
            for raw in self._probe_chunk(jobs):
                if raw is None:
                    miss_run += 1
                else:
                    miss_run = 0
                    hits += 1
                    self._note_hit(unit, raw["number"])
                    yield raw
            logger.info(
                f"  {unit}: scanned up to {min(start + CHUNK - 1, cap)}, "
                f"{hits} found, miss-run {miss_run}"
            )
            if miss_run >= tolerance:
                break
        self._mark_complete(unit)
        logger.info(f"  {unit}: complete — {hits} documents")

    def _scan_decree_laws(self) -> Generator[dict, None, None]:
        """Walk the decree-law number band, guessing the year from the anchor grid."""
        for start in range(DECREE_LEY_MIN, DECREE_LEY_MAX + 1, CHUNK):
            unit = f"decretos-ley:{start}"
            if unit in self._completed_units:
                logger.info(f"  {unit}: already done (checkpoint), skipping")
                continue
            jobs = [("decretos-ley", n, decree_ley_year_candidates(n))
                    for n in range(start, min(start + CHUNK, DECREE_LEY_MAX + 1))]
            hits = 0
            for raw in self._probe_chunk(jobs):
                if raw is not None:
                    hits += 1
                    yield raw
            self._mark_complete(unit)
            logger.info(f"  {unit}: complete — {hits} documents")

    # ------------------------------------------------------------ public API

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield raw IMPO documents for the whole corpus (BaseScraper contract)."""
        current_year = datetime.now(timezone.utc).year

        if "constitucion:1967" not in self._completed_units:
            logger.info("Fetching Constitution...")
            data = self.fetch_document("constitucion", 1967, 1967)
            if is_valid_document(data):
                yield {"path_segment": "constitucion", "number": 1967,
                       "year": 1967, "data": data}
                self._mark_complete("constitucion:1967")

        logger.info("Scanning decrees...")
        for year in range(FIRST_DECREE_YEAR, current_year + 1):
            yield from self._scan_year("decretos", year, DECREE_MAX_PER_YEAR,
                                       DECREE_MISS_TOLERANCE)

        logger.info("Scanning decree-laws...")
        yield from self._scan_decree_laws()

        logger.info("Scanning resolutions...")
        for year in range(FIRST_RESOLUTION_YEAR, current_year + 1):
            yield from self._scan_year("resoluciones", year, RESOLUTION_MAX_PER_YEAR,
                                       RESOLUTION_MISS_TOLERANCE)

    def fetch_updates(self, since=None) -> Generator[dict, None, None]:
        """Yield only documents that became available upstream since `since`.

        The comparator is ``fechaPublicacion`` -- the Diario Oficial issue that
        carried the norm. IMPO *is* the official publisher, so a norm appears in
        these databases on the day it is published; publication date is therefore
        an availability stamp, not a document date lagging behind it. It is also
        the only date the API exposes (there is no modified stamp, no ETag and no
        Last-Modified header on the JSON endpoint).

        Two narrowings, both needed. The publication cutoff decides what is
        *emitted*, so a refresh upserts the handful of new norms instead of every
        norm of the last two years. The per-unit high-water number decides where
        the probe *starts*, so it also stops re-downloading them: numbering runs
        forward through the year, and anything published after `since` is above
        the ceiling recorded on the last crawl.

        Decree-laws (1973-1985) and the 1967 Constitution are closed historical
        sets and are skipped; only decrees and resolutions still accrue numbers.
        """
        since_date = as_date_str(since)
        current_year = datetime.now(timezone.utc).year

        if since_date:
            try:
                first_year = int(since_date[:4])
            except ValueError:
                first_year = current_year - 1
        else:
            # No cutoff supplied — fall back to the last two years.
            first_year = current_year - 1
        # Publication lags promulgation by weeks and routinely crosses the new
        # year, so a December decree can surface in January under last year's
        # number. Never narrow the window past the previous year.
        first_year = min(first_year, current_year - 1)
        first_year = min(max(first_year, FIRST_DECREE_YEAR), current_year)

        logger.info(
            f"Incremental refresh: published on/after {since_date or '(no cutoff)'}, "
            f"scanning {first_year}-{current_year}"
        )

        emitted = skipped = 0
        for year in range(first_year, current_year + 1):
            for path_segment, first_year_of_type, cap, tol in (
                ("decretos", FIRST_DECREE_YEAR, DECREE_MAX_PER_YEAR,
                 DECREE_MISS_TOLERANCE),
                ("resoluciones", FIRST_RESOLUTION_YEAR, RESOLUTION_MAX_PER_YEAR,
                 RESOLUTION_MISS_TOLERANCE),
            ):
                if year < first_year_of_type:
                    continue
                unit = f"{path_segment}:{year}"
                ceiling = self._high_water.get(unit, 0)
                if not ceiling and unit in self._completed_units:
                    ceiling = self._discover_high_water(path_segment, year, cap)
                    if ceiling:
                        self._high_water[unit] = ceiling
                        logger.info(f"  {unit}: seeded high-water mark at {ceiling}")
                start = max(1, ceiling - UPDATE_BACKFILL_MARGIN) if ceiling else 1
                logger.info(f"  {unit}: probing from {start} (high-water {ceiling})")

                # A refresh deliberately re-walks the top of an already-complete
                # year, so the completeness flag must not short-circuit it.
                self._completed_units.discard(unit)
                for raw in self._scan_year(path_segment, year, cap, tol,
                                           first_number=start):
                    published = parse_impo_date(
                        (raw.get("data") or {}).get("fechaPublicacion")
                    )
                    if since_date and published and published < since_date:
                        skipped += 1
                        continue
                    emitted += 1
                    yield raw

        self._save_checkpoint()
        logger.info(
            f"Incremental refresh complete: {emitted} new/updated, "
            f"{skipped} already published before the cutoff"
        )

    def normalize(self, raw: dict) -> Optional[dict]:
        """Normalize a raw IMPO document into the standard schema."""
        data = raw.get("data") or {}
        path_segment = raw["path_segment"]
        number = raw["number"]
        year = raw["year"]

        tipo = (data.get("tipoNorma") or path_segment).strip()
        nombre = (data.get("nombreNorma") or "").strip()
        title = f"{tipo} {number}/{year}"
        if nombre:
            title = f"{title} - {nombre}"

        text = extract_text_from_articulos(data.get("articulos", []))

        preamble_parts = []
        for key, label in (("vistos", "VISTOS"), ("considerando", "CONSIDERANDO"),
                           ("firmantes", "FIRMANTES")):
            clean = strip_html(data.get(key, ""))
            if clean:
                preamble_parts.append(f"{label}:\n{clean}")
        if preamble_parts:
            text = "\n\n".join(preamble_parts) + "\n\n" + text

        if not text or len(text) <= 20:
            return None

        if path_segment == "constitucion":
            doc_id = "UY-IMPO-CONSTITUCION-1967"
            title = "Constitución de la República Oriental del Uruguay (1967)"
        else:
            doc_id = f"UY-IMPO-{path_segment.upper()}-{number}-{year}"

        return {
            "_id": doc_id,
            "_source": "UY/IMPODatosAbiertos",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": parse_impo_date(
                data.get("fechaPromulgacion") or data.get("fechaPublicacion")
            ),
            "url": f"https://www.impo.com.uy/bases/{path_segment}/{number}-{year}",
            "document_type": tipo,
            "document_number": str(number),
            "year": year,
            "path_segment": path_segment,
            "status": data.get("leyenda", ""),
        }

    # ------------------------------------------------------------------ CLI

    def run_curated_sample(self) -> int:
        """Fetch a curated spread of ~15 documents and write them to sample/."""
        logger.info("=== SAMPLE MODE: Fetching ~15 documents ===")
        targets = [("constitucion", 1967, 1967)]
        targets += [("decretos", n, y) for n, y in
                    [(1, 2024), (2, 2024), (3, 2024), (1, 2020), (50, 2020),
                     (1, 2015), (100, 2015), (1, 2010)]]
        targets += [("resoluciones", n, y) for n, y in
                    [(10, 2023), (150, 2023), (300, 2023), (1000, 2010)]]
        targets += [("decretos-ley", 14990, 1980), ("decretos-ley", 15000, 1980)]

        records = []
        for path_segment, number, year in targets:
            data = self.fetch_document(path_segment, number, year)
            if not is_valid_document(data):
                continue
            rec = self.normalize({"path_segment": path_segment, "number": number,
                                  "year": year, "data": data})
            if rec:
                records.append(rec)
                logger.info(f"  {path_segment} {number}/{year}: {len(rec['text'])} chars")

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        for rec in records:
            with open(sample_dir / f"{rec['_id']}.json", "w", encoding="utf-8") as f:
                json.dump(rec, f, ensure_ascii=False, indent=2)
        logger.info(f"=== Sample complete: {len(records)} records saved to sample/ ===")
        return len(records)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="UY/IMPODatosAbiertos bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "update", "test-api"]
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (~15 docs)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = IMPODatosAbiertosScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.sample:
        count = scraper.run_curated_sample()
        sys.exit(0 if count > 0 else 1)

    if args.command == "update":
        stats = scraper.update()
        logger.info(
            f"update complete: {stats.get('records_fetched', 0)} fetched, "
            f"{stats.get('records_new', 0)} new, {stats.get('errors', 0)} errors"
        )
        # A refresh that finds nothing new is a valid outcome, not a failure.
        sys.exit(1 if stats.get("errors", 0) and not stats.get("records_fetched") else 0)

    # Both `bootstrap` and `bootstrap-fast` stream the full corpus to
    # data/records.jsonl via BaseScraper's storage layer.
    stats = scraper.bootstrap()
    logger.info(
        f"bootstrap complete: {stats.get('records_fetched', 0)} fetched, "
        f"{stats.get('records_new', 0)} new, {stats.get('errors', 0)} errors"
    )
    sys.exit(0 if stats.get("records_fetched", 0) > 0 else 1)


if __name__ == "__main__":
    main()
