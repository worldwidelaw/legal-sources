#!/usr/bin/env python3
"""
AR/CABA-Legislation -- Ciudad Autónoma de Buenos Aires Legislation

Fetches full-text legislation from the Boletín Oficial de la Ciudad de Buenos
Aires REST API. Covers laws (Leyes) and decrees (Decretos) from 1996-present.

Data access:
  - REST API at api-restboletinoficial.buenosaires.gob.ar
  - /obtenerResultado/{params} for paginated search
  - /download/{id} for PDF documents
  - Full text extracted from PDFs via pdfplumber

Usage:
  python bootstrap.py bootstrap          # Full initial pull -> data/records.jsonl
  python bootstrap.py bootstrap-fast     # Alias for the full pull (fleet entry point)
  python bootstrap.py bootstrap --sample # Fetch 10-15 sample records
  python bootstrap.py update             # Incremental (newest first)
  python bootstrap.py test               # Quick connectivity test

Coverage note: the API only exposes a per-norm document for norms published from
2018 onward.  For 1996-2017 both ``link_documento_normas`` and
``/getUrlDocument`` come back empty -- only the whole-day gazette PDF exists --
so those norms are counted and reported but not emitted (metadata-only records
are not acceptable).
"""

import sys
import io
import json
import logging
import re
import time
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AR.CABA-Legislation")

API_BASE = "https://api-restboletinoficial.buenosaires.gob.ar"
DELAY = 2.0
PAGE_SIZE = 50

# Norm type IDs from the API
NORM_TYPES = {
    1: "Ley",
    2: "Decreto",
}

# The gazette's own archive starts in 1996; the API returns nothing before 2008.
FIRST_YEAR = 1996

# /getUrlDocument tipo values, per the API documentation at API_BASE/
DOC_TYPE_NORM = 4    # the norm itself
DOC_TYPE_ANNEX = 8   # its annex

# Norms published from this year on carry a per-norm document; earlier ones do
# not.  Probed 2026-08-29 over 120 pre-2018 norms: /getUrlDocument answers empty
# for every tipo 1-12, so calling it below the cutoff costs two requests per
# norm and can never return a URL.  The inline ``link_anexo`` field is still
# honoured below the cutoff -- it is populated for part of 2017 -- so the tail
# of the corpus is not thrown away, it just costs nothing extra to look for.
DOC_ERA_FIRST_YEAR = 2018

# Below this the "document" is an annex cover sheet, not the norm.
MIN_TEXT_CHARS = 200


class SourceUnavailable(RuntimeError):
    """Raised when the API is unreachable, so the run fails loudly instead of
    reporting an empty corpus as a success."""


def norm_year(norm: Dict[str, Any]) -> Optional[int]:
    """Four-digit year of a norm; ``anio_norma`` is stored two-digit."""
    year = norm.get("anio_norma")
    if not isinstance(year, int):
        return None
    if year >= 1000:
        return year
    return year + 2000 if year < 96 else year + 1900


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "LegalDataHunter/1.0 (legal-data-research)",
        "Accept": "application/json",
    })
    return session


class CABAFetcher:
    SOURCE_ID = "AR/CABA-Legislation"

    def __init__(self):
        self.session = get_session()
        self.data_dir = Path(__file__).parent / "data"
        self.checkpoint_path = self.data_dir / "caba_checkpoint.json"
        self.no_document = 0   # norms the API publishes no document for (pre-2018)
        self.no_text = 0       # documents that downloaded but extracted empty
        self.duplicates = 0    # norms the search endpoint returned more than once
        self.emitted_ids = set()

    def _get_json(self, url: str, timeout: int = 30) -> Any:
        last_error: Optional[Exception] = None
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 200:
                    try:
                        return resp.json()
                    except ValueError:
                        logger.warning("Non-JSON response from %s", url)
                        return None
                if resp.status_code == 429:
                    wait = 10 * (attempt + 1)
                    logger.warning("Rate limited, waiting %ds...", wait)
                    time.sleep(wait)
                    continue
                logger.warning("HTTP %d for %s", resp.status_code, url)
                return None
            except requests.RequestException as e:
                last_error = e
                logger.warning("Request error (attempt %d): %s", attempt + 1, e)
                time.sleep(5 * (attempt + 1))
        if last_error is not None:
            raise SourceUnavailable(f"{url} unreachable after 3 attempts: {last_error}")
        return None

    def _download_pdf_text(self, url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="AR/CABA-Legislation",
            source_id="",
            pdf_url=url,
            table="legislation",
        ) or ""

    def search_norms(self, norm_type: int, per_page: int = PAGE_SIZE,
                     offset: int = 0, year: Optional[int] = None) -> Dict[str, Any]:
        """Search norms by type with pagination."""
        params = f"perPage={per_page}&offset={offset}&tipoNorma={norm_type}"
        if year:
            params += f"&anio={year}"
        encoded = urllib.parse.quote(params)
        url = f"{API_BASE}/obtenerResultado/{encoded}"
        data = self._get_json(url, timeout=45)
        time.sleep(DELAY)
        if not data or not isinstance(data, dict):
            return {"normas": [], "total": 0}
        # For a year the archive does not cover the API answers `"normas": 0`
        # rather than an empty list, so coerce before anything tries to iterate.
        norms = data.get("normas") or []
        if not isinstance(norms, list):
            norms = []
        norms = [n for n in norms if isinstance(n, dict)]
        total = norms[0].get("total_count", 0) if norms else 0
        return {"normas": norms, "total": total}

    def _get_url_document(self, norm_id: Any, tipo: int) -> Optional[str]:
        """Ask the API where a norm's document lives.

        Returns an empty body (not a 404) when no document is published, which
        is the case for every norm before 2018.
        """
        if not norm_id:
            return None
        url = f"{API_BASE}/getUrlDocument/{norm_id}/{tipo}"
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=30)
            except requests.RequestException as e:
                logger.warning("getUrlDocument error (attempt %d): %s", attempt + 1, e)
                time.sleep(5 * (attempt + 1))
                continue
            if resp.status_code != 200:
                return None
            candidate = resp.text.strip().strip('"')
            return candidate if candidate.startswith("http") else None
        return None

    def _get_pdf_url(self, norm: Dict[str, Any]) -> Optional[str]:
        """Resolve the norm's own PDF.

        ``archivo_norma`` is NOT a usable filename: the stored documents carry a
        numeric suffix (ck_PL-LEY-LCABA-LCBA-6692-24-6784.pdf), so constructing
        a URL from it -- as this scraper used to -- yields a 404 HTML page for
        every norm.  Ask the API for the real URL instead.
        """
        # link_documento_normas is a list of [date, url] pairs; the url half is
        # empty for pre-2018 norms even though the pair itself is present.
        links = norm.get("link_documento_normas") or []
        if isinstance(links, list):
            for link in links:
                if isinstance(link, list) and len(link) >= 2 and link[1]:
                    return link[1]

        in_doc_era = norm_year(norm) is None or norm_year(norm) >= DOC_ERA_FIRST_YEAR

        if in_doc_era:
            resolved = self._get_url_document(norm.get("id"), DOC_TYPE_NORM)
            if resolved:
                return resolved

        # An annex is second best but still the norm's own text, unlike the
        # whole-day gazette PDF that boletines[] points at.
        anexo = norm.get("link_anexo")
        if isinstance(anexo, str) and anexo.startswith("http"):
            return anexo
        if in_doc_era:
            return self._get_url_document(norm.get("id"), DOC_TYPE_ANNEX)
        return None

    def count_norms(self, norm_type: int, year: int) -> int:
        """How many norms of one type the API holds for one year."""
        result = self.search_norms(norm_type, per_page=1, offset=0, year=year)
        return result["total"]

    def _parse_date(self, norm: Dict[str, Any]) -> Optional[str]:
        """Extract publication date from bulletin info."""
        boletines = norm.get("boletines", [])
        if boletines and isinstance(boletines, list):
            for b in boletines:
                if isinstance(b, list) and len(b) >= 2:
                    date_str = b[1]
                    try:
                        dt = datetime.strptime(date_str, "%d/%m/%Y")
                        return dt.strftime("%Y-%m-%d")
                    except (ValueError, TypeError):
                        pass
        return None

    def normalize(self, norm: Dict[str, Any], text: str) -> Dict[str, Any]:
        """Transform raw API norm into standard schema."""
        norm_type = norm.get("nombre_tipo", "")
        norm_num = norm.get("numero_norma", "")
        year = norm_year(norm) or norm.get("anio_norma", "")

        norm_id = f"AR-CABA-{norm_type}-{norm_num}-{year}" if norm_num else f"AR-CABA-{norm.get('id', '')}"
        date = self._parse_date(norm)

        title_parts = []
        if norm_type:
            title_parts.append(norm_type)
        if norm_num:
            title_parts.append(f"N° {norm_num}")
        if year:
            title_parts.append(f"({year})")
        title = " ".join(title_parts)

        summary = norm.get("sumario", "")
        if summary:
            title = f"{title} - {summary}"

        boletin_url = None
        boletines = norm.get("boletines", [])
        if boletines and isinstance(boletines, list) and len(boletines) > 0:
            b = boletines[0]
            if isinstance(b, list) and len(b) >= 1:
                boletin_url = f"https://boletinoficial.buenosaires.gob.ar/normativa/buscar/{norm.get('id', '')}"

        return {
            "_id": norm_id,
            "_source": self.SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "norm_type": norm_type,
            "norm_number": str(norm_num) if norm_num else None,
            "year": year,
            "summary": summary,
            "issuing_body": norm.get("nombre_reparticion", ""),
            "issuing_org": norm.get("organismo_emisor", ""),
            "section": norm.get("nombre_seccion", ""),
            "url": boletin_url or f"https://boletinoficial.buenosaires.gob.ar/normativa",
        }

    # ---- checkpoint -----------------------------------------------------
    #
    # The global offset walk this scraper used to do is not stable: paging
    # through tipoNorma alone returns years out of order and repeats ids across
    # disjoint offsets.  Adding &anio partitions the corpus into (type, year)
    # units whose per-year totals sum exactly to the type total, which makes the
    # walk both complete and resumable across fleet relaunches.

    def _load_checkpoint(self) -> Dict[str, Any]:
        try:
            return json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"done": []}

    def _save_checkpoint(self, state: Dict[str, Any]) -> None:
        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        self.checkpoint_path.write_text(
            json.dumps(state, ensure_ascii=False), encoding="utf-8")

    def _walk_year(self, type_id: int, year: int,
                   start_offset: int = 0) -> Generator[Dict[str, Any], None, None]:
        """Yield every raw norm of one type published in one year.

        Paging is stable once &anio pins the query, so ``start_offset`` lets a
        relaunched run pick up mid-year instead of re-fetching it.
        """
        offset = start_offset
        # The search endpoint repeats norms inside a single page (decreto 2/2026
        # comes back five times in a row), so the same id would otherwise be
        # fetched and emitted several times.
        seen_ids = set()
        while True:
            result = self.search_norms(type_id, per_page=PAGE_SIZE,
                                       offset=offset, year=year)
            norms = result["normas"]
            if not norms:
                return
            for norm in norms:
                norm_id = norm.get("id")
                if norm_id in seen_ids:
                    self.duplicates += 1
                    continue
                seen_ids.add(norm_id)
                yield norm
            offset += len(norms)
            if offset >= result["total"]:
                return

    def _record_for(self, norm: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch and normalize one norm, or None if it has no full text."""
        pdf_url = self._get_pdf_url(norm)
        if not pdf_url:
            self.no_document += 1
            logger.debug("  no document published for norm %s", norm.get("id"))
            return None
        text = self._download_pdf_text(pdf_url)
        # An annex cover sheet extracts to ~120 chars of "ANEXO - LEY N.º NNNN /
        # FIN DEL ANEXO" and nothing else; the shortest genuine norm runs well
        # past 200.  Emitting those stubs is how this source ended up with
        # sample records holding no law text at all.
        if not text or len(text.strip()) < MIN_TEXT_CHARS:
            self.no_text += 1
            logger.warning("  no usable text for norm %s (%d chars from %s)",
                           norm.get("id"), len(text or ""), pdf_url)
            return None
        record = self.normalize(norm, text)
        # Second guard: two API ids can still normalize to one _id if a norm
        # number is reused, and a collapsed key silently shrinks the corpus.
        if record["_id"] in self.emitted_ids:
            self.duplicates += 1
            logger.debug("  duplicate _id %s (api id %s)", record["_id"], norm.get("id"))
            return None
        self.emitted_ids.add(record["_id"])
        return record

    def fetch_all(self, sample: bool = False,
                  resume: bool = True) -> Generator[Dict[str, Any], None, None]:
        """Fetch all legislation, newest year first. sample=True stops at ~12."""
        sample_limit = 12 if sample else None
        count = 0
        enumerated = 0
        state = self._load_checkpoint() if (resume and not sample) else {"done": []}
        done = set(tuple(u) for u in state.get("done", []))
        partial = state.get("partial") or {}
        this_year = datetime.now(timezone.utc).year

        def persist(in_progress: Optional[Dict[str, Any]] = None) -> None:
            if sample:
                return
            snapshot: Dict[str, Any] = {"done": sorted(list(u) for u in done)}
            if in_progress:
                snapshot["partial"] = in_progress
            self._save_checkpoint(snapshot)

        for year in range(this_year, FIRST_YEAR - 1, -1):
            for type_id, type_name in NORM_TYPES.items():
                if (type_id, year) in done:
                    continue
                total = self.count_norms(type_id, year)
                if not total:
                    done.add((type_id, year))
                    continue
                start = 0
                if partial.get("unit") == [type_id, year]:
                    start = int(partial.get("offset") or 0)
                    logger.info("Resuming %s %d at offset %d", type_name, year, start)
                    partial = {}
                logger.info("Fetching %s %d (%d norms, from offset %d)...",
                            type_name, year, total, start)
                seen = start
                for norm in self._walk_year(type_id, year, start_offset=start):
                    enumerated += 1
                    seen += 1
                    record = self._record_for(norm)
                    if record:
                        count += 1
                        yield record
                        if sample_limit and count >= sample_limit:
                            logger.info("Sample limit reached (%d records)", count)
                            return
                    if seen % PAGE_SIZE == 0:
                        persist({"unit": [type_id, year], "offset": seen})
                done.add((type_id, year))
                persist()

        logger.info(
            "Fetched %d records from %d norms (%d have no document published, "
            "%d yielded no usable text, %d duplicates dropped)",
            count, enumerated, self.no_document, self.no_text, self.duplicates)
        if enumerated == 0:
            raise SourceUnavailable(
                "Enumerated 0 norms across every year -- the search API returned "
                "nothing, treat this run as failed rather than as an empty corpus.")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch norms published on or after ``since`` (YYYY-MM-DD).

        A gazette norm becomes available to us on the day its boletín is
        published, so the publication date is the right comparator here.
        """
        try:
            since_year = int(since[:4])
        except (TypeError, ValueError):
            raise SourceUnavailable(f"--since must be YYYY-MM-DD, got {since!r}")

        for year in range(datetime.now(timezone.utc).year, since_year - 1, -1):
            for type_id in NORM_TYPES:
                for norm in self._walk_year(type_id, year):
                    date = self._parse_date(norm)
                    if date and date < since:
                        continue
                    record = self._record_for(norm)
                    if record:
                        yield record

    def test(self) -> bool:
        """Quick connectivity test."""
        result = self.search_norms(1, per_page=1)
        if result["normas"]:
            logger.info("API test OK: %d laws available", result["total"])
            return True
        logger.error("API test FAILED: no results")
        return False


def _write_sample(sample_dir: Path, record: Dict[str, Any]) -> None:
    fname = re.sub(r'[^\w\-.]', '_', record["_id"]) + ".json"
    (sample_dir / fname).write_text(
        json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="AR/CABA-Legislation fetcher")
    parser.add_argument("command",
                        choices=["bootstrap", "bootstrap-fast", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--since", help="Date for incremental update (YYYY-MM-DD)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--no-resume", action="store_true",
                        help="Ignore the checkpoint and re-walk every year")
    parser.add_argument("--max-records", type=int,
                        help="Stop after N records (for verification runs)")
    args = parser.parse_args()

    fetcher = CABAFetcher()
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command == "test":
        ok = fetcher.test()
        sys.exit(0 if ok else 1)

    if args.command in ("bootstrap", "bootstrap-fast"):
        # The full path streams to data/records.jsonl -- writing only to sample/
        # is what made a completed crawl look like an empty one to the pipeline.
        records = fetcher.fetch_all(sample=args.sample, resume=not args.no_resume)
    else:
        records = fetcher.fetch_updates(args.since or "2024-01-01")

    count = 0
    if args.sample:
        for record in records:
            _write_sample(sample_dir, record)
            count += 1
            logger.info("  [%d] %s (%d chars)", count, record["_id"],
                        len(record.get("text", "")))
            if args.max_records and count >= args.max_records:
                break
        logger.info("Sample complete: %d records saved to %s", count, sample_dir)
        sys.exit(0 if count else 1)

    fetcher.data_dir.mkdir(parents=True, exist_ok=True)
    out_path = fetcher.data_dir / "records.jsonl"
    # Append: a resumed run continues the same corpus, and the loader dedups on _id.
    with out_path.open("a", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            fh.flush()
            count += 1
            if count % 50 == 0:
                logger.info("  %d records written", count)
            if args.max_records and count >= args.max_records:
                break
    logger.info("%s complete: %d records -> %s", args.command, count, out_path)
    sys.exit(0 if count else 1)


if __name__ == "__main__":
    main()
