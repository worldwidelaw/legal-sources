#!/usr/bin/env python3
"""
EU/COM-Documents — European Commission proposals & communications (COM docs).

The Commission publishes tens of thousands of preparatory documents: legislative
proposals (``COM(YYYY) NNNN final``), communications, reports, green/white papers
and recommendations. Each is stored, with full text, in the EU Publications
Office repository (CELLAR), addressable by a CELEX number of the form:

    5{YYYY}PC{NNNN}   proposal for a legislative act  ("PC")
    5{YYYY}DC{NNNN}   other COM document / communication ("DC")

(Sector 5 = preparatory acts.)

Data flow
---------
1. Enumerate every COM document via the public CELLAR SPARQL endpoint, filtering
   CELEX with a *year-scoped* regex ``^5{YYYY}(PC|DC)``. Year-scoping keeps each
   query's result set small so we never hit the ~10K SPARQL OFFSET ceiling that
   a single ``^5[0-9]{4}(PC|DC)`` (44K+ rows) would blow through.
2. Fetch the full text of each document from CELLAR via HTTP content
   negotiation:
     - communications/reports (DC) are served as OJ/Formex xHTML
       (Accept: application/xhtml+xml) → strip tags.
     - legislative proposals (PC) have no xHTML manifestation; CELLAR answers
       ``300 Multiple-Choice`` listing the born-digital PDF streams. We pick the
       English "ACT" part PDF (falling back to DOC_1) and extract with PyMuPDF.
   CELLAR content negotiation bypasses the eur-lex.europa.eu AWS-WAF that
   202-challenges datacenter IPs — so this is fleet-safe.
3. Normalize to the standard schema (doctrine).

Why not the EUR-Lex portal? EUR-Lex HTML is behind an AWS-WAF that challenges
datacenter IPs. CELLAR (publications.europa.eu) is the authoritative, anonymous,
datacenter-friendly full-text source for the same corpus. The sibling sources
EU/EESC-Opinions and EU/CoR-Opinions use the same recipe for sector-5 "AE"/"AR"
CELEX; this source extends it to "PC"/"DC" with a PDF fallback for proposals.
"""

import sys
import html
import json
import re
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

try:
    import fitz  # PyMuPDF
except ImportError:  # pragma: no cover
    fitz = None

logger = logging.getLogger("legal-data-hunter")

SPARQL_ENDPOINT = "http://publications.europa.eu/webapi/rdf/sparql"
CELLAR_CELEX = "http://publications.europa.eu/resource/celex/{celex}"

# COM documents exist from 1959 onward; go a little earlier to be safe.
MIN_YEAR = 1959

# The full corpus is ~44K documents, far more than one fleet slot can finish.
# Persist crawl progress so a torn-down/restarted run advances monotonically
# instead of re-walking 2026 forever (GH-1252).
CHECKPOINT_PATH = Path(__file__).resolve().parent / "data" / "checkpoint.json"

# Emit a heartbeat every N documents so a slow crawl is distinguishable from a
# hung one: the fleet watchdog (and a human reading the log) saw 4h of complete
# silence and assumed a hang, because nothing logged below 500-record intervals.
HEARTBEAT_EVERY = 25

# Hard caps on a single document download. ``requests`` timeouts are per-socket
# read, so a server trickling bytes indefinitely never trips them — these do.
MAX_DOC_BYTES = 80 * 1024 * 1024
MAX_DOC_SECONDS = 300

SPARQL_QUERY = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT ?celex ?date ?title WHERE {{
  ?work cdm:resource_legal_id_celex ?celex .
  FILTER(REGEX(STR(?celex), "{pattern}"))
  OPTIONAL {{ ?work cdm:work_date_document ?date . }}
  OPTIONAL {{
    ?exp cdm:expression_belongs_to_work ?work ;
         cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/ENG> ;
         cdm:expression_title ?title .
  }}
}}
ORDER BY ?celex
LIMIT {limit} OFFSET {offset}
"""

# Leading token in the Formex export is the source filename, e.g.
# "IMMC.COM%282023%29900%20final.ENG.xhtml.1_EN_ACT_part1_v4.docx" — strip it.
_FILENAME_PREFIX_RE = re.compile(r"^\s*\S+\.(?:docx|xml|fmx\.xml|xhtml[^\s]*)\s+", re.IGNORECASE)

# Streams inside a CELLAR "300 Multiple-Choice" listing.
_STREAM_RE = re.compile(
    r'href="([^"]+/DOC_\d+)"[\s\S]*?<li title="stream_name">([^<]+)</li>',
    re.IGNORECASE,
)


def _strip_html(raw_html: str) -> str:
    """Strip tags/entities from an xHTML document, returning clean text."""
    text = re.sub(r"<script[\s\S]*?</script>", " ", raw_html)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = _FILENAME_PREFIX_RE.sub("", text)
    text = re.sub(r"[ \t ]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _clean_pdf_text(text: str) -> str:
    """Tidy PyMuPDF output: collapse the EN/EN gutter markers and whitespace."""
    text = re.sub(r"[ \t ]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class COMDocumentsScraper(BaseScraper):
    """Scraper for European Commission COM documents via CELLAR."""

    PAGE_SIZE = 500

    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (+https://github.com/ZachLaik) legal-open-data",
        })
        # Disabled for sample runs so a 15-record sample never marks years done.
        self.checkpoint_enabled = True

    # ---- HTTP helper ------------------------------------------------------

    def _get(self, url, *, headers=None, params=None, max_retries=4, timeout=60,
             stream=False):
        last = None
        for attempt in range(max_retries):
            try:
                r = self.session.get(url, headers=headers, params=params,
                                     timeout=timeout, allow_redirects=True,
                                     stream=stream)
                if r.status_code == 200:
                    return r
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(min(2 ** attempt, 30))
                    continue
                return r  # 300/404 etc. — let caller decide
            except requests.RequestException as e:
                last = e
                time.sleep(min(2 ** attempt, 30))
        if last:
            raise last
        return None

    @staticmethod
    def _read_capped(r) -> bytes:
        """Read a streamed response under a hard size + wall-clock budget.

        CELLAR occasionally trickles a large stream to a throttled client; a
        plain ``r.content`` then blocks forever because the read timeout keeps
        being reset by each dribble of bytes. Raise instead of hanging.
        """
        started = time.time()
        buf = bytearray()
        for chunk in r.iter_content(64 * 1024):
            if chunk:
                buf.extend(chunk)
            if len(buf) > MAX_DOC_BYTES:
                raise RuntimeError(f"response exceeded {MAX_DOC_BYTES} bytes")
            if time.time() - started > MAX_DOC_SECONDS:
                raise RuntimeError(f"response exceeded {MAX_DOC_SECONDS}s wall clock")
        return bytes(buf)

    @classmethod
    def _read_text(cls, r) -> str:
        return cls._read_capped(r).decode(r.encoding or "utf-8", errors="replace")

    # ---- Checkpoint -------------------------------------------------------

    def _load_checkpoint(self) -> dict:
        try:
            with open(CHECKPOINT_PATH, encoding="utf-8") as fh:
                data = json.load(fh)
        except (OSError, ValueError):
            return {}
        if not isinstance(data, dict):
            return {}
        return data

    def _save_checkpoint(self, completed_years, in_progress) -> None:
        if not self.checkpoint_enabled:
            return
        payload = {
            "completed_years": sorted(completed_years, reverse=True),
            "in_progress": in_progress,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
            tmp = CHECKPOINT_PATH.with_suffix(".json.tmp")
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh)
            tmp.replace(CHECKPOINT_PATH)
        except OSError as e:
            logger.warning("Could not write checkpoint %s: %s", CHECKPOINT_PATH, e)

    # ---- Enumeration ------------------------------------------------------

    def _sparql_page(self, pattern: str, offset: int) -> list:
        query = SPARQL_QUERY.format(pattern=pattern, limit=self.PAGE_SIZE, offset=offset)
        r = self._get(
            SPARQL_ENDPOINT,
            params={"query": query, "format": "application/sparql-results+json"},
            timeout=180,
        )
        if r is None or r.status_code != 200:
            logger.warning("SPARQL page pattern=%s offset=%s failed (%s)",
                           pattern, offset, getattr(r, "status_code", "?"))
            return []
        try:
            bindings = r.json().get("results", {}).get("bindings", [])
        except ValueError:
            logger.warning("SPARQL returned non-JSON for pattern=%s offset=%s", pattern, offset)
            return []
        out = []
        for b in bindings:
            celex = b.get("celex", {}).get("value")
            if not celex:
                continue
            out.append({
                "celex": celex,
                "date": b.get("date", {}).get("value"),
                "title": b.get("title", {}).get("value"),
            })
        return out

    def _enumerate_year(self, year: int, start_offset: int = 0):
        """Yield ``(offset, metadata)`` for a single year, both PC and DC.

        ``offset`` is the SPARQL offset of the page the row came from, so the
        caller can checkpoint page-granular progress.
        """
        pattern = f"^5{year}(PC|DC)"
        offset = start_offset
        seen = set()
        while True:
            page = self._sparql_page(pattern, offset)
            if not page:
                break
            logger.info("Year %d: SPARQL offset %d → %d rows", year, offset, len(page))
            new = 0
            for meta in page:
                if meta["celex"] in seen:
                    continue
                seen.add(meta["celex"])
                new += 1
                yield offset, meta
            if len(page) < self.PAGE_SIZE:
                break
            offset += self.PAGE_SIZE
            if new == 0:
                break

    # ---- Full-text retrieval ---------------------------------------------

    def _fetch_text(self, celex: str) -> str:
        """Return clean full text for a CELEX, trying xHTML then PDF streams."""
        # 1) xHTML (communications / OJ Formex bodies).
        r = self._get(
            CELLAR_CELEX.format(celex=celex),
            headers={"Accept": "application/xhtml+xml", "Accept-Language": "en"},
            timeout=90,
            stream=True,
        )
        if r is not None and r.status_code == 200 and "xml" in (r.headers.get("content-type", "")):
            text = _strip_html(self._read_text(r))
            if len(text) >= 200:
                return text

        # 2) PDF streams (legislative proposals have no xHTML manifestation).
        if fitz is None:
            return ""
        r = self._get(
            CELLAR_CELEX.format(celex=celex),
            headers={"Accept": "application/pdf", "Accept-Language": "en"},
            timeout=90,
            stream=True,
        )
        if r is None:
            return ""
        # Direct single-PDF response.
        ctype = r.headers.get("content-type", "")
        if r.status_code == 200 and "pdf" in ctype:
            return self._pdf_bytes_to_text(self._read_capped(r))
        # 300 Multiple-Choice listing → pick the EN ACT part PDF.
        if r.status_code == 300:
            url = self._pick_pdf_stream(self._read_text(r))
            if not url:
                return ""
            pr = self._get(url, timeout=120, stream=True)
            if pr is None or pr.status_code != 200:
                return ""
            return self._pdf_bytes_to_text(self._read_capped(pr))
        return ""

    @staticmethod
    def _pick_pdf_stream(listing_html: str) -> Optional[str]:
        """Choose the best PDF stream from a CELLAR 300 listing.

        Prefer the English "ACT part1" body; fall back to the first English PDF,
        then to the first stream of any language.
        """
        streams = _STREAM_RE.findall(listing_html)
        if not streams:
            return None
        # Prefer EN ACT part1 PDF.
        for url, name in streams:
            if re.search(r"_EN_.*part1.*\.pdf$", name, re.I) or re.search(r"_EN_ACT", name, re.I):
                return url
        # Any EN pdf.
        for url, name in streams:
            if "_EN_" in name.upper() and name.lower().endswith(".pdf"):
                return url
        # First stream.
        return streams[0][0]

    @staticmethod
    def _pdf_bytes_to_text(data: bytes) -> str:
        if fitz is None or not data:
            return ""
        try:
            doc = fitz.open(stream=data, filetype="pdf")
        except Exception as e:
            logger.debug("fitz open failed: %s", e)
            return ""
        try:
            parts = [page.get_text() for page in doc]
            return _clean_pdf_text("\n".join(parts))
        finally:
            doc.close()

    # ---- BaseScraper contract --------------------------------------------

    def _current_year(self) -> int:
        return datetime.now(timezone.utc).year

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW COM-document dicts (with full text), newest year first.

        Resumable: years finished by an earlier run are skipped with no network
        calls at all, and the year in progress restarts at its last completed
        SPARQL page, so successive fleet slots advance through the ~44K corpus
        instead of re-crawling 2026 every time (GH-1252).
        """
        checkpoint = self._load_checkpoint()
        completed = {int(y) for y in checkpoint.get("completed_years", [])}
        resume = checkpoint.get("in_progress") or {}
        # The current year keeps gaining documents, so never treat it as done —
        # otherwise a fully-caught-up source would yield 0 records forever and
        # every later fleet run would look like a failure. The loader dedups.
        completed.discard(self._current_year())
        if completed:
            logger.info("Checkpoint: %d years already crawled, resuming", len(completed))

        seen_docs = 0
        for year in range(self._current_year(), MIN_YEAR - 1, -1):
            if year in completed:
                logger.info("Year %d already complete (checkpoint) — skipping", year)
                continue

            start_offset = 0
            if resume.get("year") == year:
                start_offset = int(resume.get("offset") or 0)
                if start_offset:
                    logger.info("Year %d: resuming at SPARQL offset %d", year, start_offset)
            resume = {}

            saved_offset = -1
            for offset, meta in self._enumerate_year(year, start_offset):
                if offset != saved_offset:
                    # The previous page has been fully consumed by the caller.
                    self._save_checkpoint(completed, {"year": year, "offset": offset})
                    saved_offset = offset

                celex = meta["celex"]
                try:
                    text = self._fetch_text(celex)
                except Exception as e:
                    logger.warning("Fetch failed for %s: %s", celex, e)
                    continue

                seen_docs += 1
                if seen_docs % HEARTBEAT_EVERY == 0:
                    logger.info("Heartbeat: year %d, %d documents processed this run",
                                year, seen_docs)

                if len(text) < 200:
                    logger.debug("Skip %s: text too short (%d)", celex, len(text))
                    continue
                time.sleep(1)  # be polite to CELLAR
                yield {
                    "celex": celex,
                    "date": meta.get("date"),
                    "title": meta.get("title"),
                    "text": text,
                }

            completed.add(year)
            self._save_checkpoint(completed, None)
            logger.info("Year %d complete (%d documents processed so far)", year, seen_docs)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield COM documents from the years spanning ``since`` to now.

        COM CELEX years map directly to publication years, so we only need to
        rescan the affected years; the loader dedups on _id.
        """
        start_year = since.year if isinstance(since, datetime) else int(str(since)[:4])
        cutoff = since.date().isoformat() if isinstance(since, datetime) else str(since)
        for year in range(self._current_year(), start_year - 1, -1):
            for _offset, meta in self._enumerate_year(year):
                d = meta.get("date")
                if d and d < cutoff:
                    continue
                celex = meta["celex"]
                text = self._fetch_text(celex)
                if len(text) < 200:
                    continue
                time.sleep(1)
                yield {
                    "celex": celex,
                    "date": meta.get("date"),
                    "title": meta.get("title"),
                    "text": text,
                }

    def normalize(self, raw: dict) -> dict:
        celex = raw["celex"]
        title = (raw.get("title") or "").strip()
        if not title:
            title = f"COM document {celex}"
        date = raw.get("date")
        if date and not re.match(r"^\d{4}-\d{2}-\d{2}", str(date)):
            date = None
        return {
            "_id": celex,
            "_source": "EU/COM-Documents",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "celex": celex,
            "title": title,
            "text": raw["text"],
            "date": date,
            "url": f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{celex}",
        }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    scraper = COMDocumentsScraper()

    command = sys.argv[1] if len(sys.argv) > 1 else "bootstrap"
    sample_mode = "--sample" in sys.argv

    if command in ("bootstrap", "bootstrap-fast"):
        # A 15-record sample must never mark 2026 as a crawled year.
        scraper.checkpoint_enabled = not sample_mode
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(f"\nBootstrap complete: {stats}")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats}")
    elif command == "test":
        page = scraper._sparql_page("^52024(PC|DC)", 0)
        print(f"SPARQL returned {len(page)} COM docs for 2024 page 1")
        for meta in page[:3]:
            c = meta["celex"]
            txt = scraper._fetch_text(c)
            print(f"  {c}: {(meta.get('title') or '')[:60]} -> {len(txt)} chars")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
