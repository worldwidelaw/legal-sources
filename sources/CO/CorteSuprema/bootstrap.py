#!/usr/bin/env python3
"""
CO/CorteSuprema -- Colombian Supreme Court Jurisprudence Fetcher

Fetches decisions from Colombia's Corte Suprema de Justicia via the GraphQL API
at consultaprovidenciasbk.cortesuprema.gov.co.

Document access (verified 2026-08-15, see issue #1431)
-----------------------------------------------------
The search index (Solr) is much broader than the file store behind
`/downloadFile`, so a large slice of listing hits is simply not downloadable
from anywhere -- the site's own download button 404s on them too:

  * entries listed as `.doc`   -> only the server-rendered `.pdf` exists, and
                                  it is image-only (needs OCR)
  * entries listed as `.docx`  -> `.docx` and `.pdf` both exist, both with a
    or `.pdf`                     real text layer
  * the whole PENAL room       -> every extension 404s, `/fileExists` says
                                  false; ~90K indexed documents are unreachable
  * assorted room/year subtrees (Civil 2025, Laboral 2023+, Tutelas <=2018)
                               -> same, the files were never mounted

Consequences baked into this scraper:

  1. At most TWO download attempts per document, ordered by what actually
     exists for that listing extension (was: three, two of them guaranteed
     404s -- the 30,462 misses in #1431).
  2. Per-unit (room, year) availability probe. If the first
     ``UNIT_PROBE_SIZE`` non-junk documents all fail every attempt, the unit is
     recorded as unavailable, logged loudly, and skipped.
  3. Two-phase crawl. Phase ``text`` takes everything with a real text layer
     (docx, or PDF read with PyMuPDF) and *defers* image-only PDFs to
     ``data/deferred_ocr.jsonl``. Phase ``ocr`` drains that backlog through the
     shared OCR extractor. A run that dies on the 100h fleet cap therefore
     lands the cheap documents first instead of burning the budget on OCR.
  4. Checkpoint in ``data/checkpoint.json`` (unit + paging offset + phase), so
     a capped run resumes instead of re-walking 96K listing pages.
  5. Listing entries are de-duplicated -- the API returns the same path more
     than once (the 1.5x JSONL duplication in #1431).

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
  python bootstrap.py bootstrap-fast
"""

import argparse
import io
import json
import logging
import re
import sys
import time
import zipfile
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

try:
    import fitz  # PyMuPDF -- fast text-layer read, avoids the Java extractor
except ImportError:
    fitz = None

SOURCE_ID = "CO/CorteSuprema"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"
DATA_DIR = SOURCE_DIR / "data"
RECORDS_FILE = DATA_DIR / "records.jsonl"
CHECKPOINT_FILE = DATA_DIR / "checkpoint.json"
DEFERRED_FILE = DATA_DIR / "deferred_ocr.jsonl"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CO.CorteSuprema")

BASE_URL = "https://consultaprovidenciasbk.cortesuprema.gov.co"
API_URL = f"{BASE_URL}/api"
DOWNLOAD_URL = f"{BASE_URL}/downloadFile"
FILTERS_URL = f"{BASE_URL}/filters"

ROOMS = ["Civil", "Laboral", "Penal", "Tutelas"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
}

PAGE_SIZE = 10          # server-fixed; SearchQuery has no page-size input
REQUEST_DELAY = 1.5     # between listing pages
DOC_DELAY = 0.4         # between documents (each does <=2 downloads)

# A unit is (room, year). If the first UNIT_PROBE_SIZE non-junk documents in a
# unit all fail every download attempt, the files were never mounted.
UNIT_PROBE_SIZE = 40

# Below this, a PDF's text layer is treated as absent (image-only scan).
MIN_TEXT_LAYER_CHARS_PER_PAGE = 120
MIN_TEXT_CHARS = 400

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


# --------------------------------------------------------------------------
# API
# --------------------------------------------------------------------------

def graphql_query(query: str, retries: int = 3) -> dict:
    """Execute a GraphQL query with retry logic."""
    for attempt in range(retries):
        try:
            resp = SESSION.post(API_URL, json={"query": query}, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            if "errors" in data:
                logger.warning("GraphQL errors: %s", data["errors"])
                return {}
            return data.get("data", {})
        except (requests.RequestException, ValueError) as e:
            logger.warning("Request failed (attempt %d/%d): %s", attempt + 1, retries, e)
            if attempt < retries - 1:
                time.sleep(2 ** (attempt + 1))
    return {}


def search_documents(room: str, year: str = "", start: int = 0,
                     auto_sentencia: str = "") -> tuple:
    """Search documents in a room. Returns (total_count, results_list)."""
    query = """
    {
      getSearchResult(searchQuery: {
        query: "*",
        typeOfQuery: "%s",
        start: %d,
        isExact: false,
        magistrate: "",
        year: "%s",
        autoSentencia: "%s",
        order: "reciente",
        roomTutelas: "",
        addedQueries: []
      }) {
        numOfResults
        searchResults {
          id
          title
          doctor
          fechaCreacion
          ano
          autoSentencia
          typeOfDocument
        }
      }
    }
    """ % (room, start, year, auto_sentencia)

    data = graphql_query(query)
    result = data.get("getSearchResult", {})
    return result.get("numOfResults", 0), result.get("searchResults", [])


def get_room_years(room: str) -> list:
    """Years available for a room, newest first, from the /filters endpoint."""
    try:
        resp = SESSION.get(FILTERS_URL, timeout=30)
        resp.raise_for_status()
        for item in resp.json():
            for key, value in item.items():
                if key == room:
                    years = value.get("years", {}).get("data", []) or []
                    return sorted({str(y) for y in years}, reverse=True)
    except (requests.RequestException, ValueError, AttributeError) as e:
        logger.warning("Could not read /filters for %s: %s", room, e)
    return []


def get_content_html(doc_id: str, room: str) -> str:
    """Fetch document content via GraphQL getContentSearch.

    Only ever returns the section headings ("ANTECEDENTES", "CONSIDERACIONES",
    ...), never the body, so it is not a usable full-text fallback -- kept for
    test-api diagnostics.
    """
    escaped_id = doc_id.replace('"', '\\"')
    query = """
    {
      getContentSearch(previewDocument: {
        id: "%s",
        room: "%s"
      }) {
        contentText
        title
        id
      }
    }
    """ % (escaped_id, room)

    data = graphql_query(query)
    result = data.get("getContentSearch", {})
    return result.get("contentText", "")


# --------------------------------------------------------------------------
# Download + extraction
# --------------------------------------------------------------------------

def download_file(path: str) -> Optional[bytes]:
    """POST a store path to /downloadFile. Returns bytes, or None on 404/error."""
    try:
        resp = SESSION.post(DOWNLOAD_URL, json={"path": path}, timeout=90)
    except requests.RequestException as e:
        logger.warning("downloadFile transport error for %s: %s", path, e)
        return None
    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        logger.warning("downloadFile HTTP %d for %s", resp.status_code, path)
        return None
    if len(resp.content) < 100:
        return None
    return resp.content


def docx_text(blob: bytes) -> str:
    """Extract text from .docx bytes (ZIP/XML)."""
    if blob[:2] != b"PK":
        return ""
    try:
        with zipfile.ZipFile(io.BytesIO(blob)) as zf:
            if "word/document.xml" not in zf.namelist():
                return ""
            xml_content = zf.read("word/document.xml").decode("utf-8", errors="replace")
    except (zipfile.BadZipFile, KeyError, RuntimeError) as e:
        logger.warning("Bad docx payload: %s", e)
        return ""
    # Paragraph and line breaks first, so the body does not collapse to one line.
    xml_content = re.sub(r"</w:p>", "\n", xml_content)
    xml_content = re.sub(r"<w:br[^>]*/>", "\n", xml_content)
    text = re.sub(r"<[^>]+>", "", xml_content)
    text = unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def pdf_text_layer(blob: bytes) -> str:
    """Read a PDF's text layer with PyMuPDF. Empty string for image-only PDFs.

    The server renders old .doc files to image-only PDFs, so this cheap read
    also tells us which documents genuinely need OCR.
    """
    if fitz is None or blob[:5] != b"%PDF-":
        return ""
    try:
        with fitz.open(stream=blob, filetype="pdf") as doc:
            pages = doc.page_count
            text = "\n".join(page.get_text() for page in doc)
    except Exception as e:  # fitz raises bare Exception subclasses
        logger.warning("PyMuPDF failed to open PDF: %s", e)
        return ""
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    if len(text) < MIN_TEXT_CHARS:
        return ""
    if pages and len(text) < pages * MIN_TEXT_LAYER_CHARS_PER_PAGE:
        return ""
    return text


def ocr_pdf(blob: bytes, doc_path: str) -> str:
    """Last resort: run the shared extractor (opendataloader -> ... -> OCR)."""
    text = extract_pdf_markdown(
        source=SOURCE_ID,
        source_id=doc_path,
        pdf_bytes=blob,
        table="case_law",
        force=True,
    )
    if not text:
        logger.warning("OCR yielded no text for %s", doc_path)
        return ""
    return text


def is_junk_file(doc_id: str) -> bool:
    """Check if a document path is a junk/temp file that should be skipped."""
    basename = Path(doc_id).name
    if basename.startswith("~$") or basename.startswith("~BROMIUM"):
        return True
    if basename in ("Thumbs.db", "desktop.ini", ".DS_Store"):
        return True
    if basename.startswith("prueba"):
        return True
    if not basename.lower().endswith((".docx", ".pdf", ".doc")):
        return True
    return False


def download_attempts(doc_id: str) -> list:
    """Store paths to try, in order, for a listing entry.

    Derived from what the store actually holds (issue #1431):
      .doc  -> only the rendered .pdf exists
      .docx -> .docx (text) and .pdf (also text); .docx is cheaper
      .pdf  -> .docx usually exists alongside and is cheaper
    """
    stem = re.sub(r"\.[A-Za-z]+$", "", doc_id)
    ext = Path(doc_id).suffix.lower()
    if ext == ".doc":
        return [stem + ".pdf"]
    if ext in (".docx", ".pdf"):
        return [stem + ".docx", stem + ".pdf"]
    return [stem + ".pdf"]


class Deferred(Exception):
    """Raised when a document is downloadable but needs OCR (phase 2)."""

    def __init__(self, path: str):
        super().__init__(path)
        self.path = path


def fetch_full_text(doc_id: str, allow_ocr: bool) -> str:
    """Download and extract a document's full text.

    Raises ``Deferred`` when the only available rendition is an image-only PDF
    and ``allow_ocr`` is False, so the caller can park it for phase 2.
    Returns "" when nothing is downloadable at all.
    """
    scanned_path = None

    for path in download_attempts(doc_id):
        blob = download_file(path)
        if blob is None:
            continue
        if path.lower().endswith(".docx"):
            text = docx_text(blob)
            if len(text) > 100:
                return text
            continue
        text = pdf_text_layer(blob)
        if len(text) > 100:
            return text
        # Downloadable PDF with no usable text layer -> scanned.
        scanned_path = path
        if allow_ocr:
            text = ocr_pdf(blob, path)
            if len(text) > 100:
                return text

    if scanned_path and not allow_ocr:
        raise Deferred(scanned_path)
    return ""


# --------------------------------------------------------------------------
# Normalisation
# --------------------------------------------------------------------------

def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def make_document_id(doc_id: str) -> str:
    """Create a clean document ID from the file path."""
    filename = Path(doc_id).stem
    filename = re.sub(r'^~\$', '', filename)
    return filename


def normalize(raw: dict, room: str) -> dict:
    """Transform raw API result into standard schema."""
    doc_id = raw.get("id", "")
    title = raw.get("title", "")
    title = re.sub(r'^~\$', '', title)
    title = re.sub(r'\.(docx?|pdf)$', '', title)

    date_str = raw.get("fechaCreacion")
    if date_str:
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            date_str = dt.strftime("%Y-%m-%d")
        except (ValueError, AttributeError):
            date_str = None

    year = raw.get("ano")
    if not date_str and year:
        date_str = f"{year}-01-01"

    return {
        "_id": f"CO/CorteSuprema/{room}/{make_document_id(doc_id)}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": "",  # filled in later
        "date": date_str,
        "url": "https://consultaprovidencias.cortesuprema.gov.co/",
        "sala": room,
        "magistrate": raw.get("doctor", ""),
        "year": year,
        "auto_sentencia": raw.get("autoSentencia", ""),
        "original_path": doc_id,
    }


# --------------------------------------------------------------------------
# Checkpoint
# --------------------------------------------------------------------------

def load_checkpoint() -> dict:
    if CHECKPOINT_FILE.exists():
        try:
            state = json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
            state.setdefault("units_done", [])
            state.setdefault("units_unavailable", [])
            state.setdefault("current_unit", None)
            state.setdefault("current_start", 0)
            state.setdefault("phase", "text")
            return state
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Unreadable checkpoint, starting fresh: %s", e)
    return {
        "units_done": [],
        "units_unavailable": [],
        "current_unit": None,
        "current_start": 0,
        "phase": "text",
    }


def save_checkpoint(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    tmp.replace(CHECKPOINT_FILE)


def load_written_ids() -> set:
    """Read back the _ids already streamed to records.jsonl so restarts resume."""
    written = set()
    for path in (RECORDS_FILE, DEFERRED_FILE):
        if not path.exists():
            continue
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    written.add(json.loads(line)["_id"])
                except (json.JSONDecodeError, KeyError):
                    continue
    return written


# --------------------------------------------------------------------------
# Crawl
# --------------------------------------------------------------------------

def iter_units(rooms: list, years: Optional[list]) -> list:
    """(room, year) crawl units, newest year first."""
    units = []
    for room in rooms:
        room_years = years if years else get_room_years(room)
        if not room_years:
            units.append((room, ""))
            continue
        for year in room_years:
            units.append((room, str(year)))
    return units


def crawl_unit(room: str, year: str, seen: set, allow_ocr: bool,
               start: int = 0, max_docs: int = 0,
               on_progress=None) -> Generator[dict, None, None]:
    """Yield records for one (room, year), de-duplicated and availability-probed.

    Deferred (scan-only) documents are yielded with ``text == ""`` and
    ``_needs_ocr`` set, so the caller can park them for phase 2.
    """
    total, results = search_documents(room, year=year, start=start)
    if not total:
        logger.info("Unit %s/%s: 0 results", room, year or "all")
        return
    logger.info("Unit %s/%s: %d indexed documents (from offset %d)",
                room, year or "all", total, start)

    probed = 0
    probe_hits = 0
    yielded = 0

    while start < total:
        if start > 0 or not results:
            _, results = search_documents(room, year=year, start=start)
        if not results:
            break

        for raw in results:
            doc_id = raw.get("id", "")
            if not doc_id or is_junk_file(doc_id):
                continue

            record = normalize(raw, room)
            if record["_id"] in seen:
                continue      # de-dupe: the listing repeats paths (#1431)
            seen.add(record["_id"])

            time.sleep(DOC_DELAY)
            try:
                text = fetch_full_text(doc_id, allow_ocr=allow_ocr)
            except Deferred as d:
                probe_hits += 1
                probed += 1
                record["_needs_ocr"] = d.path
                yield record
                yielded += 1
                if max_docs and yielded >= max_docs:
                    return
                continue

            probed += 1
            if not text:
                logger.debug("No downloadable rendition: %s", doc_id)
            else:
                probe_hits += 1
                record["text"] = text
                yield record
                yielded += 1
                if max_docs and yielded >= max_docs:
                    return

        start += PAGE_SIZE
        if on_progress:
            on_progress(start)

        # Availability probe: a unit whose files were never mounted 404s on
        # every single attempt. Stop rather than spend hours on guaranteed 404s.
        if probed >= UNIT_PROBE_SIZE and probe_hits == 0:
            logger.error(
                "UNIT UNAVAILABLE %s/%s: %d consecutive documents had no "
                "downloadable rendition (every extension 404s). Skipping the "
                "remaining ~%d indexed documents -- the files are not in the "
                "upstream store, see issue #1431.",
                room, year or "all", probed, max(0, total - start),
            )
            raise UnitUnavailable(room, year, total)

        time.sleep(REQUEST_DELAY)


class UnitUnavailable(Exception):
    def __init__(self, room: str, year: str, total: int):
        super().__init__(f"{room}/{year}")
        self.room, self.year, self.total = room, year, total


def fetch_all(rooms: list = None, years: list = None,
              max_per_room: int = 0, skip_ids: set = None,
              allow_ocr: bool = True) -> Generator[dict, None, None]:
    """Yield all documents with full text (sample/ad-hoc entry point)."""
    seen = set(skip_ids or ())
    for room, year in iter_units(rooms or ROOMS, years):
        try:
            for record in crawl_unit(room, year, seen, allow_ocr=allow_ocr,
                                     max_docs=max_per_room):
                if record.get("_needs_ocr"):
                    continue
                yield record
        except UnitUnavailable:
            continue


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch documents modified since a date."""
    try:
        since_dt = datetime.fromisoformat(since)
    except ValueError:
        since_dt = datetime.now(timezone.utc).replace(year=datetime.now().year - 1)

    current_year = datetime.now().year
    years = [str(y) for y in range(current_year, since_dt.year - 1, -1)]

    for record in fetch_all(years=years):
        if record.get("date") and record["date"] >= since:
            yield record


# --------------------------------------------------------------------------
# Bootstrap
# --------------------------------------------------------------------------

def save_sample(records: list) -> None:
    """Save sample records to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    for i, record in enumerate(records):
        filepath = SAMPLE_DIR / f"sample_{i:03d}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info("Saved sample %d: %s (text: %d chars)",
                    i, record["title"], len(record.get("text", "")))


def run_sample() -> None:
    logger.info("Running in SAMPLE mode")
    records = []
    # Units verified to have downloadable renditions (issue #1431): docx-era
    # units are cheap, .doc-era units exercise the scanned-PDF/OCR path.
    sample_plans = [
        ("Civil", "2023", 3),
        ("Civil", "2018", 2),
        ("Tutelas", "2023", 3),
        ("Tutelas", "2021", 3),
        ("Laboral", "2018", 2),
        ("Civil", "2021", 2),
    ]
    for room, year, target in sample_plans:
        logger.info("Sampling %s/%s (target %d)", room, year, target)
        seen = {r["_id"] for r in records}
        count = 0
        try:
            # allow_ocr=False keeps the sample fast; scanned-only units are
            # covered by the text-layer PDFs in Civil/Tutelas.
            for record in crawl_unit(room, year, seen, allow_ocr=False,
                                     max_docs=target * 6):
                if record.get("_needs_ocr"):
                    continue
                if len(record.get("text", "")) > 1000:
                    records.append(record)
                    count += 1
                if count >= target:
                    break
        except UnitUnavailable:
            logger.warning("Sample unit %s/%s unavailable, skipping", room, year)
        time.sleep(REQUEST_DELAY)

    if not records:
        print("ERROR: No records fetched!")
        sys.exit(1)

    save_sample(records)
    print(f"\nSample complete: {len(records)} records with full text")
    for r in records:
        print(f"  [{r['sala']}] {r['title']} - {len(r.get('text',''))} chars")


def run_full() -> None:
    logger.info("Running FULL bootstrap -> %s", RECORDS_FILE)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    state = load_checkpoint()
    seen = load_written_ids()
    if seen:
        logger.info("Resuming: %d documents already recorded", len(seen))

    written = 0
    deferred = 0

    def emit(handle, record):
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        handle.flush()

    # ---- Phase 1: everything with a real text layer -----------------------
    if state["phase"] == "text":
        units = iter_units(ROOMS, None)
        done = set(tuple(u) for u in state["units_done"])
        unavailable = set(tuple(u) for u in state["units_unavailable"])
        with open(RECORDS_FILE, "a", encoding="utf-8") as out, \
                open(DEFERRED_FILE, "a", encoding="utf-8") as park:
            for room, year in units:
                key = [room, year]
                if (room, year) in done or (room, year) in unavailable:
                    continue
                start = 0
                if state["current_unit"] == key:
                    start = state.get("current_start", 0)
                    logger.info("Resuming %s/%s at offset %d", room, year, start)
                state["current_unit"] = key
                state["current_start"] = start
                save_checkpoint(state)

                def progress(offset, _state=state):
                    _state["current_start"] = offset
                    save_checkpoint(_state)

                try:
                    for record in crawl_unit(room, year, seen, allow_ocr=False,
                                             start=start, on_progress=progress):
                        if record.pop("_needs_ocr", None):
                            emit(park, record)
                            deferred += 1
                        else:
                            emit(out, record)
                            written += 1
                        if (written + deferred) % 100 == 0:
                            logger.info("Phase text: %d written, %d deferred to OCR",
                                        written, deferred)
                except UnitUnavailable:
                    state["units_unavailable"].append(key)
                else:
                    state["units_done"].append(key)
                state["current_unit"] = None
                state["current_start"] = 0
                save_checkpoint(state)

        state["phase"] = "ocr"
        save_checkpoint(state)
        logger.info("Phase text complete: %d written, %d parked for OCR",
                    written, deferred)
        if state["units_unavailable"]:
            logger.error("Units with no downloadable files upstream: %s",
                         ", ".join(f"{r}/{y}" for r, y in state["units_unavailable"]))

    # ---- Phase 2: OCR the parked image-only PDFs --------------------------
    if state["phase"] == "ocr" and DEFERRED_FILE.exists():
        already = set()
        if RECORDS_FILE.exists():
            with open(RECORDS_FILE, encoding="utf-8") as f:
                for line in f:
                    try:
                        already.add(json.loads(line)["_id"])
                    except (json.JSONDecodeError, KeyError):
                        continue
        pending = []
        with open(DEFERRED_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec["_id"] not in already:
                    pending.append(rec)
        logger.info("Phase ocr: %d parked documents to extract", len(pending))
        ocr_written = 0
        with open(RECORDS_FILE, "a", encoding="utf-8") as out:
            for rec in pending:
                path = rec.pop("_needs_ocr", None) or rec.get("original_path", "")
                time.sleep(DOC_DELAY)
                blob = download_file(path)
                if blob is None:
                    logger.warning("Parked document vanished: %s", path)
                    continue
                text = ocr_pdf(blob, path)
                if len(text) <= 100:
                    continue
                rec["text"] = text
                rec["_fetched_at"] = datetime.now(timezone.utc).isoformat()
                emit(out, rec)
                ocr_written += 1
                if ocr_written % 50 == 0:
                    logger.info("Phase ocr: %d written", ocr_written)
        written += ocr_written
        logger.info("Phase ocr complete: %d written", ocr_written)

    logger.info("Bootstrap complete: %d documents written this run", written)


# --------------------------------------------------------------------------
# Diagnostics
# --------------------------------------------------------------------------

def test_api():
    """Test API connectivity, availability of each room, and extraction."""
    print("Testing filters endpoint...")
    for room in ROOMS:
        years = get_room_years(room)
        print(f"  {room}: {len(years)} years, {years[0] if years else '?'}"
              f"-{years[-1] if years else '?'}")

    print("\nProbing download availability per room (3 docs from a recent year)...")
    for room, year in [("Civil", "2023"), ("Laboral", "2018"),
                       ("Penal", "2023"), ("Tutelas", "2023")]:
        total, results = search_documents(room, year=year)
        hits = []
        for raw in results:
            if is_junk_file(raw.get("id", "")):
                continue
            for path in download_attempts(raw["id"]):
                blob = download_file(path)
                if blob is None:
                    continue
                kind = "docx" if path.endswith(".docx") else (
                    "pdf-text" if pdf_text_layer(blob) else "pdf-scan")
                hits.append(kind)
                break
            else:
                hits.append("MISS")
            if len(hits) >= 3:
                break
        print(f"  {room} {year}: {total} indexed, probe={hits}")

    print("\nExtracting one full document (Civil 2023)...")
    _, results = search_documents("Civil", year="2023")
    for raw in results:
        if is_junk_file(raw.get("id", "")):
            continue
        try:
            text = fetch_full_text(raw["id"], allow_ocr=False)
        except Deferred:
            continue
        if text:
            print(f"  {raw['title']}: {len(text)} chars")
            print(f"  Preview: {text[:300]}...")
            break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CO/CorteSuprema data fetcher")
    parser.add_argument("command", choices=["test-api", "bootstrap", "bootstrap-fast"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch sample data")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command == "test-api":
        test_api()
    elif args.sample:
        run_sample()
    else:
        run_full()
