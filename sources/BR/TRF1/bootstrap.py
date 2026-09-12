#!/usr/bin/env python3
"""
BR/TRF1 -- Federal Regional Court 1st Region (Tribunal Regional Federal da 1ª Região)

Fetches court decisions from TRF1's Pesquisa de Documentos portal.
TRF1 covers 14 judicial sections: AC, AP, AM, BA, DF, GO, MT, MA, MG, PA, PI, RO, RR, TO.

Endpoint: POST https://portal.trf1.jus.br/pesquisadocumentos/index.jsf
  - PrimeFaces/JSF form with DataTable results
  - Full text embedded in ExtClipboard widget's text parameter
  - No authentication required
  - 5 results per page, AJAX pagination

The portal caps *every* query's result set at 10,000 rows (verified: even Roraima,
the smallest section, reports rowCount:10000 for a "Todos" search). The cap is per
query, not per corpus, so the crawl slices section x document type — each slice
gets its own 10,000-row window. Searching tipoDocumento="Todos" instead returns
one undifferentiated 10,000-row window that in practice is all Sentenças, which is
why Acórdãos and Decisões were almost entirely missing from earlier crawls.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap-fast     # VPS fleet entrypoint (alias of full)
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Incremental refresh (seen-ID checkpoint)
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import html
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BR.TRF1")

SOURCE_ID = "BR/TRF1"
SAMPLE_DIR = Path(__file__).parent / "sample"
BASE_URL = "https://portal.trf1.jus.br/pesquisadocumentos"
FORM_URL = f"{BASE_URL}/"
POST_URL = f"{BASE_URL}/index.jsf"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

DELAY = 2.0
PAGE_SIZE = 5  # PrimeFaces DataTable returns 5 per page

# Judicial sections (seções judiciárias) covered by TRF1
SECTIONS = [
    ("3000", "Acre"),
    ("3100", "Amapá"),
    ("3200", "Amazonas"),
    ("3300", "Bahia"),
    ("3400", "Distrito Federal"),
    ("3500", "Goiás"),
    ("3600", "Mato Grosso"),
    ("3700", "Maranhão"),
    ("3800", "Minas Gerais"),
    ("3900", "Pará"),
    ("4000", "Piauí"),
    ("4100", "Rondônia"),
    ("4200", "Roraima"),
    ("4300", "Tocantins"),
]

# Document types. "0" (Todos) is deliberately excluded from the crawl: it shares
# the same 10,000-row cap as any other query, so it returns a single truncated
# window rather than the union of the specific types. Crawling the five concrete
# types instead multiplies the reachable ceiling by five per section.
DOC_TYPES = [
    ("1", "Acórdão"),
    ("32", "Decisão"),
    ("33", "Decisão de Antecipação de Tutela"),
    ("136", "Decisão Liminar"),
    ("128", "Sentença"),
]

# Server-side cap on any single result set. Used only for logging that a slice
# was truncated, so a coverage hole is visible in the crawl log.
RESULT_CAP = 10000

# Short, stable suffixes used in _id. Keyed on the label the portal renders in the
# results table, which is what normalize() receives.
DOC_TYPE_CODES = {
    "Acórdão": "ac",
    "Decisão": "dec",
    "Decisão de Antecipação de Tutela": "dat",
    "Decisão Liminar": "lim",
    "Sentença": "sen",
}

# How many consecutive already-seen documents an incremental slice tolerates
# before giving up on it. Generous because the listing is not date-ordered.
STOP_AFTER_SEEN = 400

# Regex to extract clipboard text from PrimeFaces ExtClipboard widget
RE_CLIP_TEXT = re.compile(r',text:"(.*?)"(?:,onSuccess)', re.DOTALL)
# Regex to extract process numbers from table cells
RE_PROCESS = re.compile(
    r'<td role="gridcell" style="width: 18%">(\d+)</td>'
)
# Regex to extract document type from download link text
RE_DOC_TYPE = re.compile(
    r'downloadLink"[^>]*>([^<]+)<br'
)
# Regex to extract row count from DataTable config
RE_ROW_COUNT = re.compile(r'rowCount:(\d+)')
# Regex to extract ViewState
RE_VIEWSTATE = re.compile(r'ViewState:[0-9]+" value="([^"]*)"')


def clean_ocr_text(text: str) -> str:
    """
    Clean text extracted from TRF1's document portal.
    Handles OCR doubled characters, control chars, and HTML artifacts.
    """
    if not text:
        return ""

    # Unescape JS string escapes
    text = text.replace("\\n", "\n").replace("\\t", "\t")
    text = text.replace("\\/", "/").replace("\\r", "\r")
    text = text.replace("\\\\", "\\")

    # Remove non-printable control characters (keep newlines, tabs, spaces)
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', text)

    # Detect and fix OCR doubled characters:
    # If we see patterns like "RREECCUURRSSOO" (every char doubled), deduplicate
    # Heuristic: if >40% of adjacent char pairs are identical, it's doubled
    lines = text.split('\n')
    cleaned_lines = []
    for line in lines:
        stripped = line.strip()
        if len(stripped) < 4:
            cleaned_lines.append(line)
            continue

        # Count adjacent identical character pairs
        pairs = sum(1 for i in range(len(stripped) - 1) if stripped[i] == stripped[i + 1])
        ratio = pairs / max(len(stripped) - 1, 1)

        if ratio > 0.35 and len(stripped) > 10:
            # Deduplicate: take every other character
            deduped = ""
            i = 0
            while i < len(stripped):
                deduped += stripped[i]
                if i + 1 < len(stripped) and stripped[i] == stripped[i + 1]:
                    i += 2
                else:
                    i += 1
            cleaned_lines.append(deduped)
        else:
            cleaned_lines.append(line)

    text = "\n".join(cleaned_lines)

    # Replace Unicode replacement chars and garbled sequences
    text = re.sub(r'[�\ufffd]+', '', text)

    # Normalize whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{4,}', '\n\n\n', text)

    # Strip leading/trailing whitespace from each line
    text = "\n".join(line.strip() for line in text.split('\n'))

    # Remove leading blank lines
    text = text.lstrip('\n')

    return text.strip()


class TRF1Scraper(BaseScraper):
    """Scraper for BR/TRF1 -- Federal Regional Court 1st Region decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.viewstate = None
        self._last_search_params = {}  # Preserve search context for pagination
        self.sample_mode = False       # Set by main(); see fetch_all()

    def _init_session(self) -> bool:
        """Get a session cookie and ViewState by visiting the search form."""
        try:
            resp = self.session.get(FORM_URL, timeout=30)
            resp.raise_for_status()
            m = RE_VIEWSTATE.search(resp.text)
            if m:
                self.viewstate = m.group(1)
                logger.info("Session initialized (ViewState acquired)")
                return True
            logger.error("No ViewState found in form page")
            return False
        except Exception as e:
            logger.error("Failed to init session: %s", e)
            return False

    def _search(self, section: str = "0", doc_type: str = "0",
                query: str = "*") -> Optional[str]:
        """
        Submit a search and return the results HTML.
        Returns None on failure.
        """
        if not self.viewstate:
            if not self._init_session():
                return None

        data = {
            "formulario": "formulario",
            "formulario:j_idt8:texto": query,
            "formulario:j_idt8:tipoDocumento_input": doc_type,
            "formulario:j_idt8:secao_input": section,
            "formulario:j_idt8:assunto": "",
            "formulario:j_idt8:tipoParte_input": "",
            "formulario:j_idt8:nomeParte": "",
            "formulario:j_idt8:ajustadorLayoutMenu1": "",
            "formulario:j_idt8:ajustadorLayoutMenu2": "",
            "formulario:j_idt8:ajustadorLayoutMenu3": "",
            "formulario:j_idt60.x": "50",
            "formulario:j_idt60.y": "15",
            "formulario:j_idt8_active": "0",
            "javax.faces.ViewState": self.viewstate,
        }

        for attempt in range(3):
            try:
                time.sleep(DELAY)
                resp = self.session.post(
                    POST_URL, data=data, timeout=60,
                    headers={"Referer": FORM_URL}
                )
                resp.raise_for_status()
                # Update ViewState for subsequent requests
                m = RE_VIEWSTATE.search(resp.text)
                if m:
                    self.viewstate = m.group(1)
                # Store search params so pagination can reuse them
                self._last_search_params = {
                    "formulario:j_idt8:texto": query,
                    "formulario:j_idt8:tipoDocumento_input": doc_type,
                    "formulario:j_idt8:secao_input": section,
                }
                return resp.text
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                wait = 5 * (attempt + 1)
                logger.warning("Search attempt %d failed: %s. Retry in %ds",
                               attempt + 1, e, wait)
                time.sleep(wait)
                self._init_session()
            except Exception as e:
                logger.error("Search request failed: %s", e)
                return None
        return None

    def _paginate(self, first: int) -> Optional[str]:
        """
        Fetch a specific page of results via AJAX pagination.
        """
        if not self.viewstate:
            return None

        data = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": "formulario:tabela",
            "javax.faces.partial.execute": "formulario:tabela",
            "javax.faces.partial.render": "formulario:tabela",
            "javax.faces.behavior.event": "page",
            "javax.faces.partial.event": "page",
            "formulario:tabela_pagination": "true",
            "formulario:tabela_first": str(first),
            "formulario:tabela_rows": str(PAGE_SIZE),
            "formulario:tabela_encodeFeature": "true",
            "formulario": "formulario",
            "formulario:j_idt8_active": "0",
            "javax.faces.ViewState": self.viewstate,
        }
        # Include search form fields to maintain server-side search context
        data.update(self._last_search_params)

        for attempt in range(3):
            try:
                time.sleep(DELAY)
                resp = self.session.post(
                    POST_URL, data=data, timeout=60,
                    headers={
                        "Referer": POST_URL,
                        "Faces-Request": "partial/ajax",
                        "X-Requested-With": "XMLHttpRequest",
                    }
                )
                resp.raise_for_status()
                m = RE_VIEWSTATE.search(resp.text)
                if m:
                    self.viewstate = m.group(1)
                return resp.text
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                wait = 5 * (attempt + 1)
                logger.warning("Pagination attempt %d failed: %s. Retry in %ds",
                               attempt + 1, e, wait)
                time.sleep(wait)
                self._init_session()
            except Exception as e:
                logger.error("Pagination failed: %s", e)
                return None
        return None

    def _parse_results(self, html_text: str) -> list:
        """
        Parse search results from HTML/AJAX response.
        Returns list of raw record dicts with process_number, doc_type, text.
        """
        records = []

        # Extract clipboard texts (one per result row)
        texts = RE_CLIP_TEXT.findall(html_text)
        # Extract process numbers
        processes = RE_PROCESS.findall(html_text)
        # Extract document types
        doc_types = RE_DOC_TYPE.findall(html_text)

        for i, text in enumerate(texts):
            proc = processes[i] if i < len(processes) else ""
            dtype = doc_types[i].strip() if i < len(doc_types) else "Desconhecido"

            cleaned = clean_ocr_text(text)
            if len(cleaned) < 30:
                continue

            records.append({
                "process_number": proc,
                "document_type": dtype,
                "text": cleaned,
            })

        return records

    def _get_total(self, html_text: str) -> int:
        """Extract total result count from HTML."""
        m = RE_ROW_COUNT.search(html_text)
        return int(m.group(1)) if m else 0

    def normalize(self, doc: dict) -> dict:
        """Transform a parsed record into the standard schema."""
        proc = doc.get("process_number", "")
        dtype = doc.get("document_type", "")
        text = doc.get("text", "")
        section = doc.get("section_name", "")

        # Build title
        title = f"{dtype} - {proc}" if dtype and proc else f"TRF1 {proc}"
        if section:
            title += f" ({section})"

        # Try to extract a date from the text (common patterns: DD/MM/YYYY)
        date = None
        date_match = re.search(r'(\d{2})/(\d{2})/(\d{4})', text)
        if date_match:
            d, m, y = date_match.groups()
            try:
                date = f"{y}-{m}-{d}"
                # Validate
                datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                date = None

        # Stable ID from process number + document type. The type is part of the
        # key because one process number carries several documents (a Sentença at
        # first instance, an Acórdão on appeal, interlocutory Decisões along the
        # way). Keying on the process alone collapsed them onto one row and kept
        # whichever the crawl happened to reach first.
        safe_proc = proc.replace(".", "").replace("-", "")
        type_code = DOC_TYPE_CODES.get(dtype, "x")
        if safe_proc:
            doc_id = f"BR-TRF1-{safe_proc}-{type_code}"
        else:
            doc_id = f"BR-TRF1-{hash(text) & 0xFFFFFFFF:08x}-{type_code}"

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": f"{BASE_URL}/",
            "language": "pt",
            "process_number": proc,
            "document_type": dtype,
            "court": "TRF1",
            "section": section,
        }

    @staticmethod
    def _key(doc: dict) -> tuple:
        """Dedup key: a process number is only unique within a document type."""
        return (doc.get("process_number", ""), doc.get("document_type", ""))

    def _fetch_slice(self, section_code: str, section_name: str,
                     doc_type: str, doc_type_name: str,
                     global_seen: set) -> Generator[dict, None, None]:
        """Walk one section x document-type slice of the portal.

        Each slice is its own 10,000-row window server-side, which is the whole
        reason the crawl is sliced rather than issuing one "Todos" query.
        """
        # Re-init session per slice to get a fresh ViewState
        if not self._init_session():
            logger.error("Failed to init session for %s / %s",
                         section_name, doc_type_name)
            return

        html_text = self._search(section=section_code, doc_type=doc_type, query="*")
        if not html_text:
            return

        total = self._get_total(html_text)
        if total == 0:
            logger.info("%s / %s: no results", section_name, doc_type_name)
            return

        if total >= RESULT_CAP:
            logger.warning(
                "%s / %s: %d documents — at the portal's %d-row cap, so this "
                "slice is truncated upstream and some documents are unreachable",
                section_name, doc_type_name, total, RESULT_CAP,
            )
        else:
            logger.info("%s / %s: %d documents", section_name, doc_type_name, total)

        count = 0
        max_pages = min((total + PAGE_SIZE - 1) // PAGE_SIZE, RESULT_CAP // PAGE_SIZE)

        for page_idx in range(max_pages):
            if page_idx == 0:
                page_html = html_text
            else:
                page_html = self._paginate(page_idx * PAGE_SIZE)
                if not page_html:
                    break

            records = self._parse_results(page_html)
            if page_idx > 0 and not records:
                # Empty page means we've reached the end
                break

            for doc in records:
                doc["section_name"] = section_name
                key = self._key(doc)
                if key in global_seen:
                    continue
                global_seen.add(key)
                yield doc
                count += 1

            if page_idx and page_idx % 50 == 0:
                logger.info("%s / %s: page %d/%d, %d records so far",
                            section_name, doc_type_name, page_idx, max_pages, count)

        logger.info("%s / %s complete: %d records", section_name, doc_type_name, count)

    def fetch_all(self, sample: bool = None) -> Generator[dict, None, None]:
        """Fetch all TRF1 decisions, iterating section x document type.

        `BaseScraper.bootstrap()` calls this with no arguments and enforces the
        sample cap itself by counting records, so the `sample` flag has to reach
        us out of band via `self.sample_mode` — passing it as an argument looked
        like it worked but never fired.
        """
        count = 0
        if sample is None:
            sample = self.sample_mode
        sample_limit = 15 if sample else None
        # Spread a sample across the document types instead of filling it from
        # whichever slice comes first. Taking 15 straight off Acre/Acórdão would
        # validate one slice and leave the other four — the ones this crawl was
        # changed to reach — untested.
        per_slice_cap = 3 if sample else None
        global_seen = set()

        for section_code, section_name in SECTIONS:
            for doc_type, doc_type_name in DOC_TYPES:
                if sample_limit is not None and count >= sample_limit:
                    logger.info("Sample limit reached: %d records", count)
                    return
                slice_count = 0
                for record in self._fetch_slice(
                    section_code, section_name, doc_type, doc_type_name, global_seen
                ):
                    yield record
                    count += 1
                    slice_count += 1
                    if sample_limit is not None and count >= sample_limit:
                        logger.info("Sample limit reached: %d records", count)
                        return
                    if per_slice_cap is not None and slice_count >= per_slice_cap:
                        break

        logger.info("Total records yielded: %d", count)

    # ── Incremental refresh (#1502) ───────────────────────────────────

    def _checkpoint_path(self) -> Path:
        return Path(__file__).parent / "data" / "trf1_checkpoint.json"

    def _load_seen(self) -> set:
        try:
            with open(self._checkpoint_path(), encoding="utf-8") as f:
                return {tuple(k) for k in json.load(f).get("seen_keys") or []}
        except (OSError, ValueError, TypeError):
            return set()

    def _save_seen(self, seen: set) -> None:
        path = self._checkpoint_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"seen_keys": sorted(list(k) for k in seen),
                       "count": len(seen),
                       "updated_at": datetime.now(timezone.utc).isoformat()}, f)
        tmp.replace(path)  # atomic: a truncated checkpoint would re-yield the corpus

    def fetch_updates(self, since: str = None) -> Generator[dict, None, None]:
        """Yield only documents no previous run has emitted.

        The comparator is a seen-key checkpoint rather than a date, and that is
        forced by the portal rather than chosen. The search form exposes no date
        field, the results table has no date column, and its three columns are
        not sortable — so there is no way to ask for "documents since X" and no
        ordering that would let a walk stop once it is past the cutoff. The dates
        in normalize() are scraped out of the decision text itself, which makes
        them a property of the document, not of when it became available to us;
        filtering on them would drop anything the portal indexed late.

        So this does not shorten the walk, and it is not pretending to: the
        listing still has to be paged through, because that listing is also where
        the full text lives (there is no per-document fetch to skip). What it
        does fix is the re-emission — the previous implementation handed the
        entire ~126K corpus back to the loader on every refresh, so a refresh run
        could not be distinguished from a first crawl, and #1502's "no incremental
        path" degradation looked from the fleet's side like a slow host. Now a
        refresh emits new documents only, and a zero-record refresh is a real
        signal that nothing new appeared.

        STOP_AFTER_SEEN bounds a slice that is entirely known territory so a
        refresh does not always cost a full crawl, but it is set high because
        the ordering is not recency-based and new documents can appear anywhere
        in a slice.
        """
        seen = self._load_seen()
        if not seen:
            logger.info(
                "No checkpoint — this first incremental run walks the whole "
                "corpus so nothing is missed; later runs emit only new documents."
            )

        crawl_seen = set()
        emitted = 0

        for section_code, section_name in SECTIONS:
            for doc_type, doc_type_name in DOC_TYPES:
                consecutive_seen = 0
                for doc in self._fetch_slice(
                    section_code, section_name, doc_type, doc_type_name, crawl_seen
                ):
                    key = self._key(doc)
                    if key in seen:
                        consecutive_seen += 1
                        if consecutive_seen >= STOP_AFTER_SEEN:
                            logger.info(
                                "%s / %s: %d consecutive known documents — "
                                "abandoning this slice",
                                section_name, doc_type_name, consecutive_seen,
                            )
                            break
                        continue
                    consecutive_seen = 0
                    seen.add(key)
                    emitted += 1
                    yield doc

        logger.info("Update complete: %d new documents (checkpoint holds %d)",
                    emitted, len(seen))
        self._save_seen(seen)


def main():
    scraper = TRF1Scraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py "
              "[bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing connectivity to TRF1 Pesquisa de Documentos...")
        if not scraper._init_session():
            logger.error("Connection test FAILED")
            sys.exit(1)

        html_text = scraper._search(section="3400", doc_type="1", query="*")
        if html_text:
            total = scraper._get_total(html_text)
            records = scraper._parse_results(html_text)
            logger.info("TRF1 OK — %d total acórdãos (DF section), %d on first page",
                        total, len(records))
            if records:
                sample_rec = scraper.normalize(records[0])
                logger.info("Sample: %s | text length: %d",
                            sample_rec["title"], len(sample_rec["text"]))
                logger.info("Text preview: %.200s", sample_rec["text"][:200])
        else:
            logger.error("Search test FAILED")
            sys.exit(1)
        return

    # bootstrap-fast is the VPS fleet entrypoint. Without this alias the wrapper's
    # invocation exited 1 on "Unknown command" and fell back to re-ingesting
    # sample/, so the fleet never ran a real crawl of this source (#1113/#1363).
    if command in ("bootstrap", "bootstrap-fast"):
        scraper.sample_mode = sample
        stats = scraper.bootstrap(sample_mode=sample, sample_size=15)
        logger.info("Bootstrap complete: %s", json.dumps(stats, indent=2))

    elif command == "update":
        # Route through BaseScraper.update() so new records are written to
        # data/records.jsonl. Counting the generator, as this used to, ran the
        # whole refresh and then threw the results away.
        stats = scraper.update()
        logger.info("Update complete: %s", json.dumps(stats, indent=2, default=str))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
