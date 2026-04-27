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

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
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

# Document types
DOC_TYPES = {
    "0": "Todos",
    "1": "Acórdão",
    "32": "Decisão",
    "33": "Decisão de Antecipação de Tutela",
    "136": "Decisão Liminar",
    "128": "Sentença",
}

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

        # Create stable ID from process number
        safe_proc = proc.replace(".", "").replace("-", "")
        doc_id = f"BR-TRF1-{safe_proc}" if safe_proc else f"BR-TRF1-{hash(text) & 0xFFFFFFFF:08x}"

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

    def _fetch_section(self, section_code: str, section_name: str,
                       sample_limit: int, count_so_far: int,
                       global_seen: set) -> Generator[dict, None, None]:
        """Fetch all decisions for a given judicial section."""
        # Re-init session for each section to get fresh ViewState
        if not self._init_session():
            logger.error("Failed to init session for section %s", section_name)
            return

        html_text = self._search(section=section_code, doc_type="0", query="*")
        if not html_text:
            return

        total = self._get_total(html_text)
        if total == 0:
            logger.info("Section %s: no results", section_name)
            return

        logger.info("Section %s: %d documents", section_name, total)

        # Parse first page
        records = self._parse_results(html_text)
        count = 0
        for doc in records:
            if count_so_far + count >= sample_limit:
                return
            proc = doc["process_number"]
            if proc in global_seen:
                continue
            global_seen.add(proc)
            doc["section_name"] = section_name
            yield doc
            count += 1

        # Paginate through remaining
        max_pages = min((total + PAGE_SIZE - 1) // PAGE_SIZE, 2000)
        for page_idx in range(1, max_pages):
            if count_so_far + count >= sample_limit:
                return

            first = page_idx * PAGE_SIZE
            html_text = self._paginate(first)
            if not html_text:
                break

            records = self._parse_results(html_text)
            if not records:
                # Empty page means we've reached the end
                break

            for doc in records:
                if count_so_far + count >= sample_limit:
                    return
                proc = doc["process_number"]
                if proc in global_seen:
                    continue
                global_seen.add(proc)
                doc["section_name"] = section_name
                yield doc
                count += 1

            if page_idx % 50 == 0:
                logger.info("Section %s: page %d/%d, %d records so far",
                            section_name, page_idx, max_pages, count)

        logger.info("Section %s complete: %d records", section_name, count)

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Fetch all TRF1 decisions, iterating by judicial section."""
        count = 0
        sample_limit = 15 if sample else 999999
        global_seen = set()

        for section_code, section_name in SECTIONS:
            if count >= sample_limit:
                return
            for record in self._fetch_section(
                section_code, section_name, sample_limit, count, global_seen
            ):
                yield record
                count += 1

        logger.info("Total records yielded: %d", count)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch recent decisions (no date filter available, re-fetches all)."""
        logger.info("TRF1 portal has no date filter — re-fetching all")
        yield from self.fetch_all()


def main():
    scraper = TRF1Scraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
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

    if command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=sample)
        logger.info("Bootstrap complete: %s", json.dumps(stats, indent=2))

    elif command == "update":
        since = (sys.argv[2] if len(sys.argv) > 2
                 and not sys.argv[2].startswith("-") else "2025-01-01")
        count = sum(1 for _ in scraper.fetch_updates(since))
        logger.info("Update complete: %d records since %s", count, since)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
