#!/usr/bin/env python3
"""
MX/PoderJudicialHidalgo -- Hidalgo State Court Decisions (Observatorio de Sentencias)

Fetches full-text court decisions (sentencias en versión pública) from the
Poder Judicial del Estado de Hidalgo "Observatorio de Sentencias" portal.

Portal:  https://www.pjhidalgo.gob.mx/transparencia/observatorio/sentencias.html
         (a single static HTML page grouping the public sentencias into tabbed
          materias — CIVIL, FAMILIAR, MERCANTIL, PENAL_ACUSATORIO,
          PENAL_TRADICIONAL — each linking directly to a full-text PDF under
          sentencias/<MATERIA>/<file>.pdf).

Strategy:
  - GET sentencias.html once -> collect every href="sentencias/.../*.pdf".
  - GET each PDF -> extract the text layer.

Note on extraction: these court PDFs embed real space glyphs but lay each
character out individually. pdfplumber's gap-based extract_text() therefore
inserts spurious extra spaces ("P a ch u ca"). We instead read the raw char
stream per visual line and concatenate it, which reproduces the document's own
spacing faithfully ("Pachuca de Soto"). If that yields little text (a scanned
PDF), we fall back to the shared extractor (opendataloader/pdfplumber/OCR).

Usage:
  python bootstrap.py test
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap-fast --full
"""

import sys
import re
import html
import logging
import time
from collections import defaultdict
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import quote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import _extract as extract_pdf_text_shared

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MX.PoderJudicialHidalgo")

BASE = "https://www.pjhidalgo.gob.mx/transparencia/observatorio"
LIST_URL = f"{BASE}/sentencias.html"

_HREF_RE = re.compile(r'href="(sentencias/[^"]+\.pdf)"', re.I)

# Friendly labels for the materia (path segment) of each sentencia.
_MATERIA = {
    "CIVIL": "Civil",
    "FAMILIAR": "Familiar",
    "MERCANTIL": "Mercantil",
    "PENAL_ACUSATORIO": "Penal (Sistema Acusatorio)",
    "PENAL_TRADICIONAL": "Penal (Sistema Tradicional)",
}


def clean_pdf_text(pdf_bytes: bytes) -> str:
    """Faithful text by concatenating the raw char stream per visual line.

    Avoids pdfplumber's gap-based space insertion, which double-spaces these
    individually-positioned-glyph court PDFs.
    """
    import io
    import pdfplumber

    out: List[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            lines = defaultdict(list)
            for c in page.chars:
                lines[round(c["top"])].append(c)
            for top in sorted(lines):
                row = sorted(lines[top], key=lambda c: c["x0"])
                out.append("".join(c["text"] for c in row).rstrip())
            # Release per-page cached layout to keep memory flat on big PDFs.
            try:
                page.flush_cache()
                page.get_textmap.cache_clear()
            except Exception:
                pass
    text = "\n".join(out)
    # Collapse runs of blank lines.
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text


class HidalgoCourtScraper(BaseScraper):
    """Scraper for MX/PoderJudicialHidalgo full-text sentencias (Observatorio)."""

    def __init__(self, source_dir=None):
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0 Safari/537.36 Legal-Data-Hunter/1.0",
            "Referer": LIST_URL,
        })

    # ── helpers ───────────────────────────────────────────────────────
    def _list_docs(self) -> List[Dict[str, Any]]:
        for attempt in range(3):
            try:
                time.sleep(0.5)
                resp = self.session.get(LIST_URL, timeout=60)
                resp.raise_for_status()
                resp.encoding = resp.apparent_encoding or "utf-8"
                htmltext = resp.text
                break
            except requests.exceptions.RequestException as e:
                logger.warning(f"GET listing attempt {attempt+1} failed: {e}")
                time.sleep(4)
        else:
            logger.error("Could not fetch listing")
            return []

        docs: List[Dict[str, Any]] = []
        seen = set()
        for m in _HREF_RE.finditer(htmltext):
            relpath = html.unescape(m.group(1))
            if relpath in seen:
                continue
            seen.add(relpath)
            docs.append({"relpath": relpath})
        logger.info(f"Listing: {len(docs)} sentencias")
        return docs

    def _pdf_url(self, relpath: str) -> str:
        # Encode each path segment (filenames carry spaces / accents).
        parts = relpath.split("/")
        return f"{BASE}/" + "/".join(quote(p) for p in parts)

    def _fetch_pdf(self, relpath: str) -> Optional[bytes]:
        url = self._pdf_url(relpath)
        for attempt in range(3):
            try:
                time.sleep(0.5)
                resp = self.session.get(url, timeout=90)
                if 400 <= resp.status_code < 500:
                    return None
                resp.raise_for_status()
                if resp.content[:4] != b"%PDF":
                    return None
                return resp.content
            except requests.exceptions.RequestException as e:
                logger.warning(f"PDF {relpath} attempt {attempt+1} failed: {e}")
                time.sleep(4)
        return None

    def _extract(self, pdf_bytes: bytes) -> str:
        try:
            text = clean_pdf_text(pdf_bytes)
        except Exception as e:
            logger.warning(f"clean_pdf_text failed ({e}); using shared extractor")
            text = ""
        if len(text) < 100:
            # Scanned or unusual PDF — let the shared backend (incl. OCR) try.
            shared = extract_pdf_text_shared(pdf_bytes) or ""
            if len(shared) > len(text):
                text = shared
        return text

    def _iter_docs(self) -> Generator[Dict[str, Any], None, None]:
        for doc in self._list_docs():
            pdf = self._fetch_pdf(doc["relpath"])
            if not pdf:
                continue
            text = self._extract(pdf)
            if not text or len(text) < 100:
                continue
            doc["_text"] = text
            yield doc

    # ── BaseScraper interface ─────────────────────────────────────────
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        yield from self._iter_docs()

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        # No reliable per-record timestamp; re-scan everything.
        yield from self._iter_docs()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        relpath = raw.get("relpath", "")
        parts = relpath.split("/")
        materia_seg = parts[1] if len(parts) >= 3 else ""
        fname = parts[-1].rsplit(".", 1)[0]

        materia = _MATERIA.get(materia_seg.upper(), materia_seg.replace("_", " ").title())

        # Filename pattern: <expediente>-<year>_<asunto>, e.g.
        #   "402-2018_NULIDAD" or "314-17_RECONOCIMIENTO_PATERNIDAD".
        expediente = ""
        anio = ""
        asunto = ""
        m = re.match(r"^\s*([0-9]+)[-/]([0-9]{2,4})[_\s-]+(.*)$", fname)
        if m:
            num, yr, rest = m.group(1), m.group(2), m.group(3)
            if len(yr) == 2:
                yr = ("20" if int(yr) < 50 else "19") + yr
            anio = yr
            expediente = f"{num}/{yr}"
            asunto = re.sub(r"[_\s]+", " ", rest).strip()
        else:
            asunto = re.sub(r"[_\s]+", " ", fname).strip()

        slug = re.sub(r"[^A-Za-z0-9]+", "-", relpath).strip("-")
        doc_id = f"MX-HID-{slug}"

        date_str = f"{anio}-01-01" if re.match(r"^(19|20)\d{2}$", anio or "") else None

        title_parts = [p for p in [materia, expediente, asunto.title()] if p]
        title = " — ".join(title_parts) if title_parts else f"Sentencia {fname}"

        return {
            "_id": doc_id,
            "_source": "MX/PoderJudicialHidalgo",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("_text", ""),
            "date": date_str,
            "url": self._pdf_url(relpath),
            "subject_area": materia,
            "asunto": asunto.title(),
            "case_number": expediente,
            "year": anio,
            "jurisdiction": "MX-HID",
        }

    # ── Connectivity test ─────────────────────────────────────────────
    def test(self) -> bool:
        docs = self._list_docs()
        if not docs:
            logger.error("Listing returned no documents")
            return False
        for doc in docs[:8]:
            pdf = self._fetch_pdf(doc["relpath"])
            if not pdf:
                continue
            text = self._extract(pdf)
            logger.info(f"Sample {doc['relpath']}: {len(text or '')} chars")
            if text and len(text) > 100:
                return True
        logger.error("No sample PDF with extractable text found")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MX/PoderJudicialHidalgo data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Only fetch a small sample")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = HidalgoCourtScraper()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)
    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"Bootstrap-fast complete: {stats}")
        if stats.get("records_new", 0) == 0 and stats.get("records_fetched", 0) == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
