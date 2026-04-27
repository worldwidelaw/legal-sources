#!/usr/bin/env python3
"""
INTL/Mercosur-TPR -- MERCOSUR Permanent Review Tribunal

Fetches TPR dispute resolution awards, advisory opinions, resolutions,
and TAL (administrative-labor tribunal) sentences with full text from PDFs.

Strategy:
  - Parse 3 static HTML index pages for PDF links
  - Download each PDF and extract full text via pdfplumber
  - ~45 documents total, all freely available

Categories:
  - Laudos TPR: tribunal awards (6 main + 2 ad hoc)
  - Laudos Brasilia: awards under the Brasilia Protocol (10 + aclaraciones)
  - Opiniones Consultivas: advisory opinions (3)
  - Resoluciones TPR: procedural resolutions (8)
  - Sentencias TAL: administrative-labor tribunal sentences (5)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # No-op (historical corpus)
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Set

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.Mercosur-TPR")

BASE_URL = "https://www.tprmercosur.org/es/"

INDEX_PAGES = [
    BASE_URL + "sol_contr_laudos.htm",
    BASE_URL + "opi_consultivas.htm",
    BASE_URL + "tal.htm",
]

# PDF paths to skip (administrative docs, not case law)
SKIP_PATTERNS = [
    "docum/adm/",        # TAL member designations
    "docum/otros/",      # Supporting materials
    "docum/DEC_",        # CMC decisions (regulatory, not case law)
]


def _classify_pdf(path: str) -> Optional[str]:
    """Classify a PDF path into a document type, or None to skip."""
    if any(skip in path for skip in SKIP_PATTERNS):
        return None
    if "laudos/bras/" in path:
        return "laudo_brasilia"
    if "laudos/" in path:
        return "laudo_tpr"
    if "opin/" in path:
        return "opinion_consultiva"
    if "res/" in path:
        return "resolucion"
    if "tal/Sent" in path:
        return "sentencia_tal"
    if "tal/Reglas" in path or "tal/Acta" in path:
        return "procedural"
    return None


def _extract_date(filename: str) -> Optional[str]:
    """Try to extract a date from the PDF filename."""
    # Pattern: _DD_MMM_YYYY_ (e.g., 23_set_2005)
    month_map = {
        "ene": "01", "feb": "02", "mar": "03", "abr": "04",
        "may": "05", "jun": "06", "jul": "07", "ago": "08",
        "set": "09", "oct": "10", "nov": "11", "dic": "12",
    }
    m = re.search(r'(\d{1,2})_(\w{3})_(\d{4})', filename)
    if m:
        day, mon, year = m.group(1), m.group(2).lower(), m.group(3)
        if mon in month_map:
            return f"{year}-{month_map[mon]}-{day.zfill(2)}"

    # Pattern: _NN_YYYY_ (year from laudo number, e.g., Laudo_01_2005)
    m = re.search(r'_(\d{2})_(\d{4})', filename)
    if m:
        return f"{m.group(2)}-01-01"

    # Pattern: just a year
    m = re.search(r'_(\d{4})_', filename)
    if m:
        return f"{m.group(1)}-01-01"

    return None


def _make_id(path: str) -> str:
    """Create a stable document ID from the PDF path."""
    # Strip docum/ prefix and .pdf suffix
    name = path.replace("docum/", "").replace(".pdf", "")
    name = re.sub(r'[^\w\-]', '_', name)
    return f"TPR-{name}"


class MercosurTPRScraper(BaseScraper):
    """Scraper for INTL/Mercosur-TPR -- MERCOSUR TPR Awards."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
        })

    def _discover_pdfs(self) -> List[Dict[str, str]]:
        """Parse index pages and discover all unique PDF documents."""
        seen: Set[str] = set()
        docs = []

        for page_url in INDEX_PAGES:
            self.rate_limiter.wait()
            try:
                resp = self.session.get(page_url, timeout=30)
                resp.raise_for_status()
            except Exception as e:
                logger.warning("Failed to fetch index %s: %s", page_url, e)
                continue

            pdf_paths = re.findall(r'href="(docum/[^"]+\.pdf)"', resp.text, re.I)

            for path in pdf_paths:
                if path in seen:
                    continue
                seen.add(path)

                doc_type = _classify_pdf(path)
                if doc_type is None:
                    continue

                filename = path.split("/")[-1]
                date = _extract_date(filename)

                # Build a readable title from the filename
                title = filename.replace(".pdf", "").replace("_es", "")
                title = title.replace("_", " ").strip()

                docs.append({
                    "path": path,
                    "url": BASE_URL + path,
                    "title": title,
                    "date": date,
                    "document_type": doc_type,
                    "id": _make_id(path),
                })

        logger.info("Discovered %d substantive PDFs across %d index pages",
                    len(docs), len(INDEX_PAGES))
        return docs

    def _download_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="INTL/Mercosur-TPR",
            source_id="",
            pdf_url=pdf_url,
            table="case_law",
        ) or ""

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw TPR record into standard schema."""
        return {
            "_id": raw["_id"],
            "_source": "INTL/Mercosur-TPR",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "document_type": raw.get("document_type", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all TPR documents."""
        docs = self._discover_pdfs()
        for i, doc_meta in enumerate(docs):
            logger.info("[%d/%d] %s", i + 1, len(docs), doc_meta["title"][:70])
            text = self._download_pdf_text(doc_meta["url"])
            if text:
                yield {
                    "_id": doc_meta["id"],
                    "title": doc_meta["title"],
                    "text": text,
                    "date": doc_meta["date"],
                    "url": doc_meta["url"],
                    "document_type": doc_meta["document_type"],
                }
            else:
                logger.warning("Skipped (no text): %s", doc_meta["title"])

    def fetch_updates(self, since=None) -> Generator[Dict[str, Any], None, None]:
        """Historical corpus — re-fetch all (small corpus)."""
        yield from self.fetch_all()

    def test_connection(self) -> bool:
        """Quick connectivity test."""
        try:
            self.rate_limiter.wait()
            resp = self.session.get(INDEX_PAGES[0], timeout=15)
            ok = resp.status_code == 200
            logger.info("Connection %s (HTTP %d)", "OK" if ok else "FAILED",
                        resp.status_code)
            return ok
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            return False

    def run_bootstrap(self, sample: bool = False):
        """Run the bootstrap process."""
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        docs = self._discover_pdfs()
        if sample:
            docs = docs[:15]

        label = "SAMPLE" if sample else "FULL"
        logger.info("Running %s bootstrap: %d documents", label, len(docs))

        count = 0
        for i, doc_meta in enumerate(docs):
            logger.info("[%d/%d] %s", i + 1, len(docs),
                        doc_meta["title"][:70])
            text = self._download_pdf_text(doc_meta["url"])
            if not text:
                logger.warning("Skipped (no text): %s", doc_meta["title"])
                continue

            raw = {
                "_id": doc_meta["id"],
                "title": doc_meta["title"],
                "text": text,
                "date": doc_meta["date"],
                "url": doc_meta["url"],
                "document_type": doc_meta["document_type"],
            }
            normalized = self.normalize(raw)

            fname = re.sub(r'[^\w\-.]', '_', f"{normalized['_id'][:80]}.json")
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info("  -> %d chars of text", len(normalized["text"]))

        logger.info("%s bootstrap complete: %d/%d records saved",
                    label, count, len(docs))
        return count


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="INTL/Mercosur-TPR Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample only (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MercosurTPRScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        scraper.run_bootstrap(sample=args.sample)
    elif args.command == "update":
        count = 0
        for doc in scraper.fetch_updates():
            normalized = scraper.normalize(doc)
            count += 1
        logger.info("Update complete: %d records", count)


if __name__ == "__main__":
    main()
