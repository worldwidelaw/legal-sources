#!/usr/bin/env python3
"""
INTL/WMO-BasicDocs -- World Meteorological Organization Basic Documents

Fetches WMO governance instruments with full text extracted from PDFs:
  - WMO-No. 15 (Basic Documents No. 1): Convention, General/Staff/Financial
    Regulations, UN Agreement, Privileges Convention, Swiss HQ Agreement
  - WMO-No. 49 (Technical Regulations): Volumes I, II, III

Strategy:
  - All documents freely available as PDFs from library.wmo.int
  - Uses curl for downloads (Python requests has SSL issues with this host)
  - pdfplumber for text extraction
  - ~10 documents total

Usage:
  python bootstrap.py bootstrap          # Full corpus -> data/records.jsonl
  python bootstrap.py bootstrap --full   # Same
  python bootstrap.py bootstrap-fast     # Same, fleet entry point
  python bootstrap.py bootstrap --sample # 6 sample records -> sample/
  python bootstrap.py update             # No-op (treaty texts rarely change)
  python bootstrap.py test               # Quick connectivity test

Downloads fail loud (WMODownloadError) on an undersized or non-PDF body so a
block/placeholder page can never be mistaken for a completed run (GH-1239).
"""

import re
import sys
import json
import logging
import subprocess
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.WMO-BasicDocs")

LIBRARY_BASE = "https://library.wmo.int"

# WMO-No. 15 section boundaries (0-indexed physical pages)
WMO15_SECTIONS = [
    {
        "id": "WMO-Convention",
        "title": "Convention of the World Meteorological Organization (as amended, 2025 edition)",
        "document_type": "convention",
        "date": "1947-10-11",
        "start_page": 6,
        "end_page": 30,
    },
    {
        "id": "WMO-GeneralRegulations",
        "title": "General Regulations of the World Meteorological Organization (2025 edition)",
        "document_type": "regulation",
        "date": "2025-01-01",
        "start_page": 30,
        "end_page": 102,
    },
    {
        "id": "WMO-StaffRegulations",
        "title": "Staff Regulations of the World Meteorological Organization (2025 edition)",
        "document_type": "regulation",
        "date": "2025-01-01",
        "start_page": 102,
        "end_page": 136,
    },
    {
        "id": "WMO-FinancialRegulations",
        "title": "Financial Regulations of the World Meteorological Organization (2025 edition)",
        "document_type": "regulation",
        "date": "2025-01-01",
        "start_page": 136,
        "end_page": 162,
    },
    {
        "id": "WMO-UNAgreement",
        "title": "Agreement between the United Nations and the World Meteorological Organization",
        "document_type": "agreement",
        "date": "1951-12-20",
        "start_page": 162,
        "end_page": 178,
    },
    {
        "id": "WMO-PrivilegesConvention",
        "title": "Convention on the Privileges and Immunities of the Specialized Agencies (Annex XVII — WMO)",
        "document_type": "convention",
        "date": "1947-11-21",
        "start_page": 178,
        "end_page": 198,
    },
    {
        "id": "WMO-SwissAgreement",
        "title": "Agreement between the Swiss Federal Council and the World Meteorological Organization (Headquarters Agreement)",
        "document_type": "agreement",
        "date": "1955-03-10",
        "start_page": 198,
        "end_page": 223,
    },
]

# WMO-No. 49 Technical Regulations (separate PDFs)
TECH_REGS = [
    {
        "id": "WMO-TechRegs-VolI",
        "title": "Technical Regulations, Volume I — General Meteorological Standards and Recommended Practices (WMO-No. 49, 2025 edition)",
        "document_type": "regulation",
        "date": "2025-01-01",
        "item_id": 35722,
        "filename": "WMO-49-vI-2025_en.pdf",
    },
    {
        "id": "WMO-TechRegs-VolII",
        "title": "Technical Regulations, Volume II — Meteorological Service for International Air Navigation (WMO-No. 49, 2018 edition, updated 2021)",
        "document_type": "regulation",
        "date": "2021-01-01",
        "item_id": 35795,
        "filename": "WMO-No49_Vol-II_2018-upd-2021_Met-Service_en.pdf",
    },
    {
        "id": "WMO-TechRegs-VolIII",
        "title": "Technical Regulations, Volume III — Hydrology (WMO-No. 49, 2021 edition)",
        "document_type": "regulation",
        "date": "2022-01-01",
        "item_id": 35631,
        "filename": "49_III_en.pdf",
    },
]

# WMO-No. 15 download
WMO15_ITEM_ID = 48992
WMO15_FILENAME = "WMO-15-2025_en.pdf"
WMO15_URL = f"{LIBRARY_BASE}/viewer/{WMO15_ITEM_ID}/download?file={WMO15_FILENAME}&type=pdf"
WMO15_RECORD_URL = f"{LIBRARY_BASE}/records/item/{WMO15_ITEM_ID}"


class WMODownloadError(RuntimeError):
    """A WMO PDF could not be retrieved as a real PDF."""


# A genuine WMO-No. 15 is ~1.5 MB; the Technical Regulations run ~1-5 MB. Any
# body far below this is a block/error page, not a truncated document.
MIN_PDF_BYTES = 50_000


def _curl_download(url: str, dest: str, timeout: int = 120,
                   min_bytes: int = MIN_PDF_BYTES) -> None:
    """Download a PDF with curl (Python requests has SSL issues with this host).

    Raises WMODownloadError instead of returning False so a blocked or
    placeholder response fails loud. The fleet previously received a
    10,535-byte body with HTTP 200 (GH-1239); `curl -s -L` without --fail
    happily saved it, and the caller only logged a warning, so the run
    exited "successfully" having fetched nothing.
    """
    try:
        result = subprocess.run(
            [
                "curl", "-s", "-L", "--http1.1", "--fail",
                "-H", "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                "--max-time", str(timeout),
                "-w", "%{http_code}",
                "-o", dest,
                url,
            ],
            capture_output=True,
            text=True,
            timeout=timeout + 30,
        )
    except Exception as e:
        raise WMODownloadError(f"curl failed for {url}: {e}") from e

    http_code = (result.stdout or "").strip() or "?"
    if result.returncode != 0:
        raise WMODownloadError(
            f"curl exit {result.returncode} (HTTP {http_code}) for {url}"
        )

    path = Path(dest)
    size = path.stat().st_size if path.exists() else 0
    if size < min_bytes:
        head = path.read_bytes()[:200] if size else b""
        raise WMODownloadError(
            f"{url} returned only {size} bytes (HTTP {http_code}) — expected "
            f">={min_bytes}. This is a block/placeholder page, not the PDF. "
            f"First bytes: {head!r}"
        )
    with open(dest, "rb") as fh:
        magic = fh.read(5)
    if magic != b"%PDF-":
        raise WMODownloadError(
            f"{url} returned {size} bytes that are not a PDF (magic {magic!r}, "
            f"HTTP {http_code}) — likely an HTML block page."
        )


def _extract_pdf_text(pdf_path: str, start_page: int = 0, end_page: int = -1) -> str:
    """Extract text from a PDF file using pdfplumber."""
    try:
        import pdfplumber
    except ImportError:
        logger.warning("pdfplumber not available, trying PyPDF2")
        try:
            from PyPDF2 import PdfReader
            reader = PdfReader(pdf_path)
            pages = reader.pages[start_page:end_page if end_page > 0 else None]
            parts = []
            for page in pages:
                text = page.extract_text() or ""
                if text.strip():
                    parts.append(text)
            return "\n\n".join(parts)
        except ImportError:
            logger.error("No PDF library available (pdfplumber or PyPDF2)")
            return ""

    with pdfplumber.open(pdf_path) as pdf:
        if end_page <= 0:
            end_page = len(pdf.pages)
        parts = []
        for i in range(start_page, min(end_page, len(pdf.pages))):
            text = pdf.pages[i].extract_text() or ""
            if text.strip():
                parts.append(text)
        return "\n\n".join(parts)


def _clean_text(text: str) -> str:
    """Clean extracted PDF text."""
    # Remove form feed characters
    text = text.replace("\f", "\n\n")
    # Collapse excessive whitespace
    text = re.sub(r"\n{4,}", "\n\n\n", text)
    # Remove page headers like "4 CONVENTION" or "GENERAL REGULATIONS 95"
    text = re.sub(r"^\d+\s+(CONVENTION|GENERAL REGULATIONS|STAFF REGULATIONS|FINANCIAL REGULATIONS)\s*$",
                  "", text, flags=re.MULTILINE)
    text = re.sub(r"^(CONVENTION|GENERAL REGULATIONS|STAFF REGULATIONS|FINANCIAL REGULATIONS)\s+\d+\s*$",
                  "", text, flags=re.MULTILINE)
    return text.strip()


class WMOBasicDocsScraper(BaseScraper):
    """Scraper for INTL/WMO-BasicDocs."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self._wmo15_path = None

    def _ensure_wmo15(self) -> Optional[str]:
        """Download WMO-No. 15 PDF if not already cached."""
        if self._wmo15_path and Path(self._wmo15_path).exists():
            return self._wmo15_path

        tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
        tmp.close()
        logger.info("Downloading WMO-No. 15 (Basic Documents No. 1)...")
        try:
            # WMO-No. 15 is ~1.5 MB; hold it to a higher bar than the generic guard.
            _curl_download(WMO15_URL, tmp.name, min_bytes=100_000)
        except WMODownloadError:
            Path(tmp.name).unlink(missing_ok=True)
            raise
        logger.info("Downloaded WMO-No. 15: %d bytes", Path(tmp.name).stat().st_size)
        self._wmo15_path = tmp.name
        return tmp.name

    def _fetch_wmo15_sections(self, sample: bool = False):
        """Yield records for each section of WMO-No. 15."""
        pdf_path = self._ensure_wmo15()
        if not pdf_path:
            return

        sections = WMO15_SECTIONS
        if sample:
            sections = sections[:4]  # Convention + 3 regulation sets

        for section in sections:
            self.rate_limiter.wait()
            logger.info("Extracting: %s", section["title"][:70])
            text = _extract_pdf_text(pdf_path, section["start_page"], section["end_page"])
            text = _clean_text(text)
            if text and len(text) > 500:
                yield {
                    "_id": section["id"],
                    "title": section["title"],
                    "text": text,
                    "date": section["date"],
                    "url": WMO15_RECORD_URL,
                    "document_type": section["document_type"],
                }
                logger.info("  -> %d chars", len(text))
            else:
                logger.warning("Skipped (insufficient text): %s", section["title"])

    def _fetch_tech_regs(self, sample: bool = False):
        """Yield records for Technical Regulations volumes."""
        regs = TECH_REGS
        if sample:
            regs = regs[:2]  # Vol I + Vol II

        for reg in regs:
            self.rate_limiter.wait()
            url = f"{LIBRARY_BASE}/viewer/{reg['item_id']}/download?file={reg['filename']}&type=pdf"
            record_url = f"{LIBRARY_BASE}/records/item/{reg['item_id']}"

            tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
            tmp.close()

            logger.info("Downloading: %s", reg["title"][:70])
            try:
                _curl_download(url, tmp.name)
                text = _clean_text(_extract_pdf_text(tmp.name))
            finally:
                Path(tmp.name).unlink(missing_ok=True)

            if text and len(text) > 500:
                yield {
                    "_id": reg["id"],
                    "title": reg["title"],
                    "text": text,
                    "date": reg["date"],
                    "url": record_url,
                    "document_type": reg["document_type"],
                }
                logger.info("  -> %d chars", len(text))
            else:
                logger.warning("Skipped (insufficient text): %s", reg["title"])

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw WMO record into standard schema."""
        return {
            "_id": raw["_id"],
            "_source": "INTL/WMO-BasicDocs",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "document_type": raw.get("document_type", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all WMO basic documents (yields RAW, per the BaseScraper contract)."""
        try:
            yield from self._fetch_wmo15_sections(sample=False)
            yield from self._fetch_tech_regs(sample=False)
        finally:
            self._cleanup()

    def _cleanup(self) -> None:
        """Drop the cached WMO-No. 15 download."""
        if self._wmo15_path:
            Path(self._wmo15_path).unlink(missing_ok=True)
            self._wmo15_path = None

    def fetch_updates(self, since=None) -> Generator[Dict[str, Any], None, None]:
        """WMO treaty texts rarely change; re-fetch all."""
        yield from self.fetch_all()

    def test_connection(self) -> bool:
        """Quick connectivity test."""
        try:
            result = subprocess.run(
                ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
                 "--http1.1", "--max-time", "15",
                 "-H", "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                 WMO15_URL],
                capture_output=True, text=True, timeout=20,
            )
            code = result.stdout.strip()
            ok = code == "200"
            logger.info("Connection %s (HTTP %s)", "OK" if ok else "FAILED", code)
            return ok
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            return False

    def run_bootstrap(self, sample: bool = False):
        """Run the bootstrap process."""
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        label = "SAMPLE" if sample else "FULL"
        logger.info("Running %s bootstrap", label)

        count = 0
        for raw in self._fetch_wmo15_sections(sample=sample):
            normalized = self.normalize(raw)
            fname = re.sub(r"[^\w\-.]", "_", f"{normalized['_id'][:80]}.json")
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            count += 1

        for raw in self._fetch_tech_regs(sample=sample):
            normalized = self.normalize(raw)
            fname = re.sub(r"[^\w\-.]", "_", f"{normalized['_id'][:80]}.json")
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            count += 1

        self._cleanup()

        logger.info("%s bootstrap complete: %d records saved", label, count)
        return count


def _run_full(scraper: "WMOBasicDocsScraper") -> None:
    """Full corpus through BaseScraper storage -> data/records.jsonl.

    The old `bootstrap --full` called run_bootstrap(), which only ever wrote
    into sample/ — so even a completely successful run left the fleet with
    nothing to ingest beyond the committed samples (GH-1239).
    """
    stats = scraper.bootstrap(sample_mode=False)
    fetched = stats.get("records_fetched", 0)
    logger.info(
        "bootstrap_fast complete: %d fetched, %d new, %d errors",
        fetched, stats.get("records_new", 0), stats.get("errors", 0),
    )
    if fetched == 0:
        raise RuntimeError(
            "0 records fetched — every library.wmo.int download failed; "
            "do not treat this as a completed run"
        )


def main():
    import argparse
    parser = argparse.ArgumentParser(description="INTL/WMO-BasicDocs Bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "bootstrap_fast", "update", "test"],
    )
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample only (~6 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WMOBasicDocsScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif args.command in ("bootstrap-fast", "bootstrap_fast"):
        _run_full(scraper)
    elif args.command == "update":
        logger.info("WMO treaty texts rarely change; use bootstrap for full re-fetch")
    elif args.command == "bootstrap":
        if args.full or not args.sample:
            _run_full(scraper)
        else:
            count = scraper.run_bootstrap(sample=True)
            if count == 0:
                logger.error("No records fetched!")
                sys.exit(1)


if __name__ == "__main__":
    main()
