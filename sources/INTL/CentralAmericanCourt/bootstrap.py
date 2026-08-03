#!/usr/bin/env python3
"""
INTL/CentralAmericanCourt -- Corte Centroamericana de Justicia (CCJ / SICA)

Fetches the full text of the official jurisprudence gazette (Gaceta Oficial)
of the Central American Court of Justice, the judicial organ of the Central
American Integration System (SICA), seated in Managua, Nicaragua since 1994.
Each Gaceta Oficial issue reproduces the Court's sentencias (definitive
judgments), resoluciones and interlocutory awards interpreting and applying
the SICA treaties/protocols; these are binding, non-appealable decisions on
member states, SICA organs and natural/legal persons = case_law. As official
edicts of an international tribunal they are public-domain government works.

Access problem & solution (no JavaScript, no CAPTCHA, no auth):
  The live host https://portal.ccj.org.ni/ (a WordPress site) TCP-times-out
  on :443 from foreign/datacenter build vantages (Nicaragua-hosted, geo/IP
  filtered) -- so a direct live fetch is not possible from most vantages.
  However the Internet Archive has full 200-status captures of the Court's
  entire Gaceta Oficial series (Nos 1-19) under
      http://portal.ccj.org.ni/ccj/wp-content/uploads/Gaceta*.pdf
  These are scanned image PDFs (no text layer), so we OCR them with tesseract
  (Spanish). OCR quality is clean (verified: continuous Spanish legal prose).

Strategy:
  1. Enumerate the archived Gaceta PDFs via the Wayback CDX API
     (statuscode:200, mimetype:application/pdf), collapsing to one capture
     per file.
  2. Download each via the Wayback raw endpoint (/web/{ts}id_/{url}).
  3. OCR all pages (fitz rasterize @200dpi -> pytesseract lang=spa).
  4. Emit one full-text record per Gaceta Oficial issue. Each issue is a
     coherent official publication of the Court's decisions for its period;
     reliable per-sentence splitting from OCR text is not attempted.

Date: each issue's OCR text is scanned for Gregorian years; the latest year
in the plausible range [1994, current] is taken as the issue date
(publication approximates the most recent decision reproduced).

Usage:
  python bootstrap.py bootstrap            # Full pull (all 19 Gacetas)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test (CDX enumerate)
"""

from __future__ import annotations

import io
import sys
import json
import logging
import re
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.CentralAmericanCourt")

SOURCE_ID = "INTL/CentralAmericanCourt"
LIVE_HOST = "https://portal.ccj.org.ni/"

# Wayback CDX enumeration of the archived Gaceta Oficial PDFs.
CDX_API = (
    "http://web.archive.org/cdx/search/cdx"
    "?url=portal.ccj.org.ni/ccj/wp-content/uploads/*"
    "&output=json&fl=original,timestamp,statuscode,mimetype"
    "&filter=statuscode:200&filter=mimetype:application/pdf"
    "&collapse=urlkey"
)

# Fallback list (original_url, timestamp) if the CDX API is unavailable.
FALLBACK_GACETAS = [
    ("http://portal.ccj.org.ni/ccj/wp-content/uploads/Gaceta1.pdf", "20220321043228"),
    ("http://portal.ccj.org.ni/ccj/wp-content/uploads/Gaceta2.pdf", "20220320190020"),
    ("http://portal.ccj.org.ni/ccj/wp-content/uploads/GacetaNo3.pdf", "20220320190050"),
    ("http://portal.ccj.org.ni/ccj/wp-content/uploads/GacetaNo4.pdf", "20220321043246"),
    ("http://portal.ccj.org.ni/ccj/wp-content/uploads/GacetaNo5.pdf", "20220321043352"),
    ("http://portal.ccj.org.ni/ccj/wp-content/uploads/GacetaNo6.pdf", "20220321043329"),
    ("http://portal.ccj.org.ni/ccj/wp-content/uploads/GacetaNo7.pdf", "20220320190144"),
    ("http://portal.ccj.org.ni/ccj/wp-content/uploads/GacetaNo8.pdf", "20220320190153"),
    ("http://portal.ccj.org.ni/ccj/wp-content/uploads/GacetaNo9.pdf", "20220321043407"),
    ("http://portal.ccj.org.ni/ccj/wp-content/uploads/GacetaNo10.pdf", "20220320190354"),
    ("http://portal.ccj.org.ni/ccj/wp-content/uploads/GacetaNo11.pdf", "20220321043146"),
    ("http://portal.ccj.org.ni/ccj/wp-content/uploads/GacetaNo12.pdf", "20220321043306"),
    ("http://portal.ccj.org.ni/ccj/wp-content/uploads/GacetaNo13.pdf", "20220321043203"),
    ("http://portal.ccj.org.ni/ccj/wp-content/uploads/Gaceta-No.14.pdf", "20220321043333"),
    ("http://portal.ccj.org.ni/ccj/wp-content/uploads/GacetaNo15.pdf", "20220321043311"),
    ("http://portal.ccj.org.ni/ccj/wp-content/uploads/GacetaNo16.pdf", "20211104234058"),
    ("http://portal.ccj.org.ni/ccj/wp-content/uploads/GacetaNo17.pdf", "20220321043345"),
    ("http://portal.ccj.org.ni/ccj/wp-content/uploads/GacetaNo18_opt.pdf", "20220320190206"),
    ("http://portal.ccj.org.ni/ccj/wp-content/uploads/GacetaNo19.pdf", "20220320190252"),
]

GACETA_NO_RE = re.compile(r"Gaceta(?:No|-No\.)?[.\s]*?(\d{1,2})", re.I)
YEAR_RE = re.compile(r"\b(19\d\d|20\d\d)\b")

USER_AGENT = "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/ZachLaik)"

OCR_DPI = 200
OCR_LANG = "spa"


class CentralAmericanCourtScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": USER_AGENT})
        self._cache_dir = Path(__file__).parent / "data" / "pdf_cache"
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    # ── enumeration ────────────────────────────────────────────────
    def _enumerate_gacetas(self) -> list[tuple[str, str]]:
        """Return list of (original_url, timestamp) for the archived Gacetas."""
        try:
            r = self._session.get(CDX_API, timeout=45)
            r.raise_for_status()
            rows = r.json()
            out = []
            for row in rows[1:]:  # skip header row
                original, timestamp = row[0], row[1]
                if re.search(r"/Gaceta", original, re.I):
                    out.append((original, timestamp))
            if out:
                # sort by gaceta number for stable ordering
                out.sort(key=lambda p: self._gaceta_no(p[0]) or 999)
                logger.info("CDX enumerated %d Gaceta PDFs", len(out))
                return out
        except Exception as e:
            logger.warning("CDX enumeration failed (%s); using fallback list", e)
        return list(FALLBACK_GACETAS)

    @staticmethod
    def _gaceta_no(url: str) -> int | None:
        m = GACETA_NO_RE.search(url.rsplit("/", 1)[-1])
        return int(m.group(1)) if m else None

    # ── fetching + OCR ─────────────────────────────────────────────
    def _wayback_raw_url(self, original: str, timestamp: str) -> str:
        return f"http://web.archive.org/web/{timestamp}id_/{original}"

    def _download_cached(self, original: str, wb_url: str) -> bytes | None:
        """Download via cache: read local file if present, else fetch + save.

        Uses curl (robust against the LibreSSL/urllib3 keep-alive stalls seen
        against web.archive.org) with a requests fallback.
        """
        fname = original.rsplit("/", 1)[-1]
        cache_path = self._cache_dir / fname
        if cache_path.exists() and cache_path.stat().st_size > 1000:
            return cache_path.read_bytes()
        # Try curl first.
        import subprocess
        try:
            subprocess.run(
                ["curl", "-sS", "-L", "--max-time", "180", "-A", USER_AGENT,
                 "-o", str(cache_path), wb_url],
                check=True, capture_output=True, timeout=200,
            )
            if cache_path.exists() and cache_path.stat().st_size > 1000:
                return cache_path.read_bytes()
        except Exception as e:
            logger.warning("curl download failed for %s: %s", wb_url, e)
        # Fallback to requests.
        data = self._download(wb_url)
        if data:
            cache_path.write_bytes(data)
        return data

    def _download(self, url: str) -> bytes | None:
        for attempt in range(3):
            try:
                r = self._session.get(url, timeout=(15, 120), stream=True)
                r.raise_for_status()
                data = bytearray()
                for chunk in r.iter_content(chunk_size=65536):
                    if chunk:
                        data.extend(chunk)
                    if len(data) > 60_000_000:
                        logger.warning("PDF too large, aborting: %s", url)
                        return None
                return bytes(data)
            except Exception as e:
                logger.warning("download attempt %d failed for %s: %s", attempt + 1, url, e)
                time.sleep(2 * (attempt + 1))
        return None

    def _ocr(self, pdf_bytes: bytes) -> tuple[str, int]:
        """Rasterize all pages @OCR_DPI and OCR with tesseract (Spanish)."""
        import fitz  # type: ignore
        import pytesseract  # type: ignore
        from PIL import Image  # type: ignore

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        parts = []
        try:
            npages = doc.page_count
            for i in range(npages):
                page = doc.load_page(i)
                pix = page.get_pixmap(dpi=OCR_DPI)
                img = Image.open(io.BytesIO(pix.tobytes("png")))
                try:
                    txt = pytesseract.image_to_string(img, lang=OCR_LANG)
                except Exception as e:
                    logger.warning("OCR page %d failed: %s", i, e)
                    continue
                if txt and txt.strip():
                    parts.append(txt.strip())
        finally:
            doc.close()
        return "\n\n".join(parts).strip(), npages

    # ── raw generator ──────────────────────────────────────────────
    def _iter_raw(self) -> Generator[dict, None, None]:
        for original, timestamp in self._enumerate_gacetas():
            no = self._gaceta_no(original)
            wb_url = self._wayback_raw_url(original, timestamp)
            logger.info("Fetching Gaceta %s -> %s", no, wb_url)
            pdf_bytes = self._download_cached(original, wb_url)
            if not pdf_bytes:
                logger.warning("skip Gaceta %s (download failed)", no)
                continue
            text, npages = self._ocr(pdf_bytes)
            if not text or len(text) < 500:
                logger.warning("skip Gaceta %s (OCR yielded %d chars)", no, len(text))
                continue
            yield {
                "gaceta_no": no,
                "original_url": original,
                "wayback_url": wb_url,
                "pages": npages,
                "text": text,
            }
            time.sleep(1)

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_raw()

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        # Archived static gazette series; treat as full re-scan.
        yield from self._iter_raw()

    # ── normalization ──────────────────────────────────────────────
    def _issue_date(self, text: str) -> str | None:
        years = [int(y) for y in YEAR_RE.findall(text)]
        current = datetime.now(timezone.utc).year
        years = [y for y in years if 1994 <= y <= current]
        if not years:
            return None
        return f"{max(years)}-12-31"

    def normalize(self, raw: dict) -> dict:
        no = raw.get("gaceta_no")
        text = (raw.get("text") or "").strip()
        if not text:
            return None
        doc_id = f"gaceta-{no}" if no is not None else hashlib.sha1(
            raw.get("original_url", "").encode()
        ).hexdigest()[:12]
        date = self._issue_date(text)
        title = (
            f"Gaceta Oficial de la Corte Centroamericana de Justicia No. {no}"
            if no is not None
            else "Gaceta Oficial de la Corte Centroamericana de Justicia"
        )
        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": raw.get("original_url"),
            "wayback_url": raw.get("wayback_url"),
            "gaceta_no": no,
            "pages": raw.get("pages"),
            "language": "es",
            "jurisdiction": "INTL/SICA",
            "court": "Corte Centroamericana de Justicia",
        }

    # ── connectivity test ──────────────────────────────────────────
    def test_api(self) -> bool:
        gacetas = self._enumerate_gacetas()
        logger.info("Enumerated %d Gaceta PDFs", len(gacetas))
        if not gacetas:
            return False
        # Verify the first is downloadable + OCR-able.
        original, timestamp = gacetas[0]
        data = self._download_cached(original, self._wayback_raw_url(original, timestamp))
        if not data:
            logger.error("could not download %s", original)
            return False
        text, npages = self._ocr(data)
        logger.info("Gaceta %s: %d pages, %d OCR chars",
                    self._gaceta_no(original), npages, len(text))
        return len(text) > 500


def main():
    import argparse

    parser = argparse.ArgumentParser(description="INTL/CentralAmericanCourt bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CentralAmericanCourtScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
