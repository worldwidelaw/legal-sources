#!/usr/bin/env python3
"""
RU/RegionalLegislation -- Official regional legislation of the subjects of the
Russian Federation (Органы государственной власти субъектов Российской Федерации),
published on the Official Internet Portal of Legal Information
(publication.pravo.gov.ru).

This is the sub-national ("oblast-level") companion to RU/PravoGovRu (federal
laws only). The portal's "subjects" block carries the officially-published legal
acts of every federal subject (republics, krais, oblasts, autonomous okrugs,
federal cities): regional laws (законы субъекта), decrees/ordinances of the head
of the region (указы / постановления главы), government resolutions and orders
(постановления / распоряжения правительства) and ministerial acts. ~1.5M acts.
All are official government-edict works = legislation.

Access (no JavaScript, no CAPTCHA, no auth) — a clean JSON API:

  1. Enumerate documents (newest first), 200 per page:
       GET /api/Documents?block=subjects&PageSize=200&Index={N}
     -> {"items":[{eoNumber, complexName, name, number, documentDate,
                   signatoryAuthorityId, publishDateShort, ...}], itemsTotalCount,
         pagesTotalCount}
     (Valid PageSize values are 10 / 30 / 100 / 200.)

  2. Resolve the issuing authority (region body) name:
       GET /api/SignatoryAuthorities?blockCode=subjects  ->  [{id, name}, ...]

  3. Full text: the signed act PDF at
       GET /file/pdf?eoNumber={eoNumber}
     These are scanned/signed images (no text layer). fitz cannot composite the
     1-bit embedded image when rendering the page, so we extract the embedded
     image object of each page directly and OCR it with tesseract (lang=rus).
     The (rare) born-digital act with a real text layer is used as-is.

Strategy:
  Page through /api/Documents newest-first; for each act download the PDF and
  extract full text (text-layer if present, else per-page embedded-image OCR).
  Metadata (title, date, act number, issuing authority) comes from the API item.

Usage:
  python bootstrap.py bootstrap            # Full pull (all regional acts)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples (newest first)
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
"""

from __future__ import annotations

import io
import sys
import json
import logging
import time
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
logger = logging.getLogger("legal-data-hunter.RU.RegionalLegislation")

BASE_URL = "http://publication.pravo.gov.ru"
DOCS_API = BASE_URL + "/api/Documents?block=subjects&PageSize=200&Index={index}"
AUTH_API = BASE_URL + "/api/SignatoryAuthorities?blockCode=subjects"
PDF_URL = BASE_URL + "/file/pdf?eoNumber={eo}"
DOC_URL = BASE_URL + "/document/{eo}"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

OCR_LANG = "rus"
OCR_MAX_PAGES = 40  # bound OCR time on very long acts
MIN_TEXT_CHARS = 150


def _iso_date(value: str | None) -> str | None:
    """Parse an ISO-ish '2026-07-08T00:00:00' or '08.07.2026' to 'YYYY-MM-DD'."""
    if not value:
        return None
    value = value.strip()
    # ISO with T
    if "T" in value or (len(value) >= 10 and value[4] == "-"):
        return value[:10]
    # DD.MM.YYYY
    parts = value.split(".")
    if len(parts) == 3 and len(parts[2]) == 4:
        d, m, y = parts
        try:
            return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
        except ValueError:
            return None
    return None


def _extract_full_text(pdf_bytes: bytes) -> str:
    """Extract full text from a pravo.gov.ru signed-act PDF.

    Prefers a real text layer; otherwise OCRs the embedded image of each page
    (the pages render blank via get_pixmap because the 1-bit image is not
    composited, so we OCR the extracted image object directly).
    """
    import fitz  # PyMuPDF
    from PIL import Image
    import pytesseract

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        text_layer = "".join(page.get_text() for page in doc).strip()
        if len(text_layer) >= 200:
            return text_layer

        parts: list[str] = []
        n_pages = min(doc.page_count, OCR_MAX_PAGES)
        for pno in range(n_pages):
            page = doc[pno]
            for img in page.get_images(full=True):
                xref = img[0]
                try:
                    info = doc.extract_image(xref)
                    im = Image.open(io.BytesIO(info["image"])).convert("L")
                    t = pytesseract.image_to_string(im, lang=OCR_LANG)
                except Exception as e:  # pragma: no cover
                    logger.debug(f"    OCR page {pno} failed: {e}")
                    continue
                if t and t.strip():
                    parts.append(t.strip())
        if doc.page_count > OCR_MAX_PAGES:
            logger.warning(
                f"    OCR capped at {OCR_MAX_PAGES} pages (PDF has {doc.page_count})"
            )
        return "\n\n".join(parts).strip()
    finally:
        doc.close()


class RURegionalLegislationScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})
        self._auth_map: dict | None = None

    # ---------------------------------------------------------------- http
    def _get(self, url: str, want_json: bool = False):
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                r = self.session.get(url, timeout=90, allow_redirects=True)
                if r.status_code == 200:
                    return r
                logger.warning(f"GET {url} -> HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"GET failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------- authority map
    def authority_map(self) -> dict:
        if self._auth_map is not None:
            return self._auth_map
        self._auth_map = {}
        r = self._get(AUTH_API, want_json=True)
        if r is not None:
            try:
                for a in r.json():
                    self._auth_map[a["id"]] = a.get("name")
            except Exception as e:
                logger.warning(f"Authority map parse failed: {e}")
        logger.info(f"Authority map: {len(self._auth_map)} issuing bodies")
        return self._auth_map

    # ----------------------------------------------------------- discovery
    def _iter_index(self, sample: bool = False) -> Generator[dict, None, None]:
        """Yield document metadata dicts from the /api/Documents pager."""
        index = 1
        total_pages = None
        while True:
            r = self._get(DOCS_API.format(index=index), want_json=True)
            if r is None:
                return
            try:
                payload = r.json()
            except Exception as e:
                logger.error(f"Documents page {index} JSON parse failed: {e}")
                return
            items = payload.get("items") or []
            if total_pages is None:
                total_pages = payload.get("pagesTotalCount")
                logger.info(
                    f"Regional acts: {payload.get('itemsTotalCount')} docs "
                    f"across {total_pages} pages"
                )
            if not items:
                return
            for it in items:
                yield it
            if sample:
                return
            index += 1
            if total_pages and index > total_pages:
                return

    def _fetch_one(self, item: dict) -> dict | None:
        eo = item.get("eoNumber")
        if not eo:
            return None
        r = self._get(PDF_URL.format(eo=eo))
        if r is None or not r.content or not r.content[:5].startswith(b"%PDF"):
            return None
        try:
            text = _extract_full_text(r.content)
        except Exception as e:
            logger.warning(f"  {eo}: extraction failed: {e}")
            return None
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"  {eo}: thin text ({len(text)} chars) — skipped")
            return None
        authority = self.authority_map().get(item.get("signatoryAuthorityId"))
        return {
            "eo_number": eo,
            "title": (item.get("complexName") or item.get("name") or "").strip()
            or f"Документ {eo}",
            "act_name": (item.get("name") or "").strip() or None,
            "act_number": (item.get("number") or "").strip() or None,
            "date": _iso_date(item.get("documentDate") or item.get("publishDateShort")),
            "publish_date": _iso_date(item.get("publishDateShort")),
            "authority": authority,
            "region_code": eo[:4] if len(eo) >= 4 else None,
            "url": DOC_URL.format(eo=eo),
            "pdf_url": PDF_URL.format(eo=eo),
            "text": text,
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for item in self._iter_index(sample=sample):
            rec = self._fetch_one(item)
            if rec:
                yield rec
                emitted += 1
                logger.info(
                    f"  {rec['eo_number']} OK ({len(rec['text'])} chars) "
                    f"[{rec.get('authority') or '?'}]"
                )
                if sample and emitted >= 12:
                    return

    # --------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing pravo.gov.ru regional-legislation API...")
        items = list(self._iter_index(sample=True))
        if len(items) < 50:
            logger.error(f"API test FAILED: index page too small ({len(items)})")
            return False
        ok = 0
        for it in items[:6]:
            rec = self._fetch_one(it)
            if rec and len(rec["text"]) >= MIN_TEXT_CHARS:
                logger.info(
                    f"  {rec['eo_number']} OK ({len(rec['text'])} chars, "
                    f"date={rec['date']}, {rec.get('authority')})"
                )
                ok += 1
            if ok >= 2:
                break
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: could not extract full text")
        return False

    # ---------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"RU/RegionalLegislation/{raw['eo_number']}",
            "_source": "RU/RegionalLegislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "eo_number": raw["eo_number"],
            "title": raw["title"],
            "act_number": raw.get("act_number"),
            "issuer": raw.get("authority"),
            "region_code": raw.get("region_code"),
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "publish_date": raw.get("publish_date"),
            "jurisdiction": "RU",
            "language": "ru",
        }

    # ------------------------------------------------------------- fetch
    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self._iter_raw(sample=False):
            date = raw.get("publish_date") or raw.get("date")
            if not since or (date and date >= since):
                yield raw
            elif date and date < since:
                # newest-first: once we pass the cutoff we can stop
                return


def main():
    import argparse

    parser = argparse.ArgumentParser(description="RU/RegionalLegislation bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = RURegionalLegislationScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
