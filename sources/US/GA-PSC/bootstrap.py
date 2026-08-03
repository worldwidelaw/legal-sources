#!/usr/bin/env python3
"""
US/GA-PSC -- Georgia Public Service Commission Orders

Fetches the full text of Orders issued by the Georgia Public Service Commission
(GPSC) adjudicating utility dockets (electric, natural-gas, telecommunications,
transportation) -- rate cases, certificate/territorial-service transfers,
Integrated Resource Plans, Universal Service Fund disbursements, complaints and
other proceedings. Each Order is an administrative adjudication of a specific
docket by the Commission = case_law. Public domain (US state government edict).

Strategy (official public FACTS JSON API -- no auth):
  The GPSC "FACTS" (Filing And Case Tracking System) is backed by a PUBLIC,
  no-auth JSON API at https://services.psc.ga.gov/api/v1/External/Public/
  (documented by its own Swagger at /swagger/v1/swagger.json), reachable from a
  residential build vantage (psc.ga.gov + services.psc.ga.gov both 200).

  1. DISCOVERY + METADATA: POST /Post/DocumentFilingsFilter with
     documentDescription="ORDER" and a filing-date window returns the document
     filings whose description contains "ORDER". Paginate via startIndex
     (1-based offset) / pageSize against totalCount. Each row carries
     documentId, docketId (== the public GPSC docket number, e.g. 56001),
     companyName (the filer -- "GPSC" == Commission-issued), description and
     filedDate (= real issue date). We keep only Commission-issued Orders
     (companyName contains "GPSC").

  2. ATTACHMENT IDS: the server-rendered document detail page
     /search/facts-document/?documentId={id} lists the born-digital attachment
     download links as
     services.psc.ga.gov/.../Get/Document/DownloadFile/{documentId}/{attId}.

  3. FULL TEXT: each attachment is downloaded from that DownloadFile endpoint.
     GPSC Orders are born-digital Microsoft Word (.docx) documents (text layer,
     no OCR needed); some are born-digital PDFs. Text is extracted from every
     attachment and de-duplicated (an Order is frequently attached as both a
     .docx and an identical .pdf). fitz/PyMuPDF handles PDFs (Tesseract OCR
     fallback for the rare image-only scan); python-stdlib zipfile parses the
     .docx word/document.xml.

Newest-first date-window iteration means the framework's sample pull draws from
clean modern born-digital Orders.

Usage:
  python bootstrap.py bootstrap            # Full pull (all Orders)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import io
import json
import logging
import re
import time
import zipfile
import html as html_mod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import fitz  # PyMuPDF

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.GA-PSC")

API_BASE = "https://services.psc.ga.gov/api/v1/External/Public"
FILTER_URL = f"{API_BASE}/Post/DocumentFilingsFilter"
DOWNLOAD_URL = f"{API_BASE}/Get/Document/DownloadFile"
SITE_BASE = "https://psc.ga.gov"
DOC_PAGE = f"{SITE_BASE}/search/facts-document/?documentId="

# Corpus floor: FACTS filings thin out before the mid-1990s.
FIRST_YEAR = 1995
PAGE_SIZE = 100

DOWNLOAD_RE = re.compile(rb"DownloadFile/(\d+)/(\d+)")


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_iso_date(s: str) -> str | None:
    """Convert a FACTS 'YYYY-MM-DDT00:00:00' string to an ISO date."""
    if not s:
        return None
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    return m.group(0) if m else None


def _date_windows():
    """Yield (from, to) 'YYYY-MM-DD' half-year windows, newest first, to FIRST_YEAR."""
    now = datetime.now(timezone.utc)
    year = now.year
    while year >= FIRST_YEAR:
        yield (f"{year}-07-01", f"{year}-12-31")
        yield (f"{year}-01-01", f"{year}-06-30")
        year -= 1


class GAPSCScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "*/*",
            },
            timeout=90,
        )
        self.delay = 1.0

    # ---- low-level fetch helpers -------------------------------------------

    def _get(self, url: str, retries: int = 4) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.content
                if resp.status_code == 404:
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _post_json(self, url: str, body: dict, retries: int = 4):
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.post(
                    url,
                    data=json.dumps(body),
                    headers={"Content-Type": "application/json"},
                )
                if resp.status_code == 200:
                    return resp.json()
                logger.warning(f"HTTP {resp.status_code} POST {url}")
            except Exception as e:
                logger.warning(f"Error POST {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- discovery ----------------------------------------------------------

    def _fetch_window(self, dfrom: str, dto: str) -> Generator[dict, None, None]:
        """Yield raw GPSC-issued Order metadata dicts for one date window."""
        start = 1
        while True:
            body = {
                "documentDescription": "ORDER",
                "filingDateFrom": dfrom,
                "filingDateTo": dto,
                "startIndex": start,
                "pageSize": PAGE_SIZE,
            }
            data = self._post_json(FILTER_URL, body)
            if not data:
                return
            rows = data.get("searchDocumentFilings") or []
            total = data.get("totalCount") or 0
            if not rows:
                return
            for r in rows:
                company = (r.get("companyName") or "").strip()
                # Keep only Commission-issued Orders (GPSC), skip party filings.
                if "GPSC" not in company.upper():
                    continue
                yield {
                    "documentId": r.get("documentId"),
                    "docketId": r.get("docketId"),
                    "description": (r.get("description") or "").strip() or None,
                    "date": parse_iso_date(r.get("filedDate")),
                    "industry": (r.get("industryName") or [None])[0]
                    if r.get("industryName")
                    else None,
                    "url": f"{DOC_PAGE}{r.get('documentId')}",
                }
            start += PAGE_SIZE
            if start > total:
                return

    def _attachment_ids(self, document_id: int) -> list[str]:
        """Scrape the server-rendered document detail page for attachment ids."""
        page = self._get(f"{DOC_PAGE}{document_id}")
        if not page:
            return []
        ids = []
        for did, aid in DOWNLOAD_RE.findall(page):
            if did.decode() == str(document_id):
                a = aid.decode()
                if a not in ids:
                    ids.append(a)
        return ids

    # ---- text extraction ----------------------------------------------------

    def _ocr_pdf(self, doc) -> str:
        try:
            import pytesseract
            from PIL import Image
        except Exception:
            return ""
        parts = []
        for page in doc:
            try:
                pix = page.get_pixmap(dpi=200)
                img = Image.open(io.BytesIO(pix.tobytes("png")))
                parts.append(pytesseract.image_to_string(img))
            except Exception as e:
                logger.debug(f"OCR page failed: {e}")
        return "\n".join(parts)

    def _extract_pdf(self, data: bytes) -> str:
        try:
            doc = fitz.open(stream=data, filetype="pdf")
        except Exception as e:
            logger.debug(f"PDF open failed: {e}")
            return ""
        try:
            text = clean_text("\n".join(page.get_text() for page in doc))
            if len(text) < 200:
                ocr = clean_text(self._ocr_pdf(doc))
                if len(ocr) > len(text):
                    text = ocr
            return text
        finally:
            doc.close()

    def _extract_docx(self, data: bytes) -> str:
        try:
            z = zipfile.ZipFile(io.BytesIO(data))
        except Exception as e:
            logger.debug(f"DOCX open failed: {e}")
            return ""
        parts = []
        for name in ("word/document.xml", "word/footnotes.xml", "word/endnotes.xml"):
            try:
                xml = z.read(name).decode("utf-8", "ignore")
            except KeyError:
                continue
            # Preserve paragraph and line breaks, drop tags, unescape entities.
            xml = re.sub(r"</w:p>", "\n", xml)
            xml = re.sub(r"<w:br\s*/?>", "\n", xml)
            xml = re.sub(r"<w:tab\s*/?>", " ", xml)
            xml = re.sub(r"<[^>]+>", "", xml)
            parts.append(html_mod.unescape(xml))
        return clean_text("\n".join(parts))

    def _extract_attachment(self, data: bytes) -> str:
        if not data:
            return ""
        if data[:5] == b"%PDF-":
            return self._extract_pdf(data)
        if data[:2] == b"PK":  # docx/zip container
            return self._extract_docx(data)
        return ""

    def _document_text(self, document_id: int) -> str:
        """Download every attachment for a document and combine deduped text."""
        combined = ""
        norm_seen = ""
        for aid in self._attachment_ids(document_id):
            data = self._get(f"{DOWNLOAD_URL}/{document_id}/{aid}")
            text = self._extract_attachment(data)
            if not text or len(text) < 50:
                continue
            key = re.sub(r"\s+", "", text.lower())
            # Skip an attachment that merely duplicates already-collected text
            # (Orders are often attached as both .docx and identical .pdf).
            if key and key in norm_seen:
                continue
            combined = (combined + "\n\n" + text).strip() if combined else text
            norm_seen += key
        return combined

    # ---- normalize ----------------------------------------------------------

    def normalize(self, raw: dict) -> dict | None:
        document_id = raw.get("documentId")
        if document_id is None:
            return None
        text = self._document_text(document_id)
        if not text or len(text) < 200:
            logger.debug(f"docId {document_id}: no full text ({len(text)} chars)")
            return None

        docket = raw.get("docketId")
        desc = raw.get("description")
        title = "GA PSC Order"
        if docket:
            title += f" — Docket {docket}"
        if desc:
            short = desc if len(desc) <= 200 else desc[:197] + "..."
            title += f" ({short})"

        return {
            "_id": f"US/GA-PSC/{document_id}",
            "_source": "US/GA-PSC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "document_id": str(document_id),
            "docket_number": str(docket) if docket else None,
            "industry": raw.get("industry"),
            "title": title,
            "description": desc,
            "text": text,
            "url": raw.get("url"),
            "date": raw.get("date"),
        }

    # ---- api test -----------------------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing GA PSC FACTS Orders API...")
        try:
            rec = None
            for dfrom, dto in _date_windows():
                for raw in self._fetch_window(dfrom, dto):
                    rec = self.normalize(raw)
                    if rec:
                        break
                if rec:
                    logger.info(f"  Window {dfrom}..{dto} produced full text")
                    break
            if rec and len(rec["text"]) > 200:
                logger.info(
                    f"  Full text OK ({len(rec['text'])} chars, "
                    f"docId {rec['document_id']}, docket={rec.get('docket_number')}, "
                    f"date={rec.get('date')})"
                )
                logger.info("API test PASSED")
                return True
            logger.error("  Full-text extraction failed")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # ---- iteration ----------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield raw Order metadata dicts, newest window first.

        The framework calls normalize() on each raw dict, which downloads the
        attachments and extracts the full text.
        """
        seen = set()
        empty_streak = 0
        for dfrom, dto in _date_windows():
            got = False
            for raw in self._fetch_window(dfrom, dto):
                key = raw.get("documentId")
                if key in seen:
                    continue
                seen.add(key)
                got = True
                yield raw
            if got:
                empty_streak = 0
            else:
                empty_streak += 1
                if empty_streak >= 6:
                    logger.info("Reached 6 consecutive empty windows; stopping.")
                    break

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/GA-PSC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = GAPSCScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        return

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
