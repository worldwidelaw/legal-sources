#!/usr/bin/env python3
"""
BH/MOLA-Laws -- Bahrain Ministry of Legal Affairs: Consolidated Laws

Fetches consolidated laws and legislative decrees published in English/Arabic
by the Kingdom of Bahrain's Ministry of Legal Affairs (MoLA).

Strategy (rewritten 2026-09-09 for the mola.gov.bh redesign — issue #1602):
  The listing at /EN/Legislation/Laws/ is no longer a server-rendered table.
  It is an ASP.NET Razor Pages app whose grid is populated by an XHR:

      POST /EN/Legislation/Laws/?handler=GetLegislations
      headers: RequestVerificationToken: <__RequestVerificationToken from page>
      form:    pageNo, pageSize, searchKeyWord, searchYear, searchCategory,
               searchdocumentLanguage, searchStatus, searchLawNo,
               searchGazetteNo, sortOption
      -> {"items": [...], "totalRecords": N}

  Each item carries structured metadata (lawNo, gazetteNo, date, status,
  abstract) plus `documentInEnglish` / `documentInArabic` PDF paths. The PDFs
  are bilingual consolidated texts (English translation + Arabic original,
  amendments incorporated inline); 9 of the laws are Arabic-only.

  Text is extracted with PyMuPDF (fitz).

Notes:
  - The static assets and the XHR are behind a WAF that rejects short/absent
    User-Agent strings; a full browser UA + Referer is required.
  - pageSize is honoured up to at least 500, so the whole corpus (66 laws as
    of 2026-09-09) arrives in one request; pagination is still implemented
    defensively against totalRecords.
  - Full mode streams to data/records.jsonl (what the ingest pipeline reads).
    Sample mode writes sample/*.json.
  - Fails loudly (exit 1) when the listing yields no laws, so a selector /
    handler change can never masquerade as a successful empty run.

Data: ~66 consolidated laws & legislative decrees (bilingual, full text).
License: Bahrain Government Open Data (commercial use OK with attribution).
Rate limit: 0.5 req/sec.

Usage:
  python bootstrap.py bootstrap            # Full pull -> data/records.jsonl
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
from html import unescape
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

try:
    import fitz  # PyMuPDF
except ImportError:
    print("ERROR: PyMuPDF not installed. Run: pip3 install PyMuPDF")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BH.MOLA-Laws")

BASE_URL = "https://www.mola.gov.bh"
LISTING_URL = f"{BASE_URL}/EN/Legislation/Laws/"
HANDLER_URL = f"{LISTING_URL}?handler=GetLegislations"
PAGE_SIZE = 200

BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

TOKEN_RE = re.compile(
    r'name="__RequestVerificationToken"[^>]*value="([^"]+)"'
)
TAG_RE = re.compile(r"<[^>]+>")

# Alef forms that can pair with LAM into the لا ligature (see _page_text).
ALEF_FORMS = {"ا", "أ", "إ", "آ", "ٱ"}
LAM = "ل"


def clean_text(text: str) -> str:
    """Collapse excessive whitespace while preserving paragraph breaks."""
    if not text:
        return ""
    lines = [ln.strip() for ln in text.replace("\r", "\n").split("\n")]
    out = []
    blank = 0
    for ln in lines:
        if ln:
            out.append(ln)
            blank = 0
        else:
            blank += 1
            if blank <= 1:
                out.append("")
    return "\n".join(out).strip()


def strip_html(value: str) -> str:
    """Remove tags and decode entities from an abstract/title fragment."""
    if not value:
        return ""
    return unescape(TAG_RE.sub(" ", value)).replace("\xa0", " ").strip()


class BHMOLALawsScraper(BaseScraper):
    """Scraper for BH/MOLA-Laws -- Bahrain consolidated laws."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": BROWSER_UA,
            "Accept-Language": "en,ar;q=0.8",
        })
        self._token: Optional[str] = None

    # ── listing API ─────────────────────────────────────────────────

    def _get_token(self) -> str:
        """Load the listing page for its antiforgery token + session cookies."""
        if self._token:
            return self._token
        last_err = None
        for attempt in range(3):
            try:
                self.rate_limiter.wait()
                resp = self.session.get(LISTING_URL, timeout=60)
                if resp.status_code == 200:
                    m = TOKEN_RE.search(resp.text)
                    if m:
                        self._token = m.group(1)
                        return self._token
                    last_err = "no __RequestVerificationToken in listing page"
                else:
                    last_err = f"HTTP {resp.status_code}"
            except requests.RequestException as e:
                last_err = str(e)
            logger.warning(f"Listing page attempt {attempt + 1}: {last_err}")
            time.sleep(5 * (attempt + 1))
        raise RuntimeError(f"Could not load MoLA listing page: {last_err}")

    def _post_page(self, page_no: int) -> dict:
        """POST the GetLegislations handler for one page of results."""
        token = self._get_token()
        payload = {
            "pageNo": page_no,
            "pageSize": PAGE_SIZE,
            "searchKeyWord": "",
            "searchLawNo": "",
            "searchGazetteNo": "",
            "sortOption": "",
        }
        last_err = None
        for attempt in range(3):
            try:
                self.rate_limiter.wait()
                resp = self.session.post(
                    HANDLER_URL,
                    data=payload,
                    headers={
                        "Referer": LISTING_URL,
                        "RequestVerificationToken": token,
                        "X-Requested-With": "XMLHttpRequest",
                        "Accept": "application/json, text/javascript, */*; q=0.01",
                    },
                    timeout=60,
                )
                if resp.status_code == 200:
                    return resp.json()
                last_err = f"HTTP {resp.status_code}"
                if resp.status_code in (400, 403, 419):
                    # Token may have rotated — re-acquire and retry.
                    self._token = None
                    token = self._get_token()
            except (requests.RequestException, ValueError) as e:
                last_err = str(e)
            logger.warning(
                f"GetLegislations page {page_no} attempt {attempt + 1}: {last_err}"
            )
            time.sleep(5 * (attempt + 1))
        raise RuntimeError(
            f"GetLegislations failed for page {page_no}: {last_err}"
        )

    def _list_laws(self) -> list:
        """Walk the handler until every advertised record is collected."""
        first = self._post_page(1)
        total = int(first.get("totalRecords") or 0)
        items = list(first.get("items") or [])
        if total <= 0 or not items:
            raise RuntimeError(
                "MoLA GetLegislations returned no laws "
                f"(totalRecords={total}, items={len(items)}) — "
                "the handler or its payload shape has changed"
            )

        page = 1
        while len(items) < total:
            page += 1
            more = self._post_page(page).get("items") or []
            if not more:
                logger.warning(
                    f"Page {page} empty at {len(items)}/{total} — stopping walk"
                )
                break
            items.extend(more)

        laws = []
        seen = set()
        for it in items:
            detail = (it.get("url") or "").strip()
            pdf = (it.get("documentInEnglish") or "").strip() \
                or (it.get("documentInArabic") or "").strip()
            if not detail or not pdf:
                logger.warning(
                    f"Skipping entry without detail url or PDF: {it.get('title')!r}"
                )
                continue
            slug = detail.rstrip("/").rsplit("/", 1)[-1]
            if slug in seen:
                continue
            seen.add(slug)
            laws.append({
                "slug": slug,
                "law_no": (it.get("lawNo") or "").strip(),
                "gazette_no": (it.get("gazetteNo") or "").strip(),
                "date": (it.get("date") or "").strip(),
                "status": strip_html(it.get("statusLegalText") or it.get("status") or ""),
                "document_label": (it.get("documentLabel") or "").strip(),
                "title": strip_html(it.get("title") or ""),
                "abstract": strip_html(it.get("abstract") or ""),
                "detail_url": detail if detail.startswith("http") else BASE_URL + detail,
                "pdf_url": pdf if pdf.startswith("http") else BASE_URL + pdf,
                "pdf_path": pdf,
                "has_english": bool((it.get("documentInEnglish") or "").strip()),
            })
        return laws

    # ── document fetch ──────────────────────────────────────────────

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download a law PDF with retries."""
        for attempt in range(3):
            try:
                self.rate_limiter.wait()
                resp = self.session.get(
                    url, timeout=120, headers={"Referer": LISTING_URL}
                )
                if resp.status_code == 200 and len(resp.content) > 1000:
                    return resp.content
                if resp.status_code == 404:
                    return None
                logger.warning(f"{url}: HTTP {resp.status_code}")
            except requests.RequestException as e:
                logger.warning(f"{url} attempt {attempt + 1}: {e}")
            if attempt < 2:
                time.sleep(5 * (attempt + 1))
        return None

    @staticmethod
    def _page_text(page) -> str:
        """
        Extract a page's text, repairing decomposed lam-alef ligatures.

        The MoLA PDFs draw لا as a single ligature glyph whose ToUnicode maps
        to two code points emitted in *visual* (right-to-left) order, so plain
        get_text() yields ALEF+LAM where the document says LAM+ALEF —
        e.g. إخلاء becomes إخالء, silently unmatchable by keyword search.
        A genuine definite article ال is indistinguishable by code point, but
        the ligature's alef is always zero-width (the following lam carries the
        whole advance), which is the discriminator used here.
        """
        rawdict = page.get_text("rawdict")
        blocks = []
        for block in rawdict["blocks"]:
            if block.get("type") != 0:  # skip images
                continue
            lines = []
            for line in block.get("lines", []):
                parts = []
                for span in line["spans"]:
                    chars = span["chars"]
                    buf = [c["c"] for c in chars]
                    for i in range(len(chars) - 1):
                        bbox = chars[i]["bbox"]
                        if (
                            buf[i] in ALEF_FORMS
                            and buf[i + 1] == LAM
                            and bbox[2] - bbox[0] < 0.01
                        ):
                            buf[i], buf[i + 1] = buf[i + 1], buf[i]
                    parts.append("".join(buf))
                lines.append("".join(parts))
            blocks.append("\n".join(lines))
        return "\n".join(blocks)

    def _extract_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF bytes using PyMuPDF."""
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages = []
        for page in doc:
            text = self._page_text(page)
            if text and text.strip():
                pages.append(text.strip())
        doc.close()
        return clean_text("\n\n".join(pages))

    def _iter_laws(self) -> Generator[dict, None, None]:
        laws = self._list_laws()
        logger.info(f"Found {len(laws)} laws on MoLA listing")
        for law in laws:
            pdf_bytes = self._download_pdf(law["pdf_url"])
            if pdf_bytes is None:
                logger.warning(f"Law {law['slug']}: PDF unavailable")
                continue
            text = self._extract_text(pdf_bytes)
            if not text or len(text) < 100:
                logger.warning(
                    f"Law {law['slug']}: insufficient text ({len(text)} chars)"
                )
                continue
            yield {**law, "text": text, "pdf_size": len(pdf_bytes)}

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_laws()

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        # Listing is small (~66 laws); re-scan everything and let the
        # publication date filter. Upsert dedups downstream.
        since_str = since.date().isoformat() if isinstance(since, datetime) else str(since)
        for raw in self._iter_laws():
            if raw.get("date") and raw["date"] < since_str:
                continue
            yield raw

    def normalize(self, raw: dict) -> dict:
        year = raw["date"][:4] if raw.get("date") else ""
        title = raw.get("title") or (
            f"Bahrain Law No. ({raw.get('law_no')}) of {year}".strip()
        )
        return {
            "_id": f"BH-MOLA-{raw['slug']}",
            "_source": "BH/MOLA-Laws",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": raw.get("date") or None,
            "url": raw["detail_url"],
            "pdf_url": raw["pdf_url"],
            "law_number": raw.get("law_no") or None,
            "gazette_number": raw.get("gazette_no") or None,
            "year": year or None,
            "status": raw.get("status") or None,
            "summary": raw.get("abstract") or None,
            "language": "ar" if not raw.get("has_english") else "ar+en",
            "pdf_size_bytes": raw.get("pdf_size"),
        }


# ── CLI ─────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="BH/MOLA-Laws bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test-api"]
    )
    parser.add_argument("--sample", action="store_true", help="Fetch 15 sample records only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = BHMOLALawsScraper()

    if args.command == "test-api":
        logger.info("Testing connectivity...")
        laws = scraper._list_laws()
        logger.info(f"Listing OK — {len(laws)} laws found")
        return

    source_dir = Path(__file__).parent
    sample_mode = args.sample
    limit = 15 if sample_mode else None

    jsonl_file = None
    sample_dir = source_dir / "sample"
    if sample_mode:
        sample_dir.mkdir(exist_ok=True)
    else:
        data_dir = source_dir / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        jsonl_file = open(data_dir / "records.jsonl", "w", encoding="utf-8")

    count = 0
    try:
        for raw in scraper.fetch_all():
            record = scraper.normalize(raw)
            if sample_mode:
                with open(sample_dir / f"{record['_id']}.json", "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            else:
                jsonl_file.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
            logger.info(
                f"[{count}] {record['_id']}: {len(record['text'])} chars, "
                f"{record['title'][:60]}"
            )
            if limit and count >= limit:
                break
    finally:
        if jsonl_file is not None:
            jsonl_file.close()

    dest = sample_dir if sample_mode else source_dir / "data/records.jsonl"
    logger.info(f"Done. {count} laws written to {dest}")

    # Fail loud: an empty run is a scraper break, not a successful no-op.
    if count == 0:
        logger.error("No laws with full text were produced — failing")
        sys.exit(1)


if __name__ == "__main__":
    main()
