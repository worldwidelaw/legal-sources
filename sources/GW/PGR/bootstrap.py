#!/usr/bin/env python3
"""
GW/PGR -- Guinea-Bissau, Procuradoria-Geral da República, Legislação

Full text of the statutory texts published by the Attorney General's Office
of Guinea-Bissau (Procuradoria-Geral da República) at https://pgr.gw/ —
the civil and civil-procedure codes, the labour-procedure code, the organic
laws of the Public Prosecution Service and of the courts, the 2010 security
reform package, anti-trafficking and terrorism-financing legislation, OHADA
uniform acts, CPLP judicial-cooperation conventions, and one full Boletim
Oficial supplement.

Strategy (open WordPress REST, no auth, no captcha):

  GET /wp-json/wp/v2/media?per_page=100&mime_type=application/pdf
      -> 200 JSON, X-WP-Total: 41, one page. Each item carries
         ``source_url`` (the PDF) plus title, slug and upload date.

  The PDFs are then downloaded and extracted with the shared
  ``common/pdf_extract`` backends.

GOTCHAS:
  - The media library holds a handful of non-legal uploads left over from
    site testing ("teste", "PDF Scanner 090924 5.22.10"); they are filtered
    out by title.
  - Several texts were uploaded twice under slightly different titles
    ("Dec 6-80 (2)" / "Decreto 6-80", "Código de Processo Civil" twice).
    Records de-duplicate on a hash of the extracted text, so the second
    upload of an identical text is dropped rather than ingested twice.
  - A minority of the uploads are scans with no text layer (e.g.
    ``GuineBissau.LeiOrganicaMP``, 25 pages, 0 extractable chars). With no
    OCR backend they cannot yield full text and are skipped, logged and
    counted rather than ingested as empty records.
  - The WordPress upload date (2024-08) is NOT the date of the law. The
    enactment date comes from the Portuguese date line in the head of the
    document ("... de 22 de Junho de 2010"), but only when its year agrees
    with the year encoded in the title ("Lei n.º 08_2010" -> 2010) —
    otherwise the first date in the head is the date of a *cited* older
    instrument, which is how the terrorism-financing law first came out
    dated 1973. Where the title carries no year the latest head date wins,
    and where the body date is unusable the title year is used with
    ``date_is_approximate: true``.

Usage:
  python bootstrap.py bootstrap            # Full pull (all documents)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
import threading
import time
import unicodedata
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper  # noqa: E402
from common.pdf_extract import _extract as extract_pdf_text  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GW.PGR")

SOURCE_ID = "GW/PGR"
BASE_URL = "https://pgr.gw"
MEDIA_URL = f"{BASE_URL}/wp-json/wp/v2/media"

USER_AGENT = (
    "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://legaldatahunter.com)"
)
REQUEST_DELAY = 1.0
MAX_RETRIES = 5
MIN_TEXT_CHARS = 500

# Uploads left over from site testing — not legal documents.
JUNK_TITLE_RE = re.compile(r"^\s*(teste|pdf[\s_-]*scanner|img[\s_-]*\d|screenshot)", re.I)

TAG_RE = re.compile(r"<[^>]+>")

PT_MONTHS = {
    "janeiro": 1, "fevereiro": 2, "marco": 3, "abril": 4, "maio": 5,
    "junho": 6, "julho": 7, "agosto": 8, "setembro": 9, "outubro": 10,
    "novembro": 11, "dezembro": 12,
}
PT_DATE_RE = re.compile(
    r"(\d{1,2})\s+de\s+([A-Za-zÀ-ÿ]+)\s+de\s+((?:19|20)\d{2})", re.I
)
# "Lei n.º 08_2010", "Decreto Lei n.º 06_2010", "Codigo-Civil-Legislacao-2006"
TITLE_YEAR_RE = re.compile(r"(?:^|[^0-9])((?:19|20)\d{2})(?:[^0-9]|$)")
# "Dec 4-80" / "Decreto 6-80" — two-digit year in the 1970s-80s decree series
SHORT_YEAR_RE = re.compile(r"\b\d{1,2}\s*[-/]\s*(\d{2})\b")


def strip_html(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", unescape(TAG_RE.sub(" ", value))).strip()


def clean_pdf_text(raw: Optional[str]) -> str:
    if not raw:
        return ""
    text = raw.replace("\xa0", " ").replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def deaccent(value: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", value) if not unicodedata.combining(c)
    )


def pt_dates_in_head(text: str) -> list[str]:
    """Every Portuguese date line in the head of the document, in order.

    Only the head is scanned — the enactment date sits in the Boletim
    Oficial masthead or the preamble, while the body cites the dates of
    other instruments.
    """
    found: list[str] = []
    for m in PT_DATE_RE.finditer(text[:8000]):
        day, month_name, year = m.groups()
        month = PT_MONTHS.get(deaccent(month_name).lower())
        if not month:
            continue
        try:
            found.append(datetime(int(year), month, int(day)).date().isoformat())
        except ValueError:
            continue
    return found


def pick_date(text: str, title: str) -> tuple[Optional[str], bool]:
    """Best enactment date for a document, and whether it is approximate.

    The year encoded in the title ("Lei n.º 12/2011") is authoritative, so
    a body date is only accepted when its year agrees; that keeps a cited
    older instrument's date from being mistaken for the enactment date.
    Where the title carries no year, the LATEST date in the head is taken
    — a text can cite earlier instruments but not later ones.
    """
    candidates = pt_dates_in_head(text)
    year = year_from_title(title)
    if year:
        for cand in candidates:
            if cand.startswith(f"{year}-"):
                return cand, False
        return f"{year}-01-01", True
    if candidates:
        return max(candidates), False
    return None, False


def year_from_title(title: str) -> Optional[int]:
    m = TITLE_YEAR_RE.search(title)
    if m:
        return int(m.group(1))
    m = SHORT_YEAR_RE.search(title)
    if m:
        two = int(m.group(1))
        # The decree series in these titles is 1970s-80s.
        return 1900 + two if two >= 60 else 2000 + two
    return None


def normalize_title(title: str) -> str:
    """'Lei n.º 08_2010 LEI ORG POP' -> 'Lei n.º 08/2010 LEI ORG POP'.

    A few titles carry a symbol-font artefact from the upload (a Private Use
    Area codepoint standing in for the slash, e.g. 'Supl. B. O. n. 32\\uf0222018');
    it is restored to '/' rather than dropped, which would fuse '32' and '2018'.
    """
    cleaned = re.sub("[\\ue000-\\uf8ff]", "/", title)
    cleaned = re.sub(r"(\d)_(\d{4})\b", r"\1/\2", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


class GWPgrScraper(BaseScraper):
    """Scraper for the PGR Guinea-Bissau legislation library."""

    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "application/json,application/pdf,*/*;q=0.8",
                "Accept-Language": "pt,en;q=0.8",
            }
        )
        self._seen_lock = threading.Lock()
        self._seen_hashes: set[str] = set()
        self._no_text: list[str] = []

    # ------------------------------------------------------------------ HTTP

    def _get(self, url: str, params: Optional[dict] = None) -> requests.Response:
        delay = 2.0
        last_exc: Optional[Exception] = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.get(url, params=params, timeout=300)
                if resp.status_code in (429, 500, 502, 503, 504):
                    retry_after = resp.headers.get("Retry-After")
                    wait = float(retry_after) if (retry_after or "").isdigit() else delay
                    logger.warning(
                        "HTTP %s for %s — retry %s/%s in %.0fs",
                        resp.status_code, url, attempt, MAX_RETRIES, wait,
                    )
                    time.sleep(min(wait, 120))
                    delay = min(delay * 2, 120)
                    continue
                resp.raise_for_status()
                return resp
            except requests.RequestException as exc:
                last_exc = exc
                logger.warning(
                    "Request error for %s — retry %s/%s in %.0fs (%s)",
                    url, attempt, MAX_RETRIES, delay, exc,
                )
                time.sleep(delay)
                delay = min(delay * 2, 120)
        if last_exc:
            raise last_exc
        raise RuntimeError(f"Exhausted retries for {url}")

    # --------------------------------------------------------------- listing

    def _media_items(self) -> list[dict]:
        items: list[dict] = []
        page = 1
        while True:
            resp = self._get(
                MEDIA_URL,
                params={
                    "per_page": 100,
                    "mime_type": "application/pdf",
                    "page": page,
                    "orderby": "date",
                    "order": "desc",
                },
            )
            batch = resp.json()
            if not isinstance(batch, list) or not batch:
                break
            items.extend(batch)
            total_pages = int(resp.headers.get("X-WP-TotalPages", "1") or 1)
            if page >= total_pages:
                break
            page += 1
            time.sleep(REQUEST_DELAY)

        if not items:
            raise RuntimeError(
                f"{MEDIA_URL} returned no PDF media — the REST API was closed "
                "or pgr.gw is blocking this vantage"
            )

        out = []
        for item in items:
            title = strip_html((item.get("title") or {}).get("rendered"))
            if JUNK_TITLE_RE.match(title):
                continue
            source_url = item.get("source_url")
            if not source_url:
                continue
            out.append(
                {
                    "media_id": item.get("id"),
                    "title": normalize_title(title) or f"Documento {item.get('id')}",
                    "slug": item.get("slug"),
                    "pdf_url": source_url,
                    "page_url": item.get("link") or source_url,
                    "uploaded": (item.get("date") or "")[:10] or None,

                }
            )
        return out

    def fetch_all(self) -> Generator[dict, None, None]:
        items = self._media_items()
        logger.info("PGR media library: %s legal PDFs", len(items))
        for item in items:
            yield item

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """New uploads only — the library is small and uploaded in batches."""
        if isinstance(since, datetime):
            since_iso = since.date().isoformat()
        else:
            since_iso = str(since)[:10]
        for item in self._media_items():
            if item["uploaded"] and item["uploaded"] >= since_iso:
                yield item

    # ------------------------------------------------------------- normalize

    def normalize(self, raw: dict) -> Optional[dict]:
        pdf_url = raw.get("pdf_url")
        if not pdf_url:
            return None
        try:
            resp = self._get(pdf_url)
        except Exception as exc:  # noqa: BLE001 — logged, record skipped
            logger.warning("PDF download failed for %s: %s", raw.get("title"), exc)
            return None

        content = resp.content
        if not content.startswith(b"%PDF-"):
            logger.warning(
                "Not a PDF for %s (%s bytes, starts %r)",
                raw.get("title"), len(content), content[:16],
            )
            return None

        text = clean_pdf_text(extract_pdf_text(content))
        if len(text) < MIN_TEXT_CHARS:
            # Scans with no text layer — no OCR backend, so no full text.
            logger.warning(
                "No text layer for %s (%s chars) — scanned upload, skipped",
                raw.get("title"), len(text),
            )
            self._no_text.append(str(raw.get("title")))
            return None

        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        with self._seen_lock:
            if digest in self._seen_hashes:
                logger.info(
                    "Duplicate upload skipped: %s (identical text already ingested)",
                    raw.get("title"),
                )
                return None
            self._seen_hashes.add(digest)

        title = raw["title"]
        date, date_approx = pick_date(text, title)

        return {
            "_id": f"gw-pgr-{raw['media_id']}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title[:500],
            "text": text,
            "date": date,
            "url": pdf_url,
            "country": "GW",
            "language": "pt",
            "publisher": "Procuradoria-Geral da República da Guiné-Bissau",
            "media_id": raw["media_id"],
            "slug": raw.get("slug"),
            "uploaded_at": raw.get("uploaded"),
            "date_is_approximate": date_approx,
        }

    # ------------------------------------------------------------------ test

    def test_api(self) -> bool:
        logger.info("Testing pgr.gw WordPress REST ...")
        try:
            items = self._media_items()
            logger.info("  media OK — %s legal PDFs", len(items))
            for item in items:
                rec = self.normalize(item)
                if rec and rec.get("text"):
                    logger.info(
                        "  %s OK — %s chars, date=%s, title=%r",
                        rec["_id"], len(rec["text"]), rec["date"], rec["title"][:60],
                    )
                    logger.info("API test PASSED")
                    return True
            logger.error("  no item yielded full text")
            return False
        except Exception as exc:  # noqa: BLE001
            logger.error("API test FAILED: %s", exc)
            return False


def main():
    parser = argparse.ArgumentParser(description="GW/PGR bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api", "updates"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--count", type=int, default=15, help="Sample size")
    parser.add_argument("--since", type=str, help="ISO date for updates")
    args = parser.parse_args()

    scraper = GWPgrScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        if scraper._no_text:
            logger.info("Scanned uploads with no text layer: %s", scraper._no_text)
        return

    if args.command == "updates":
        since = args.since or datetime.now(timezone.utc).date().isoformat()
        for raw in scraper.fetch_updates(since):
            rec = scraper.normalize(raw)
            if rec:
                print(json.dumps(rec, ensure_ascii=False))
        return

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=args.count)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")
    if scraper._no_text:
        logger.info("Scanned uploads with no text layer: %s", scraper._no_text)


if __name__ == "__main__":
    main()
