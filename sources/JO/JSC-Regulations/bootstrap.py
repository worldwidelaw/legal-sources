#!/usr/bin/env python3
"""
JO/JSC-Regulations — Jordan Securities Commission (laws, regulations, instructions)

Fetches the English-language legal corpus of the Jordan Securities Commission
(JSC): the Securities Law, capital-market regulations, instructions to financial
services companies, bases, Islamic-sukuk regulating legislation, regulatory
decisions, and related legislation.

Strategy:
  1. Hit the homepage once to obtain the `lang` session cookie (the site issues
     an infinite redirect to "/" without it).
  2. For each English legislation listing page (/Links2/en/<category>), parse the
     content table. Each row is:
        <td><a href="/Uploads/Files/<file>.pdf"><img></a></td>
        <td>Description</td>
        <td>Issue Date / Year</td>
  3. Download each PDF through the cookie'd session (PDFs return HTTP 451 without
     it) and extract full text (opendataloader-pdf → pdfplumber → pypdf).
  4. Skip records that yield no usable text (scanned image-only PDFs) and
     deduplicate by PDF URL.

Usage:
  python bootstrap.py bootstrap           # Full pull
  python bootstrap.py bootstrap --sample  # Sample records for validation
  python bootstrap.py test-api            # Quick connectivity test
"""

import re
import sys
import json
import html
import logging
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin, unquote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JO.JSC-Regulations")

BASE_URL = "https://www.jsc.gov.jo"
SOURCE_ID = "JO/JSC-Regulations"

# English legislation listing path -> (label, _type)
CATEGORIES = {
    "laws": ("Laws", "legislation"),
    "Regulations": ("Regulations", "legislation"),
    "islamic_sukuk_regulating_legislations": ("Islamic Sukuk Regulating Legislations", "legislation"),
    "related_legislations": ("Related Legislations", "legislation"),
    "instructions": ("Instructions", "doctrine"),
    "bases": ("Bases", "doctrine"),
    "regulatory_decisions": ("Regulatory Decisions", "doctrine"),
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept": "text/html, application/pdf, */*",
}

_TR_RE = re.compile(r"<tr\b.*?</tr>", re.DOTALL | re.IGNORECASE)
_TD_RE = re.compile(r"<td\b[^>]*>(.*?)</td>", re.DOTALL | re.IGNORECASE)
_HREF_RE = re.compile(r"href=['\"]([^'\"]+\.pdf[^'\"]*)['\"]", re.IGNORECASE)
_TAG_RE = re.compile(r"<[^>]+>")
_DATE_RE = re.compile(r"(\d{1,2})\s*[./-]\s*(\d{1,2})\s*[./-]\s*(\d{4})")
_YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(_TAG_RE.sub(" ", s))).strip()


def _parse_date(cells_text: str) -> Optional[str]:
    m = _DATE_RE.search(cells_text)
    if m:
        a, b, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        # site uses Month.Day.Year inside parentheses sometimes, but the date
        # column is typically Day/Month/Year — guard both orders.
        for day, month in ((a, b), (b, a)):
            try:
                return datetime(year, month, day).date().isoformat()
            except ValueError:
                continue
    m = _YEAR_RE.search(cells_text)
    if m:
        return f"{m.group(1)}-01-01"
    return None


class JSCRegulationsScraper(BaseScraper):

    def __init__(self):
        super().__init__(str(Path(__file__).parent))
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._primed = False

    def _prime(self):
        """Obtain the `lang` cookie; without it the site redirect-loops and PDFs 451."""
        if not self._primed:
            try:
                self.session.get(BASE_URL + "/", timeout=40)
            except Exception as e:
                logger.warning("Priming session failed: %s", e)
            self._primed = True

    def _get(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        self._prime()
        try:
            return self.session.get(url, timeout=timeout)
        except Exception as e:
            logger.warning("GET failed %s: %s", url, e)
            return None

    def _list_items(self, category: str) -> Generator[tuple[str, str, Optional[str]], None, None]:
        """Yield (title, absolute_pdf_url, date) for a category listing page."""
        resp = self._get(f"{BASE_URL}/Links2/en/{category}")
        if resp is None or resp.status_code != 200:
            logger.warning("Category page failed (%s): %s",
                           getattr(resp, "status_code", "?"), category)
            return
        for row in _TR_RE.findall(resp.text):
            href_m = _HREF_RE.search(row)
            if not href_m:
                continue
            pdf = urljoin(BASE_URL, href_m.group(1)).replace("/jo//", "/jo/")
            cells = [_clean(c) for c in _TD_RE.findall(row)]
            # title = longest non-empty cell text (the Description column)
            title = max(cells, key=len) if cells else ""
            if not title:
                title = _clean(unquote(href_m.group(1).rsplit("/", 1)[-1]))[:200]
            date = _parse_date(" ".join(cells))
            yield title, pdf, date

    def _build_record(self, title: str, pdf_url: str, date: Optional[str],
                      label: str, doc_type: str,
                      seen_pdfs: Optional[set] = None) -> Optional[dict]:
        norm_pdf = pdf_url.split("?")[0]
        if seen_pdfs is not None:
            if norm_pdf in seen_pdfs:
                return None
            seen_pdfs.add(norm_pdf)

        doc_id = "jsc-" + hashlib.sha1(norm_pdf.encode()).hexdigest()[:16]
        table = "legislation" if doc_type == "legislation" else "doctrine"

        resp = self._get(pdf_url)
        if resp is None or resp.status_code != 200:
            logger.warning("PDF fetch failed (%s): %s",
                           getattr(resp, "status_code", "?"), title[:60])
            return None

        text = extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=doc_id,
            pdf_bytes=resp.content,
            table=table,
        )
        if not text or len(text) < 200:
            logger.warning("No usable text (scanned?): %s", title[:60])
            return None

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": pdf_url,
            "document_type": label.lower(),
            "category": label,
            "language": "en",
        }

    # ── BaseScraper interface ────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        seen_pdfs: set = set()
        for category, (label, doc_type) in CATEGORIES.items():
            logger.info("Fetching category: %s", label)
            for title, pdf_url, date in self._list_items(category):
                record = self._build_record(title, pdf_url, date, label, doc_type, seen_pdfs)
                if record:
                    yield record
                time.sleep(1)

    def fetch_updates(self, since=None) -> Generator[dict, None, None]:
        # Listing pages carry no reliable incremental filter; re-scan all
        # categories. Idempotency is handled downstream via dedup by PDF URL.
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        return raw


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="JO/JSC-Regulations scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = JSCRegulationsScraper()

    if args.command == "test-api":
        for category, (label, _t) in CATEGORIES.items():
            items = list(scraper._list_items(category))
            logger.info("%s: %d items", label, len(items))
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command in ("bootstrap", "update"):
        limit = 15 if args.sample else None
        count = 0
        for record in scraper.fetch_all():
            count += 1
            if args.sample or count <= 15:
                out_path = sample_dir / f"{count:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(
                "[%d] %s — %d chars (%s)",
                count,
                record["title"][:55],
                len(record.get("text", "")),
                record.get("category"),
            )
            if limit and count >= limit:
                break

        logger.info("Done: %d records fetched", count)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
