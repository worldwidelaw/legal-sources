#!/usr/bin/env python3
"""
US/CherokeeNationCourts -- Cherokee Nation Supreme Court / Judicial Appeals
Tribunal opinions and orders.

Fetches the full text of every decision the Judicial Branch of the Cherokee
Nation publishes on cherokeecourts.org: opinions, judgments, orders, minute
orders and dismissals of the Supreme Court of the Cherokee Nation (2003-),
of its predecessor the Judicial Appeals Tribunal (JAT, 1975-2003), and the
Court's own administrative directives (the SC-AD series) plus the Supreme
Court and District Court Rules.

Strategy (static DNN index pages -> scanned PDFs -> OCR):

  The Court publishes one hub page,

      /Supreme-Court/Supreme-Court-Case-Opinions-and-Information

  which links seven era archives -- 1975-1995, 1996-1997, 1998-2001,
  2002-2006, 2007-2012, 2013-2016 and SC-2017-01-to-current -- and each era
  archive is a plain ``<li><a href="...pdf">docket + caption</a></li>`` list.
  Walking the hub, the era pages and the Supreme Court landing page yields
  the whole published corpus (~350 documents back to 1975) with no search
  form, no pagination and no session state.

  ONE DOCUMENT IS ONE RECORD. Unlike a code, a docket's papers are separate
  filings that are cited separately ("SC-17-07 37-Final Order 2-22-21"), so
  they are not merged; several documents share a docket number and carry the
  same ``case_number``.

  EVERY PDF ON THIS SITE IS A SCAN. Checked across all eras (1995, 2007,
  2019, 2025) -- each page carries exactly one full-page image and a zero
  character text layer -- so there is no born-digital path and the text
  comes from OCR via the shared ``common.pdf_extract`` cascade
  (opendataloader -> pdfplumber -> pypdf -> tesseract). The first three
  return nothing here; tesseract is what produces the body, at roughly ten
  seconds a document. THE SOURCE THEREFORE REQUIRES TESSERACT: without it
  every record is empty and the run fails loud rather than writing stubs.

  Metadata comes from three places that agree with each other:
    * the anchor text on the index page -- "JAT-96-02 James Stockton v.
      Cherokee Nation" -- which is the case caption;
    * the file name -- "SC-19-03 13-Opinion 3-14-19.pdf" -- which carries the
      docket, the filing sequence number, the document type and the date;
    * the OCR'd body, used to recover a date when the file name omits one
      (a handful of recent files are named "SC-24-03 13 - Opinion.pdf").

COVERAGE LIMITS (deliberate, see README):
  * DISTRICT COURT decisions are not published here. Cherokee Nation District
    Court records live on a Tyler Technologies Odyssey portal
    (portal-okcherokeenation.tylertech.cloud) which is a docket-search
    application, not a published-opinion corpus; it is out of scope.
  * ODCR (odcr.com) is a third-party for-profit aggregator whose terms forbid
    automated access. Deliberately not a source (see issue #1499).
  * The individual ``/Supreme-Court/SC-YYYY-NN-Party-v-Party`` pages are
    pending-case dockets carrying party filings (petitions in error,
    designations of record), not decisions of the Court; only decision PDFs
    are collected.

Usage:
  python bootstrap.py bootstrap            # Full pull (all decisions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import html as htmllib
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import quote, unquote, urljoin, urlparse

import requests

try:
    import fitz  # PyMuPDF — page count only; these PDFs have no text layer.
except Exception:  # pragma: no cover
    fitz = None

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from common.base_scraper import BaseScraper  # noqa: E402
from common.pdf_extract import extract_pdf_markdown  # noqa: E402

logger = logging.getLogger("legal-data-hunter")

BASE = "https://www.cherokeecourts.org"
INDEX_URL = f"{BASE}/Supreme-Court/Supreme-Court-Case-Opinions-and-Information"
LANDING_URL = f"{BASE}/Supreme-Court"

# OCR output for a one-page minute order runs ~600 chars; anything shorter is
# a failed scan rather than a short order.
MIN_BODY_CHARS = 300
# A corpus this size never legitimately collapses; below it the index layout
# changed or the host is refusing us, and the run must fail loud (#1402).
MIN_EXPECTED_DOCS = 100

# "SC-19-03", "JAT-96-02", "SC-2025-08", "SC-AD-22-04", "JAT-95-07C08".
DOCKET_RE = re.compile(
    r"^(JAT|SC)(-AD)?[-\s]*(\d{2,4})[-\s]*(\d{1,3}[A-Z]?\d{0,2})", re.I
)
# The trailing date a file name prints: "... 3-14-19.pdf", "... 9-23-1998.pdf".
FILENAME_DATE_RE = re.compile(r"(\d{1,2})-(\d{1,2})-(\d{2}|\d{4})\s*$")
# A minority spell the month instead: "JAT-03-05 Opinion Mar12-03.pdf".
FILENAME_MONTH_RE = re.compile(
    r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s*"
    r"(\d{1,2})\s*[-–]\s*(\d{2}|\d{4})\s*$",
    re.I,
)
# DNN appends its own revision stamp: "?ver=2019-03-14-095952-593".
VER_RE = re.compile(r"[?&]ver=(\d{4})-(\d{2})-(\d{2})")
# A file-stamp the Court prints on page 1: "FILED  JUL 24 1995" / "MAY 8, 2023".
BODY_DATE_RE = re.compile(
    r"\b(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)[A-Z]*\.?\s+"
    r"(\d{1,2}),?\s+((?:19|20)\d{2})\b",
    re.I,
)
# The document type sits between the filing sequence number and the date:
# "SC-19-03 13-Opinion 3-14-19"  ->  "Opinion".
DOC_TYPES = (
    "final order", "minute order", "amended opinion", "amended order",
    "opinion", "judgment", "dismissal", "order", "notice", "mandate",
    "decree", "certification", "remand", "rules", "rule",
)


def _clean(fragment: str) -> str:
    """Strip tags/entities from an HTML fragment and collapse whitespace."""
    text = htmllib.unescape(re.sub(r"<[^>]+>", " ", fragment))
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def _slug(value: str) -> str:
    return re.sub(r"-{2,}", "-", re.sub(r"[^a-z0-9]+", "-", value.lower())).strip("-")


def _tidy(text: str) -> str:
    """Collapse the ragged whitespace and stray page numbers OCR leaves."""
    lines = [ln.rstrip() for ln in text.splitlines()
             if not re.fullmatch(r"\s*\d{1,3}\s*", ln.strip())]
    body = re.sub(r"[ \t]+", " ", "\n".join(lines))
    return re.sub(r"\n{3,}", "\n\n", body).strip()


def _iso(month: int, day: int, year: int) -> Optional[str]:
    if year < 100:
        year += 2000 if year <= 40 else 1900
    try:
        return datetime(year, month, day).date().isoformat()
    except ValueError:
        return None


def _encode(path: str) -> str:
    """Re-encode an index href: the file names carry spaces and commas."""
    parts = urlparse(path)
    return urljoin(BASE, quote(unquote(parts.path)) + (f"?{parts.query}" if parts.query else ""))


class CherokeeNationCourtsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/126.0.0.0 Safari/537.36"
                )
            }
        )
        self.delay = 1.0
        # Sample mode walks documents spread across the whole 1975-2026 span
        # instead of the first N, so the samples cover both courts.
        self.spread_docs = 0

    # ---- low-level ----------------------------------------------------------

    def _get(self, url: str, binary: bool = False, retries: int = 3):
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                r = self.session.get(url, timeout=180)
                if r.status_code == 200:
                    return r.content if binary else r.text
                if r.status_code == 404:
                    logger.warning(f"HTTP 404 for {url}")
                    return None
                logger.warning(f"HTTP {r.status_code} for {url}")
            except Exception as e:
                logger.warning(f"GET error {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- index parsing ------------------------------------------------------

    def _index_pages(self) -> list[str]:
        """The hub, the landing page and every era archive linked from the hub."""
        hub = self._get(INDEX_URL)
        if not hub:
            raise RuntimeError(
                f"opinions hub unreachable: {INDEX_URL} — host refusing this vantage?"
            )
        eras = sorted(
            {urljoin(BASE, htmllib.unescape(h).split("?")[0])
             for h in re.findall(r'href="(/Supreme-Court/[^"?#]+)"', hub)
             # Era archives are named by year span or by leading docket; the
             # form/info pages and the per-case dockets are not decisions.
             if re.search(r"/(?:\d{4}-\d{4}|SC-\d{4}-\d{2}-to-current)$", h)}
        )
        if not eras:
            raise RuntimeError(
                f"opinions hub parsed to 0 era archives — layout changed? {INDEX_URL}"
            )
        logger.info(f"index: {len(eras)} era archives linked from the hub")
        return [INDEX_URL, LANDING_URL] + eras

    def _parse_documents(self, page: str, page_url: str) -> list[dict]:
        """Every decision PDF an index page links, with its anchor caption."""
        out: list[dict] = []
        for m in re.finditer(
            r'(?is)<a\s[^>]*href="([^"]*\.pdf[^"]*)"[^>]*>(.*?)</a>', page
        ):
            href = htmllib.unescape(m.group(1))
            caption = _clean(m.group(2))
            name = unquote(href.split("?")[0].rsplit("/", 1)[-1])
            stem = re.sub(r"\.pdf$", "", name, flags=re.I)
            docket = DOCKET_RE.match(stem) or DOCKET_RE.match(caption)
            if not docket:
                # The print order form for the annotated code and other
                # non-decision attachments carry no docket number.
                continue
            out.append(
                {
                    "url": _encode(href),
                    "file_name": name,
                    "caption": caption,
                    "index_url": page_url,
                    **self._parse_name(stem, caption, href),
                }
            )
        return out

    @staticmethod
    def _parse_name(stem: str, caption: str, href: str) -> dict:
        """Docket, sequence, document type and date, from the file name."""
        m = DOCKET_RE.match(stem) or DOCKET_RE.match(caption)
        series, admin, year, number = m.group(1).upper(), bool(m.group(2)), m.group(3), m.group(4)
        year_full = int(year) if len(year) == 4 else (
            2000 + int(year) if int(year) <= 40 else 1900 + int(year)
        )
        case_number = (
            f"{series}{'-AD' if admin else ''}-{year_full if len(year) == 4 else year}-{number}"
        )

        tail = stem[m.end():]
        seq = re.match(r"\s*(\d{1,3})\b", tail)
        rest = tail[seq.end():] if seq else tail

        date = None
        cut = None
        dm = FILENAME_DATE_RE.search(stem)
        if dm:
            date = _iso(int(dm.group(1)), int(dm.group(2)), int(dm.group(3)))
            cut = dm.start()
        else:
            mm = FILENAME_MONTH_RE.search(stem)
            if mm:
                try:
                    month = datetime.strptime(mm.group(1)[:3].title(), "%b").month
                except ValueError:
                    month = None
                if month:
                    date = _iso(month, int(mm.group(2)), int(mm.group(3)))
                    cut = mm.start()
        if cut is not None:
            rest = rest[: cut - (m.end() + (seq.end() if seq else 0))]

        # DNN's own revision stamp is when the scan was uploaded, not when the
        # Court signed the paper, so it is only a last resort (see fetch_all).
        vm = VER_RE.search(href)
        ver_date = _iso(int(vm.group(2)), int(vm.group(3)), int(vm.group(1))) if vm else None

        label = re.sub(r"[\s_-]+", " ", rest).strip(" -_.")
        doc_type = next(
            (t for t in DOC_TYPES if re.search(rf"\b{re.escape(t)}\b", label, re.I)),
            None,
        )
        if doc_type is None:
            doc_type = "administrative order" if admin else "decision"

        return {
            "case_number": case_number,
            "series": series,
            "docket_year": year_full,
            "sequence": int(seq.group(1)) if seq else None,
            "document_type": doc_type.title(),
            "document_label": label or None,
            "administrative": admin,
            "filename_date": date,
            "ver_date": ver_date,
        }

    def _entries(self) -> list[dict]:
        docs: dict[str, dict] = {}
        for page_url in self._index_pages():
            page = self._get(page_url)
            if not page:
                logger.warning(f"index page unreachable: {page_url}")
                continue
            found = self._parse_documents(page, page_url)
            logger.info(f"{page_url.rsplit('/', 1)[-1]}: {len(found)} decision PDFs")
            for doc in found:
                # A document can be linked from two eras (a docket that spans
                # them); the first listing wins so the era attribution is the
                # docket's own.
                docs.setdefault(doc["url"], doc)

        entries = sorted(
            docs.values(),
            key=lambda d: (d["docket_year"], d["case_number"], d["sequence"] or 0),
        )
        if len(entries) < MIN_EXPECTED_DOCS:
            raise RuntimeError(
                f"only {len(entries)} decision PDFs found across all index pages "
                f"(expected >= {MIN_EXPECTED_DOCS}) — layout change or IP block"
            )
        logger.info(
            f"index: {len(entries)} decision documents, "
            f"{len({e['case_number'] for e in entries})} dockets, "
            f"{min(e['docket_year'] for e in entries)}-{max(e['docket_year'] for e in entries)}"
        )
        if self.spread_docs and len(entries) > self.spread_docs:
            last = len(entries) - 1
            picks = sorted({round(i * last / max(1, self.spread_docs - 1))
                            for i in range(self.spread_docs)})
            entries = [entries[i] for i in picks]
        return entries

    # ---- PDF text -----------------------------------------------------------

    def _pdf_text(self, pdf: bytes, doc_id: str) -> Optional[str]:
        """OCR the scan. Every PDF on this site is image-only (see docstring)."""
        text = extract_pdf_markdown(
            "US/CherokeeNationCourts", doc_id, pdf_bytes=pdf, table="case_law"
        )
        if text and len(text.strip()) >= MIN_BODY_CHARS:
            return _tidy(text)
        return None

    @staticmethod
    def _page_count(pdf: bytes) -> Optional[int]:
        if fitz is None:
            return None
        try:
            doc = fitz.open(stream=pdf, filetype="pdf")
            pages = doc.page_count
            doc.close()
            return pages
        except Exception:
            return None

    @staticmethod
    def _body_date(text: str) -> Optional[str]:
        """The Court's own file stamp, for files whose name omits a date."""
        m = BODY_DATE_RE.search(text[:3000])
        if not m:
            return None
        try:
            month = datetime.strptime(m.group(1)[:3].title(), "%b").month
        except ValueError:
            return None
        return _iso(month, int(m.group(2)), int(m.group(3)))

    # ---- normalize ----------------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        text = raw.get("text") or ""
        if len(text) < MIN_BODY_CHARS:
            return None

        court = (
            "Judicial Appeals Tribunal of the Cherokee Nation"
            if raw["series"] == "JAT"
            else "Supreme Court of the Cherokee Nation"
        )
        caption = raw.get("caption") or ""
        # The anchor text repeats the docket; the caption is what follows it.
        party = re.sub(r"^\s*(JAT|SC)(-AD)?[-\s]*[\dA-Z]+[-\s]*[\dA-Z]*\s*[-–]?\s*", "",
                       caption, flags=re.I).strip(" -–,")
        title = " — ".join(
            p for p in (raw["case_number"], party or None, raw["document_type"]) if p
        )

        return {
            "_id": f"CNSC-{_slug(raw['file_name'])}",
            "_source": "US/CherokeeNationCourts",
            "_type": "doctrine" if raw["administrative"] else "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title[:500],
            "text": text,
            "date": raw.get("date"),
            "url": raw["url"],
            "index_url": raw["index_url"],
            "court": court,
            "case_number": raw["case_number"],
            "case_name": party or None,
            "document_type": raw["document_type"],
            "document_label": raw.get("document_label"),
            "filing_sequence": raw.get("sequence"),
            "docket_year": raw["docket_year"],
            "file_name": raw["file_name"],
            "pages": raw.get("pages"),
            "extraction": "ocr",
            "language": "en",
            "jurisdiction": "US-CHEROKEE-NATION",
            "publisher": "Judicial Branch of the Cherokee Nation",
        }

    # ---- iteration ----------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        entries = self._entries()
        emitted = failed = 0
        for entry in entries:
            pdf = self._get(entry["url"], binary=True)
            if not pdf or pdf[:4] != b"%PDF":
                logger.warning(f"not a PDF: {entry['url']}")
                failed += 1
                continue
            doc_id = f"CNSC-{_slug(entry['file_name'])}"
            text = self._pdf_text(pdf, doc_id)
            if not text:
                # Every PDF here is a scan, so this means OCR is unavailable
                # or the scan is unreadable.
                logger.warning(f"no extractable text: {entry['file_name']}")
                failed += 1
                continue
            emitted += 1
            yield {
                **entry,
                "text": text,
                "pages": self._page_count(pdf),
                # The Court's own file stamp beats DNN's upload stamp; the
                # file name beats both because it is what the clerk typed.
                "date": (entry["filename_date"]
                         or self._body_date(text)
                         or entry["ver_date"]),
            }

        if emitted == 0:
            raise RuntimeError(
                f"0 of {len(entries)} documents yielded text — every PDF on this "
                "site is a scan, so this almost always means tesseract/OCR is "
                "unavailable on this host (see README)"
            )
        logger.info(f"fetch_all: {emitted} documents with text, {failed} skipped")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # New decisions are appended to the current-era archive and the file
        # names are stable, so a re-walk is the refresh; the loader dedups _id.
        yield from self.fetch_all()

    # ---- connectivity test --------------------------------------------------

    def test_api(self) -> bool:
        self.spread_docs = 3
        try:
            for raw in self.fetch_all():
                rec = self.normalize(raw)
                if rec:
                    logger.info(
                        f"test-api normalize: {rec['_id']} — {len(rec['text'])} chars, "
                        f"court={rec['court']}, date={rec['date']}"
                    )
                    return True
            logger.error("test-api: no document with extractable text")
            return False
        except Exception as e:
            logger.error(f"test-api FAILED: {e}")
            return False
        finally:
            self.spread_docs = 0


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/CherokeeNationCourts bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test-api"], help="Command"
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    scraper = CherokeeNationCourtsScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        return

    if args.sample:
        scraper.spread_docs = 16  # spares in case a scan is unreadable
    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
