#!/usr/bin/env python3
"""
US/NavajoCourts -- Navajo Nation Supreme Court — Opinions

Fetches the full text of published slip opinions of the Supreme Court of the
Navajo Nation — the highest court of the largest tribal judiciary in the United
States, with jurisdiction over the Navajo Nation reservation spanning parts of
Arizona, New Mexico and Utah. Each opinion disposes of a docketed appeal
(SC-CV-* civil, SC-CR-* criminal, SC-SP-* special proceeding, SC-NB-* bar
admission) = case_law.

Strategy (static FrontPage index + born-digital PDFs):

  The Judicial Branch of the Navajo Nation publishes its Supreme Court opinions
  as a single static HTML index at ``/suctopinions.htm``. The page is a table
  grouped by "{YEAR} OPINIONS" headings; each opinion is a 3-cell row:

      | (item no.) | <a href="NNSC{year}/{file}.pdf">SC-CV-09-17</a>
                   | <u>Case caption</u>. <i>Opinion</i>. (February 1, 2021)

  so the docket number, case caption and decision date are all carried by the
  index itself — no per-case detail page exists. Two standing court orders
  (uniform citation system; law-student practice rule) are linked separately
  from a bullet list under ``SupCtMisc/``.

  Note the anchor TEXT holds the docket number while the FILE name varies by
  era (``SC-CV-18-17.pdf`` in recent years, ``01Arviso-v-Muskett.pdf`` in
  2017/2018), so the docket must be read from the link text, not the URL.

  Full text is retrieved by downloading each opinion PDF and extracting text
  with PyMuPDF/fitz — every opinion on the index is born-digital and carries a
  clean text layer.

  Coverage: 2013–2021 plus two undated standing orders (1995, 2004). Directory
  listing is disabled (HTTP 403) and no ``NNSC2022+`` directory exists, so the
  index is the complete online corpus; pre-2013 opinions were published only in
  the print Navajo Reporter. The court's own year-by-year opinion *summaries*
  (``NNSCSummaries/*.html``) are deliberately NOT captured — they carry an
  explicit disclaimer that they are educational paraphrases which "may not be
  relied on or otherwise cited in legal proceedings".

  The official host is ``courts.navajo-nsn.gov``. The formerly used
  ``navajocourts.org`` domain has lapsed and now redirects to an unrelated
  squatted domain — do not use it.

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import html as htmllib
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import quote, urljoin

import requests

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from common.base_scraper import BaseScraper  # noqa: E402
from common.pdf_extract import extract_pdf_markdown  # noqa: E402

logger = logging.getLogger("legal-data-hunter")

BASE = "https://courts.navajo-nsn.gov"
INDEX_URL = f"{BASE}/suctopinions.htm"

# Minimum extracted body length to count a PDF as text-bearing.
MIN_BODY_CHARS = 500

# "SC-CV-09-17", "SC-SP-01-20", "SC-NB-05-18", "SC-CR-01-14" ...
DOCKET_RE = re.compile(r"\bSC-[A-Z]{2}-\d{2}-\d{2}\b", re.I)

MONTHS = (
    "January|February|March|April|May|June|July|August|September|October|"
    "November|December"
)
# The decision date is the first date inside the trailing parenthetical:
# "(February 1, 2021)", "(December 20. 2012)" — the index has stray periods —
# and compound forms such as "(October 8, 2014, nunc pro tunc as of September
# 26, 2014)" or "(January 4, 2013 recon. den. February 22, 2013)", so the
# closing paren is deliberately not part of the match.
DATE_RE = re.compile(rf"\(\s*({MONTHS})\s+(\d{{1,2}})\s*[,.]\s*(\d{{4}})\b", re.I)
YEAR_ONLY_RE = re.compile(r"\((\d{4})\)")


def _clean(fragment: str) -> str:
    """Strip tags/entities from an HTML fragment and collapse whitespace."""
    text = htmllib.unescape(re.sub(r"<[^>]+>", " ", fragment))
    return re.sub(r"\s+", " ", text).replace(" ", " ").strip()


class NavajoCourtsScraper(BaseScraper):

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
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            }
        )
        self.delay = 1.0

    # ---- low-level ----------------------------------------------------------

    def _get(self, url: str, binary: bool = False, retries: int = 3):
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                r = self.session.get(url, timeout=90)
                if r.status_code == 200:
                    if binary:
                        return r.content
                    # FrontPage pages declare windows-1252.
                    return r.content.decode("windows-1252", "replace")
                if r.status_code in (403, 404):
                    logger.warning(f"HTTP {r.status_code} for {url}")
                    return None
                logger.warning(f"HTTP {r.status_code} for {url}")
            except Exception as e:
                logger.warning(f"GET error {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- index parsing ------------------------------------------------------

    @staticmethod
    def _abs(href: str) -> str:
        """Absolute, percent-encoded PDF URL (some filenames contain ', ')."""
        return urljoin(BASE + "/", quote(href, safe="/:%"))

    @staticmethod
    def _parse_date(text: str) -> tuple[Optional[str], Optional[int]]:
        """Return (ISO date, year) parsed from an index description cell."""
        m = DATE_RE.search(text)
        if m:
            try:
                dt = datetime.strptime(
                    f"{m.group(1).title()} {m.group(2)} {m.group(3)}", "%B %d %Y"
                )
                return dt.date().isoformat(), dt.year
            except Exception:
                pass
        m = YEAR_ONLY_RE.search(text)
        if m:
            return None, int(m.group(1))
        return None, None

    @staticmethod
    def _caption(cell_html: str, desc: str) -> Optional[str]:
        """Case caption for a row.

        The index consistently underlines the caption, so the <u> element is
        the reliable anchor — the rest of the cell is a free-text holding
        summary that in some years runs to several hundred words. Fall back to
        the text before the "Opinion." marker, then to the whole cell.
        """
        underlined = re.findall(r"(?is)<u[^>]*>(.*?)</u>", cell_html)
        for frag in underlined:
            text = _clean(frag).strip(" .,")
            if len(text) > 3:
                return text[:400]
        head = re.split(r"(?i)\.\s*Opinion\b", desc)[0]
        head = re.sub(r"\s*\(.*?\)\s*$", "", head).strip(" .,")
        return (head or desc)[:400] or None

    def _parse_index(self, page: str) -> list[dict]:
        """Return one entry per opinion PDF linked from the index page."""
        entries: list[dict] = []
        seen: set[str] = set()

        # --- opinion table rows: | no. | docket link | caption + date | -------
        year = None
        for chunk in re.split(r"(?i)<tr[^>]*>", page)[1:]:
            row = re.split(r"(?i)</tr>", chunk)[0]
            heading = re.search(r"(\d{4})\s+OPINIONS", _clean(row), re.I)
            if heading:
                year = int(heading.group(1))
            cells = re.findall(r"(?is)<td[^>]*>(.*?)</td>", row)
            if len(cells) < 2:
                continue
            link = None
            docket_text = ""
            for cell in cells:
                m = re.search(r'(?is)href="([^"]+\.pdf)"[^>]*>(.*?)</a>', cell)
                if m:
                    link, docket_text = m.group(1), _clean(m.group(2))
                    break
            if not link:
                continue
            desc = _clean(cells[-1])
            iso, parsed_year = self._parse_date(desc)
            docket = DOCKET_RE.search(docket_text) or DOCKET_RE.search(desc)
            entry = {
                "href": link,
                "docket": docket.group(0).upper() if docket else None,
                "caption": self._caption(cells[-1], desc),
                "date": iso,
                "year": parsed_year or year,
            }
            key = self._entry_key(entry)
            if key in seen:
                continue
            seen.add(key)
            entries.append(entry)

        # --- standing orders in the bullet list above the table ---------------
        # One bullet's link text belongs to a different order than its href
        # (a long-standing typo on the page), so the docket is read from the
        # file name and the link text is only trusted as a caption when the two
        # agree.
        for m in re.finditer(r'(?is)href="(SupCtMisc/[^"]+\.pdf)"[^>]*>(.*?)</a>', page):
            href, label = m.group(1), _clean(m.group(2))
            iso, year = self._parse_date(label)
            href_docket = DOCKET_RE.search(href)
            label_docket = DOCKET_RE.search(label)
            docket = (href_docket or label_docket)
            caption = None
            if not label_docket or (
                href_docket and label_docket.group(0).upper() == href_docket.group(0).upper()
            ):
                caption = re.sub(
                    r"\s*,?\s*No\.\s*SC-[A-Z]{2}-\d{2}-\d{2}\s*", " ", label, flags=re.I
                )
                caption = re.sub(r"\s*\(\d{4}\)\s*$", "", caption).strip(" .,") or None
            entry = {
                "href": href,
                "docket": docket.group(0).upper() if docket else None,
                "caption": caption,
                "date": iso,
                "year": year,
            }
            key = self._entry_key(entry)
            if key in seen:
                continue
            seen.add(key)
            entries.append(entry)

        return entries

    @staticmethod
    def _entry_key(entry: dict) -> str:
        """Same docket in the same year-directory == the same opinion.

        The index links a few opinions twice (e.g. NNSC2020/SC-CV-13-15.pdf and
        "NNSC2020/SC-CV-13-15, Opinion.pdf"), so key on directory + docket and
        fall back to the raw path when no docket number could be read.
        """
        folder = entry["href"].split("/")[0]
        return f"{folder}/{entry['docket']}" if entry["docket"] else entry["href"]

    # ---- normalize ----------------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        pdf_url = self._abs(raw["href"])
        pdf = self._get(pdf_url, binary=True)
        if not pdf or pdf[:4] != b"%PDF":
            logger.warning(f"not a PDF: {pdf_url}")
            return None

        pages = None
        if fitz is not None:
            try:
                doc = fitz.open(stream=pdf, filetype="pdf")
                pages = doc.page_count
                doc.close()
            except Exception:
                pass

        folder = raw["href"].split("/")[0]
        docket_slug = raw["docket"] or re.sub(
            r"[^A-Za-z0-9]+", "-", raw["href"].rsplit("/", 1)[-1][:-4]
        )
        doc_id = f"NavajoCourts-{folder}-{docket_slug}"

        # Shared cascade: opendataloader → pdfplumber → pypdf → PyMuPDF → OCR.
        # Roughly 40% of the index (all of 2021, most of 2018) is scanned with
        # no text layer, so the OCR tail is what captures those years — it only
        # runs on hosts where tesseract is installed.
        full = extract_pdf_markdown(
            "US/NavajoCourts", doc_id, pdf_bytes=pdf, table="case_law"
        )
        if not full:
            logger.warning(f"no extractable text (scanned, no OCR available): {pdf_url}")
            return None

        body = re.sub(r"[ \t]+", " ", full)
        body = re.sub(r"\n{3,}", "\n\n", body).strip()
        if len(body) < MIN_BODY_CHARS:
            logger.warning(f"insufficient text ({len(body)} chars) in {pdf_url}")
            return None

        docket = raw["docket"]
        caption = raw.get("caption") or docket or docket_slug
        title = f"{caption} ({docket})" if docket and docket not in caption else caption

        return {
            "_id": doc_id,
            "_source": "US/NavajoCourts",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title[:500],
            "text": body,
            "date": raw.get("date"),
            "year": raw.get("year"),
            "url": pdf_url,
            "index_url": INDEX_URL,
            "case_number": docket,
            "case_name": raw.get("caption"),
            "pages": pages,
            "language": "en",
            "court": "Navajo Nation Supreme Court",
            "jurisdiction": "US-NAVAJO",
        }

    # ---- iteration ----------------------------------------------------------

    def _entries(self) -> list[dict]:
        page = self._get(INDEX_URL)
        if not page:
            raise RuntimeError(f"opinion index unreachable: {INDEX_URL}")
        entries = self._parse_index(page)
        if not entries:
            raise RuntimeError(
                f"opinion index parsed to 0 entries — layout changed? {INDEX_URL}"
            )
        logger.info(f"index: {len(entries)} distinct opinions")
        # Newest first so sample mode fills from the most recent term.
        return sorted(entries, key=lambda e: (e.get("year") or 0), reverse=True)

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._entries()

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # The index is a single static page rebuilt whenever an opinion is
        # released; re-walk it and let the loader dedup on _id.
        yield from self._entries()

    # ---- connectivity test --------------------------------------------------

    def test_api(self) -> bool:
        try:
            entries = self._entries()
            logger.info(f"test-api: {len(entries)} opinions on the index")
            # The newest years are scanned image-only, so walk until a
            # text-bearing opinion is reached rather than judging on entry 0.
            for entry in entries[:15]:
                rec = self.normalize(entry)
                if rec:
                    logger.info(
                        f"test-api normalize: {rec['_id']} — {len(rec['text'])} chars, "
                        f"date={rec['date']}"
                    )
                    return True
            logger.error("test-api: no extractable text in the first 15 opinions")
            return False
        except Exception as e:
            logger.error(f"test-api FAILED: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/NavajoCourts bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test-api"], help="Command"
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    scraper = NavajoCourtsScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        return

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
