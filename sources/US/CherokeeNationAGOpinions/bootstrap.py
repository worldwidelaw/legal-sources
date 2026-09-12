#!/usr/bin/env python3
"""
US/CherokeeNationAGOpinions -- Cherokee Nation Attorney General Opinions

Fetches the full text of the formal opinions of the Attorney General of the
Cherokee Nation, the largest federally recognized tribe in the United States.
An AG opinion answers a question of Cherokee Nation law put by the Principal
Chief, a Council member or a tribal official ("Does the Principal Chief violate
the Cherokee Nation Freedom of Information Act if he holds a Legislative
Conference without an agenda posted?"). It construes the Constitution and the
Cherokee Nation Code but does not decide a case and does not bind a court, so
these are collected as DOCTRINE, not case law. Cherokee court decisions are
US/CherokeeNationCourts; the code itself is US/CherokeeNationCode.

Strategy (static listing + PDFs, with OCR for the scanned back-catalogue):

  The Office of the Attorney General publishes every opinion from one page,
  attorneygeneral.cherokee.org/opinions/, as an Umbraco "document listing":

      <li>
        <a href="/media/qwxnm15u/2025-cnag-02.pdf">2025-CNAG-02</a>
        <div class="document-listing-metadata">248.1 KB -- Created:6/13/2025 | Updated:6/13/2025</div>
        <div>An opinion addressing candidate withdrawal.</div>
      </li>

  The listing paginates at 7 per page, but ``pageSize`` is honoured, so one
  request with ``?term=&page=1&pageSize=200`` returns the whole series in a
  single page. The page count is read back from the pager and asserted, so a
  future cap on pageSize surfaces as an error instead of a silent truncation.

  The Umbraco media hash in each path rotates whenever a file is re-uploaded,
  so the links are read off the listing every run rather than hardcoded.

TEXT: only the most recent handful of opinions are born-digital; 53 of the 59
  published as of 2026-08 are scans of the signed original with NO text layer.
  PyMuPDF is tried first and the shared extraction cascade (which ends in
  tesseract OCR) picks up the rest. The scans are clean typescript on white,
  so OCR reads them well, but an environment with no tesseract will produce
  only the ~6 born-digital opinions — the run therefore fails loudly rather
  than reporting a thin corpus as complete.

DATES: each opinion prints its own "Date Decided: July 21, 2006" under the
  caption; that is the date used. The listing's "Created:" date is only when
  the OAG uploaded the PDF (all of the pre-2012 scans were uploaded at once)
  and is kept separately as ``published_date``.

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
from urllib.parse import urljoin

import requests

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from common.base_scraper import BaseScraper  # noqa: E402
from common.pdf_extract import extract_pdf_markdown  # noqa: E402

logger = logging.getLogger("legal-data-hunter")

BASE = "https://attorneygeneral.cherokee.org"
INDEX_URL = f"{BASE}/opinions/"
# The listing honours pageSize; 200 covers the whole series (59 as of 2026-08)
# with room to grow, and the pager is checked afterwards to prove it did.
LISTING_URL = f"{INDEX_URL}?term=&page=1&pageSize=200"

# A PDF below this many characters has no usable text layer and needs OCR.
MIN_TEXT_LAYER_CHARS = 400
# Below this an opinion is a cover sheet, not an opinion.
MIN_BODY_CHARS = 300
# The OAG had published 59 opinions (2006-2025) when this scraper was written;
# a run that finds far fewer means the listing changed shape, not that the
# series shrank.
EXPECTED_MIN_OPINIONS = 40

# <li> ... <a href="/media/../x.pdf">2025-CNAG-02</a> ... metadata ... blurb ... </li>
ITEM_RE = re.compile(
    r'(?is)<li>\s*<a\s+href="(?P<url>/media/[^"]+\.pdf)"[^>]*>(?P<label>.*?)</a>'
    r'(?P<rest>.*?)</li>'
)
CREATED_RE = re.compile(r"Created\s*:\s*(\d{1,2})/(\d{1,2})/(\d{4})")
UPDATED_RE = re.compile(r"Updated\s*:\s*(\d{1,2})/(\d{1,2})/(\d{4})")
SIZE_RE = re.compile(r"([\d.]+\s*(?:KB|MB|GB))", re.I)
# "?term=&page=5&pageSize=7" links in the pager.
PAGER_RE = re.compile(r"[?&]page=(\d+)&pageSize=(\d+)")
# "2025-CNAG-02" / "2006-CNAG-1" — the citation the OAG numbers opinions with.
CITATION_RE = re.compile(r"\b(19|20)(\d{2})\s*-\s*CNAG\s*-\s*(\d{1,2})\b", re.I)
# "Opinion Number: 2006-CNAG-1" printed inside the document.
OPINION_NO_RE = re.compile(r"Opinion\s+Number\s*:?\s*([^\n]{0,40})", re.I)
# "Date Decided: July 21, 2006" — the operative date.
DATE_DECIDED_RE = re.compile(
    r"Date\s+(?:Decided|Issued)\s*:?\s*([A-Z][a-z]+)\s+(\d{1,2}),?\s+((?:19|20)\d{2})",
    re.I,
)
# "Question Submitted by: The Honorable David Thornton, District 3"
SUBMITTED_BY_RE = re.compile(r"Question\s+Submitted\s+by\s*:?\s*([^\n]{0,120})", re.I)


def _clean(fragment: str) -> str:
    """Strip tags/entities from an HTML fragment and collapse whitespace."""
    text = htmllib.unescape(re.sub(r"<[^>]+>", " ", fragment))
    return re.sub(r"\s+", " ", text).replace("\xa0", " ").strip()


def _slug(value: str) -> str:
    return re.sub(r"-{2,}", "-", re.sub(r"[^a-z0-9]+", "-", value.lower())).strip("-")


def _tidy(text: str) -> str:
    """Collapse ragged PDF/OCR whitespace and drop bare page numbers."""
    lines = [
        ln.rstrip()
        for ln in text.replace("\xa0", " ").splitlines()
        if not re.fullmatch(r"\s*\d{1,3}\s*", ln)
    ]
    body = re.sub(r"[ \t]+", " ", "\n".join(lines))
    return re.sub(r"\n{3,}", "\n\n", body).strip()


def _iso(month: int, day: int, year: int) -> Optional[str]:
    try:
        return datetime(year, month, day).date().isoformat()
    except ValueError:
        return None


def _citation(*candidates: str) -> Optional[str]:
    """Normalize whichever of the label / filename / body carries the number.

    The OAG writes the same citation three ways — "2025-CNAG-02" on the
    listing, "25-cnag-01" in a filename, "2006-CNAG-1" inside the document —
    so the sequence number is zero-padded and the year expanded to four digits.
    """
    for candidate in candidates:
        m = CITATION_RE.search(candidate or "")
        if m:
            return f"{m.group(1)}{m.group(2)}-CNAG-{int(m.group(3)):02d}"
    # A filename can drop the century: "25-cnag-01.pdf".
    for candidate in candidates:
        m = re.search(r"\b(\d{2})\s*-\s*CNAG\s*-\s*(\d{1,2})\b", candidate or "", re.I)
        if m:
            return f"20{m.group(1)}-CNAG-{int(m.group(2)):02d}"
    return None


def _date_decided(text: str) -> Optional[str]:
    """The "Date Decided:" line the opinion prints under its caption."""
    m = DATE_DECIDED_RE.search(text)
    if not m:
        return None
    try:
        month = datetime.strptime(m.group(1)[:3].title(), "%b").month
    except ValueError:
        return None
    return _iso(month, int(m.group(2)), int(m.group(3)))


class CherokeeNationAGOpinionsScraper(BaseScraper):

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
        # Sample mode spreads its picks across the whole series so the samples
        # span 2006-2025 — i.e. cover the OCR'd scans, not just the handful of
        # recent born-digital opinions.
        self.max_docs = 0

    # ---- low-level ----------------------------------------------------------

    def _get(self, url: str, binary: bool = False, retries: int = 3):
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                r = self.session.get(url, timeout=180)
                if r.status_code == 200:
                    return r.content if binary else r.text
                logger.warning(f"HTTP {r.status_code} for {url}")
                if r.status_code in (403, 404):
                    return None
            except Exception as e:
                logger.warning(f"GET error {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- listing ------------------------------------------------------------

    def _entries(self) -> list[dict]:
        page = self._get(LISTING_URL)
        if not page:
            raise RuntimeError(f"opinions listing unreachable: {LISTING_URL}")

        entries: list[dict] = []
        seen: set[str] = set()
        for m in ITEM_RE.finditer(page):
            url = urljoin(BASE + "/", htmllib.unescape(m.group("url")).split("?")[0])
            if url in seen:
                continue
            seen.add(url)
            rest = m.group("rest")
            created = CREATED_RE.search(rest)
            updated = UPDATED_RE.search(rest)
            # Everything after the size/date metadata block is the OAG's own
            # one-line description of what the opinion answers.
            summary = _clean(re.sub(r"(?is)<div class=\"document-listing-metadata\">.*?</div>", "", rest))
            label = _clean(m.group("label"))
            entries.append(
                {
                    "url": url,
                    "label": label,
                    "citation": _citation(label, url),
                    "summary": summary or None,
                    "file_size": (SIZE_RE.search(rest).group(1) if SIZE_RE.search(rest) else None),
                    "published_date": (
                        _iso(int(created.group(1)), int(created.group(2)), int(created.group(3)))
                        if created else None
                    ),
                    "updated_date": (
                        _iso(int(updated.group(1)), int(updated.group(2)), int(updated.group(3)))
                        if updated else None
                    ),
                }
            )

        if not entries:
            raise RuntimeError(
                f"opinions listing parsed to 0 documents — layout changed? {LISTING_URL}"
            )
        # The pager is still rendered when pageSize covers everything, but it
        # collapses to a single page. More than one page left means the site
        # started capping pageSize and this run would be a silent truncation.
        pages = {int(p) for p, _ in PAGER_RE.findall(page)}
        if pages - {1}:
            raise RuntimeError(
                f"listing still paginated at pageSize=200 (pages {sorted(pages)}) — "
                f"only {len(entries)} of the series fetched; walk the pager instead"
            )
        if len(entries) < EXPECTED_MIN_OPINIONS:
            raise RuntimeError(
                f"only {len(entries)} opinions on the listing, expected at least "
                f"{EXPECTED_MIN_OPINIONS} — layout changed? {LISTING_URL}"
            )

        # Newest first on the page; keep that order so a capped run still gets
        # the current opinions, but spread the picks over the whole series.
        if self.max_docs and len(entries) > self.max_docs:
            last = len(entries) - 1
            picks = sorted({
                round(i * last / max(1, self.max_docs - 1)) for i in range(self.max_docs)
            })
            entries = [entries[i] for i in picks]
        logger.info(f"listing: {len(entries)} opinions")
        return entries

    # ---- PDF text -----------------------------------------------------------

    def _pdf_text(self, pdf: bytes, doc_id: str) -> tuple[Optional[str], Optional[int], str]:
        """Text layer if the PDF has one, else the OCR cascade.

        Returns (text, page_count, how) where ``how`` is "text_layer" or "ocr"
        so the record can record which it was and a thin corpus caused by a
        missing tesseract is visible in the output rather than silent.
        """
        pages = None
        if fitz is not None:
            try:
                doc = fitz.open(stream=pdf, filetype="pdf")
                pages = doc.page_count
                text = "\n".join(page.get_text() for page in doc)
                doc.close()
                if len(text.strip()) >= MIN_TEXT_LAYER_CHARS:
                    return text, pages, "text_layer"
            except Exception as e:
                logger.warning(f"PyMuPDF failed for {doc_id}: {e}")
        text = extract_pdf_markdown(
            "US/CherokeeNationAGOpinions", doc_id, pdf_bytes=pdf, table="doctrine"
        )
        if text and len(text.strip()) >= MIN_BODY_CHARS:
            return text, pages, "ocr"
        return None, pages, "none"

    # ---- normalize ----------------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        text = raw.get("text") or ""
        if len(text) < MIN_BODY_CHARS:
            return None

        citation = raw.get("citation") or raw["label"]
        summary = raw.get("summary")
        # The listing's blurb is often just the citation repeated; only use it
        # as a title when it actually says what the opinion is about.
        subject = summary if summary and not CITATION_RE.fullmatch(summary.strip()) else None
        if subject:
            subject = re.sub(rf"^{re.escape(citation)}\s*[-–—:]?\s*", "", subject).strip()
        title = f"{citation} — {subject}" if subject else f"Cherokee Nation AG Opinion {citation}"

        return {
            "_id": f"CNAG-{_slug(citation)}",
            "_source": "US/CherokeeNationAGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title[:500],
            "text": text,
            "date": raw.get("date_decided") or raw.get("published_date"),
            "url": raw["url"],
            "index_url": INDEX_URL,
            "document_type": "attorney_general_opinion",
            "citation": citation,
            "opinion_number": raw.get("opinion_number") or citation,
            "year": int(citation[:4]) if citation[:4].isdigit() else None,
            "summary": subject,
            "submitted_by": raw.get("submitted_by"),
            "date_decided": raw.get("date_decided"),
            "published_date": raw.get("published_date"),
            "updated_date": raw.get("updated_date"),
            "extraction": raw.get("extraction"),
            "pages": raw.get("pages"),
            "file_size": raw.get("file_size"),
            "language": "en",
            "jurisdiction": "US-CHEROKEE-NATION",
            "publisher": "Cherokee Nation Office of the Attorney General",
        }

    # ---- iteration ----------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        entries = self._entries()
        ocr_used = text_layer = skipped = 0
        for entry in entries:
            pdf = self._get(entry["url"], binary=True)
            if not pdf or pdf[:4] != b"%PDF":
                logger.warning(f"not a PDF: {entry['url']}")
                skipped += 1
                continue

            doc_id = _slug(entry["citation"] or entry["label"] or entry["url"])
            text, pages, how = self._pdf_text(pdf, f"CNAG-{doc_id}")
            if not text:
                logger.warning(
                    f"no extractable text for {entry['label']} "
                    f"({entry['url']}) — scanned and OCR unavailable?"
                )
                skipped += 1
                continue
            if how == "ocr":
                ocr_used += 1
            else:
                text_layer += 1

            body = _tidy(text)
            number = OPINION_NO_RE.search(body)
            submitted = SUBMITTED_BY_RE.search(body)
            yield {
                **entry,
                "text": body,
                "pages": pages,
                "extraction": how,
                "citation": entry["citation"] or _citation(body) or entry["label"],
                "opinion_number": (
                    _citation(number.group(1)) if number else None
                ) or entry["citation"],
                "submitted_by": _clean(submitted.group(1)) if submitted else None,
                "date_decided": _date_decided(body),
            }

        logger.info(
            f"extracted {text_layer} from the text layer, {ocr_used} by OCR, "
            f"{skipped} unreadable"
        )
        # Most of the back-catalogue is scanned, so an environment without OCR
        # yields only the recent born-digital opinions. Say so loudly instead
        # of letting a ~6-record run look like a complete corpus.
        if skipped and not ocr_used:
            raise RuntimeError(
                f"{skipped} of {len(entries)} opinions are scans and no OCR backend "
                "was available (install tesseract) — corpus would be incomplete"
            )

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # The OAG issues one to six opinions a year and republishes the whole
        # listing; re-walking it is cheap and the loader dedups on _id.
        yield from self.fetch_all()

    # ---- connectivity test --------------------------------------------------

    def test_api(self) -> bool:
        self.max_docs = 2
        try:
            entries = self._entries()
            logger.info(f"test-api: {len(entries)} opinions selected from the listing")
            for raw in self.fetch_all():
                rec = self.normalize(raw)
                if rec:
                    logger.info(
                        f"test-api normalize: {rec['_id']} — {len(rec['text'])} chars, "
                        f"date={rec['date']}, via={rec['extraction']}"
                    )
                    return True
            logger.error("test-api: no opinion with extractable text")
            return False
        except Exception as e:
            logger.error(f"test-api FAILED: {e}")
            return False
        finally:
            self.max_docs = 0


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/CherokeeNationAGOpinions bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test-api"], help="Command"
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    scraper = CherokeeNationAGOpinionsScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        return

    if args.sample:
        # A couple spare in case a scan defeats OCR entirely.
        scraper.max_docs = 14
    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
