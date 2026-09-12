#!/usr/bin/env python3
"""
US/MashantucketPequotLaws -- Mashantucket Pequot Tribal Laws (M.P.T.L.)

Fetches the full text of the codified body of law of the Mashantucket
(Western) Pequot Tribal Nation, a federally recognized sovereign tribe in
Connecticut: the Tribal Constitution and Bylaws, the Rules of Court, and the
numbered titles of the Mashantucket Pequot Tribal Laws (M.P.T.L.) — judiciary,
criminal law, gaming, child welfare, employment, taxation, land use, cannabis,
sovereign immunity and so on = legislation.

Strategy (static index + born-digital PDFs):

  The Tribal Court's law portal publishes every enacted title as its own
  official PDF and links them all from one page, ``/tribal-laws/``:

      <a href="/globalassets/laws/title-3-gaming.pdf">TITLE 3 GAMING</a>

  So the index is walked once and each ``/globalassets/laws/*.pdf`` link
  becomes one record. Only that directory is collected — the same page also
  links practice forms and e-filing standing orders from ``/globalassets/``
  proper, which are not enacted law.

  Five document kinds live side by side under ``/globalassets/laws/`` and are
  labelled in ``document_type``:
    * ``title``          — a single numbered M.P.T.L. title (current text)
    * ``constitution``   — the Tribal Constitution & Bylaws
    * ``rules_of_court`` — the Mashantucket Pequot Rules of Court
    * ``compilation``    — the bound 2008 code volumes and their 2009–2014
                           pocket parts (historical editions, kept because they
                           are the official text for their period)
    * ``supplement``     — the dated current supplement to the M.P.T.L.

  Full text comes from the PDFs via the shared ``common.pdf_extract`` cascade.
  Nearly all are born-digital and yield a clean text layer (a single title runs
  30K–50K characters; the bound volumes exceed 1M).

  The current titles and the rules volume are split to PROVISION level, because
  each PDF prints the official citation of a provision on a line of its own
  immediately above that provision:

      2 M.P.T.L. ch. 1 § 2          <- numbered titles
      § 2. Definitions

      M.P.R.C.P. 3                  <- rules volume, which bundles five
      Rule 3. Commencement of Action    separately cited codes (M.P.R.C.P.,
                                        M.P.R.E., M.P.R.A.P., M.P.J.C.,
                                        M.P.L.C.C.)

  Those anchor lines are the split points, so one record == one section, rule
  or canon — the unit the code is cited at, and a far better retrieval unit
  than a 100,000-character title. The historical ``compilation`` volumes and
  the ``supplement`` are deliberately kept WHOLE: they restate the same
  citations in superseded form, and splitting them would put two texts under
  one citation. A PDF with no anchors at all (a "RESERVED" placeholder title,
  or the scanned constitution) is likewise emitted as a single record.

  Effective dates are not published as a field. Where the file name records the
  Tribal Council resolution that last amended a title
  (``...-current-as-of-tcr072723-01.pdf`` = TCR of 07/27/2023) that date is
  parsed; the constitution's label carries its own date. Otherwise the date the
  PDF itself was produced is used, which is when the Office of Legal Counsel
  last recompiled that title.

Note: the tribe's *case law* — West's Mashantucket Pequot Reports — is a
commercial publication and is NOT open; only the codified laws are.

Usage:
  python bootstrap.py bootstrap            # Full pull (all laws)
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

BASE = "https://law.mptn-nsn.gov"
INDEX_URL = f"{BASE}/tribal-laws/"

# Only enacted law lives here; /globalassets/ proper also holds practice forms.
LAWS_PREFIX = "/globalassets/laws/"

# Minimum extracted body length to count a PDF as text-bearing.
MIN_BODY_CHARS = 500
# Below this a split unit is a table-of-contents echo, not a provision. Kept
# low because real one-sentence rules exist ("Rule 2. One Form of Action").
MIN_SECTION_CHARS = 60

# Document kinds that are split to provision level (see module docstring).
SPLIT_TYPES = ("title", "rules_of_court")

# "2 M.P.T.L. ch. 1 § 2", "24 M.P.T.L. ch. 3 § 1a" — alone on its line.
MPTL_ANCHOR_RE = re.compile(
    r"(?m)^[ \t#*]*(\d{1,3})[ \t]+M\.[ \t]?P\.[ \t]?T\.[ \t]?L\.[ \t]*"
    r"ch\.[ \t]*([0-9]{1,3}[A-Za-z]?)[ \t]*,?[ \t]*§+[ \t]*"
    r"([0-9]{1,3}[A-Za-z]?(?:\.[0-9]+)?)[ \t#*]*$"
)
# "M.P.R.C.P. 3", "M.P.R.E. 401", "M.P.J.C. § 2, Canon 4", "M.P.L.C.C. § 2, Rule 6.1"
RULE_ANCHOR_RE = re.compile(
    r"(?m)^[ \t#*]*(M\.P\.(?:[A-Z]\.)+)[ \t]*"
    r"((?:§[ \t]*)?[0-9A-Za-z][0-9A-Za-z.,§ \t-]{0,32}?)[ \t#*]*$"
)
# Heading printed under an anchor: "§ 2. Definitions", "Rule 3. Commencement".
HEADING_RE = re.compile(
    r"^[ \t#*]*(?:§+[ \t]*[0-9A-Za-z.]+|Rule[ \t]+[0-9A-Za-z.]+"
    r"|Canon[ \t]+[0-9]+|Preamble)[ \t]*[.:]?[ \t]*(.*?)[ \t#*]*$",
    re.I | re.M,
)

# "TITLE 3 GAMING", "TITLE 53 M.P.T.L. SOVEREIGN IMMUNITY"
TITLE_NO_RE = re.compile(r"^\s*TITLE\s+(\d+)\b", re.I)
# "...-current-as-of-tcr072723-01.pdf" — Tribal Council Resolution of 07/27/23.
TCR_RE = re.compile(r"tcr(\d{2})(\d{2})(\d{2})\b", re.I)
# "(Dated 09-19-2012)"
LABEL_DATE_RE = re.compile(r"\b(\d{2})-(\d{2})-(\d{4})\b")


def _clean(fragment: str) -> str:
    """Strip tags/entities from an HTML fragment and collapse whitespace."""
    text = htmllib.unescape(re.sub(r"<[^>]+>", " ", fragment))
    return re.sub(r"\s+", " ", text).replace(" ", " ").strip()


def _slug(value: str) -> str:
    return re.sub(r"-{2,}", "-", re.sub(r"[^a-z0-9]+", "-", value.lower())).strip("-")


def _tidy(text: str) -> str:
    """Drop running page numbers and collapse the PDF's ragged whitespace."""
    lines = [
        ln.rstrip()
        for ln in _clean_nbsp(text).splitlines()
        if not re.fullmatch(r"\s*\d{1,3}\s*", ln)
    ]
    body = re.sub(r"[ \t]+", " ", "\n".join(lines))
    return re.sub(r"\n{3,}", "\n\n", body).strip()


def _clean_nbsp(text: str) -> str:
    return text.replace(" ", " ")


def split_provisions(text: str, doc_type: str, title_no: Optional[int]) -> list[dict]:
    """Cut a PDF's text into one unit per official citation anchor line.

    Numbered titles anchor on ``N M.P.T.L. ch. C § S`` — restricted to this
    title's own number so that a cross-reference to another title printed at
    the start of a line is not mistaken for an anchor. The rules volume has no
    title number and anchors on the five ``M.P.*`` rule-code citations instead.
    Returns [] when the PDF carries no anchors (RESERVED placeholder, scan).
    """
    marks: list[tuple[int, int, str, str]] = []  # (start, end, citation, key)
    for m in MPTL_ANCHOR_RE.finditer(text):
        if title_no is not None and int(m.group(1)) != title_no:
            continue
        citation = f"{m.group(1)} M.P.T.L. ch. {m.group(2)} § {m.group(3)}"
        marks.append((m.start(), m.end(), citation,
                      f"t{m.group(1)}-ch{m.group(2)}-s{m.group(3)}".lower()))
    if doc_type == "rules_of_court":
        for m in RULE_ANCHOR_RE.finditer(text):
            locator = re.sub(r"\s+", " ", m.group(2)).strip(" .,")
            citation = f"{m.group(1)} {locator}".strip()
            marks.append((m.start(), m.end(), citation, _slug(citation)))

    marks.sort(key=lambda mark: mark[0])
    units: list[dict] = []
    seen: set[str] = set()
    for i, (_, end, citation, key) in enumerate(marks):
        stop = marks[i + 1][0] if i + 1 < len(marks) else len(text)
        body = _tidy(text[end:stop])
        # Every citation is printed twice — once in the table of contents (where
        # it is followed by dot leaders, not text) and once over the provision.
        # The short TOC entry is filtered by length, the duplicate key by `seen`.
        if len(body) < MIN_SECTION_CHARS or key in seen:
            continue
        seen.add(key)
        heading = HEADING_RE.match(body)
        units.append(
            {
                "citation": citation,
                "key": key,
                "heading": (re.sub(r"\s+", " ", heading.group(1)).strip(" .")
                            or None) if heading else None,
                "text": body,
            }
        )
    return units


class MashantucketPequotLawsScraper(BaseScraper):

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
        # Sample mode takes one provision per PDF, from PDFs spread across the
        # whole index, so the 12 samples exercise every document_type instead
        # of all landing inside title 1, chapter 1.
        self.per_doc_cap = 0
        self.spread_docs = 0

    # ---- low-level ----------------------------------------------------------

    def _get(self, url: str, binary: bool = False, retries: int = 3):
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                r = self.session.get(url, timeout=180)
                if r.status_code == 200:
                    return r.content if binary else r.text
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
        return urljoin(BASE + "/", quote(href, safe="/:%"))

    @staticmethod
    def _classify(label: str, href: str) -> str:
        blob = f"{label} {href}".lower()
        if TITLE_NO_RE.match(label) or re.search(r"/title-\d+|^\d+\s*m\.p\.t\.l\.", blob):
            return "title"
        if "constitution" in blob:
            return "constitution"
        if "rules-of-court" in blob or "rules of court" in blob:
            return "rules_of_court"
        if "supplement" in blob or "supplment" in blob:  # site's own typo
            return "supplement"
        if "pocket-part" in blob or re.search(r"/titles-\d", blob):
            return "compilation"
        return "law"

    @staticmethod
    def _parse_date(label: str, href: str) -> Optional[str]:
        """Effective/amendment date, where the label or file name records one."""
        m = TCR_RE.search(href)
        if m:
            mm, dd, yy = (int(g) for g in m.groups())
            try:
                return datetime(2000 + yy, mm, dd).date().isoformat()
            except ValueError:
                pass
        m = LABEL_DATE_RE.search(label)
        if m:
            mm, dd, yyyy = (int(g) for g in m.groups())
            try:
                return datetime(yyyy, mm, dd).date().isoformat()
            except ValueError:
                pass
        # "01.08.2026-current-supplment-of-mptl.pdf"
        m = re.search(r"\b(\d{2})\.(\d{2})\.(\d{4})\b", href)
        if m:
            mm, dd, yyyy = (int(g) for g in m.groups())
            try:
                return datetime(yyyy, mm, dd).date().isoformat()
            except ValueError:
                pass
        return None

    def _parse_index(self, page: str) -> list[dict]:
        entries: list[dict] = []
        seen: set[str] = set()
        for m in re.finditer(r'(?is)href="([^"]+\.pdf)"[^>]*>(.*?)</a>', page):
            href, label = m.group(1), _clean(m.group(2))
            path = href.split("?")[0]
            if LAWS_PREFIX not in path.lower():
                continue  # practice forms / standing orders, not enacted law
            key = path.lower()
            if key in seen:
                continue
            seen.add(key)
            title_no = TITLE_NO_RE.match(label)
            entries.append(
                {
                    "href": path,
                    "label": label or path.rsplit("/", 1)[-1][:-4],
                    "doc_type": self._classify(label, path),
                    "title_no": int(title_no.group(1)) if title_no else None,
                    "date": self._parse_date(label, path),
                }
            )
        return entries

    # ---- PDF text -----------------------------------------------------------

    def _pdf_text(self, pdf: bytes, doc_id: str) -> tuple[Optional[str], Optional[int]]:
        """Text layer via PyMuPDF, falling back to the shared extract cascade.

        PyMuPDF is the primary path because the citation anchors this scraper
        splits on only survive as standalone lines in its raw text output.
        ``extract_pdf_markdown`` (opendataloader → pdfplumber → pypdf → OCR) is
        the fallback for the one PDF that is a pure scan, the constitution.
        """
        pages = None
        if fitz is not None:
            try:
                doc = fitz.open(stream=pdf, filetype="pdf")
                pages = doc.page_count
                text = "\n".join(page.get_text() for page in doc)
                doc.close()
                if len(text.strip()) >= MIN_BODY_CHARS:
                    return text, pages
            except Exception as e:
                logger.warning(f"PyMuPDF failed for {doc_id}: {e}")
        text = extract_pdf_markdown(
            "US/MashantucketPequotLaws", doc_id, pdf_bytes=pdf, table="legislation"
        )
        if text and len(text.strip()) >= MIN_BODY_CHARS:
            return text, pages
        return None, pages

    @staticmethod
    def _pdf_built_date(pdf: bytes) -> Optional[str]:
        """Date the PDF itself was produced — when that title was recompiled."""
        if fitz is None:
            return None
        try:
            doc = fitz.open(stream=pdf, filetype="pdf")
            meta = doc.metadata or {}
            doc.close()
        except Exception:
            return None
        for key in ("creationDate", "modDate"):
            m = re.match(r"D:(\d{4})(\d{2})(\d{2})", meta.get(key) or "")
            if m:
                try:
                    return datetime(*(int(g) for g in m.groups())).date().isoformat()
                except ValueError:
                    continue
        return None

    # ---- normalize ----------------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        text = raw.get("text") or ""
        if len(text) < MIN_SECTION_CHARS:
            return None

        citation, heading, label = raw.get("citation"), raw.get("heading"), raw["label"]
        if citation and heading:
            title = f"{citation} — {heading}"
        elif citation:
            title = f"{citation} ({label})"
        elif len(label) > 6:
            title = label
        else:
            title = f"M.P.T.L. — {label}"

        return {
            "_id": f"MPTL-{raw['key']}",
            "_source": "US/MashantucketPequotLaws",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title[:500],
            "text": text,
            "date": raw.get("date"),
            "url": raw["url"],
            "index_url": INDEX_URL,
            "document_type": raw["doc_type"],
            "document": label,
            "title_number": raw.get("title_no"),
            "citation": citation or (
                f"{raw['title_no']} M.P.T.L." if raw.get("title_no") is not None else None
            ),
            "section_heading": heading,
            "amended_by_tcr": raw.get("tcr_date"),
            "pages": raw.get("pages"),
            "language": "en",
            "jurisdiction": "US-MASHANTUCKET-PEQUOT",
            "publisher": "Mashantucket Pequot Tribal Nation",
        }

    # ---- iteration ----------------------------------------------------------

    def _entries(self) -> list[dict]:
        page = self._get(INDEX_URL)
        if not page:
            raise RuntimeError(f"tribal-laws index unreachable: {INDEX_URL}")
        entries = self._parse_index(page)
        if not entries:
            raise RuntimeError(
                f"tribal-laws index parsed to 0 law PDFs — layout changed? {INDEX_URL}"
            )
        logger.info(f"index: {len(entries)} law documents")
        # Numbered titles first (the substantive code), then the rest.
        entries.sort(key=lambda e: (e["title_no"] is None, e["title_no"] or 0, e["href"]))
        if self.spread_docs:
            step = max(1, len(entries) // self.spread_docs)
            entries = entries[::step][: self.spread_docs]
        return entries

    def fetch_all(self) -> Generator[dict, None, None]:
        for entry in self._entries():
            pdf_url = self._abs(entry["href"])
            pdf = self._get(pdf_url, binary=True)
            if not pdf or pdf[:4] != b"%PDF":
                logger.warning(f"not a PDF: {pdf_url}")
                continue

            doc_slug = _slug(entry["href"].rsplit("/", 1)[-1][:-4])
            text, pages = self._pdf_text(pdf, f"MPTL-{doc_slug}")
            if not text:
                logger.warning(
                    f"no extractable text (scanned, no OCR available): {pdf_url}"
                )
                continue

            common = {
                **entry,
                "url": pdf_url,
                "pages": pages,
                "tcr_date": entry.get("date"),
                "date": entry.get("date") or self._pdf_built_date(pdf),
            }

            units = []
            if entry["doc_type"] in SPLIT_TYPES:
                units = split_provisions(text, entry["doc_type"], entry["title_no"])
            if not units:
                body = _tidy(text)
                if len(body) < MIN_BODY_CHARS:
                    logger.warning(f"insufficient text ({len(body)} chars): {pdf_url}")
                    continue
                units = [{"citation": None, "key": doc_slug, "heading": None,
                          "text": body}]
            logger.info(f"{entry['label']}: {len(units)} record(s)")

            for emitted, unit in enumerate(units, start=1):
                yield {**common, **unit}
                if self.per_doc_cap and emitted >= self.per_doc_cap:
                    break

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # Amendments are published by replacing a title's PDF in place, so the
        # only reliable refresh is to re-walk the index; the loader dedups _id.
        yield from self.fetch_all()

    # ---- connectivity test --------------------------------------------------

    def test_api(self) -> bool:
        self.per_doc_cap = 1
        try:
            entries = self._entries()
            logger.info(f"test-api: {len(entries)} law documents on the index")
            for raw in self.fetch_all():
                rec = self.normalize(raw)
                if rec:
                    logger.info(
                        f"test-api normalize: {rec['_id']} — {len(rec['text'])} chars, "
                        f"type={rec['document_type']}, date={rec['date']}"
                    )
                    return True
            logger.error("test-api: no provision with extractable text")
            return False
        except Exception as e:
            logger.error(f"test-api FAILED: {e}")
            return False
        finally:
            self.per_doc_cap = 0


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/MashantucketPequotLaws bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test-api"], help="Command"
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    scraper = MashantucketPequotLawsScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        return

    if args.sample:
        scraper.per_doc_cap = 1
        scraper.spread_docs = 14  # a couple spare in case a PDF has no text layer
    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
