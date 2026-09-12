#!/usr/bin/env python3
"""
US/ChickasawNationCode -- Chickasaw Nation Code and Constitution

Fetches the full text of the codified law of the Chickasaw Nation, a
federally recognized sovereign tribe headquartered in Ada, Oklahoma: the
Constitution of the Chickasaw Nation and the 21 titles of the Chickasaw
Nation Code (courts and procedure, offences and penalties, elections,
domestic relations, finance and taxation, lands, utilities, ...) =
legislation.

Strategy (static index + born-digital PDFs):

  The Nation runs a dedicated code portal. Every title is served as its own
  official PDF straight off a clean path — ``code.chickasaw.net/Title-05``
  returns ``application/pdf``, no HTML wrapper — and the government site
  links all of them from one page:

      <a href="https://code.chickasaw.net/Title-05">Title 05 Courts and Procedures</a>

  So the index is walked once, and the Constitution PDF (linked from
  chickasaw.net/Our-Nation/Government/Chickasaw-Constitution.aspx) is
  appended to it. Both are born-digital and yield a clean text layer.

  Titles are split to SECTION level, which is the unit the code is cited at
  ("10 C.N.C. § 10-101.4"). The split is possible because each title PDF
  prints every section number twice in two distinguishable cases:

      Section 10-101.4            <- table of contents, mixed case
      Definitions.

      SECTION 10-101.4            <- the provision itself, upper case
      DEFINITIONS.

  Anchoring on the UPPER-case form therefore lands on the body and skips the
  contents listing entirely; the anchor's own title number is checked against
  the title being parsed so a cross-reference printed at the start of a line
  cannot be mistaken for an anchor. Title 5 alone yields ~600 sections, so
  one record is one section rather than a 1,000,000-character title blob.

  Chapter and article context is carried down from the most recent
  ``CHAPTER n`` / ``ARTICLE X`` heading above the section. Dates come from
  the legislative history each section prints at its end — ``(TL11-003,
  12/17/93; PR29-006, 8/17/12)`` — the most recent of which is when that
  section was last amended; a section with no history falls back to the
  title's own "(Amended as of MM/DD/YYYY)" line.

COVERAGE LIMITS (deliberate, see README):
  * The Chickasaw Nation Judicial Department does NOT publish court opinions
    online — there is no opinions page and no public docket portal on
    judicial.chickasaw.net. District Court dockets are carried by ODCR
    (odcr.com), a third-party for-profit aggregator; it is NOT collected here
    because it is not the official publisher and its terms are restrictive
    (see issue #1499). This source is legislation only.
  * The District Court Rules PDF linked from judicial.chickasaw.net is a
    scan with no text layer. It is fetched, and kept only if OCR is available
    in the environment; otherwise it is skipped with a warning. The same
    rules are in any case enacted in Code Title 5, which IS collected.

Usage:
  python bootstrap.py bootstrap            # Full pull (all titles)
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

CODE_BASE = "https://code.chickasaw.net"
GOV_BASE = "https://chickasaw.net"
JUDICIAL_BASE = "https://judicial.chickasaw.net"

INDEX_URL = f"{GOV_BASE}/Our-Nation/Government/Chickasaw-Nation-Code.aspx"
CONSTITUTION_URL = f"{GOV_BASE}/Our-Nation/Government/Chickasaw-Constitution.aspx"
DISTRICT_COURT_URL = f"{JUDICIAL_BASE}/Courts/District-Court.aspx"

# Minimum extracted body length to count a PDF as text-bearing.
MIN_BODY_CHARS = 500
# Below this a split unit is a stub, not a provision. Kept low because real
# one-line sections exist ("SECTION 10-101.1 / RESERVED.").
MIN_SECTION_CHARS = 20

# "SECTION 10-101.4" alone on its line — the body form (see module docstring).
# The trailing ".s" is optional: most titles number chapter-then-section
# ("SECTION 5-101.4") but a few recent acts use a flat series instead
# ("SECTION 11-102" in the Wildlife Conservation Act of 2022).
# Most titles print the caption on the next line, but some (e.g. title 20) keep
# it on the anchor line — "SECTION 20-100.1  TITLE." — so an upper-case tail is
# allowed and becomes the heading.
SECTION_RE = re.compile(
    r"(?m)^[ \t]*SECTION[ \t]+(\d{1,2})-(\d{1,4})(?:\.(\d{1,3}[A-Za-z]?))?"
    r"[ \t]*[.:]?[ \t]*([^\n]{0,120}?)[ \t]*$"
)
CHAPTER_RE = re.compile(r"(?m)^[ \t]*CHAPTER[ \t]+(\d{1,3}[A-Za-z]?)[ \t]*$")
ARTICLE_RE = re.compile(r"(?m)^[ \t]*ARTICLE[ \t]+([A-Z]{1,3})[ \t]*$")
# Legislative history: "(TL11-003, 12/17/93; PR29-006, 8/17/12)"
HISTORY_DATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{2}|\d{4})\b")
# The title PDF's own currency line: "(Amended as of 04/20/2026)"
AMENDED_AS_OF_RE = re.compile(r"amended as of[ \t]*:?[ \t]*(\d{1,2})/(\d{1,2})/(\d{2}|\d{4})", re.I)
# The constitution spells its own date out: "as amended as of June 21, 2002"
AMENDED_WORDS_RE = re.compile(
    r"amended as of\s+([A-Z][a-z]+)\s+(\d{1,2}),?\s+(\d{4})", re.I
)
# "Title 05     Courts and Procedures"
TITLE_LABEL_RE = re.compile(r"^\s*Title\s+0*(\d{1,2})\b[\s.:-]*(.*)$", re.I)
# Running page furniture printed on every page: "Page 10-5"
PAGE_FURNITURE_RE = re.compile(r"(?m)^[ \t]*Page[ \t]+\d{1,2}-\d{1,4}[ \t]*$")


def _clean(fragment: str) -> str:
    """Strip tags/entities from an HTML fragment and collapse whitespace."""
    text = htmllib.unescape(re.sub(r"<[^>]+>", " ", fragment))
    return re.sub(r"\s+", " ", text).replace(" ", " ").strip()


def _slug(value: str) -> str:
    return re.sub(r"-{2,}", "-", re.sub(r"[^a-z0-9]+", "-", value.lower())).strip("-")


def _tidy(text: str) -> str:
    """Drop the running page furniture and collapse the PDF's ragged whitespace."""
    text = PAGE_FURNITURE_RE.sub("", text.replace(" ", " "))
    lines = [ln.rstrip() for ln in text.splitlines()
             if not re.fullmatch(r"\s*\d{1,3}\s*", ln)]
    body = re.sub(r"[ \t]+", " ", "\n".join(lines))
    return re.sub(r"\n{3,}", "\n\n", body).strip()


def _iso(month: int, day: int, year: int) -> Optional[str]:
    """Normalize a legislative-history date; 2-digit years are 19xx/20xx."""
    if year < 100:
        year += 2000 if year <= 30 else 1900
    try:
        return datetime(year, month, day).date().isoformat()
    except ValueError:
        return None


def _last_history_date(body: str) -> Optional[str]:
    """Most recent date in a section's legislative history = last amended."""
    dates = [
        iso for iso in (
            _iso(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            for m in HISTORY_DATE_RE.finditer(body)
        ) if iso
    ]
    return max(dates) if dates else None


def _caption(tail: str) -> Optional[str]:
    """Normalize a caption printed on the anchor line itself."""
    text = re.sub(r"\s+", " ", tail or "").strip(" .:")
    return text.title() if text else None


def _heading(body: str) -> Optional[str]:
    """The upper-case caption printed directly under a SECTION anchor."""
    caption: list[str] = []
    for line in body.splitlines():
        line = line.strip()
        if not line:
            if caption:
                break
            continue
        # Captions are set in caps; the provision text that follows is not.
        if line.upper() != line or len(caption) >= 3:
            break
        caption.append(line)
        if line.endswith("."):
            break
    text = re.sub(r"\s+", " ", " ".join(caption)).strip(" .")
    return text.title() if text else None


def split_sections(text: str, title_no: int) -> list[dict]:
    """Cut a title PDF's text into one unit per ``SECTION n-ccc.s`` anchor.

    Returns [] when the PDF carries no anchors, in which case the caller
    keeps the title whole.
    """
    anchors = [
        m for m in SECTION_RE.finditer(text)
        # The anchor's own title number must be this title's, so that a
        # cross-reference to another title starting a line is not an anchor;
        # and any tail on the anchor line must be a caption (set in caps),
        # not the running text of a sentence that mentions a section.
        if int(m.group(1)) == title_no and m.group(4).upper() == m.group(4)
    ]
    if not anchors:
        return []

    # Chapter/article headings that precede each anchor, for context fields.
    structure = sorted(
        [(m.start(), "chapter", m.group(1)) for m in CHAPTER_RE.finditer(text)]
        + [(m.start(), "article", m.group(1)) for m in ARTICLE_RE.finditer(text)]
    )

    units: dict[str, dict] = {}
    for i, m in enumerate(anchors):
        stop = anchors[i + 1].start() if i + 1 < len(anchors) else len(text)
        body = _tidy(text[m.end():stop])
        section = f"{m.group(1)}-{m.group(2)}"
        if m.group(3):
            section += f".{m.group(3)}"
        key = _slug(section)
        # A section number can appear twice if a title sets its contents
        # listing in caps as well; the listing entry is the shorter of the
        # two, so the longest body per section number wins.
        if len(body) < MIN_SECTION_CHARS or len(body) <= len(units.get(key, {}).get("text", "")):
            continue
        chapter = article = None
        for pos, kind, value in structure:
            if pos > m.start():
                break
            if kind == "chapter":
                chapter, article = value, None  # a new chapter resets articles
            else:
                article = value
        units[key] = {
            "citation": f"{title_no} C.N.C. § {section}",
            "key": key,
            "section": section,
            "chapter": chapter,
            "article": article,
            "heading": _caption(m.group(4)) or _heading(body),
            "text": body,
            "section_date": _last_history_date(body),
        }
    return list(units.values())


class ChickasawNationCodeScraper(BaseScraper):

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
        # Sample mode takes one section per PDF, from PDFs spread across the
        # whole code, so the 12 samples span titles instead of all landing
        # inside title 1, chapter 1.
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

    def _parse_titles(self, page: str) -> list[dict]:
        entries: list[dict] = []
        seen: set[str] = set()
        for m in re.finditer(
            r'(?is)href="(https?://code\.chickasaw\.net/[^"]+)"[^>]*>(.*?)</a>', page
        ):
            url, label = m.group(1).split("?")[0], _clean(m.group(2))
            if url.rstrip("/").lower().endswith("table-of-contents"):
                continue  # the contents listing repeats every title's headings
            match = TITLE_LABEL_RE.match(label) or re.search(r"/Title-0*(\d{1,2})$", url)
            if not match:
                continue
            title_no = int(match.group(1))
            name = _clean(match.group(2)) if match.lastindex and match.lastindex > 1 else ""
            if url in seen:
                continue
            seen.add(url)
            entries.append(
                {
                    "url": url,
                    "label": f"Title {title_no} — {name}" if name else f"Title {title_no}",
                    "doc_type": "title",
                    "title_no": title_no,
                    "title_name": name or None,
                }
            )
        entries.sort(key=lambda e: e["title_no"])
        return entries

    def _constitution_entry(self) -> Optional[dict]:
        page = self._get(CONSTITUTION_URL)
        if not page:
            logger.warning("constitution page unreachable — skipping")
            return None
        m = re.search(r'href="([^"]*Constitu[^"]*\.pdf[^"]*)"', page, re.I)
        if not m:
            logger.warning("no Constitution PDF link found on the constitution page")
            return None
        return {
            "url": urljoin(GOV_BASE + "/", htmllib.unescape(m.group(1))),
            "label": "Constitution of the Chickasaw Nation",
            "doc_type": "constitution",
            "title_no": None,
            "title_name": None,
        }

    def _court_rules_entry(self) -> Optional[dict]:
        page = self._get(DISTRICT_COURT_URL)
        if not page:
            return None
        m = re.search(r'href="([^"]*District_Court_Rules[^"]*)"', page, re.I)
        if not m:
            return None
        return {
            "url": urljoin(JUDICIAL_BASE + "/", htmllib.unescape(m.group(1))),
            "label": "Chickasaw Nation District Court Rules",
            "doc_type": "rules_of_court",
            "title_no": None,
            "title_name": None,
        }

    # ---- PDF text -----------------------------------------------------------

    def _pdf_text(self, pdf: bytes, doc_id: str) -> tuple[Optional[str], Optional[int]]:
        """Text layer via PyMuPDF, falling back to the shared extract cascade.

        PyMuPDF is the primary path because the SECTION anchors this scraper
        splits on only survive as standalone lines in its raw text output.
        ``extract_pdf_markdown`` (opendataloader → pdfplumber → pypdf → OCR)
        is the fallback for the scanned District Court Rules.
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
            "US/ChickasawNationCode", doc_id, pdf_bytes=pdf, table="legislation"
        )
        if text and len(text.strip()) >= MIN_BODY_CHARS:
            return text, pages
        return None, pages

    @staticmethod
    def _document_date(text: str) -> Optional[str]:
        """The currency date a title or the constitution prints on page 1."""
        head = text[:4000]
        m = AMENDED_AS_OF_RE.search(head)
        if m:
            return _iso(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        m = AMENDED_WORDS_RE.search(head)
        if m:
            try:
                month = datetime.strptime(m.group(1)[:3], "%b").month
            except ValueError:
                return None
            return _iso(month, int(m.group(2)), int(m.group(3)))
        return None

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
        for key in ("modDate", "creationDate"):
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
        else:
            title = label

        return {
            "_id": f"CNC-{raw['key']}",
            "_source": "US/ChickasawNationCode",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title[:500],
            "text": text,
            "date": raw.get("section_date") or raw.get("date"),
            "url": raw["url"],
            "index_url": INDEX_URL,
            "document_type": raw["doc_type"],
            "document": label,
            "title_number": raw.get("title_no"),
            "title_name": raw.get("title_name"),
            "citation": citation or (
                f"{raw['title_no']} C.N.C." if raw.get("title_no") is not None
                else "Const. Chickasaw Nation"
            ),
            "section": raw.get("section"),
            "chapter": raw.get("chapter"),
            "article": raw.get("article"),
            "section_heading": heading,
            "title_amended_as_of": raw.get("date"),
            "pages": raw.get("pages"),
            "language": "en",
            "jurisdiction": "US-CHICKASAW-NATION",
            "publisher": "The Chickasaw Nation",
        }

    # ---- iteration ----------------------------------------------------------

    def _entries(self) -> list[dict]:
        page = self._get(INDEX_URL)
        if not page:
            raise RuntimeError(f"code index unreachable: {INDEX_URL}")
        entries = self._parse_titles(page)
        if not entries:
            raise RuntimeError(
                f"code index parsed to 0 titles — layout changed? {INDEX_URL}"
            )
        for extra in (self._constitution_entry(), self._court_rules_entry()):
            if extra:
                entries.append(extra)
        logger.info(f"index: {len(entries)} documents "
                    f"({sum(1 for e in entries if e['doc_type'] == 'title')} code titles)")
        if self.spread_docs and len(entries) > self.spread_docs:
            # The constitution and the rules come first, so a sample run that
            # stops at its record cap still covers them; the titles behind
            # them are picked evenly spaced so the samples span the whole code
            # rather than its first N titles.
            others = [e for e in entries if e["doc_type"] != "title"]
            titles = [e for e in entries if e["doc_type"] == "title"]
            want = max(1, self.spread_docs - len(others))
            if len(titles) > want:
                last = len(titles) - 1
                picks = sorted({round(i * last / max(1, want - 1))
                                for i in range(want)})
                titles = [titles[i] for i in picks]
            entries = others + titles
        return entries

    def fetch_all(self) -> Generator[dict, None, None]:
        for entry in self._entries():
            pdf = self._get(entry["url"], binary=True)
            if not pdf or pdf[:4] != b"%PDF":
                logger.warning(f"not a PDF: {entry['url']}")
                continue

            doc_slug = _slug(
                f"title-{entry['title_no']}" if entry["title_no"] is not None
                else entry["doc_type"]
            )
            text, pages = self._pdf_text(pdf, f"CNC-{doc_slug}")
            if not text:
                logger.warning(
                    f"no extractable text (scanned, no OCR available): {entry['url']}"
                )
                continue

            common = {
                **entry,
                "pages": pages,
                "date": self._document_date(text) or self._pdf_built_date(pdf),
            }

            units = []
            if entry["doc_type"] == "title":
                units = split_sections(text, entry["title_no"])
            if not units:
                body = _tidy(text)
                if len(body) < MIN_BODY_CHARS:
                    logger.warning(f"insufficient text ({len(body)} chars): {entry['url']}")
                    continue
                units = [{"citation": None, "key": doc_slug, "heading": None,
                          "text": body, "section": None, "chapter": None,
                          "article": None, "section_date": None}]
            logger.info(f"{entry['label']}: {len(units)} record(s)")

            if self.per_doc_cap == 1 and len(units) > 1:
                # One-per-document mode is sampling: a title's § 1 is always
                # the short "Title." provision, so take a typical one instead.
                units = [sorted(units, key=lambda u: len(u["text"]))[len(units) // 2]]

            for emitted, unit in enumerate(units, start=1):
                yield {**common, **unit}
                if self.per_doc_cap and emitted >= self.per_doc_cap:
                    break

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # Amendments are published by replacing a title's PDF in place, so the
        # only reliable refresh is to re-walk the code; the loader dedups _id.
        yield from self.fetch_all()

    # ---- connectivity test --------------------------------------------------

    def test_api(self) -> bool:
        self.per_doc_cap = 1
        try:
            entries = self._entries()
            logger.info(f"test-api: {len(entries)} documents on the code index")
            for raw in self.fetch_all():
                rec = self.normalize(raw)
                if rec:
                    logger.info(
                        f"test-api normalize: {rec['_id']} — {len(rec['text'])} chars, "
                        f"type={rec['document_type']}, date={rec['date']}"
                    )
                    return True
            logger.error("test-api: no section with extractable text")
            return False
        except Exception as e:
            logger.error(f"test-api FAILED: {e}")
            return False
        finally:
            self.per_doc_cap = 0


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/ChickasawNationCode bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test-api"], help="Command"
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    scraper = ChickasawNationCodeScraper()

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
