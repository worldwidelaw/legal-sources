#!/usr/bin/env python3
"""
US/CherokeeNationCode -- Cherokee Nation Code Annotated (CNCA)

Fetches the full text of the codified law of the Cherokee Nation, a federally
recognized sovereign tribe headquartered in Tahlequah, Oklahoma: the Act of
Union of 1839, the three Constitutions (1839, 1975, 1999) and the 85 titles of
the Cherokee Nation Code Annotated, plus the post-codification amendment
volumes the Attorney General publishes alongside it.

Strategy (static index + one born-digital consolidated PDF):

  The Office of the Attorney General publishes the whole code as a single
  word-searchable PDF -- 1,788 pages, ~4.6 million characters -- linked from

      https://attorneygeneral.cherokee.org/tribal-code/

  together with four amendment volumes (titles 10A, 21, 22 and 47) that were
  enacted after the consolidated volume was compiled. The listing carries each
  file's own "Created:" / "Updated:" stamp, which is where the record dates
  come from.

  Everything is BORN-DIGITAL: PyMuPDF returns a clean text layer, so no OCR is
  involved and the extraction is deterministic.

  Titles are split to SECTION level, which is the unit the code is cited at
  ("21 CNCA § 1289.6"). The consolidated PDF is unusually regular for a
  scanned-era tribal code -- it prints

      TITLE 21                 <- title anchor, alone on its line
      CRIMES AND PUNISHMENTS   <- title name, the next non-numeric line
      CHAPTER 1                <- chapter context
      GENERAL PROVISIONS
      § 101. Short title       <- section anchor, alone on its line

  -- and, crucially, it carries NO per-chapter contents listing, so a section
  number appears exactly once and the anchor always lands on the provision.
  Cross-references are written inline ("under 1 CNCA § 317") and so never
  start a line; the anchors are line-anchored for that reason. Where a number
  does repeat (the amendment volumes restate sections of the base code), the
  longest body wins within a document and the amendment keeps its own _id.

  The four constitutional instruments precede TITLE 1 and have no § numbering,
  so they are split on their own heading lines and emitted whole.

COVERAGE NOTE (see README):
  The AG's own page says "Please refer to the statutes-at-large as the
  authoritative text for specific code provisions", and the amendment volumes
  carry a notice pointing at cherokee.legistar.com/Legislation.aspx. This
  source is the CODE -- the consolidated statement of law -- not the
  statutes-at-large; the Legistar acts database is a separate corpus and is
  deliberately out of scope here.

Usage:
  python bootstrap.py bootstrap            # Full pull (all sections)
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
INDEX_URL = f"{BASE}/tribal-code/"

# The consolidated volume alone runs ~4.6M chars; anything near zero means the
# text layer went missing or the host served us something else.
MIN_DOC_CHARS = 20_000
# A real one-line section ("§ 104. Repealed") is short; below this it is a stub.
MIN_SECTION_CHARS = 20
# The code has 85 titles and ~3,300 sections. A collapse means the layout
# changed or the PDF was replaced with a scan — fail loud (#1402 class).
MIN_EXPECTED_SECTIONS = 1_500

# "TITLE 10A" / "TITLE 21", alone on its line.
TITLE_RE = re.compile(r"(?m)^[ \t]*TITLE[ \t]+(\d{1,3}[A-Z]?)[ \t]*$")
CHAPTER_RE = re.compile(r"(?m)^[ \t]*CHAPTER[ \t]+([0-9]{1,3}[A-Z]?)[ \t]*\.?[ \t]*$")
# Code articles are numbered ("ARTICLE 2"); the constitutions use roman
# numerals and are handled as whole documents, so only the numeric form here.
ARTICLE_RE = re.compile(r"(?m)^[ \t]*ARTICLE[ \t]+([0-9]{1,3}[A-Z]?)[ \t]*\.?[ \t]*")
PART_RE = re.compile(r"(?m)^[ \t]*PART[ \t]+([IVXL]{1,6})\.[ \t]*(.{0,80}?)[ \t]*$")
# "§ 1289.6  Possession of firearm" — the number may carry dots, dashes and a
# letter suffix ("§ 402A", "§ 2-1-101").
SECTION_RE = re.compile(
    r"(?m)^[ \t]*§[ \t]*(\d[0-9A-Za-z]*(?:[.\-][0-9A-Za-z]+)*)[ \t]*\.?[ \t]*(.{0,160}?)[ \t]*$"
)
# The instruments that precede TITLE 1, each alone on its line.
INSTRUMENT_RE = re.compile(
    r"(?m)^[ \t]*(ACT OF UNION[^\n]*|CONSTITUTION OF THE CHEROKEE NATION[^\n]*)[ \t]*$"
)
# "2.2 MB -- Created:3/11/2021 | Updated:4/6/2021"
UPDATED_RE = re.compile(r"Updated:\s*(\d{1,2})/(\d{1,2})/(\d{4})")
CREATED_RE = re.compile(r"Created:\s*(\d{1,2})/(\d{1,2})/(\d{4})")


def _clean(fragment: str) -> str:
    text = htmllib.unescape(re.sub(r"<[^>]+>", " ", fragment))
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def _slug(value: str) -> str:
    return re.sub(r"-{2,}", "-", re.sub(r"[^a-z0-9]+", "-", value.lower())).strip("-")


def _tidy(text: str) -> str:
    """Drop the bare page numbers the PDF prints and collapse whitespace."""
    lines = [ln.rstrip() for ln in text.splitlines()
             if not re.fullmatch(r"\s*\d{1,4}\s*", ln)]
    body = re.sub(r"[ \t]+", " ", "\n".join(lines))
    return re.sub(r"\n{3,}", "\n\n", body).strip()


def _iso(month: int, day: int, year: int) -> Optional[str]:
    try:
        return datetime(year, month, day).date().isoformat()
    except ValueError:
        return None


def _title_name(text: str, start: int) -> Optional[str]:
    """The title's name: the first line after the anchor that is not a page number."""
    for line in text[start:start + 400].splitlines():
        line = line.strip()
        if not line or re.fullmatch(r"\d{1,4}", line):
            continue
        # "AMUSEMENTS AND SPORTS1" — the compiler's footnote marker.
        return re.sub(r"\d+$", "", line).strip(" .") or None
    return None


def split_titles(text: str) -> list[tuple[str, Optional[str], str]]:
    """Cut a code PDF into (title_number, title_name, body) blocks."""
    anchors = list(TITLE_RE.finditer(text))
    blocks: list[tuple[str, Optional[str], str]] = []
    for i, m in enumerate(anchors):
        stop = anchors[i + 1].start() if i + 1 < len(anchors) else len(text)
        blocks.append((m.group(1), _title_name(text, m.end()), text[m.end():stop]))
    return blocks


def split_sections(block: str, title_no: str) -> list[dict]:
    """Cut a title block into one unit per ``§ n`` anchor."""
    anchors = list(SECTION_RE.finditer(block))
    if not anchors:
        return []

    structure = sorted(
        [(m.start(), "chapter", m.group(1)) for m in CHAPTER_RE.finditer(block)]
        + [(m.start(), "article", m.group(1)) for m in ARTICLE_RE.finditer(block)]
        + [(m.start(), "part", f"{m.group(1)}. {m.group(2)}".strip(" ."))
           for m in PART_RE.finditer(block)]
    )

    units: dict[str, dict] = {}
    for i, m in enumerate(anchors):
        stop = anchors[i + 1].start() if i + 1 < len(anchors) else len(block)
        body = _tidy(block[m.end():stop])
        section = m.group(1).rstrip(".")
        key = _slug(f"{title_no}-{section}")
        # A number can repeat where an amendment volume restates the base
        # text; the longest body is the provision, the short one a caption.
        if len(body) < MIN_SECTION_CHARS or len(body) <= len(units.get(key, {}).get("text", "")):
            continue
        chapter = article = part = None
        for pos, kind, value in structure:
            if pos > m.start():
                break
            if kind == "part":
                part, chapter, article = value, None, None
            elif kind == "chapter":
                chapter, article = value, None
            else:
                article = value
        units[key] = {
            "key": key,
            "citation": f"{title_no} CNCA § {section}",
            "section": section,
            "chapter": chapter,
            "article": article,
            "part": part,
            "heading": _clean(m.group(2)).strip(" .-") or None,
            "text": body,
        }
    return list(units.values())


def split_instruments(front: str) -> list[dict]:
    """The Act of Union and the three Constitutions that precede TITLE 1."""
    anchors = list(INSTRUMENT_RE.finditer(front))
    out: list[dict] = []
    for i, m in enumerate(anchors):
        stop = anchors[i + 1].start() if i + 1 < len(anchors) else len(front)
        name = _clean(m.group(1))
        body = _tidy(front[m.start():stop])
        if len(body) < MIN_SECTION_CHARS:
            continue
        year = re.search(r"\b(1[89]\d{2}|20\d{2})\b", name)
        out.append(
            {
                "key": _slug(name),
                "citation": name.title(),
                "section": None,
                "chapter": None,
                "article": None,
                "part": None,
                "heading": name.title(),
                "text": body,
                "instrument_year": int(year.group(1)) if year else None,
            }
        )
    return out


class CherokeeNationCodeScraper(BaseScraper):

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
                ),
                "Referer": INDEX_URL,
            }
        )
        self.delay = 1.0
        # Sample mode takes one section per title, from titles spread across
        # the whole code, so the samples span it instead of all landing in
        # title 1 chapter 1.
        self.per_title_cap = 0
        self.spread_titles = 0

    # ---- low-level ----------------------------------------------------------

    def _get(self, url: str, binary: bool = False, retries: int = 3):
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                r = self.session.get(url, timeout=300)
                if r.status_code == 200:
                    return r.content if binary else r.text
                logger.warning(f"HTTP {r.status_code} for {url}")
                if r.status_code == 404:
                    return None
            except Exception as e:
                logger.warning(f"GET error {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- index parsing ------------------------------------------------------

    def _documents(self) -> list[dict]:
        """The consolidated code PDF and the amendment volumes, with their dates."""
        page = self._get(INDEX_URL)
        if not page:
            raise RuntimeError(f"tribal code index unreachable: {INDEX_URL}")

        docs: list[dict] = []
        for m in re.finditer(
            r'(?is)<li>\s*<a[^>]*href="(/media/[^"]+\.pdf)"[^>]*>(.*?)</a>(.*?)</li>', page
        ):
            href, label, meta = m.group(1), _clean(m.group(2)), m.group(3)
            um, cm = UPDATED_RE.search(meta), CREATED_RE.search(meta)
            date = None
            for match in (um, cm):
                if match:
                    date = _iso(int(match.group(1)), int(match.group(2)), int(match.group(3)))
                    break
            # The consolidated volume is the one whose file name is not a
            # single-title amendment.
            amendment = bool(re.search(r"amend", href + label, re.I))
            docs.append(
                {
                    "url": urljoin(BASE, href),
                    "label": label or href.rsplit("/", 1)[-1],
                    "doc_type": "amendment" if amendment else "code",
                    "date": date,
                    "slug": _slug(re.sub(r"\.pdf$", "", href.rsplit("/", 1)[-1], flags=re.I)),
                }
            )

        if not any(d["doc_type"] == "code" for d in docs):
            raise RuntimeError(
                f"no consolidated code PDF on {INDEX_URL} — layout changed? "
                f"(found: {[d['label'] for d in docs]})"
            )
        # Consolidated volume first so a capped sample run always covers it.
        docs.sort(key=lambda d: (d["doc_type"] != "code", d["label"]))
        logger.info(
            f"index: {len(docs)} PDFs "
            f"({sum(1 for d in docs if d['doc_type'] == 'amendment')} amendment volumes)"
        )
        return docs

    # ---- PDF text -----------------------------------------------------------

    def _pdf_text(self, pdf: bytes, doc_id: str) -> tuple[Optional[str], Optional[int]]:
        """PyMuPDF first: the TITLE/§ anchors only survive as standalone lines
        in its raw output. The shared cascade is the fallback."""
        pages = None
        if fitz is not None:
            try:
                doc = fitz.open(stream=pdf, filetype="pdf")
                pages = doc.page_count
                text = "\n".join(page.get_text() for page in doc)
                doc.close()
                if len(text.strip()) >= MIN_DOC_CHARS:
                    return text, pages
            except Exception as e:
                logger.warning(f"PyMuPDF failed for {doc_id}: {e}")
        text = extract_pdf_markdown(
            "US/CherokeeNationCode", doc_id, pdf_bytes=pdf, table="legislation"
        )
        if text and len(text.strip()) >= MIN_DOC_CHARS:
            return text, pages
        return None, pages

    # ---- normalize ----------------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        text = raw.get("text") or ""
        if len(text) < MIN_SECTION_CHARS:
            return None

        citation, heading = raw.get("citation"), raw.get("heading")
        title = f"{citation} — {heading}" if citation and heading else (citation or heading or raw["label"])
        if raw["doc_type"] == "amendment" and citation:
            title += " (2021 amendments)"

        return {
            "_id": f"CNCA-{raw['doc_slug']}-{raw['key']}",
            "_source": "US/CherokeeNationCode",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title[:500],
            "text": text,
            "date": raw.get("date"),
            "url": raw["url"],
            "index_url": INDEX_URL,
            "document_type": raw["doc_type"] if raw.get("section") else (
                "constitution" if "CONSTITUTION" in (heading or "").upper() else "founding_document"
            ),
            "document": raw["label"],
            "citation": citation,
            "title_number": raw.get("title_no"),
            "title_name": raw.get("title_name"),
            "section": raw.get("section"),
            "chapter": raw.get("chapter"),
            "article": raw.get("article"),
            "part": raw.get("part"),
            "section_heading": heading,
            "pages": raw.get("pages"),
            "language": "en",
            "jurisdiction": "US-CHEROKEE-NATION",
            "publisher": "Cherokee Nation Office of the Attorney General",
        }

    # ---- iteration ----------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        sections = 0
        for doc in self._documents():
            pdf = self._get(doc["url"], binary=True)
            if not pdf or pdf[:4] != b"%PDF":
                logger.warning(f"not a PDF: {doc['url']}")
                continue
            text, pages = self._pdf_text(pdf, f"CNCA-{doc['slug']}")
            if not text:
                logger.warning(f"no extractable text: {doc['url']}")
                continue

            common = {**doc, "doc_slug": doc["slug"], "pages": pages}

            if doc["doc_type"] == "code":
                blocks = split_titles(text)
                if not blocks:
                    raise RuntimeError(
                        f"{doc['label']}: 0 TITLE anchors in {len(text)} chars — "
                        "the PDF layout changed or a scan replaced the text layer"
                    )
                # Everything before TITLE 1 is the Act of Union and the
                # Constitutions; they carry no § numbering.
                front = text[: TITLE_RE.search(text).start()]
                for unit in split_instruments(front):
                    sections += 1
                    yield {**common, "title_no": None, "title_name": None, **unit}
            else:
                # An amendment volume covers exactly one title and heads
                # itself "Title 21 Amendments" rather than with the code's
                # standalone "TITLE 21" anchor, so the number comes from the
                # file name and the whole PDF is that title's block.
                m = re.search(r"title-(\d{1,3}[a-z]?)", doc["slug"], re.I)
                if not m:
                    logger.warning(f"cannot infer title number for {doc['label']} — skipped")
                    continue
                blocks = [(m.group(1).upper(), doc["label"], text)]

            picked = blocks
            if self.spread_titles and len(blocks) > self.spread_titles:
                last = len(blocks) - 1
                picks = sorted({round(i * last / max(1, self.spread_titles - 1))
                                for i in range(self.spread_titles)})
                picked = [blocks[i] for i in picks]

            for title_no, title_name, block in picked:
                if (title_name or "").upper().strip("[]") == "RESERVED":
                    continue
                units = split_sections(block, title_no)
                if not units:
                    continue
                if self.per_title_cap:
                    # Sampling: a title's first section is always the short
                    # "Short title" provision, so take a typical one instead.
                    units = sorted(units, key=lambda u: len(u["text"]))
                    units = [units[len(units) // 2]][: self.per_title_cap]
                sections += len(units)
                for unit in units:
                    yield {**common, "title_no": title_no,
                           "title_name": title_name, **unit}

            logger.info(f"{doc['label']}: {len(blocks)} titles, {sections} records so far")

        if not self.spread_titles and sections < MIN_EXPECTED_SECTIONS:
            raise RuntimeError(
                f"only {sections} sections parsed (expected >= {MIN_EXPECTED_SECTIONS}) "
                "— the consolidated PDF changed shape or was truncated"
            )

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # The AG republishes the volume in place, so the only reliable refresh
        # is to re-walk it; the loader dedups on _id.
        yield from self.fetch_all()

    # ---- connectivity test --------------------------------------------------

    def test_api(self) -> bool:
        self.per_title_cap, self.spread_titles = 1, 3
        try:
            for raw in self.fetch_all():
                rec = self.normalize(raw)
                if rec:
                    logger.info(
                        f"test-api normalize: {rec['_id']} — {len(rec['text'])} chars, "
                        f"citation={rec['citation']}, date={rec['date']}"
                    )
                    return True
            logger.error("test-api: no section with extractable text")
            return False
        except Exception as e:
            logger.error(f"test-api FAILED: {e}")
            return False
        finally:
            self.per_title_cap = self.spread_titles = 0


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/CherokeeNationCode bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test-api"], help="Command"
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    scraper = CherokeeNationCodeScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        return

    if args.sample:
        scraper.per_title_cap = 1
        scraper.spread_titles = 10  # + the 4 founding instruments = 14 candidates
    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
