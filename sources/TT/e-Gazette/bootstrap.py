"""
Legal Data Hunter — TT/e-Gazette

Trinidad & Tobago Official Gazette / Government Printery e-Gazette.

Source site (open Apache directory listing):
  https://printery.gov.tt/e-gazette/

Access strategy (no API; open directory of per-document PDFs):
  1. The e-gazette root lists year directories (2000, 2001, 2023-2026).
  2. Recent years (2023+) are organized into subfolders:
       Acts/            -> enacted Acts of Parliament (legislation)
       Legal Notices/   -> statutory instruments / subsidiary legislation
       Gazette(s)/, Bills/, Hansard/, Debate.../  (not collected here)
     We collect "Acts" and "Legal Notices" only — both are legislation with
     full text. Bills are not yet law; Gazette compilations and Hansard
     debates are out of scope for the legislation namespace.
  3. Each PDF is digital-native; full text is extracted with pdfplumber.

Records whose PDF yields no extractable text (scanned-image documents) are
skipped — this source only contributes full-text records.

Two upstream quirks are corrected here (#1412):

  * **Bundled Legal Notices.** A handful of PDFs in the "Legal Notices"
    folders carry a *range* in the filename ("Legal Notice No. 177-190 of
    2023.pdf") and contain many separate statutory instruments in one file.
    Stored as a single record they defeat article-level segmentation, since
    fourteen instruments' section numbering collides inside one "document".
    Such files are split on their `LEGAL NOTICE NO. n` headers into one
    record per notice, each with the same `_id` shape a standalone notice
    would have (`TT-GAZ-Legal-Notice-No-{n}-of-{year}`).

  * **Overlaid duplicate text.** Some Acts (e.g. Act No. 2 of 2026) ship
    schedule pages whose text is drawn twice at a slight x/y offset. Because
    the two copies land inside pdfplumber's line-merge tolerance, their glyphs
    sort together and the line comes out interleaved
    ("ItIetmem FIFRIRSTS TC COOLULUMMNN"). `_page_text` detects that signature
    and drops the redundant copy before extracting.
"""

import io
import re
import sys
import time
import difflib
import logging
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin, unquote

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter")

BASE = "https://www.printery.gov.tt/e-gazette/"

# Minimum extracted characters for a PDF to count as "full text" (filters
# out scanned-image documents that yield little/no text).
MIN_TEXT_CHARS = 400

# Folder names (case-insensitive substring match) we treat as legislation.
WANTED_FOLDERS = ("act", "legal notice")

# "Legal Notice No. 177-190 of 2023.pdf" — a single PDF holding a run of
# separate instruments. Matched on the filename so only these few files pay
# for an extra download during enumeration.
BUNDLE_RANGE_RE = re.compile(r"No\.?\s*(\d+)\s*[-–—]\s*(\d+)")

# Start-of-notice header inside a bundled Legal Supplement PDF.
NOTICE_HEADER_RE = re.compile(r"^[ \t]*LEGAL\s+NOTICE\s+NO\.?\s*(\d+)\b", re.M)

# The running header the Government Printer puts at the top of every page; it
# belongs to the page, not to the notice that happens to start below it.
SUPPLEMENT_HEADER_RE = re.compile(
    r"^\s*(?:\d+\s+)?Legal Supplement Part [A-Z]\b.*$", re.M | re.I)

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}


class SourceScraper(BaseScraper):
    """Scraper for TT/e-Gazette (legislation, full text via PDF)."""

    def __init__(self):
        super().__init__(Path(__file__).parent)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (compatible; LegalDataHunter/1.0; open legal data research)"
            ),
            "Accept-Language": "en-TT,en;q=0.9",
        })

    # ── HTTP helpers ──────────────────────────────────────────────

    def _get(self, url: str) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=90)
                if resp.status_code == 200:
                    return resp
                logger.debug(f"HTTP {resp.status_code} for {url}")
            except requests.RequestException as e:
                logger.debug(f"Request error for {url}: {e}")
            time.sleep(2 * (attempt + 1))
        return None

    @staticmethod
    def _list_links(html: str) -> list:
        """Return href values from an Apache directory listing, excluding
        sort/parent links."""
        out = []
        for href in re.findall(r'href="([^"?][^"]*)"', html):
            if href.startswith("/") or href.startswith("http"):
                continue
            out.append(href)
        return out

    # ── Enumeration ───────────────────────────────────────────────

    def _list_years(self) -> list:
        # The /e-gazette/ index now 302-redirects to the printery homepage,
        # whose year links are ABSOLUTE URLs (e.g. ".../e-gazette/2026/") rather
        # than the relative "2026/" of the old Apache listing. Match both forms
        # so a redesign of the index page can't silently yield 0 years (#1130).
        resp = self._get(BASE)
        if not resp:
            return []
        years = sorted(
            {int(m) for m in re.findall(r'href="(?:[^"]*/e-gazette/)?(\d{4})/"', resp.text)},
            reverse=True,
        )
        return years

    def _list_wanted_folders(self, year: int) -> list:
        """Return (category, folder_url) for Acts / Legal Notices folders of a year."""
        year_url = urljoin(BASE, f"{year}/")
        resp = self._get(year_url)
        if not resp:
            return []
        folders = []
        for href in self._list_links(resp.text):
            if not href.endswith("/"):
                continue
            name = unquote(href).rstrip("/")
            low = name.lower()
            if any(w in low for w in WANTED_FOLDERS):
                folders.append((name, urljoin(year_url, href)))
        return folders

    def _list_pdfs(self, folder_url: str) -> list:
        resp = self._get(folder_url)
        if not resp:
            return []
        pdfs = []
        for href in self._list_links(resp.text):
            if href.lower().endswith(".pdf"):
                pdfs.append(urljoin(folder_url, href))
        return pdfs

    # ── Per-document fetch ────────────────────────────────────────

    def _extract_pdf_text(self, pdf_url: str) -> str:
        resp = self._get(pdf_url)
        if not resp:
            return ""
        data = resp.content
        head = data.find(b"%PDF")
        if head > 0:
            data = data[head:]
        try:
            with pdfplumber.open(io.BytesIO(data)) as pdf:
                parts = [_page_text(page) for page in pdf.pages]
        except Exception as e:
            logger.debug(f"PDF parse failed for {pdf_url}: {e}")
            return ""
        text = "\n".join(parts)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        return text

    # ── Abstract methods ──────────────────────────────────────────

    def _emit(self, year: int, category: str, pdf_url: str) -> Generator[dict, None, None]:
        """Yield one raw item per *instrument* in a PDF.

        Ordinary files yield a single item and leave the download to
        normalize(). Files whose name carries a notice range hold several
        instruments, so they are fetched here and split; each segment carries
        its own text so the PDF is still downloaded only once.
        """
        filename = unquote(pdf_url.rsplit("/", 1)[-1])
        base = {"year": year, "category": category, "pdf_url": pdf_url, "filename": filename}

        if not BUNDLE_RANGE_RE.search(filename):
            yield base
            return

        text = self._extract_pdf_text(pdf_url)
        segments = _split_bundle(text)
        if not segments:
            # Range in the name but no per-notice headers to cut on — keep it
            # whole rather than silently dropping the document.
            logger.warning(f"Bundled name but no notice headers, storing whole: {filename}")
            yield dict(base, text=text)
            return

        logger.info(f"Split {filename} into {len(segments)} notices")
        bundle_date = _first_date(text)
        for number, body in segments:
            yield dict(
                base,
                text=body,
                notice_number=number,
                bundle_filename=filename,
                fallback_date=bundle_date,
            )

    def fetch_all(self) -> Generator[dict, None, None]:
        for year in self._list_years():
            folders = self._list_wanted_folders(year)
            if not folders:
                continue
            for category, folder_url in folders:
                pdfs = self._list_pdfs(folder_url)
                logger.info(f"Year {year} / {category}: {len(pdfs)} PDFs")
                for pdf_url in pdfs:
                    yield from self._emit(year, category, pdf_url)
                    time.sleep(0.3)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        # The directory listing exposes no per-file modified filter; restrict the
        # year sweep to the year of `since` onward. Dedup is handled by the base class.
        start_year = since.year
        for year in self._list_years():
            if year < start_year:
                break
            for category, folder_url in self._list_wanted_folders(year):
                for pdf_url in self._list_pdfs(folder_url):
                    yield from self._emit(year, category, pdf_url)
                    time.sleep(0.3)

    def normalize(self, raw: dict) -> Optional[dict]:
        # Bundled notices arrive pre-split with their own text; everything else
        # is downloaded here so the concurrent path can overlap the fetches.
        text = raw.get("text") or self._extract_pdf_text(raw["pdf_url"])
        if len(text) < MIN_TEXT_CHARS:
            # Scanned-image document without OCR-able text — skip (no full text).
            return None

        filename = raw["filename"]
        notice_number = raw.get("notice_number")
        if notice_number is not None:
            doc_number = f"Legal Notice No. {notice_number} of {raw['year']}"
            subject = _subject_from_text(text)
            title = f"{doc_number} — {subject}" if subject else doc_number
            doc_id = f"TT-GAZ-{_slug(doc_number)}"
        else:
            title, doc_number = _title_from_filename(filename, text)
            doc_id = f"TT-GAZ-{_slug(filename)}"

        # For a split notice the gazette's own supplement header is the
        # publication date; the first date inside the body is often a survey or
        # plan date years earlier, so the header wins when we have it.
        date = raw.get("fallback_date") or _first_date(text) or f"{raw['year']}-01-01"

        record = {
            "_id": doc_id,
            "_source": "TT/e-Gazette",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": raw["pdf_url"],
            "pdf_url": raw["pdf_url"],
            "document_number": doc_number,
            "category": raw["category"],
            "year": raw["year"],
            "language": "en",
            "jurisdiction": "TT",
        }
        if raw.get("bundle_filename"):
            record["bundle_filename"] = raw["bundle_filename"]
        return record


# ── Overlaid-duplicate-text removal ───────────────────────────────

def _draw_runs(page) -> list:
    """Group a page's characters into draw runs — one entry per distinct text
    baseline. Two overlaid copies of the same line sit on baselines a fraction
    of a point apart, so they stay separate here even though pdfplumber's
    3pt line tolerance would merge (and interleave) them."""
    by_top = defaultdict(list)
    for ch in page.chars:
        by_top[round(ch["top"], 2)].append(ch)
    runs = []
    for top in sorted(by_top):
        chars = sorted(by_top[top], key=lambda c: c["x0"])
        text = re.sub(r"\s+", " ", "".join(c["text"] for c in chars)).strip()
        if not text:
            continue
        runs.append({
            "top": top,
            "x0": chars[0]["x0"],
            "x1": max(c["x1"] for c in chars),
            "text": text,
            "chars": chars,
        })
    return runs


def _x_overlap(a: dict, b: dict) -> float:
    """Overlap of two runs' x-extents as a fraction of the narrower run."""
    overlap = min(a["x1"], b["x1"]) - max(a["x0"], b["x0"])
    return overlap / max(1.0, min(a["x1"] - a["x0"], b["x1"] - b["x0"]))


def _redundant_runs(runs: list) -> set:
    """Return the `top` keys of runs that are a second rendering of another
    run, or None if the page carries no overlaid text at all.

    Pass 1 only looks for an exact repeat within 2pt — that is the unambiguous
    signature of a doubled draw. The looser passes run *only* on pages where
    pass 1 found something, so ordinary pages are never touched.
    """
    drop = set()
    for i, a in enumerate(runs):
        for b in runs[i + 1:]:
            if b["top"] - a["top"] > 2:
                break
            if len(a["text"]) < 8 or len(b["text"]) < 8 or _x_overlap(a, b) < 0.5:
                continue
            if a["text"] == b["text"] or b["text"] in a["text"]:
                drop.add(b["top"])
            elif a["text"] in b["text"]:
                drop.add(a["top"])
    if not drop:
        return None

    # Pass 2: the two copies often wrap their cell text differently, so the
    # repeat is near-identical rather than identical. Keep the longer line.
    live = [r for r in runs if r["top"] not in drop]
    for i, a in enumerate(live):
        if a["top"] in drop:
            continue
        for b in live[i + 1:]:
            if b["top"] - a["top"] > 13:
                break
            if b["top"] in drop or len(a["text"]) < 8 or len(b["text"]) < 8:
                continue
            if abs(a["x0"] - b["x0"]) > 15 or _x_overlap(a, b) < 0.5:
                continue
            if difflib.SequenceMatcher(None, a["text"], b["text"]).ratio() < 0.75:
                continue
            drop.add((b if len(b["text"]) <= len(a["text"]) else a)["top"])

    # Pass 3: the two copies wrap differently, so a line of one can land on top
    # of an unrelated line of the other and interleave again. Only sub-point
    # offsets at an identical font size qualify — small capitals sit ~2pt below
    # their leading capital at a different size, and marginal markers like "(a)"
    # share a band with the row text they annotate; neither is a shadow copy.
    live = [r for r in runs if r["top"] not in drop]
    for i, a in enumerate(live):
        if a["top"] in drop:
            continue
        for b in live[i + 1:]:
            if b["top"] - a["top"] > 1.0:
                break
            if b["top"] in drop or _x_overlap(a, b) < 0.5:
                continue
            if len(a["text"]) < 8 or len(b["text"]) < 8:
                continue
            if round(a["chars"][0]["size"], 1) != round(b["chars"][0]["size"], 1):
                continue
            drop.add((b if len(b["text"]) <= len(a["text"]) else a)["top"])
    return drop


def _page_text(page) -> str:
    """Extract a page's text, first removing overlaid duplicate renderings."""
    try:
        drop = _redundant_runs(_draw_runs(page))
    except Exception as e:  # geometry is best-effort; never lose the page
        logger.debug(f"Overlay detection failed: {e}")
        drop = None
    if not drop:
        return page.extract_text() or ""
    kept = page.filter(
        lambda obj: obj.get("object_type") != "char"
        or round(obj["top"], 2) not in drop
    )
    return kept.extract_text() or ""


# ── Bundled Legal Notices ─────────────────────────────────────────

def _split_bundle(text: str) -> list:
    """Split a Legal Supplement PDF holding several notices into
    [(notice_number, notice_text), ...]. Returns [] if it holds only one."""
    marks = list(NOTICE_HEADER_RE.finditer(text))
    if len(marks) < 2:
        return []
    segments = []
    for i, mark in enumerate(marks):
        end = marks[i + 1].start() if i + 1 < len(marks) else len(text)
        body = text[mark.start():end]
        # The page header printed above the *next* notice was swept into this
        # segment by the split; it names the following page, not this notice.
        body = SUPPLEMENT_HEADER_RE.sub("", body).strip()
        body = re.sub(r"\n{3,}", "\n\n", body)
        segments.append((int(mark.group(1)), body))
    return segments


# ── Helpers ───────────────────────────────────────────────────────

def _slug(filename: str) -> str:
    s = re.sub(r"\.pdf$", "", filename, flags=re.I)
    s = re.sub(r"[^A-Za-z0-9]+", "-", s).strip("-")
    return s


def _title_from_filename(filename: str, text: str) -> tuple:
    """Derive (title, document_number) from the PDF filename, falling back to
    the document text where the filename carries no descriptive title."""
    base = re.sub(r"\.pdf$", "", filename, flags=re.I).replace("_", " ").strip()

    # Acts: "Act No. 3 of 2026 - The Copyright (Amendment) Act, 2026"
    m = re.match(r"(Act\s+No\.?\s*\d+\s+of\s+\d{4})\s*[-–]\s*(.+)", base, re.I)
    if m:
        return m.group(2).strip(), m.group(1).strip()

    # Legal Notice / Gazette with no descriptive title in filename:
    # use the filename as a label and try to pull a subject line from the text.
    doc_number = base
    subject = _subject_from_text(text)
    # Avoid "Act No. 2 of 2026 — Act No. 2 of 2026" style duplication when the
    # text subject just echoes the filename label.
    if subject and subject.lower() not in base.lower() and base.lower() not in subject.lower():
        title = f"{base} — {subject}"
    else:
        title = base
    return title, doc_number


# The instrument's own citation, set in caps in the notice head, e.g.
# "THE LAND ACQUISITION (POSSESSION OF LAND PRIOR TO / FORMAL VESTING IN THE
# STATE) (NO. 2) ORDER, 2024" — wrapped over two lines, so the class spans \s.
# Anchored at a line start because the head also names the parent Act and the
# making power ("MADE BY THE PRESIDENT UNDER SECTION 4(1) OF THE ...") and only
# the citation begins its own line.
CITATION_RE = re.compile(
    r"^THE\s+[A-Z0-9''‘’()./\-–,:;\s]{5,200}?"
    r"(?:ORDER|REGULATIONS|RULES|BY-?LAWS|NOTICE|PROCLAMATION|ACT)"
    r"\s*,\s*\d{4}\b", re.M)

# Standalone instrument-type headings used when there is no cited short title.
HEADING_WORDS = {
    "A PROCLAMATION", "PROCLAMATION", "ORDER", "ORDERS", "NOTICE",
    "RESOLUTION", "ERRATUM", "REGULATIONS", "RULES", "DIRECTIONS",
    "APPOINTMENT", "WARRANT",
}


# Head lines that are furniture rather than the notice's subject.
BOILERPLATE_RE = re.compile(
    r"^\s*(?:LEGAL NOTICE NO|REPUBLIC OF TRINIDAD|PRINTED AND PUBLISHED|\[|"
    r"No\.\s*\d+\s+of\s+\d{4}\.?\s*$)|CHAP\.", re.I)


def _last_caps_block(text: str) -> str:
    """The final run of consecutive capitalised heading lines before the body —
    e.g. "NOTICE OF LAND LIKELY TO BE REQUIRED / FOR A PUBLIC PURPOSE" — used
    when the instrument has no cited short title."""
    blocks, current = [], []
    for line in text.splitlines()[:30]:
        stripped = line.strip()
        letters = [c for c in stripped if c.isalpha()]
        is_heading = (
            len(letters) >= 3
            and sum(c.isupper() for c in letters) / len(letters) >= 0.9
            and not BOILERPLATE_RE.search(stripped)
        )
        if is_heading:
            current.append(stripped)
            continue
        if current:
            blocks.append(" ".join(current))
            current = []
        # Stop at the first line of running prose — anything in caps below it
        # is a schedule heading or the signatory block, not the subject.
        if len(stripped) > 60 and any(c.islower() for c in stripped):
            break
    if current:
        blocks.append(" ".join(current))
    return blocks[-1][:200] if blocks else ""


def _subject_from_text(text: str) -> str:
    """Best-effort subject line for a Legal Notice.

    The notice head carries the instrument's own short title in capitals; that
    is a far better subject than any body line, so try it first. Body prose is
    only a last resort because line wrapping means an arbitrary body line
    usually starts mid-sentence.
    """
    head = text[:2000]
    # Candidate citations nest: one starting at the parent Act's line runs all
    # the way through to the instrument's own trailing year. The latest-starting
    # candidate is therefore the instrument itself.
    citation, pos = None, 0
    while True:
        found = CITATION_RE.search(head, pos)
        if not found:
            break
        citation, pos = found.group(0), found.start() + 1
    if citation:
        return re.sub(r"\s+", " ", citation).strip()

    for line in text.splitlines()[:20]:
        if line.strip().upper() in HEADING_WORDS:
            return line.strip()

    # Acts printed without a descriptive filename carry their long title in the
    # enacting formula ("AN ACT to amend the Motor Vehicles and Road Traffic
    # Act, Chap. 48:50, ..."), which runs past several abbreviating periods.
    long_title = re.search(r"\bAN ACT\b.{5,220}", re.sub(r"\s+", " ", text[:2000]))
    if long_title:
        return long_title.group(0).rsplit(" ", 1)[0].rstrip(",;") + "…"

    heading = _last_caps_block(text)
    if heading:
        return heading

    for line in text.splitlines():
        line = line.strip()
        if len(line) < 8 or len(line) > 160:
            continue
        low = line.lower()
        if low.startswith(("legal notice", "trinidad and tobago gazette", "vol.", "republic of")):
            continue
        # A subject line typically references an Act/Order/Regulations.
        if re.search(r"\b(Act|Order|Regulations|Rules|Notice|Proclamation)\b", line):
            return re.sub(r"\s+", " ", line)
    return ""


def _first_date(text: str) -> Optional[str]:
    """Find the first 'Nth Month, YYYY' or 'Month DD, YYYY' date and return ISO."""
    # 12th February, 2026  /  1st January 2025
    m = re.search(
        r"(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+),?\s+(\d{4})", text)
    if m:
        d, mon, y = m.group(1), m.group(2).lower(), m.group(3)
        if mon in MONTHS:
            return f"{int(y):04d}-{MONTHS[mon]:02d}-{int(d):02d}"
    # February 12, 2026
    m = re.search(r"([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})", text)
    if m:
        mon, d, y = m.group(1).lower(), m.group(2), m.group(3)
        if mon in MONTHS:
            return f"{int(y):04d}-{MONTHS[mon]:02d}-{int(d):02d}"
    return None


# ── CLI Entry Point ───────────────────────────────────────────────

def main():
    scraper = SourceScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    # bootstrap-fast is the VPS fleet entrypoint; alias it to the full bootstrap
    # path so it runs the full corpus (streamed to data/records.jsonl by
    # BaseScraper) instead of erroring out and falling back to samples (#1130).
    if command in ("bootstrap", "bootstrap-fast"):
        if sample_mode and command != "bootstrap-fast":
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, {stats['records_updated']} updated, {stats['records_skipped']} skipped")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

    import json
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
