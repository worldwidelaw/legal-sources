#!/usr/bin/env python3
"""
US/WA-BIIA -- Washington Board of Industrial Insurance Appeals, Significant Decisions.

The Board of Industrial Insurance Appeals hears appeals from orders of the
Washington Department of Labor & Industries under the Industrial Insurance Act,
Title 51 RCW. Most of its decisions are unpublished "Decisions and Orders"; a
small, curated set is designated **Significant Decisions** under RCW 51.52.160
and WAC 263-12-195, and those are the Board's precedential body of workers'
compensation case law, cited as `In re Christopher Aalmo, BIIA Dec., 87 4382
(1989)`.

Access notes
------------
`www.biia.wa.gov` does not complete a TLS handshake (the manifest entry's
"connection error"); the apex host `biia.wa.gov` answers 200 for everything and
is what this scraper uses.

There is no API and no directory listing, but the Board publishes two static
index pages that between them enumerate the whole Significant Decision corpus:

  * ``/SDNameIndex.html``    -- one table row per decision: case name, docket
                               number, year, and the href of the PDF.
  * ``/SDSubjectIndex.html`` -- the same decisions filed under the Board's own
                               subject headings (``AGGRAVATION (RCW 51.32.160)``
                               and sub-headings), with the published headnote.

Neither index is a superset of the other -- a handful of decisions appear only
in the subject index, and one name-index row lost its link -- so both are
parsed and unioned. The subject index also supplies the subject headings and
the headnote text that the name index has no column for.

Each decision is a born-digital PDF at ``/SDPDF/{docket}.pdf`` carrying the
headnote page followed by the full Board order, so the text layer extracts
cleanly.

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap          # full pull
  python bootstrap.py bootstrap-fast     # full pull (VPS wrapper alias)
"""

import sys
import re
import json
import time
import html as html_lib
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.WA-BIIA")

# The apex host, not www: www.biia.wa.gov never completes the TLS handshake.
BASE_URL = "https://biia.wa.gov"
NAME_INDEX_URL = BASE_URL + "/SDNameIndex.html"
SUBJECT_INDEX_URL = BASE_URL + "/SDSubjectIndex.html"

MAX_ATTEMPTS = 5
# The Board's Significant Decision series starts with appeals docketed in the
# 1960s; used only to sanity-check that an index parse is not truncated.
FIRST_YEAR = 1970
# Below this a "decision" is a cover page whose order body failed to extract.
# The tersest Significant Decisions still run several thousand characters once
# the headnote, the findings and the signature block are counted.
MIN_TEXT_CHARS = 800

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

TAG_RE = re.compile(r"<[^>]+>")
TR_RE = re.compile(r"<tr\b[^>]*>(.*?)</tr>", re.S | re.I)
TD_RE = re.compile(r"<td\b[^>]*>(.*?)</td>", re.S | re.I)
SDPDF_HREF_RE = re.compile(r'href="(SDPDF/[^"]+)"', re.I)
YEAR_RE = re.compile(r"\(?((?:19|20)\d{2})\)?")
HEADING_RE = re.compile(r"<h([34])\b[^>]*>(.*?)</h\1>", re.S | re.I)
# The subject index writes each decision as `...In re NAME, BIIA Dec.,
# <a href="SDPDF/...">DOCKET</a> (YEAR)`, so the anchor itself carries the
# docket and the case name is whatever the sentence before it ends with. A
# single headnote sometimes cites two decisions, so paragraphs are split on
# their anchors rather than matched whole.
SD_ANCHOR_RE = re.compile(r'(<a\s[^>]*href="SDPDF/[^"]+"[^>]*>.*?</a>)', re.S | re.I)
CASE_NAME_RE = re.compile(r"In re\s+(.{3,120}?)\s*,\s*BIIA\s+Dec\.,?\s*$", re.I)
TRAILING_YEAR_RE = re.compile(r"^\s*\(((?:19|20)\d{2})\)")
# Where the Board's headnote stops and its citation begins.
HEADNOTE_END_RE = re.compile(r"\bIn re\s", re.I)

# The two signature-block forms the Board has used:
#   "Dated this 10th day of March, 1989."
#   "Dated: August 24, 2009."
# Case-sensitive on "Dated": the body of an order is full of lowercase
# "the Department order dated June 20, 2014" references to the order under
# appeal, and those are not the decision's own date.
DATED_LONG_RE = re.compile(
    r"\bDated\s+this\s+(\d{1,2})\s*(?:st|nd|rd|th)?\s+day\s+of\s+"
    r"([A-Z][a-z]+),?\s+((?:19|20)\d{2})"
)
DATED_SHORT_RE = re.compile(
    r"\bDated:?\s+([A-Z][a-z]+)\s+(\d{1,2}),?\s+((?:19|20)\d{2})"
)

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}


def strip_tags(fragment: str) -> str:
    return re.sub(
        r"\s+", " ", html_lib.unescape(TAG_RE.sub(" ", fragment or ""))
    ).replace(" ", " ").strip()


def _ymd(year: str, month_name: str, day: str) -> Optional[str]:
    month = MONTHS.get(month_name.lower())
    if not month:
        return None
    day_number = int(day)
    if not 1 <= day_number <= 31:
        return None
    return f"{int(year):04d}-{month:02d}-{day_number:02d}"


def decision_date(text: str, year: Optional[int] = None) -> Optional[str]:
    """The signature date of the order.

    The Board's own order is the second half of the PDF (the headnote page
    comes first) and quotes the Department order it reviews, so an early
    "dated October 22, 1987" is the appealed order rather than the decision.
    The index's year is what disambiguates: among the signature dates that fall
    in it, the last one is the date the Board signed. Without an index year the
    last signature date in the document is taken.
    """
    candidates = [
        (match.start(), _ymd(match.group(3), match.group(2), match.group(1)))
        for match in DATED_LONG_RE.finditer(text)
    ] + [
        (match.start(), _ymd(match.group(3), match.group(1), match.group(2)))
        for match in DATED_SHORT_RE.finditer(text)
    ]
    dated = [(position, iso) for position, iso in candidates if iso]
    if year:
        in_year = [pair for pair in dated if int(pair[1][:4]) == year]
        if in_year:
            return max(in_year)[1]
        return None
    return max(dated)[1] if dated else None


def normalize_docket(value: str) -> str:
    """`87 4382` / `93 W469` as the Board writes it, whitespace collapsed."""
    return re.sub(r"[\s ]+", " ", (value or "").strip()).upper()


def pdf_stem(path: str) -> str:
    return re.sub(r"\.pdf$", "", path.rsplit("/", 1)[-1], flags=re.I)


def doc_id(docket: str, path: str) -> str:
    """A stable id per decision, keyed on the docket the Board cites.

    Two decisions in the same appeal are published separately and share one
    docket -- `57009(1).pdf` and `57009(2).pdf` are both *In re Bill Murray*,
    57,009 -- so whatever the file name carries beyond the docket is appended,
    or the ids would collide and the later decision would overwrite the
    earlier one.
    """
    docket = normalize_docket(docket)
    stem = pdf_stem(path)
    key = docket or stem
    stem_alnum = re.sub(r"[^A-Za-z0-9]+", "", stem).upper()
    docket_alnum = re.sub(r"[^A-Za-z0-9]+", "", docket).upper()
    if docket_alnum and stem_alnum.startswith(docket_alnum):
        suffix = stem_alnum[len(docket_alnum):]
        if suffix:
            key = f"{key}-{suffix}"
    return "US-WA-BIIA-SD-" + re.sub(r"[^A-Za-z0-9]+", "-", key.upper()).strip("-")


def _title_cased_name(index_name: str, text: str) -> str:
    """The case name as the decision itself spells it, when it agrees.

    The name index is set in all caps (``AALMO, CHRISTOPHER, DEC'D``) while the
    PDF's first line carries the mixed-case form (``Aalmo, Christopher, Dec'd``)
    that the Board uses in its citations. The PDF wins whenever the two are the
    same string, so a mangled or unrelated first line cannot rename a decision.
    """
    first_line = next(
        (line.strip(" #*\t") for line in (text or "").splitlines() if line.strip()),
        "",
    )
    normalized = re.sub(r"\s+", " ", first_line).strip()
    if normalized and normalized.upper() == re.sub(r"\s+", " ", index_name).upper():
        return normalized
    return index_name


class WABIIAScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.data_dir = Path(__file__).parent / "data"
        self.checkpoint_path = self.data_dir / "wa_biia_checkpoint.json"
        self.rate_limit_delay = float(
            self.config.get("fetch", {}).get("rate_limit_delay", 1)
        )
        self._index = None

    # ------------------------------------------------------------------ HTTP

    def _get(self, url: str, attempts: int = MAX_ATTEMPTS, timeout: int = 90):
        delay = 3.0
        for attempt in range(attempts):
            try:
                resp = self.session.get(url, timeout=timeout)
                resp.raise_for_status()
                return resp
            except Exception as exc:
                logger.warning(
                    f"GET {url} failed (attempt {attempt + 1}/{attempts}): {exc}"
                )
                if attempt < attempts - 1:
                    time.sleep(delay)
                    delay = min(delay * 2, 60.0)
        return None

    def _get_index_page(self, url: str) -> str:
        resp = self._get(url, timeout=180)
        if resp is None:
            raise RuntimeError(
                f"The Significant Decision index at {url} did not answer. It is "
                f"the only enumeration of the decision PDFs ({BASE_URL}/SDPDF/ "
                f"has no directory listing), so the corpus cannot be crawled "
                f"without it. Note that www.biia.wa.gov never completes a TLS "
                f"handshake -- the apex host is the reachable one."
            )
        resp.encoding = resp.encoding or "utf-8"
        return resp.text

    # -------------------------------------------------------------- indexing

    def _parse_name_index(self, html: str) -> dict:
        """`{pdf path (lowercased): entry}` from the case-name index.

        Each row is `<td>case name</td><td><a href=...>docket</a></td>
        <td>(year)</td>`. Rows without a link (the Board has at least one)
        are dropped here and recovered from the subject index.
        """
        entries = {}
        for row in TR_RE.findall(html):
            cells = TD_RE.findall(row)
            href = SDPDF_HREF_RE.search(row)
            if len(cells) != 3 or not href:
                continue
            path = href.group(1)
            year = YEAR_RE.search(strip_tags(cells[2]))
            entries[path.lower()] = {
                "path": path,
                "url": f"{BASE_URL}/{path}",
                "case_name": strip_tags(cells[0]),
                "docket": normalize_docket(strip_tags(cells[1])),
                "year": int(year.group(1)) if year else None,
                "subjects": [],
                "headnote": None,
            }
        return entries

    def _parse_subject_index(self, html: str) -> dict:
        """`{pdf path (lowercased): {subjects, headnote, ...}}`.

        Headings are `<h3>` subjects with optional `<h4>` sub-subjects, and the
        `<p>` under them holds the Board's headnote followed by its citation,
        which is where a subject-index-only decision's case name, docket and
        year come from. A headnote that covers two decisions cites both in the
        same paragraph, so each anchor is read with the text on either side of
        it rather than the paragraph being matched as a whole.
        """
        body = html[html.find('<div id="SD-content">'):] or html
        chunks = re.split(r"(<h[34]\b[^>]*>.*?</h[34]>)", body, flags=re.S | re.I)
        subject = sub_subject = None
        entries = {}
        for chunk in chunks:
            heading = HEADING_RE.match(chunk)
            if heading:
                if heading.group(1) == "3":
                    subject, sub_subject = strip_tags(heading.group(2)), None
                else:
                    sub_subject = strip_tags(heading.group(2))
                continue

            heading_label = subject or ""
            if sub_subject:
                heading_label = f"{heading_label} — {sub_subject}".strip(" —")

            for paragraph in re.split(r"</p>", chunk, flags=re.I):
                segments = SD_ANCHOR_RE.split(paragraph)
                # split() alternates text, anchor, text, anchor, ..., text
                for position in range(1, len(segments), 2):
                    anchor = segments[position]
                    href = SDPDF_HREF_RE.search(anchor)
                    if not href:
                        continue
                    path = href.group(1)
                    entry = entries.setdefault(
                        path.lower(),
                        {
                            "path": path,
                            "url": f"{BASE_URL}/{path}",
                            "case_name": None,
                            "docket": None,
                            "year": None,
                            "subjects": [],
                            "headnote": None,
                        },
                    )
                    if heading_label and heading_label not in entry["subjects"]:
                        entry["subjects"].append(heading_label)

                    before = strip_tags(segments[position - 1])
                    after = strip_tags(segments[position + 1])
                    if not entry["docket"]:
                        entry["docket"] = normalize_docket(strip_tags(anchor))
                    case_name = CASE_NAME_RE.search(before)
                    if case_name and not entry["case_name"]:
                        entry["case_name"] = case_name.group(1).strip(" ,")
                    year = TRAILING_YEAR_RE.search(after)
                    if year and not entry["year"]:
                        entry["year"] = int(year.group(1))
                    # The headnote runs from the start of the paragraph to the
                    # first citation; the first (most specific) subject the
                    # decision is filed under supplies the wording.
                    if not entry["headnote"]:
                        cut = HEADNOTE_END_RE.search(before)
                        headnote = (before[: cut.start()] if cut else before)
                        headnote = headnote.strip().strip(".…").strip()
                        if len(headnote) > 40:
                            entry["headnote"] = headnote
        return entries

    def index(self) -> list:
        """Every Significant Decision, from both index pages, oldest first."""
        if self._index is not None:
            return self._index

        entries = self._parse_name_index(self._get_index_page(NAME_INDEX_URL))
        if len(entries) < 500:
            raise RuntimeError(
                f"The case-name index at {NAME_INDEX_URL} yielded only "
                f"{len(entries)} decisions; the Board publishes roughly 950. "
                f"The page layout has changed or the response was truncated."
            )

        by_subject = self._parse_subject_index(
            self._get_index_page(SUBJECT_INDEX_URL)
        )
        recovered = 0
        for key, subject_entry in by_subject.items():
            entry = entries.get(key)
            if entry is None:
                # Only in the subject index: its citation carries the metadata.
                entries[key] = subject_entry
                recovered += 1
                continue
            entry["subjects"] = subject_entry["subjects"]
            entry["headnote"] = subject_entry["headnote"]
            for field in ("case_name", "docket", "year"):
                if not entry.get(field):
                    entry[field] = subject_entry.get(field)

        rows = sorted(
            entries.values(),
            key=lambda entry: (entry.get("year") or 0, entry["path"].lower()),
        )
        logger.info(
            f"index: {len(rows)} significant decisions "
            f"({recovered} found only in the subject index, "
            f"{sum(1 for r in rows if r['subjects'])} with subject headings)"
        )
        self._index = rows
        return rows

    def years(self) -> list:
        return sorted({row["year"] for row in self.index() if row.get("year")})

    def year_rows(self, year: int) -> list:
        return [row for row in self.index() if row.get("year") == year]

    # ------------------------------------------------------------ checkpoint

    def _load_checkpoint(self) -> list:
        try:
            with open(self.checkpoint_path, encoding="utf-8") as f:
                return [int(year) for year in json.load(f).get("years_done", [])]
        except FileNotFoundError:
            return []
        except (ValueError, AttributeError, TypeError) as exc:
            logger.warning(f"Could not read {self.checkpoint_path}: {exc}")
            return []

    def _mark_year_done(self, year: int) -> None:
        done = self._load_checkpoint()
        if year in done:
            return
        done.append(year)
        self.data_dir.mkdir(exist_ok=True)
        tmp = self.checkpoint_path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"years_done": sorted(done)}, f)
        tmp.replace(self.checkpoint_path)

    def sample_rows(self, size: int) -> list:
        """A spread of decisions across the whole published range."""
        years = self.years()
        if not years:
            return self.index()[:size]
        step = max(1, len(years) / size)
        picks, seen = [], set()
        for index in range(size * 2):
            year = years[min(int(index * step) % len(years), len(years) - 1)]
            for row in self.year_rows(year):
                if row["path"].lower() in seen:
                    continue
                seen.add(row["path"].lower())
                picks.append(row)
                break
        return picks

    # -------------------------------------------------------------- scraping

    def fetch_all(self) -> Generator[dict, None, None]:
        done = set(self._load_checkpoint())
        for year in self.years():
            if year in done:
                logger.info(f"{year}: already written (checkpoint), skipping")
                continue
            rows = self.year_rows(year)
            logger.info(f"{year}: {len(rows)} significant decisions")
            for row in rows:
                yield row
            self._mark_year_done(year)
        # Decisions whose index row carried no year still have to be written.
        undated = [row for row in self.index() if not row.get("year")]
        if undated:
            logger.info(f"undated: {len(undated)} significant decisions")
            for row in undated:
                yield row

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Decisions from `since`'s year onward.

        The indexes date a decision only to the year, so the year is the
        finest cutoff available here; the loader dedups on `_id`.
        """
        for row in self.index():
            year = row.get("year")
            if year is None or year >= since.year:
                yield row

    def normalize(self, raw: dict) -> Optional[dict]:
        url = (raw.get("url") or "").strip()
        if not url:
            return None
        record_id = doc_id(raw.get("docket") or "", raw.get("path") or "")

        resp = self._get(url)
        if resp is None:
            logger.warning(f"Skipping unreachable decision: {url}")
            return None
        if not resp.content.startswith(b"%PDF"):
            logger.warning(
                f"Skipping non-PDF response ({resp.headers.get('Content-Type')}): "
                f"{url}"
            )
            return None

        text = extract_pdf_markdown(
            "US/WA-BIIA", record_id, pdf_bytes=resp.content
        ) or ""
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars): {url}")
            return None

        case_name = (raw.get("case_name") or "").strip() or pdf_stem(raw["path"])
        case_name = _title_cased_name(case_name, text)
        docket = normalize_docket(raw.get("docket") or "") or pdf_stem(raw["path"])
        year = raw.get("year")
        date = decision_date(text, year)
        if not year and date:
            year = int(date[:4])

        citation = f"In re {case_name}, BIIA Dec., {docket}"
        if year:
            citation += f" ({year})"

        return {
            "_id": record_id,
            "_source": "US/WA-BIIA",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": citation,
            "text": text,
            "date": date,
            "url": url,
            "court": (
                "Washington State Board of Industrial Insurance Appeals"
            ),
            "jurisdiction": "US-WA",
            "docket_number": docket,
            "citation": citation,
            "case_name": case_name,
            "subjects": raw.get("subjects") or None,
            "headnote": raw.get("headnote"),
            "significant": True,
            "year": year,
            "source_format": "pdf",
            "language": "en",
        }

    # ------------------------------------------------------------------ CLI

    def test_api(self) -> bool:
        try:
            rows = self.index()
        except Exception as exc:
            logger.error(f"Significant Decision index unreachable: {exc}")
            return False
        years = self.years()
        if not years or years[0] > FIRST_YEAR or len(rows) < 500:
            logger.error(
                f"Index looks truncated: {len(rows)} decisions spanning "
                f"{years[:1]}-{years[-1:]}"
            )
            return False
        record = self.normalize(rows[0])
        if not record or not record.get("text"):
            logger.error(f"No full text from probe decision {rows[0].get('url')}")
            return False
        logger.info(
            f"OK: {len(rows)} decisions listed for {years[0]}-{years[-1]}; probe "
            f"{record['_id']} ({record['date']}) {len(record['text'])} chars "
            f"— {record['title']}"
        )
        return True

    def run_curated_sample(self, size: int = 15) -> int:
        logger.info(f"=== SAMPLE MODE: {size} decisions ===")
        records = []
        for row in self.sample_rows(size):
            if len(records) >= size:
                break
            try:
                record = self.normalize(row)
            except Exception as exc:
                logger.warning(f"normalize failed for {row.get('url')}: {exc}")
                continue
            if record:
                records.append(record)
                logger.info(f"  {record['_id']}: {len(record['text'])} chars")
            time.sleep(self.rate_limit_delay)

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        for record in records:
            with open(sample_dir / f"{record['_id']}.json", "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info(f"=== Sample complete: {len(records)} records ===")
        return len(records)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="US/WA-BIIA bootstrap")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = WABIIAScraper()
    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)
    if args.sample:
        sys.exit(0 if scraper.run_curated_sample() > 0 else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
    else:
        stats = scraper.bootstrap()
    logger.info(
        f"{args.command} complete: {stats.get('records_fetched', 0)} fetched, "
        f"{stats.get('records_new', 0)} new, {stats.get('errors', 0)} errors"
    )
    sys.exit(0 if stats.get("records_fetched", 0) > 0 else 1)


if __name__ == "__main__":
    main()
