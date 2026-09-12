#!/usr/bin/env python3
"""
LB/BCCL — Banking Control Commission of Lebanon Circulars

Fetches regulatory circulars from the BCCL website.

Strategy:
  1. Fetch the circulars HTML page and parse the TablePress table
     for metadata (date, number, description, addressee, PDF URL)
  2. Download each Arabic PDF
  3. Extract full text through common.pdf_extract, which applies the
     geometry-based RTL reorder and presentation-form normalization

The site answers 503 for the listing page far more often than not, so
there is a fallback list of the circular numbers whose PDFs are known to
resolve. That list carries *only* numbers — every other field is read out
of the circular's own first page (see `parse_pdf_header`).

Data:
  - ~90+ circulars from 1967-2025
  - Language: Arabic (PDFs), English (metadata, when the listing page is up)
  - License: Lebanese government publication

Usage:
  python bootstrap.py bootstrap          # Full pull → data/records.jsonl
  python bootstrap.py bootstrap --sample # 15 sample records → sample/
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://bccl.gov.lb"
SOURCE_ID = "LB/BCCL"
SAMPLE_DIR = Path(__file__).parent / "sample"
DATA_DIR = Path(__file__).parent / "data"
REQUEST_DELAY = 2.0
MIN_TEXT_LENGTH = 50


# ── HTML table parser ──────────────────────────────────────────────

class _TableParser(HTMLParser):
    """Parse a TablePress HTML table into rows of cells."""

    def __init__(self):
        super().__init__()
        self.rows: List[List[str]] = []
        self._in_table = False
        self._in_row = False
        self._in_cell = False
        self._current_row: List[str] = []
        self._current_cell: List[str] = []
        self._cell_href: Optional[str] = None
        self._skip_header = True

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == "table":
            self._in_table = True
        elif tag == "thead":
            self._skip_header = True
        elif tag == "tbody":
            self._skip_header = False
        elif tag == "tr" and self._in_table:
            self._in_row = True
            self._current_row = []
        elif tag in ("td", "th") and self._in_row:
            self._in_cell = True
            self._current_cell = []
            self._cell_href = None
        elif tag == "a" and self._in_cell:
            href = attrs_dict.get("href", "")
            if href.endswith(".pdf"):
                self._cell_href = href
        elif tag == "br" and self._in_cell:
            self._current_cell.append("; ")

    def handle_endtag(self, tag):
        if tag == "table":
            self._in_table = False
        elif tag == "tr" and self._in_row:
            self._in_row = False
            if not self._skip_header and self._current_row:
                self.rows.append(self._current_row)
        elif tag in ("td", "th") and self._in_cell:
            self._in_cell = False
            cell_text = "".join(self._current_cell).strip()
            if self._cell_href:
                cell_text = self._cell_href
            self._current_row.append(cell_text)

    def handle_data(self, data):
        if self._in_cell:
            self._current_cell.append(data)


def parse_circulars_table(html_content: str) -> List[Dict[str, str]]:
    """Parse the circulars HTML table into a list of circular metadata dicts."""
    parser = _TableParser()
    parser.feed(html_content)
    circulars = []
    for row in parser.rows:
        if len(row) < 5:
            continue
        pdf_url = row[4].strip()
        if not pdf_url.startswith("http"):
            if pdf_url.startswith("/"):
                pdf_url = BASE_URL + pdf_url
            elif pdf_url.endswith(".pdf"):
                pdf_url = BASE_URL + "/" + pdf_url
            else:
                continue
        circulars.append({
            "date": row[0].strip(),
            "number": row[1].strip(),
            "description": row[2].strip(),
            "addressee": row[3].strip(),
            "pdf_url": pdf_url,
        })
    return circulars


# ── Reading the circular's own header ──────────────────────────────

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")

# "بيروت في <date>" / "بيروت، في <date>" — the dateline.
_DATELINE_RE = re.compile(r"بيروت[،\s]*\s*في")
# "الموضوع: <subject>" — the subject line.
_SUBJECT_RE = re.compile(r"الموضوع\s*:\s*(.+)")
# "موجه إلى <addressee>" / "موجهّ إلى <addressee>".
_ADDRESSEE_RE = re.compile(r"موجه\S*\s*إلى\s*(.+)")
# Arabic letters only — no digits, diacritics or punctuation.
_ARABIC_LETTER_RE = re.compile(r"[ء-ي]")


def _parse_dateline(line: str) -> Optional[str]:
    """Pull an ISO date out of a circular's ``بيروت في ...`` line.

    The digits survive the RTL reorder but their *grouping* does not always:
    a justified dateline can arrive as ``١٩٨١١٢ /١٧ /`` rather than
    ``١٩٨١/١٢/١٧``. So work from the ordered digit groups instead of a format
    string, and only commit to a date when the four-digit year sits at one end
    — year-in-the-middle is genuinely ambiguous and gets a null rather than a
    coin flip.
    """
    groups = re.findall(r"\d+", line.translate(_ARABIC_DIGITS))
    # A run like "198112" is a year glued to its month by the lost spacing.
    if len(groups) == 2 and len(groups[0]) in (5, 6):
        groups = [groups[0][:4], groups[0][4:], groups[1]]
    if len(groups) != 3:
        return None

    if len(groups[0]) == 4:
        year, a, b = groups
    elif len(groups[2]) == 4:
        b, a, year = groups
    else:
        return None

    month, day = a, b
    if int(month) > 12 >= int(day):
        month, day = day, month
    try:
        return datetime(int(year), int(month), int(day)).strftime("%Y-%m-%d")
    except ValueError:
        return None


def _is_subject(candidate: str) -> bool:
    """Reject a subject that is really a stray cross-reference date.

    A wrapped subject can leave the ``الموضوع:`` line holding nothing but the
    tail of an inline citation — circular 1-IEF renders as
    ``الموضوع: تاريخ ١٨/٨/٢٠٠٠`` with the actual subject on the line below.
    Twelve Arabic letters is comfortably more than ``تاريخ`` and comfortably
    less than the shortest real subject in the corpus.
    """
    return len(_ARABIC_LETTER_RE.findall(candidate)) >= 12


def parse_pdf_header(text: str) -> Dict[str, Optional[str]]:
    """Read date, Arabic subject and addressee off a circular's first page."""
    out: Dict[str, Optional[str]] = {"date": None, "subject": None, "addressee": None}
    lines = [ln.strip() for ln in text.split("\n")[:20]]
    for i, line in enumerate(lines):
        if not line:
            continue
        if out["date"] is None and _DATELINE_RE.search(line):
            out["date"] = _parse_dateline(line)
        if out["subject"] is None:
            m = _SUBJECT_RE.search(line)
            if m:
                candidates = [m.group(1)] + [ln for ln in lines[i + 1:i + 3] if ln]
                for cand in candidates:
                    cand = cand.strip().rstrip(".").strip()
                    if _is_subject(cand):
                        out["subject"] = cand
                        break
        if out["addressee"] is None:
            m = _ADDRESSEE_RE.search(line)
            if m and m.group(1).strip():
                out["addressee"] = m.group(1).strip()
    return out


# ── Fetching ───────────────────────────────────────────────────────

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return s


def fetch_circulars_page(session: requests.Session) -> Optional[str]:
    """Try to fetch the live circulars HTML page."""
    url = f"{BASE_URL}/circulars/"
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 200:
            return resp.text
        logger.warning("Circulars page returned %d", resp.status_code)
    except Exception as e:
        logger.warning("Failed to fetch circulars page: %s", e)
    return None


# Circular numbers whose PDF resolves under /Documents/ArabicCirculars/,
# newest first. Used only when the listing page is down — which is most of the
# time, so this list deliberately holds numbers and nothing else. It used to
# carry a hand-written English description and date per circular, and for the
# older entries those were invented: no. 68 is about booking clients'
# precious-metal transactions, not "Banking Supervision Procedures"; no. 254 is
# about Istisna'a operations by Islamic banks dated 2007-07-18, not "Corporate
# Governance" dated 2008-12-30; no. 222 is IT-security guidance dated
# 2000-08-18, not "Periodic Templates Required" dated 2003-06-05. Every field
# below the number now comes from the circular itself.
KNOWN_CIRCULAR_NUMBERS = [
    "302", "301", "4", "300", "1-IEF", "299", "298", "297", "296", "295",
    "294", "293", "292", "291", "290", "289", "9", "288", "287", "1-Comptoirs",
    "3", "286", "285", "284", "283", "282", "281", "280", "279", "277",
    "276", "275", "274", "273", "272", "271", "269", "267", "266", "264",
    "263", "262", "261", "257", "256", "255", "254", "253", "252", "251",
    "250", "249", "247", "246", "243", "242", "241", "238", "236", "233",
    "222", "221", "219", "214", "208", "206", "205", "199", "195", "188",
    "180", "174", "173", "157", "94", "80", "68", "31", "30", "29",
    "27", "26", "25", "23", "21", "20", "19", "17", "15", "11",
    "8", "7",
]


def _pdf_url(number: str) -> str:
    return f"{BASE_URL}/Documents/ArabicCirculars/BCCLCircularNo{number.replace('-', '')}.pdf"


def get_circulars_metadata(session: requests.Session) -> List[Dict[str, str]]:
    """Get circular metadata from the live page, or the number list as fallback."""
    html = fetch_circulars_page(session)
    if html:
        circulars = parse_circulars_table(html)
        if circulars:
            logger.info("Parsed %d circulars from live page", len(circulars))
            return circulars
        logger.warning("No circulars parsed from live page, using fallback")

    logger.info("Listing page unavailable — using the known circular numbers")
    return [
        {"date": "", "number": n, "description": "", "addressee": "", "pdf_url": _pdf_url(n)}
        for n in KNOWN_CIRCULAR_NUMBERS
    ]


# ── Normalize ──────────────────────────────────────────────────────

def normalize(circular: Dict[str, str], pdf_text: str) -> Dict[str, Any]:
    """Normalize a circular record into standard schema."""
    number = circular["number"]
    header = parse_pdf_header(pdf_text)

    iso_date = None
    if circular.get("date"):
        try:
            iso_date = datetime.strptime(circular["date"], "%Y/%m/%d").strftime("%Y-%m-%d")
        except ValueError:
            pass
    if iso_date is None:
        iso_date = header["date"]

    # The listing page's English description is authoritative when we have it;
    # otherwise the circular's own Arabic الموضوع line is the real subject.
    subject = circular.get("description") or header["subject"]
    title = f"BCCL Circular No. {number}"
    if subject:
        title = f"{title}: {subject}"

    return {
        "_id": f"BCCL-Circular-{number}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": pdf_text,
        "date": iso_date,
        "url": circular["pdf_url"],
        "circular_number": number,
        "subject_ar": header["subject"],
        "addressee": circular.get("addressee") or header["addressee"] or "",
    }


# ── Bootstrap ──────────────────────────────────────────────────────

def fetch_all(sample: bool = False) -> Iterator[Dict[str, Any]]:
    """Fetch all BCCL circulars with full text."""
    session = _session()
    circulars = get_circulars_metadata(session)
    logger.info("Found %d circulars to process", len(circulars))

    limit = 15 if sample else len(circulars)
    success = 0
    errors = 0

    for i, circ in enumerate(circulars[:limit]):
        pdf_url = circ["pdf_url"]
        if not pdf_url:
            logger.warning("No PDF URL for circular %s, skipping", circ["number"])
            errors += 1
            continue

        logger.info("[%d/%d] Fetching circular %s",
                    i + 1, min(limit, len(circulars)), circ["number"])

        try:
            resp = session.get(pdf_url, timeout=60)
            if resp.status_code != 200:
                logger.warning("PDF %s returned %d", pdf_url, resp.status_code)
                errors += 1
                time.sleep(REQUEST_DELAY)
                continue

            # force=True: the rows this replaces are exactly the ones already in
            # Neon holding the old character-reversed text, and without it the
            # helper skips them as present and the refresh emits nothing.
            pdf_text = extract_pdf_markdown(
                source=SOURCE_ID,
                source_id=f"BCCL-Circular-{circ['number']}",
                pdf_bytes=resp.content,
                table="legislation",
                force=True,
            )
            if not pdf_text or len(pdf_text.strip()) < MIN_TEXT_LENGTH:
                logger.warning("Insufficient text from circular %s (%d chars)",
                               circ["number"], len(pdf_text or ""))
                errors += 1
                time.sleep(REQUEST_DELAY)
                continue

            success += 1
            yield normalize(circ, pdf_text.strip())

        except Exception as e:
            logger.error("Error fetching circular %s: %s", circ["number"], e)
            errors += 1

        time.sleep(REQUEST_DELAY)

    logger.info("Done: %d success, %d errors out of %d attempted",
                success, errors, min(limit, len(circulars)))


def fetch_updates(since) -> Iterator[Dict[str, Any]]:
    """Yield circulars dated on or after `since`.

    BCCL issues sequentially numbered circulars and does not revise one in
    place, so the date the circular carries is also the date it became
    available to us.
    """
    if isinstance(since, datetime):
        since = since.strftime("%Y-%m-%d")
    since = str(since)[:10]
    for record in fetch_all():
        if record["date"] and record["date"] >= since:
            yield record


def main():
    parser = argparse.ArgumentParser(description="LB/BCCL bootstrap")
    sub = parser.add_subparsers(dest="command")
    boot = sub.add_parser("bootstrap", help="Fetch circulars")
    boot.add_argument("--sample", action="store_true", help="Fetch 15 sample records")
    args = parser.parse_args()

    if args.command != "bootstrap":
        parser.print_help()
        sys.exit(1)

    count = 0
    if args.sample:
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        for record in fetch_all(sample=True):
            (SAMPLE_DIR / f"{record['_id']}.json").write_text(
                json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            count += 1
            logger.info("Saved %s (%d chars text)", record["_id"], len(record["text"]))
    else:
        # Stream to data/records.jsonl: a full run used to write only the first
        # 15 records into sample/ and persist nothing else, so a fleet run had
        # nothing to ingest and the pipeline fell back to the bundled samples
        # (issue #798 class).
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with (DATA_DIR / "records.jsonl").open("w", encoding="utf-8") as fh:
            for record in fetch_all():
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
                fh.flush()
                count += 1
                logger.info("Wrote %s (%d chars text)", record["_id"], len(record["text"]))

    logger.info("Total records: %d", count)
    if count == 0:
        logger.error("No records fetched — check connectivity and PDF access")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
