#!/usr/bin/env python3
"""
UK/SocialWorkEngland -- Social Work England -- Fitness-to-practise hearing decisions.

Social Work England (SWE) is the specialist statutory regulator of the ~100,000
social workers in England, established under the Children and Social Work Act 2017
and operational since December 2019 (it took over regulation from the HCPC). Its
adjudicators sit in fitness-to-practise final hearings, interim-order hearings and
substantive-order review hearings held under the Social Workers Regulations 2018.
Their final determinations set out the allegation, the panel's reasoned findings of
fact / impairment and the sanction (removal, suspension, conditions of practice,
warning) and are binding professional-regulator case law -- distinct from the other
UK professional-regulator tribunals already covered (UK/HCPTS health & care
professions, UK/GMC doctors, UK/SDT solicitors, UK/BTAS barristers).

Access & structure (all public, no auth):
  - Each concluded hearing has a server-rendered detail page at
    /umbraco/surface/hearingdetails/details/{id}  (integer hearing id). The page
    carries the registrant's name + registration number, the outcome, notes and
    (for upcoming hearings) the full allegations, plus a "Hearing details" block
    (type / date / location).
  - A concluded hearing links its full written determination as one or more
    "Outcome documents" -- born-digital PDFs served from
    /umbraco/surface/hearingdetails/download?docid={docid}&hearingid={id} . Final
    hearings run ~10-30 pages / 20k-40k chars of reasoned decision. No OCR needed.
  - Old decisions are removed from the site under SWE's publication policy, so the
    live corpus is a rolling window of recently-published hearings; hearing ids are
    a sparse integer sequence. Removed / never-published ids render a fixed
    "Page Not Found" page and are skipped.

Strategy:
  - Enumerate hearing ids over a sliding integer window (from MIN_ID upward, the
    ceiling auto-extends until a long run of consecutive misses past the last valid
    id). Skip "Page Not Found" pages.
  - For each valid page, parse the metadata + on-page sections, then download and
    text-extract every linked Outcome-document PDF (PyMuPDF, born-digital).
  - Keep a record only when at least one Outcome-document PDF yields real text
    (i.e. a concluded hearing with a published determination); upcoming hearings
    with only a charge sheet and no determination are skipped.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental (recent hearing ids)
  python bootstrap.py test               # Quick connectivity test
"""

import re
import io
import sys
import html
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

import fitz  # PyMuPDF

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UK.SocialWorkEngland")

BASE_URL = "https://www.socialworkengland.org.uk"
DETAIL = "/umbraco/surface/hearingdetails/details/{id}"
NOT_FOUND = "Page Not Found"

# Enumeration window. Old hearings are removed under the publication policy, so the
# live floor rises over time; MIN_ID sits below the current floor (~5000 as of
# 2026-07). Before the first valid hearing is found we scan up to
# MIN_ID + INITIAL_SPAN (so a risen floor on a later run can't trip the ceiling
# before any real hearing is reached); after that the ceiling auto-extends past the
# last valid id (see _iterate).
MIN_ID = 5000
INITIAL_SPAN = 3000     # how far above MIN_ID to search before the first valid id
MAX_CONSEC_MISS = 400   # stop after this many consecutive misses past last valid id
HARD_CEILING = 40000    # absolute safety stop

TAG_RE = re.compile(r"<[^>]+>")
FILE_LINK_RE = re.compile(
    r'<a[^>]*class="c-file_link"[^>]*href="([^"]+)"[^>]*>(.*?)</a>', re.S | re.I)
NAME_RE = re.compile(
    r'/searchregister/socialworker/[^"]*"[^>]*>(.*?)</a>\s*</h2>', re.S | re.I)
REGNO_RE = re.compile(r'/searchregister/socialworker/([A-Za-z0-9]+)', re.I)
# a labelled "Hearing details" field: <h4 ...>Label</h4> ... <p class=c-title_sub>Value</p>
SIDE_FIELD_TMPL = (r'>\s*{label}\s*</h4>\s*<p[^>]*class="[^"]*c-title_sub[^"]*"[^>]*>'
                   r'(.*?)</p>')
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}
DATE_RE = re.compile(r"\b(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\b")


def _strip(fragment: str) -> str:
    """HTML fragment -> single-line readable text."""
    s = html.unescape(TAG_RE.sub(" ", fragment or ""))
    return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()


def _block_text(fragment: str) -> str:
    """HTML fragment -> multi-line readable text (paragraph breaks preserved)."""
    h = re.sub(r"<(script|style)\b.*?</\1>", " ", fragment or "", flags=re.S | re.I)
    h = re.sub(r"(?i)</(p|h1|h2|h3|h4|li|tr|div|blockquote)\s*>", "\n", h)
    h = re.sub(r"(?i)<br\s*/?>", "\n", h)
    h = html.unescape(TAG_RE.sub("", h)).replace("\xa0", " ")
    lines, out, blanks = [ln.strip() for ln in h.split("\n")], [], 0
    for ln in lines:
        if ln:
            blanks = 0
            out.append(ln)
        else:
            blanks += 1
            if blanks <= 1:
                out.append("")
    return "\n".join(out).strip()


def _main_section(page: str) -> str:
    """The <section class="o-content"> block holding name/outcome/notes/allegations."""
    m = re.search(r'<section[^>]*o-content[^>]*>(.*?)</section>', page, re.S | re.I)
    return m.group(1) if m else ""


def _labelled_section(section: str, label: str) -> str:
    """Text under an <h3>Label</h3> up to the next <h3> or end of section."""
    m = re.search(
        r'<h3[^>]*>\s*' + re.escape(label) + r'\s*</h3>(.*?)(?=<h3[^>]*>|\Z)',
        section, re.S | re.I)
    return _block_text(m.group(1)) if m else ""


def _side_field(page: str, label: str) -> Optional[str]:
    m = re.search(SIDE_FIELD_TMPL.format(label=re.escape(label)), page, re.S | re.I)
    if not m:
        return None
    v = _strip(m.group(1))
    return v or None


def _parse_date(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    m = DATE_RE.search(text)  # first date of a range "21 January 2026 to 22 ..."
    if not m:
        return None
    day, mon, year = int(m.group(1)), MONTHS.get(m.group(2).lower()), int(m.group(3))
    if not mon or not (1 <= day <= 31) or not (2015 <= year <= 2035):
        return None
    return f"{year:04d}-{mon:02d}-{day:02d}"


def _pdf_text(data: bytes) -> str:
    if not data or data[:4] != b"%PDF":
        return ""
    try:
        with fitz.open(stream=io.BytesIO(data), filetype="pdf") as doc:
            return "\n".join(page.get_text() for page in doc).strip()
    except Exception as e:
        logger.debug(f"PDF extract failed: {e}")
        return ""


class SocialWorkEnglandScraper(BaseScraper):
    """Scraper for Social Work England fitness-to-practise hearing decisions."""

    def __init__(self):
        super().__init__(Path(__file__).parent)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
            },
            timeout=60,
        )

    # -- HTTP ------------------------------------------------------------
    def _get_html(self, url: str) -> Optional[str]:
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
        except Exception as e:
            logger.debug(f"GET {url} failed: {e}")
            return None
        if resp.status_code != 200:
            return None
        return resp.content.decode("utf-8", "replace")

    def _get_bytes(self, url: str) -> Optional[bytes]:
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
        except Exception as e:
            logger.debug(f"GET(bytes) {url} failed: {e}")
            return None
        if resp.status_code != 200:
            return None
        return resp.content

    # -- parsing ---------------------------------------------------------
    def _build_raw(self, hid: int) -> Optional[Dict[str, Any]]:
        page = self._get_html(BASE_URL + DETAIL.format(id=hid))
        if not page or NOT_FOUND in page:
            return None
        section = _main_section(page)
        nm = NAME_RE.search(section) or NAME_RE.search(page)
        name = _strip(nm.group(1)) if nm else None
        rm = REGNO_RE.search(section) or REGNO_RE.search(page)
        regno = rm.group(1) if rm else None

        # Full determination(s): download every linked Outcome-document PDF.
        pdf_sections: List[str] = []
        for href, _label in FILE_LINK_RE.findall(page):
            href = html.unescape(href).strip()
            if "download" not in href.lower():
                continue
            data = self._get_bytes(BASE_URL + href if href.startswith("/") else href)
            txt = _pdf_text(data) if data else ""
            if len(txt) >= 200:
                pdf_sections.append(txt)
        if not pdf_sections:
            # No published determination (upcoming hearing or interim order with no
            # document) -> no full text to store.
            return None

        outcome = _labelled_section(section, "Outcome")
        notes = _labelled_section(section, "Notes")
        allegations = _labelled_section(section, "Allegations")

        parts: List[str] = []
        if allegations:
            parts.append("Allegations:\n" + allegations)
        parts.extend(pdf_sections)
        if notes:
            parts.append("Notes:\n" + notes)
        text = "\n\n".join(p for p in parts if p).strip()

        return {
            "id": hid,
            "url": BASE_URL + DETAIL.format(id=hid),
            "name": name,
            "registration_number": regno,
            "hearing_type": _side_field(page, "Hearing type"),
            "location": _side_field(page, "Location of hearing"),
            "date": _parse_date(_side_field(page, "Hearing date")),
            "outcome": outcome or None,
            "text": text,
        }

    # -- core ------------------------------------------------------------
    def _iterate(self) -> Generator[Dict[str, Any], None, None]:
        """Walk hearing ids from MIN_ID upward, yielding raw determinations.

        The ceiling auto-extends: we keep scanning while within MAX_CONSEC_MISS
        ids of the last hearing that produced a determination. MIN_ID is used as
        the initial anchor so the (all-404) gap below the live floor never trips
        the ceiling before the first real hearing is reached.
        """
        hid = MIN_ID
        last_valid: Optional[int] = None
        while hid <= HARD_CEILING:
            if last_valid is None:
                if hid - MIN_ID > INITIAL_SPAN:
                    break  # never found a single valid hearing
            elif hid - last_valid > MAX_CONSEC_MISS:
                break
            raw = self._build_raw(hid)
            if raw is not None:
                last_valid = hid
                yield raw
            hid += 1

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        produced = 0
        for raw in self._iterate():
            produced += 1
            yield raw
        if produced == 0:
            raise RuntimeError(
                "SocialWorkEngland enumerated hearing ids but extracted 0 "
                "determinations — site blocked or page layout changed")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        since_date = since.date()
        for raw in self._iterate():
            d = raw.get("date")
            if d:
                try:
                    if datetime.strptime(d, "%Y-%m-%d").date() < since_date:
                        continue
                except ValueError:
                    pass
            yield raw

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = (raw.get("text") or "").strip()
        if len(text) < 200:
            return None
        name = raw.get("name") or "Social Work England registrant"
        htype = raw.get("hearing_type")
        title = name
        if htype:
            title = f"{name} — {htype}"
        return {
            "_id": f"UK-SocialWorkEngland-{raw['id']}",
            "_source": "UK/SocialWorkEngland",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw["url"],
            "registrant": name,
            "registration_number": raw.get("registration_number"),
            "hearing_type": htype,
            "location": raw.get("location"),
            "outcome": raw.get("outcome"),
            "court": "Social Work England (Fitness to Practise adjudicators)",
            "jurisdiction": "GB-ENG",
            "language": "en",
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing Social Work England hearing enumeration...")
        got = 0
        hid = MIN_ID
        misses = 0
        while hid <= HARD_CEILING and got < 3 and misses < 600:
            raw = self._build_raw(hid)
            if raw:
                got += 1
                misses = 0
                print(f"  [{raw['id']}] {raw.get('name')} "
                      f"[{raw.get('hearing_type')}] {raw.get('date')}: "
                      f"{len(raw['text'])} chars - OK")
            else:
                misses += 1
            hid += 1
        if got == 0:
            print("  No determinations found — check site reachability.")


def main():
    scraper = SocialWorkEnglandScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)
    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command in ("bootstrap", "bootstrap-fast"):
        if sample_mode:
            logger.info("Running bootstrap in sample mode")
            stats = scraper.bootstrap(sample_mode=True, sample_size=12)
        else:
            logger.info("Running full bootstrap")
            stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Bootstrap complete: {stats}")
    elif command == "update":
        since = datetime.now(timezone.utc) - timedelta(days=30)
        stats = scraper.update() if hasattr(scraper, "update") else None
        logger.info(f"Update complete: {stats}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
