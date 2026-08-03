#!/usr/bin/env python3
"""
UK/ScotHealthEducationChamber -- First-tier Tribunal for Scotland (Health and
Education Chamber) -- Additional Support Needs (ASN) decisions.

The Health and Education Chamber of the First-tier Tribunal for Scotland hears
references and claims about the additional support needs of children and young
people (Education (Additional Support for Learning) (Scotland) Act 2004),
disability-discrimination claims against schools (Equality Act 2010), and related
placing-request appeals. Its decisions are binding, appealable to the Upper
Tribunal for Scotland -- i.e. adjudicative case law for the GB-SCT jurisdiction
(NOT covered by UK/CaseLaw, which is England & Wales + reserved UK tribunals
only, nor by the sibling Scottish chambers already in the corpus:
UK/ScotHousingChamber, UK/ScotTaxChamber, UK/ScotLocalTaxChamber).

The Chamber publishes every anonymised decision on the Scottish Courts and
Tribunals Service (SCTS) subsite healthandeducationchamber.scot. The decisions
are a single server-rendered Drupal "Views" listing:

    https://healthandeducationchamber.scot/additional-support-needs/decisions?page=N

Each 0-indexed page holds 10 rows; every row carries the Chamber reference
(e.g. FTS/HEC/AR/24/0132), a decision date (<time datetime>), a category
(e.g. "Placing Request", "Co-ordinated Support Plan", "Disability Claim") and a
link to the decision detail page:

    https://healthandeducationchamber.scot/additional-support-needs/decisions/{node_id}

The detail page carries the FULL decision text inline as born-digital HTML inside
the `field--name-field-decision-text` container (no PDF download or OCR needed).
A born-digital PDF of the same decision is also linked under
`/sites/default/files/decisions/add/{ref}.pdf` and is used as a fallback when the
inline text field is missing or too short.

Strategy:
  - Page the single listing (?page=N, Drupal 0-indexed), splitting each page on
    `class="views-row"` and parsing the node id, Chamber ref, decision date and
    category from each row.
  - Fetch each detail page and extract the inline decision text from the
    `field--name-field-decision-text` field (preferred). If absent/short, fall
    back to downloading the linked born-digital PDF and extracting with PyMuPDF.
  - One record per decision.

Data:
  - ~360 anonymised full-text decisions, ~2018-present (grows over time)
  - Language: English
  - Auth: None (free public access)

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental (recent decisions)
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import html
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UK.ScotHealthEducationChamber")

BASE_URL = "https://healthandeducationchamber.scot"
LISTING = "/additional-support-needs/decisions"

MAX_PAGES = 400  # safety ceiling (10 rows/page; corpus ~36 pages)

# --- listing regexes ---
DETAIL_RE = re.compile(r'href="(/additional-support-needs/decisions/(\d+))"', re.I)
REF_RE = re.compile(r"Decision reference:\s*<a[^>]*>(.*?)</a>", re.S | re.I)
TIME_RE = re.compile(r'<time[^>]*datetime="([^"]+)"', re.I)
CAT_RE = re.compile(
    r'views-field-field-category.*?field-content">(.*?)<', re.S | re.I)

# --- detail regexes ---
# The full inline decision text lives in the field--name-field-decision-text
# wrapper. Capture from its field--item to the start of the next sibling field /
# the node footer.
DEC_TEXT_RE = re.compile(
    r'field--name-field-decision-text.*?<div class="field--item">(.*)', re.S | re.I)
PDF_RE = re.compile(r'href="(/sites/default/files/[^"]+?\.pdf)"', re.I)
H1_REF_RE = re.compile(r"<h1[^>]*>.*?<span>(.*?)</span>", re.S | re.I)
TAG_RE = re.compile(r"<[^>]+>")

_MONTHS = ("January|February|March|April|May|June|July|August|September|"
           "October|November|December")
_MONTH_NUM = {m: i for i, m in enumerate(
    ("january february march april may june july august september october "
     "november december").split(), start=1)}
TEXT_DATE_RE = re.compile(
    r"\b(\d{1,2})(?:st|nd|rd|th)?\s+(" + _MONTHS + r")\s+(\d{4})\b", re.I)


def _strip(s: str) -> str:
    return html.unescape(TAG_RE.sub("", s or "")).strip()


def _clean(text: str) -> str:
    lines = [ln.rstrip() for ln in (text or "").replace("\r", "").split("\n")]
    out, blanks = [], 0
    for ln in lines:
        if ln.strip():
            blanks = 0
            out.append(ln.strip())
        else:
            blanks += 1
            if blanks <= 1:
                out.append("")
    return "\n".join(out).strip()


def _date_from_text(text: str) -> Optional[str]:
    matches = TEXT_DATE_RE.findall(text or "")
    if not matches:
        return None
    day, mon, year = matches[-1]
    month = _MONTH_NUM.get(mon.lower())
    if not month:
        return None
    try:
        return f"{int(year):04d}-{month:02d}-{int(day):02d}"
    except (ValueError, TypeError):
        return None


def _inline_text(detail_html: str) -> str:
    """Extract the full decision text from the field--name-field-decision-text
    container of an ASN decision detail page (born-digital inline HTML)."""
    m = DEC_TEXT_RE.search(detail_html)
    if not m:
        return ""
    raw = m.group(1)
    # Cut at the first structural boundary that follows the decision body.
    for cut in ("field--name-field", "node__links", "<footer",
                "In this section", "Related links", '<div class="region'):
        k = raw.find(cut)
        if k > 500:
            raw = raw[:k]
            break
    # Newline on block-level boundaries; space on inline tags (<span>, <strong>,
    # <u>, <a> ...) so heavily inline-styled sentences are not split word-by-word.
    raw = re.sub(r"(?i)</?(p|div|br|li|tr|h[1-6]|table|ul|ol|blockquote)\b[^>]*>",
                 "\n", raw)
    t = html.unescape(TAG_RE.sub(" ", raw))
    t = re.sub(r"[ \t]+", " ", t)
    return _clean(t)


def _pdf_text(pdf_bytes: bytes) -> str:
    if fitz is not None:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            try:
                text = "\n".join(page.get_text() for page in doc).strip()
            finally:
                doc.close()
            if len(text) >= 120:
                return text
        except Exception as e:
            logger.debug(f"fitz extract failed: {e}")
    try:
        from common import pdf_extract as _pe
        for fn in ("_extract_with_pdfplumber", "_extract_with_pypdf"):
            f = getattr(_pe, fn, None)
            if f:
                try:
                    t = f(pdf_bytes)
                    if t and len(t) >= 120:
                        return t
                except Exception:
                    continue
    except Exception:
        pass
    return ""


class ScotHealthEducationChamberScraper(BaseScraper):
    """Scraper for the First-tier Tribunal for Scotland (Health and Education
    Chamber) Additional Support Needs decisions (Drupal listing + inline HTML)."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
            },
            timeout=90,
        )

    # -- HTTP helpers ----------------------------------------------------
    def _get_html(self, url: str) -> Optional[str]:
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
        except Exception as e:
            logger.warning(f"GET {url} failed: {e}")
            return None
        if resp.status_code != 200:
            logger.debug(f"GET {url}: HTTP {resp.status_code}")
            return None
        return resp.content.decode("utf-8", "replace")

    def _fetch_pdf(self, url: str) -> Optional[bytes]:
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
        except Exception as e:
            logger.warning(f"pdf {url}: {e}")
            return None
        if resp.status_code != 200:
            return None
        data = resp.content
        if not data[:5].startswith(b"%PDF"):
            return None
        return data

    # -- listing parsing -------------------------------------------------
    def _parse_rows(self, page_html: str) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        seen = set()
        for chunk in re.split(r'class="views-row"', page_html)[1:]:
            dm = DETAIL_RE.search(chunk)
            if not dm:
                continue
            node_id = dm.group(2)
            if node_id in seen:
                continue
            seen.add(node_id)
            rm = REF_RE.search(chunk)
            case_ref = _strip(rm.group(1)) if rm else f"FTS-HEC-{node_id}"
            tm = TIME_RE.search(chunk)
            date = tm.group(1)[:10] if tm else None
            cm = CAT_RE.search(chunk)
            category = _strip(cm.group(1)) if cm else ""
            rows.append({
                "node_id": node_id,
                "detail_path": dm.group(1),
                "case_ref": case_ref,
                "date": date,
                "category": category,
            })
        return rows

    def _build_raw(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        detail_html = self._get_html(urljoin(BASE_URL, row["detail_path"]))
        if not detail_html:
            return None
        text = _inline_text(detail_html)
        # PDF fallback if the inline text field is missing/short.
        if len(text) < 400:
            pm = PDF_RE.search(detail_html)
            if pm:
                pdf = self._fetch_pdf(urljoin(BASE_URL, pm.group(1)))
                if pdf:
                    ptext = _pdf_text(pdf)
                    if len(ptext) > len(text):
                        text = _clean(ptext)
        if len(text) < 200:
            return None
        # Prefer the H1 reference on the detail page when the listing ref is thin.
        hm = H1_REF_RE.search(detail_html)
        if hm:
            h1ref = _strip(hm.group(1))
            if h1ref and "/" in h1ref:
                row = dict(row, case_ref=h1ref)
        raw = dict(row)
        raw["text"] = text
        return raw

    # -- core ------------------------------------------------------------
    def _iter_listing(self, limit: Optional[int] = None
                      ) -> Generator[Dict[str, Any], None, None]:
        produced = 0
        page = 0
        empty_streak = 0
        while page < MAX_PAGES:
            page_html = self._get_html(f"{BASE_URL}{LISTING}?page={page}")
            if page_html is None:
                empty_streak += 1
                if empty_streak >= 2:
                    break
                page += 1
                continue
            rows = self._parse_rows(page_html)
            if not rows:
                break
            empty_streak = 0
            for row in rows:
                raw = self._build_raw(row)
                if raw:
                    produced += 1
                    yield raw
                    if limit and produced >= limit:
                        return
            page += 1

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        produced = 0
        for raw in self._iter_listing():
            produced += 1
            yield raw
        if produced == 0:
            raise RuntimeError(
                "Health & Education Chamber listing returned 0 decisions — "
                "site blocked, layout changed, or all decisions unreadable"
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Incremental: the listing is newest-first, so stop once we pass rows
        older than `since`."""
        since_date = since.date()
        page = 0
        stop = False
        while page < MAX_PAGES and not stop:
            page_html = self._get_html(f"{BASE_URL}{LISTING}?page={page}")
            if not page_html:
                break
            rows = self._parse_rows(page_html)
            if not rows:
                break
            for row in rows:
                d = row.get("date")
                if d:
                    try:
                        if datetime.strptime(d, "%Y-%m-%d").date() < since_date:
                            stop = True
                            continue
                    except ValueError:
                        pass
                raw = self._build_raw(row)
                if raw:
                    yield raw
            page += 1

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = _clean(raw.get("text", "") or "")
        if len(text) < 200:
            return None
        node_id = raw.get("node_id", "")
        case_ref = raw.get("case_ref") or f"FTS-HEC-{node_id}"
        date = raw.get("date") or _date_from_text(text)
        return {
            "_id": f"UK-ScotHEC-{node_id}",
            "_source": "UK/ScotHealthEducationChamber",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"{case_ref} — First-tier Tribunal for Scotland "
                     f"(Health and Education Chamber)",
            "text": text,
            "date": date,
            "url": urljoin(BASE_URL, raw.get("detail_path", "")),
            "case_ref": case_ref,
            "category": raw.get("category", ""),
            "court": "First-tier Tribunal for Scotland (Health and Education Chamber)",
            "jurisdiction": "GB-SCT",
            "language": "en",
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing Health & Education Chamber (ASN) listing...")
        page_html = self._get_html(f"{BASE_URL}{LISTING}?page=0")
        print(f"  listing page 0: {'OK' if page_html else 'FAILED'}")
        if page_html:
            rows = self._parse_rows(page_html)
            print(f"  parsed {len(rows)} rows on page 0")
            if rows:
                raw = self._build_raw(rows[0])
                if raw:
                    print(f"  first decision {rows[0]['case_ref']}: "
                          f"{len(raw['text'])} chars extracted - OK")


def main():
    scraper = ScotHealthEducationChamberScraper()
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
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        else:
            logger.info("Running full bootstrap")
            stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Bootstrap complete: {stats}")
    elif command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
