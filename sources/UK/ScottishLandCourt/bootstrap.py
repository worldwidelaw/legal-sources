#!/usr/bin/env python3
"""
UK/ScottishLandCourt -- The Scottish Land Court -- Significant decisions.

The Scottish Land Court (An Cùirt Fearainn) is a Scottish court of record that
determines disputes about agricultural tenancies, crofting, common grazings and
related land matters under (among others) the Crofters (Scotland) Act 1993, the
Agricultural Holdings (Scotland) Acts and the Land Reform (Scotland) Acts. Its
decisions are binding adjudicative case law for the GB-SCT jurisdiction and are
NOT covered by UK/CaseLaw (England & Wales + reserved UK tribunals only) nor by
UK/LandsTribunalScotland (a distinct body dealing with valuation/title matters).

The Court publishes its "Significant decisions 2007 to date" on its own website
as a single server-rendered HTML table (one row per decision: party names, the
neutral citation e.g. "[2026] SLC 7", and the date issued). Each row links to a
standalone HTML decision document under /decisions/{slug} carrying the FULL text
of the opinion. Two document layouts occur:

  1. Modern decisions -- a <header> (court, parties, case reference, panel, date)
     followed by a <main> element holding the numbered opinion paragraphs.
  2. Older "reported" decisions (slug ending .rub) -- a single-document rubric
     report (headnote + full opinion) with the text directly in <body>.

Both layouts are clean, born-digital HTML with no site navigation, so the full
text is recovered by taking the <body> text (dropping script/style) for both.

Access notes:
  - The site's HTTPS certificate does not match the hostname, so requests are
    made over plain HTTP (http://www.scottish-land-court.org.uk). Content is
    public; no auth.

Strategy:
  - GET /decisions/recent-decisions, parse each <tr> for (case name, detail
    slug, neutral citation, date issued DD.MM.YY).
  - GET each /decisions/{slug} document, extract parties (h2) + full body text.
  - One record per decision.

Data:
  - ~305 significant decisions, 2007-present (born-digital HTML full text)
  - Language: English
  - Auth: None (free public access)

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records for validation
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
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UK.ScottishLandCourt")

# HTTP (not HTTPS) -- the site's TLS cert does not match the hostname.
BASE_URL = "http://www.scottish-land-court.org.uk"
LISTING = "/decisions/recent-decisions"

TAG_RE = re.compile(r"<[^>]+>")
ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
CELL_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S | re.I)
# Anchor into a /decisions/{slug} detail document.
DETAIL_A_RE = re.compile(
    r'<a\s+href="(/decisions/[^"]+)"[^>]*>(.*?)</a>', re.S | re.I)
CITATION_RE = re.compile(r"\[\s*(20\d{2})\s*\]\s*SLC\s*(\d+)", re.I)
# Date issued in the listing cell, e.g. 14.07.26
DDMMYY_RE = re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(\d{2})\b")

_MONTHS = ("January|February|March|April|May|June|July|August|September|"
           "October|November|December")
_MONTH_NUM = {m: i for i, m in enumerate(
    ("january february march april may june july august september october "
     "november december").split(), start=1)}
TEXT_DATE_RE = re.compile(
    r"\b(\d{1,2})(?:st|nd|rd|th)?\s+(" + _MONTHS + r")\s+(\d{4})\b", re.I)


def _strip(s: str) -> str:
    return html.unescape(TAG_RE.sub(" ", s or "")).replace("\xa0", " ").strip()


def _clean(text: str) -> str:
    text = html.unescape(text or "").replace("\r", "").replace("\xa0", " ")
    lines = [ln.rstrip() for ln in text.split("\n")]
    out, blanks = [], 0
    for ln in lines:
        s = ln.strip()
        if s:
            blanks = 0
            out.append(s)
        else:
            blanks += 1
            if blanks <= 1:
                out.append("")
    return "\n".join(out).strip()


def _date_from_ddmmyy(cell: str) -> Optional[str]:
    m = DDMMYY_RE.search(cell or "")
    if not m:
        return None
    day, mon, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
    year = 2000 + yy
    try:
        return f"{year:04d}-{mon:02d}-{day:02d}"
    except (ValueError, TypeError):
        return None


def _date_from_text(text: str) -> Optional[str]:
    """Fallback: the first '<day> <Month> <year>' in the decision (Scottish Land
    Court decisions carry the issue date in the header block)."""
    m = TEXT_DATE_RE.search(text or "")
    if not m:
        return None
    day, mon, year = m.group(1), m.group(2), m.group(3)
    month = _MONTH_NUM.get(mon.lower())
    if not month:
        return None
    try:
        return f"{int(year):04d}-{month:02d}-{int(day):02d}"
    except (ValueError, TypeError):
        return None


def _slug_from_path(path: str) -> str:
    return unquote(path.rstrip("/").rsplit("/", 1)[-1])


def _body_text(doc_html: str) -> str:
    """Full text of a standalone decision document: strip script/style/head,
    take the <body> inner text. Works for both the <header>+<main> layout and
    the older '.rub' rubric layout."""
    h = re.sub(r"<(script|style|head)\b.*?</\1>", " ", doc_html, flags=re.S | re.I)
    bm = re.search(r"<body[^>]*>(.*?)</body>", h, re.S | re.I)
    body = bm.group(1) if bm else h
    # Convert block boundaries to newlines so paragraphs stay separated.
    body = re.sub(r"(?i)</(p|h1|h2|h3|h4|li|tr|div|blockquote|br)\s*>", "\n", body)
    body = re.sub(r"(?i)<br\s*/?>", "\n", body)
    text = TAG_RE.sub("", body)
    return _clean(text)


def _extract_parties(doc_html: str) -> Optional[str]:
    m = re.search(r"<h2[^>]*>(.*?)</h2>", doc_html, re.S | re.I)
    if m:
        parties = _strip(re.sub(r"(?i)<br\s*/?>", " ", m.group(1)))
        parties = re.sub(r"\s+", " ", parties).strip()
        if parties:
            return parties
    # Fallback: the <title> is "Decision: {parties}".
    tm = re.search(r"<title[^>]*>(.*?)</title>", doc_html, re.S | re.I)
    if tm:
        t = _strip(tm.group(1))
        return re.sub(r"^\s*Decision:\s*", "", t).strip() or None
    return None


class ScottishLandCourtScraper(BaseScraper):
    """Scraper for the Scottish Land Court significant decisions (HTML)."""

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
            timeout=60,
        )

    # -- HTTP ------------------------------------------------------------
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

    # -- listing ---------------------------------------------------------
    def _parse_listing(self, page_html: str) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        seen = set()
        for rm in ROW_RE.finditer(page_html):
            row = rm.group(1)
            am = DETAIL_A_RE.search(row)
            if not am:
                continue
            detail_path = am.group(1)
            slug = _slug_from_path(detail_path)
            if slug in seen:
                continue
            cells = CELL_RE.findall(row)
            case_name = _strip(cells[0]) if cells else ""
            cite_m = CITATION_RE.search(_strip(am.group(2)))
            citation = f"[{cite_m.group(1)}] SLC {cite_m.group(2)}" if cite_m else None
            date = None
            if len(cells) >= 3:
                date = _date_from_ddmmyy(_strip(cells[2]))
            seen.add(slug)
            rows.append({
                "detail_path": detail_path,
                "slug": slug,
                "case_name": case_name,
                "citation": citation,
                "date": date,
            })
        return rows

    def _get_rows(self) -> List[Dict[str, Any]]:
        page = self._get_html(BASE_URL + LISTING)
        if not page:
            return []
        return self._parse_listing(page)

    def _build_raw(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        doc = self._get_html(urljoin(BASE_URL, row["detail_path"]))
        if not doc:
            return None
        text = _body_text(doc)
        if len(text) < 300:
            return None
        raw = dict(row)
        raw["text"] = text
        raw["parties"] = _extract_parties(doc)
        return raw

    # -- core ------------------------------------------------------------
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        rows = self._get_rows()
        produced = 0
        for row in rows:
            raw = self._build_raw(row)
            if raw:
                produced += 1
                yield raw
        if produced == 0:
            raise RuntimeError(
                "Scottish Land Court listing returned 0 decisions — site "
                "blocked, layout changed, or documents unreadable"
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        since_date = since.date()
        for row in self._get_rows():
            d = row.get("date")
            if d:
                try:
                    if datetime.strptime(d, "%Y-%m-%d").date() < since_date:
                        continue
                except ValueError:
                    pass
            raw = self._build_raw(row)
            if raw:
                yield raw

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = _clean(raw.get("text", "") or "")
        if len(text) < 300:
            return None
        slug = raw.get("slug", "")
        parties = raw.get("parties") or raw.get("case_name") or slug
        citation = raw.get("citation")
        case_ref = "SLC/" + slug.replace("SLC.", "").replace(".rub", "").replace(".", "/") \
            if slug.upper().startswith("SLC.") else slug
        title_bits = [parties]
        if citation:
            title_bits.append(f"[{citation}]" if not citation.startswith("[") else citation)
        title = " ".join(title_bits).strip()
        date = raw.get("date") or _date_from_text(text)
        return {
            "_id": f"UK-SLC-{slug}",
            "_source": "UK/ScottishLandCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": urljoin(BASE_URL, raw.get("detail_path", "")),
            "case_ref": case_ref,
            "citation": citation,
            "parties": parties,
            "court": "Scottish Land Court",
            "jurisdiction": "GB-SCT",
            "language": "en",
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing Scottish Land Court listing...")
        rows = self._get_rows()
        print(f"  parsed {len(rows)} decision rows")
        if rows:
            raw = self._build_raw(rows[0])
            if raw:
                print(f"  first decision {rows[0]['slug']} "
                      f"({rows[0].get('citation')}): "
                      f"{len(raw['text'])} chars extracted - OK")


def main():
    scraper = ScottishLandCourtScraper()
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
