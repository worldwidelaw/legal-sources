#!/usr/bin/env python3
"""
UK/ScotTaxChamber -- First-tier Tribunal for Scotland Tax Chamber -- Decisions.

The First-tier Tribunal for Scotland Tax Chamber (FTS Tax Chamber) decides
appeals against decisions of Revenue Scotland on the devolved Scottish taxes --
Land and Buildings Transaction Tax (LBTT) and Scottish Landfill Tax (SLfT) -- and
related penalty and review matters under the Revenue Scotland and Tax Powers Act
2014. Its administration is provided by the Scottish Courts and Tribunals Service
(SCTS). Its written decisions carry the neutral citation "[YYYY] FTSTC N" and are
adjudicative case law for the GB-SCT (Scotland) jurisdiction, NOT covered by
UK/CaseLaw (England & Wales superior courts + reserved UK tribunals, indexed via
the National Archives Find Case Law service; FTS Tax Chamber decisions are not on
that service).

Site: https://taxtribunals.scot/ -- a single server-rendered page that embeds one

    <table class="decision-table-summary"> ... </table>

per decision. Each table carries the metadata rows (Appellant, Respondent,
Tribunal members, Decision Date, Application Type, Tax Type, Notes/subject) and a
"Decision Document" row linking to the born-digital decision PDF at

    decisions/[YYYY] FTSTC N.pdf

Strategy:
  - GET the homepage, split it into decision-summary tables.
  - Parse each table's label/value rows and the PDF href.
  - Download each born-digital PDF and extract full text with PyMuPDF
    (pdfplumber/pypdf fallback). No OCR needed.
  - One record per decision. Decision date is taken from the "Decision Date" row
    ("20 May 2026"), falling back to the year in the neutral citation.

Data:
  - ~70+ full-text decisions, 2016-present
  - Language: English
  - Auth: None (free public access)
  - Licence: SCTS terms (personal / in-house use only) -- commercial-restricted

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
from urllib.parse import urljoin, quote

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
logger = logging.getLogger("legal-data-hunter.UK.ScotTaxChamber")

BASE_URL = "https://taxtribunals.scot/"

TAG_RE = re.compile(r"<[^>]+>")
# One decision per <table class="decision-table-summary"> ... </table>.
TABLE_RE = re.compile(
    r'<table[^>]*class="[^"]*decision-table-summary[^"]*"[^>]*>(.*?)</table>',
    re.I | re.S,
)
# Label/value rows inside a decision table.
ROW_RE = re.compile(r"<tr[^>]*>\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>\s*</tr>",
                    re.I | re.S)
# PDF href (relative, e.g. decisions/[2026] FTSTC 4.pdf).
PDF_HREF_RE = re.compile(r'href="([^"]*decisions/[^"]+?\.pdf)"', re.I)
CITATION_RE = re.compile(r"\[(\d{4})\]\s*FTSTC\s*(\d+)", re.I)

_MONTH_NUM = {m: i for i, m in enumerate(
    ("january february march april may june july august september october "
     "november december").split(), start=1)}
# "20 May 2026" / "3 September 2019"
TEXT_DATE_RE = re.compile(
    r"\b(\d{1,2})\s+(January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+(\d{4})\b", re.I)


def _strip(s: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(TAG_RE.sub(" ", s or ""))).strip()


def _parse_date(text: str) -> Optional[str]:
    m = TEXT_DATE_RE.search(text or "")
    if not m:
        return None
    day, mon, year = m.groups()
    month = _MONTH_NUM.get(mon.lower())
    if not month:
        return None
    try:
        return f"{int(year):04d}-{month:02d}-{int(day):02d}"
    except (ValueError, TypeError):
        return None


def _encode_href(href: str) -> str:
    """Turn a raw relative href with spaces/brackets into an absolute, safely
    percent-encoded URL (keep the path separators intact)."""
    absolute = urljoin(BASE_URL, href.strip())
    # Split scheme+host from path so we only quote the path portion.
    m = re.match(r"^(https?://[^/]+)(/.*)$", absolute)
    if not m:
        return quote(absolute, safe=":/%[]")
    host, path = m.groups()
    return host + quote(path, safe="/%")


def _pdf_text(pdf_bytes: bytes) -> str:
    """Full text of a born-digital decision PDF via PyMuPDF, with a shared
    pdfplumber/pypdf fallback."""
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


class ScotTaxChamberScraper(BaseScraper):
    """Scraper for First-tier Tribunal for Scotland Tax Chamber decisions
    (single-page HTML summary tables + born-digital PDFs)."""

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
            logger.warning(f"pdf {url}: HTTP {resp.status_code}")
            return None
        data = resp.content
        if not data[:5].startswith(b"%PDF"):
            logger.debug(f"pdf {url}: not a PDF")
            return None
        return data

    # -- discovery / parsing ---------------------------------------------
    def _parse_tables(self, page_html: str) -> List[Dict[str, Any]]:
        """Return one raw dict per decision-summary table (metadata + pdf href),
        WITHOUT fetching PDFs."""
        out: List[Dict[str, Any]] = []
        for block in TABLE_RE.findall(page_html):
            fields: Dict[str, str] = {}
            for label_html, value_html in ROW_RE.findall(block):
                label = _strip(label_html).lower()
                value = _strip(value_html)
                if label:
                    fields[label] = value
            pm = PDF_HREF_RE.search(block)
            if not pm:
                continue
            href = html.unescape(pm.group(1))
            cm = CITATION_RE.search(block) or CITATION_RE.search(href)
            citation = f"[{cm.group(1)}] FTSTC {cm.group(2)}" if cm else ""
            out.append({
                "citation": citation,
                "appellant": fields.get("appellant", ""),
                "respondent": fields.get("respondent", ""),
                "tribunal": fields.get("tribunal", ""),
                "decision_date": fields.get("decision date", ""),
                "application_type": fields.get("application type", ""),
                "tax_type": fields.get("tax type", ""),
                "notes": fields.get("notes", ""),
                "pdf_href": href,
            })
        return out

    def _decisions(self) -> List[Dict[str, Any]]:
        page = self._get_html(BASE_URL)
        if not page:
            return []
        return self._parse_tables(page)

    def _hydrate(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        pdf = self._fetch_pdf(_encode_href(raw["pdf_href"]))
        if not pdf:
            return None
        try:
            text = _pdf_text(pdf)
        except Exception as e:
            logger.debug(f"extract {raw['pdf_href']}: {e}")
            text = ""
        if not text:
            return None
        raw = dict(raw)
        raw["text"] = text
        return raw

    # -- core ------------------------------------------------------------
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        decisions = self._decisions()
        produced = 0
        seen = set()
        for raw in decisions:
            key = raw.get("citation") or raw.get("pdf_href")
            if key in seen:
                continue
            seen.add(key)
            hydrated = self._hydrate(raw)
            if hydrated:
                produced += 1
                yield hydrated
        if produced == 0:
            raise RuntimeError(
                "FTS Tax Chamber homepage returned 0 usable decisions — site "
                "blocked, layout changed, or all PDFs unreadable"
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Incremental: the corpus is small and all on one page, so walk it and
        emit decisions whose Decision Date is on/after `since`."""
        for raw in self._decisions():
            date = _parse_date(raw.get("decision_date", ""))
            if date and since and date < since.strftime("%Y-%m-%d"):
                continue
            hydrated = self._hydrate(raw)
            if hydrated:
                yield hydrated

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = _clean(raw.get("text", "") or "")
        if len(text) < 200:
            return None
        citation = raw.get("citation", "").strip()
        appellant = raw.get("appellant", "").strip()
        respondent = raw.get("respondent", "").strip()
        parties = " v ".join([p for p in (appellant, respondent) if p])
        title = f"{citation}: {parties}".strip().rstrip(":").strip() or citation
        if not citation:
            # Derive a stable id from the pdf filename when citation is absent.
            citation = Path(raw.get("pdf_href", "")).stem
        slug = re.sub(r"[^A-Za-z0-9]+", "-", citation).strip("-")
        date = _parse_date(raw.get("decision_date", ""))
        if not date:
            cm = CITATION_RE.search(citation)
            if cm:
                date = f"{cm.group(1)}-01-01"
        return {
            "_id": f"UK-ScotTaxChamber-{slug}",
            "_source": "UK/ScotTaxChamber",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": _encode_href(raw.get("pdf_href", "")),
            "case_ref": citation,
            "appellant": appellant,
            "respondent": respondent,
            "tribunal_members": raw.get("tribunal", ""),
            "application_type": raw.get("application_type", ""),
            "tax_type": raw.get("tax_type", ""),
            "summary": raw.get("notes", ""),
            "court": "First-tier Tribunal for Scotland Tax Chamber",
            "jurisdiction": "GB-SCT",
            "language": "en",
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing FTS Tax Chamber homepage...")
        decisions = self._decisions()
        print(f"  parsed {len(decisions)} decision tables")
        if not decisions:
            return
        raw = self._hydrate(decisions[0])
        if raw:
            rec = self.normalize(raw)
            print(f"  {rec['case_ref']} ({rec['date']}): "
                  f"{len(rec['text'])} chars extracted - OK")


def main():
    scraper = ScotTaxChamberScraper()
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
