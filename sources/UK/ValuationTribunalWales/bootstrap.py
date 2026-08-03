#!/usr/bin/env python3
"""
UK/ValuationTribunalWales -- Valuation Tribunal for Wales (Tribiwnlys Prisio
Cymru) -- Decisions.

The Valuation Tribunal for Wales (VTW) is the independent statutory tribunal for
Wales that resolves disputes about property valuation for:
  - non-domestic (business) rating -- entries in the rating list,
  - council tax -- banding and liability, and
  - land drainage rates.
Its written decisions (with a statement of reasons) are binding and appealable to
the Upper Tribunal (Lands Chamber) or by way of case stated to the High Court --
i.e. adjudicative case law for the GB-WLS (Wales) jurisdiction, NOT covered by
UK/CaseLaw (which is England & Wales superior courts + reserved UK tribunals,
indexed via the National Archives Find Case Law service; VTW decisions are not on
that service). Distinct from UK/ValuationTribunalEngland (the separate VTE).

Site: https://valuationtribunalwales.net -- a public search portal backed by a
single JSON-over-POST web service. There is no server-rendered HTML listing: the
portal issues XHR POSTs to /WebServer/index.php and renders the returned HTML
fragments client-side. We drive that same web service directly (no browser
needed):

  1. Decisions listing (web_id 4). POST
       request={"appeals":{"what":"html","query":{...}},"page":{...}}
     with query criteria
       cri-app-con-val=1            (concluded / contested appeals)
       cri-app-dtx-val=1            (only appeals that have a decision document)
       cri-app-dec-typ="between"    + cri-app-dec-fro / cri-app-dec-too  (ISO date
                                     window on the decision date)
     The response's appeals.aps-rws is an HTML <tr> table; each row carries the
     list metadata (reference, type, billing authority, property, status) and an
     onclick='appeal(<internal id>)' handle.

  2. Appeal detail. POST
       request={"appeal":{"query":{"id":<id>},"what":"html","grp":"app"},"page":{...}}
     The response's appeal.app fragment is a label/value table (case number,
     reference, appeal type, decision date, billing authority, list year, ...)
     and the English decision-text cell carries onclick='decisiontext(<doc>,<lan>)'.

  3. Decision PDF. GET
       http://valuationtribunalwales.net/PDFServer/?doc=<doc>&lan=<lan>
     (lan 2 = English, lan 1 = Welsh) -- a born-digital PDF with a clean text
     layer; extract with PyMuPDF (shared pdfplumber/pypdf fallback). No OCR.

The full-text decision corpus runs from 2018 to present (~700+ decisions); the
portal only exposes decision documents from 2018 onward.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental (recent decisions)
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import html
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

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
logger = logging.getLogger("legal-data-hunter.UK.ValuationTribunalWales")

BASE_URL = "https://valuationtribunalwales.net"
WEBSERVER = f"{BASE_URL}/WebServer/index.php"
PDFSERVER = f"{BASE_URL}/PDFServer/"

# The portal exposes decision documents from 2018 onward.
FIRST_YEAR = 2018

TAG_RE = re.compile(r"<[^>]+>")
# Appeal rows in the decisions listing carry onclick='appeal(<internal id>)'.
APPEAL_ID_RE = re.compile(r"onclick='appeal\((\d+)\)'")
# A label/value cell in the appeal detail table: id='app-<key>-val' ...>value</td>
DETAIL_CELL_RE = re.compile(
    r"id='app-([a-z]+)-val'[^>]*>(.*?)</td>", re.S)
# The English decision-text cell wires onclick='decisiontext(<doc>,<lan>)'.
DTE_RE = re.compile(
    r"id='app-dte-val'[^>]*onclick='decisiontext\((\d+),(\d+)\)'")
DTW_RE = re.compile(
    r"id='app-dtw-val'[^>]*onclick='decisiontext\((\d+),(\d+)\)'")
# First data cell of each listing row -> Tribunal date; used to seed the title.
DATE_DMY_RE = re.compile(r"\b(\d{2})/(\d{2})/(\d{4})\b")

# Appeal-type prefixes -> human category (best-effort; keeps the raw code too).
TYPE_CATEGORY = {
    "N": "Non-domestic rating",
    "C": "Council tax",
    "D": "Land drainage rate",
}


def _strip(s: str) -> str:
    return html.unescape(TAG_RE.sub("", s or "")).strip()


def _iso_from_dmy(s: str) -> Optional[str]:
    m = DATE_DMY_RE.search(s or "")
    if not m:
        return None
    dd, mm, yyyy = m.groups()
    try:
        d, mo, y = int(dd), int(mm), int(yyyy)
        if 1 <= mo <= 12 and 1 <= d <= 31 and 1900 < y < 2100:
            return f"{y:04d}-{mo:02d}-{d:02d}"
    except (ValueError, TypeError):
        pass
    return None


def _category(appeal_type: str) -> str:
    code = (appeal_type or "").strip().upper()
    return TYPE_CATEGORY.get(code[:1], "") if code else ""


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
            if len(text) >= 80:
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
                    if t and len(t) >= 80:
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


class ValuationTribunalWalesScraper(BaseScraper):
    """Scraper for Valuation Tribunal for Wales decisions (JSON-over-POST portal
    web service + born-digital decision PDFs)."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "*/*",
                "Accept-Language": "en-GB,en;q=0.9",
            },
            timeout=120,
        )
        self._seen: set = set()

    # -- portal web service ---------------------------------------------
    def _page(self) -> Dict[str, Any]:
        return {"web_id": 4, "language": "english", "user": "", "pass": ""}

    def _post(self, request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        request["page"] = self._page()
        self.rate_limiter.wait()
        try:
            resp = self.client.post(
                WEBSERVER, data={"request": json.dumps(request)})
        except Exception as e:
            logger.warning(f"POST WebServer failed: {e}")
            return None
        if resp.status_code != 200:
            logger.debug(f"POST WebServer: HTTP {resp.status_code}")
            return None
        try:
            return json.loads(resp.content.decode("utf-8", "replace"))
        except Exception as e:
            logger.warning(f"WebServer response not JSON: {e}")
            return None

    def _list_window(self, fro: str, too: str) -> List[Dict[str, str]]:
        """Return the listing rows (appeal id + list metadata) whose decision
        date falls in [fro, too] (ISO datetime strings)."""
        req = {"appeals": {"what": "html", "query": {
            "cri-app-con-val": 1,
            "cri-app-dtx-val": 1,
            "cri-app-dec-typ": "between",
            "cri-app-dec-fro": fro,
            "cri-app-dec-too": too,
        }}}
        j = self._post(req)
        if not j:
            return []
        rws = (j.get("appeals") or {}).get("aps-rws", "") or ""
        rows: List[Dict[str, str]] = []
        for tr in re.findall(r"<tr\b.*?</tr>", rws, re.S):
            m = APPEAL_ID_RE.search(tr)
            if not m:
                continue
            cells = [_strip(c) for c in re.findall(r"<td\b.*?</td>", tr, re.S)]
            # Column order (aps-hed): Tribunal Date, Appeal Reference, Type,
            # Billing Authority, Primary Property, Property Band, Property RV,
            # Property Effective Date, Property Description, Status, Appeal Date, ...
            def col(i: int) -> str:
                return cells[i] if i < len(cells) else ""
            rows.append({
                "id": m.group(1),
                "tribunal_date": col(0),
                "reference": col(1),
                "type": col(2),
                "billing_authority": col(3),
                "primary_property": col(4),
                "property_description": col(8),
                "status": col(9),
            })
        return rows

    def _appeal_detail(self, appeal_id: str) -> Optional[Dict[str, str]]:
        """Fetch the appeal detail; return its label/value fields plus the
        English (or Welsh fallback) decision-document id/language."""
        j = self._post({"appeal": {"query": {"id": int(appeal_id)},
                                    "what": "html", "grp": "app"}})
        if not j:
            return None
        app = (j.get("appeal") or {}).get("app", "") or ""
        if not app:
            return None
        fields: Dict[str, str] = {}
        for key, raw in DETAIL_CELL_RE.findall(app):
            fields[key] = _strip(raw)
        doc = DTE_RE.search(app) or DTW_RE.search(app)
        if not doc:
            return None
        fields["_doc"] = doc.group(1)
        fields["_lan"] = doc.group(2)
        return fields

    def _fetch_pdf(self, doc: str, lan: str) -> Optional[bytes]:
        self.rate_limiter.wait()
        try:
            resp = self.client.get(
                PDFSERVER, params={"doc": doc, "lan": lan})
        except Exception as e:
            logger.warning(f"pdf doc={doc}: {e}")
            return None
        if resp.status_code != 200:
            logger.warning(f"pdf doc={doc}: HTTP {resp.status_code}")
            return None
        data = resp.content
        if not data[:5].startswith(b"%PDF"):
            logger.debug(f"pdf doc={doc}: not a PDF")
            return None
        return data

    # -- record assembly -------------------------------------------------
    def _build_raw(self, row: Dict[str, str]) -> Optional[Dict[str, Any]]:
        detail = self._appeal_detail(row["id"])
        if not detail:
            return None
        pdf = self._fetch_pdf(detail["_doc"], detail["_lan"])
        if not pdf:
            return None
        try:
            text = _pdf_text(pdf)
        except Exception as e:
            logger.debug(f"extract doc={detail['_doc']}: {e}")
            text = ""
        if not text:
            return None
        return {
            "appeal_id": row["id"],
            "doc_id": detail["_doc"],
            "language_code": detail["_lan"],
            "case_number": detail.get("num", ""),
            "reference": detail.get("ref") or row.get("reference", ""),
            "appeal_type": detail.get("typ") or row.get("type", ""),
            "decision_date": detail.get("dec", ""),
            "tribunal_dates": detail.get("trd", ""),
            "billing_authority": detail.get("bil") or row.get("billing_authority", ""),
            "list_year": detail.get("yea", ""),
            "vo_lo": detail.get("vol", ""),
            "status": detail.get("sta") or row.get("status", ""),
            "primary_property": row.get("primary_property", ""),
            "property_description": row.get("property_description", ""),
            "tribunal_date": row.get("tribunal_date", ""),
            "text": text,
        }

    # -- core ------------------------------------------------------------
    def _iter_windows(self, years: List[int],
                      limit: Optional[int] = None
                      ) -> Generator[Dict[str, Any], None, None]:
        produced = 0
        for year in years:
            fro = f"{year}-01-01T00:00:00.000Z"
            too = f"{year}-12-31T23:59:59.000Z"
            rows = self._list_window(fro, too)
            logger.info(f"{year}: {len(rows)} decided appeals")
            for row in rows:
                if row["id"] in self._seen:
                    continue
                self._seen.add(row["id"])
                raw = self._build_raw(row)
                if raw:
                    produced += 1
                    yield raw
                    if limit and produced >= limit:
                        return

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        this_year = datetime.now(timezone.utc).year
        years = list(range(FIRST_YEAR, this_year + 1))
        produced = 0
        for raw in self._iter_windows(years):
            produced += 1
            yield raw
        if produced == 0:
            raise RuntimeError(
                "Valuation Tribunal for Wales portal returned 0 decisions -- "
                "web service blocked, response shape changed, or all PDFs "
                "unreadable"
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Incremental: re-scan the current and previous calendar year (covers
        any realistic `since`; the loader dedups on _id)."""
        this_year = datetime.now(timezone.utc).year
        years = [y for y in (this_year - 1, this_year) if y >= FIRST_YEAR]
        for raw in self._iter_windows(years):
            yield raw

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = _clean(raw.get("text", "") or "")
        if len(text) < 200:
            return None
        appeal_id = raw.get("appeal_id", "")
        reference = raw.get("reference", "").strip()
        appeal_type = raw.get("appeal_type", "").strip()
        prop = (raw.get("primary_property")
                or raw.get("property_description") or "").strip()
        category = _category(appeal_type)
        # Title: reference + property; fall back to a category/id label.
        base = reference or f"VTW appeal {appeal_id}"
        title = f"{base}: {prop}".strip().rstrip(":").strip() or base
        date = (_iso_from_dmy(raw.get("decision_date", ""))
                or _iso_from_dmy(raw.get("tribunal_date", "")))
        lang = "cy" if raw.get("language_code") == "1" else "en"
        return {
            "_id": f"UK-ValuationTribunalWales-{appeal_id}",
            "_source": "UK/ValuationTribunalWales",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": BASE_URL + "/",
            "case_ref": reference,
            "case_number": raw.get("case_number", ""),
            "appeal_type": appeal_type,
            "category": category,
            "property": prop,
            "billing_authority": raw.get("billing_authority", ""),
            "list_year": raw.get("list_year", ""),
            "valuation_office": raw.get("vo_lo", ""),
            "status": raw.get("status", ""),
            "tribunal_dates": raw.get("tribunal_dates", ""),
            "court": "Valuation Tribunal for Wales",
            "jurisdiction": "GB-WLS",
            "language": lang,
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing Valuation Tribunal for Wales portal web service...")
        this_year = datetime.now(timezone.utc).year
        fro = f"{this_year}-01-01T00:00:00.000Z"
        too = f"{this_year}-12-31T23:59:59.000Z"
        rows = self._list_window(fro, too)
        print(f"  {this_year} decisions listing: {len(rows)} rows")
        if not rows:
            print("  (no rows -- trying previous year)")
            fro = f"{this_year-1}-01-01T00:00:00.000Z"
            too = f"{this_year-1}-12-31T23:59:59.000Z"
            rows = self._list_window(fro, too)
            print(f"  {this_year-1} decisions listing: {len(rows)} rows")
        if not rows:
            return
        raw = self._build_raw(rows[0])
        if raw:
            print(f"    appeal {raw['appeal_id']} ({raw['reference']}): "
                  f"{len(raw['text'])} chars extracted - OK")


def main():
    scraper = ValuationTribunalWalesScraper()
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
