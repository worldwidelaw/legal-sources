#!/usr/bin/env python3
"""
UK/ComplaintsCommissioner -- Office of the Complaints Commissioner
(The Financial Regulators Complaints Commissioner) -- Final Reports.

The Complaints Commissioner is the independent statutory reviewer of complaints
about how the UK's financial regulators have carried out (or failed to carry
out) their functions. The Complaints Scheme is established under the Financial
Services Act 2012 (ss. 84-87). The Commissioner investigates complaints against:
  - the FCA  (Financial Conduct Authority)
  - the PRA  (Prudential Regulation Authority)
  - the Bank of England
  - the PSR  (Payment Systems Regulator)
and issues a reasoned FINAL REPORT for each complaint setting out the complaint,
the regulator's account, the Commissioner's analysis, findings on whether the
complaint is upheld, and any recommendations (e.g. an apology or ex-gratia
payment). These are binding-in-practice adjudications on individual complaints =
case law, distinct from the regulators' own enforcement notices (UK/FCA,
UK/PRA) and the sectoral ombudsmen (UK/FinancialOmbudsman, UK/PHSO, etc.).

Access & structure (all public, no auth):
  - frccommissioner.org.uk publishes every final report as a born-digital PDF
    under /wp-content/uploads/. The reports are indexed on four static archive
    pages, one per regulator:
        /final-reports/fca-the-financial-conduct-authority/      (~1000)
        /final-reports/pra-the-prudential-regulation-authority/  (~22)
        /final-reports/boe-the-bank-of-england/                  (~3)
        /final-reports/psr-the-payment-systems-regulator/        (~3)
    Each archive page lists anchors of the form
        <a href="/wp-content/uploads/{ref}-Issued-{date}.-Published-{date}.pdf">
           {ref} - Issued {date}. Published {date}</a>
    where {ref} is the case reference (e.g. 202500483, or older FCAxxxxx / GE-...
    style references). The anchor text supplies the case reference and the
    "Issued" (decision) and "Published" dates.
  - Each PDF is born-digital (real text layer, no OCR): a header with the case
    reference and issued date followed by the numbered final report.

Strategy:
  - Fetch each regulator archive page; parse every anchor pointing at a
    /wp-content/uploads/*.pdf report; download the PDF and extract its text
    layer (PyMuPDF, with a shared pdfplumber/pypdf fallback); yield full text
    with the parsed reference / dates / regulator.
  - The stable _id is the PDF filename stem (upper-cased), which carries the
    case reference; re-runs accumulate and the pipeline dedups on _id.

Data:
  - ~1000+ full-text final reports. Language: English. Auth: none.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental (recent issued first)
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
except Exception:  # pragma: no cover
    fitz = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UK.ComplaintsCommissioner")

SITE_BASE = "https://frccommissioner.org.uk"

# regulator archive pages -> regulator code / name
ARCHIVES = [
    ("FCA", "Financial Conduct Authority",
     "/final-reports/fca-the-financial-conduct-authority/"),
    ("PRA", "Prudential Regulation Authority",
     "/final-reports/pra-the-prudential-regulation-authority/"),
    ("BOE", "Bank of England",
     "/final-reports/boe-the-bank-of-england/"),
    ("PSR", "Payment Systems Regulator",
     "/final-reports/psr-the-payment-systems-regulator/"),
]

_WS_RE = re.compile(r"[ \t]+")
_MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}
# anchors to a report PDF under /wp-content/uploads/
_ANCHOR_RE = re.compile(
    r'<a[^>]+href="([^"]*/wp-content/uploads/[^"]+\.pdf)"[^>]*>(.*?)</a>',
    re.I | re.S)


def _pdf_text(pdf_bytes: bytes) -> str:
    """Full text of a born-digital report PDF via PyMuPDF, with a shared
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


def _strip_tags(fragment: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", " ", fragment or ""))


def _clean(text: str) -> str:
    text = html.unescape(text or "").replace("\r", "").replace("\xa0", " ")
    lines = [_WS_RE.sub(" ", ln).rstrip() for ln in text.split("\n")]
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


def _parse_dates(anchor_text: str, filename: str) -> Dict[str, Optional[str]]:
    """Extract Issued / Published ISO dates from the anchor text (preferred) or
    the filename. Returns {'issued': iso|None, 'published': iso|None}."""
    src = html.unescape(anchor_text or "") + " " + (filename or "").replace("-", " ")

    def _find(label: str) -> Optional[str]:
        m = re.search(
            label + r"\D{0,4}(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", src, re.I)
        if not m:
            return None
        mi = _MONTHS.get(m.group(2).lower())
        if not mi:
            return None
        try:
            return f"{int(m.group(3)):04d}-{mi:02d}-{int(m.group(1)):02d}"
        except Exception:
            return None

    issued = _find("Issued")
    published = _find("Publi")  # tolerate 'Publsihed'/'Published' typos
    return {"issued": issued, "published": published}


def _case_ref(anchor_text: str, stem: str) -> str:
    """Case reference: the leading token of the anchor text, else the filename
    stem up to the first 'Issued'/date."""
    t = _WS_RE.sub(" ", _strip_tags(anchor_text)).strip()
    m = re.match(r"([A-Za-z0-9][A-Za-z0-9\-/_.]{2,})", t)
    if m:
        ref = m.group(1).strip("-/._")
        if re.search(r"\d", ref):
            return ref
    m2 = re.match(r"([A-Za-z0-9][A-Za-z0-9\-_.]*?)(?:-?Issued|-?\d{4})", stem, re.I)
    if m2 and re.search(r"\d", m2.group(1)):
        return m2.group(1).strip("-/._")
    return stem


class ComplaintsCommissionerScraper(BaseScraper):
    """Scraper for Office of the Complaints Commissioner final reports."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=SITE_BASE,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
                "Accept-Language": "en-GB,en;q=0.9",
                "Referer": SITE_BASE + "/",
            },
            timeout=60,
            respect_robots=False,
        )

    # -- HTTP ------------------------------------------------------------
    def _get(self, url: str) -> Optional[bytes]:
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
        except Exception as e:
            logger.warning(f"GET {url} failed: {e}")
            return None
        if resp.status_code != 200:
            logger.debug(f"{url}: HTTP {resp.status_code}")
            return None
        return resp.content

    def _get_pdf(self, url: str) -> Optional[bytes]:
        body = self._get(url)
        if not body:
            return None
        if not body[:5].startswith(b"%PDF"):
            logger.debug(f"{url}: not a PDF")
            return None
        return body

    # -- parsing ---------------------------------------------------------
    def _list_rows(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        seen = set()
        reachable = False
        for code, name, path in ARCHIVES:
            body = self._get(SITE_BASE + path)
            if not body:
                continue
            reachable = True
            page_html = body.decode("utf-8", errors="replace")
            for href, anchor_text in _ANCHOR_RE.findall(page_html):
                url = urljoin(SITE_BASE + path, href)
                # only reports hosted on this site; skip regulator responses
                # (letters/annexes) hosted off-site or clearly non-report files
                if "frccommissioner.org.uk" not in url:
                    continue
                stem = url.rsplit("/", 1)[-1][:-4]  # strip .pdf
                low = stem.lower()
                if any(k in low for k in ("-response", "-annex", "-letter",
                                          "fca-response", "annex")):
                    continue
                key = stem.upper()
                if key in seen:
                    continue
                seen.add(key)
                dates = _parse_dates(anchor_text, stem)
                rows.append({
                    "url": url,
                    "stem": stem,
                    "guid": key,
                    "regulator_code": code,
                    "regulator_name": name,
                    "case_ref": _case_ref(anchor_text, stem),
                    "issued": dates["issued"],
                    "published": dates["published"],
                })
        if not reachable:
            raise RuntimeError(
                "Complaints Commissioner archive pages unreachable — "
                "frccommissioner.org.uk blocked or URLs changed")
        if not rows:
            raise RuntimeError(
                "Complaints Commissioner archive pages returned no report PDFs "
                "— the /final-reports/ layout changed")
        logger.info(f"Complaints Commissioner: {len(rows)} final reports listed")
        return rows

    def _build_raw(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        pdf = self._get_pdf(row["url"])
        if not pdf:
            return None
        text = _clean(_pdf_text(pdf))
        if len(text) < 150:
            return None
        # fall back to a date inside the PDF header if the anchor lacked one
        issued = row.get("issued")
        if not issued:
            dm = re.search(r"(\d{1,2})\s+([A-Za-z]+)\s+(20\d{2})", text[:800])
            if dm and dm.group(2).lower() in _MONTHS:
                issued = (f"{int(dm.group(3)):04d}-"
                          f"{_MONTHS[dm.group(2).lower()]:02d}-"
                          f"{int(dm.group(1)):02d}")
        raw = dict(row)
        raw["text"] = text
        raw["date"] = issued or row.get("published")
        return raw

    # -- core ------------------------------------------------------------
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        produced = 0
        for row in self._list_rows():
            raw = self._build_raw(row)
            if raw:
                produced += 1
                yield raw
        if produced == 0:
            raise RuntimeError(
                "Complaints Commissioner listed reports but extracted 0 texts "
                "— the report PDF scheme changed")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        since_date = since.date()
        for row in self._list_rows():
            raw = self._build_raw(row)
            if not raw:
                continue
            d = raw.get("date")
            if d:
                try:
                    if datetime.strptime(d, "%Y-%m-%d").date() < since_date:
                        continue
                except ValueError:
                    pass
            yield raw

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = _clean(raw.get("text", "") or "")
        if len(text) < 150:
            return None
        ref = raw.get("case_ref") or raw.get("guid")
        reg = raw.get("regulator_code") or ""
        title = f"Complaints Commissioner Final Report {ref}"
        if reg:
            title += f" ({reg})"
        if raw.get("date"):
            title += f" — {raw['date']}"
        return {
            "_id": f"UK-ComplaintsCommissioner-{raw['guid']}",
            "_source": "UK/ComplaintsCommissioner",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url"),
            "case_reference": ref,
            "regulator": raw.get("regulator_name"),
            "regulator_code": reg or None,
            "issued_date": raw.get("issued"),
            "published_date": raw.get("published"),
            "court": "Office of the Complaints Commissioner "
                     "(Financial Regulators Complaints Commissioner)",
            "jurisdiction": "GB",
            "language": "en",
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing Complaints Commissioner archive pages...")
        rows = self._list_rows()
        print(f"  Listed {len(rows)} final reports")
        got = 0
        for row in rows:
            raw = self._build_raw(row)
            if raw:
                got += 1
                print(f"  {raw.get('case_ref')} [{raw.get('regulator_code')}] "
                      f"{raw.get('date')}: {len(raw['text'])} chars - OK")
            if got >= 3:
                break
        if got == 0:
            print("  No reports extracted — check PDF access")


def main():
    scraper = ComplaintsCommissionerScraper()
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
