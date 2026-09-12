#!/usr/bin/env python3
"""
US/EPA-EAB -- U.S. EPA Environmental Appeals Board (EAB) Decisions & Orders

Fetches the full text of the final agency adjudicative decisions and orders
issued by the Environmental Appeals Board (EAB), the appellate tribunal within
the U.S. Environmental Protection Agency that decides administrative appeals
of:
  * permit decisions (PSD/Title V under the Clean Air Act, NPDES under the
    Clean Water Act, RCRA hazardous-waste, UIC underground-injection, and
    ocean-dumping permits), and
  * civil administrative penalty / compliance orders under CAA, CWA, RCRA,
    SDWA, FIFRA, TSCA, EPCRA, etc.

Each decision resolves a specific docketed appeal (e.g. "CAA 24-12C",
"UIC 24-02") -> case_law. The Board's precedential opinions are published as
the *Environmental Administrative Decisions* (E.A.D.) reporter. EAB decisions
are federal-government works in the public domain (17 U.S.C. § 105).

Access (no JavaScript, no CAPTCHA, no auth):
  The EAB publishes its docket in a public Lotus Domino database:

      https://yosemite.epa.gov/oa/EAB_Web_Docket.nsf

  Enumeration uses Domino's structured XML feed (?ReadViewEntries) over the
  Board's decision views:
    * "Closed+Dockets"                              (contested appeals; each
      docket page carries an "Index of Filings" that links the Board's
      order/decision documents)
    * "Unpublished~Final~Orders"                    (routine final orders;
      each entry IS a filing doc with the order PDF attached directly)
    * "Significant Interlocutory Decisions"
    * "EAB Decisions Reviewed by the Federal Courts"

  For each view entry (identified by a Domino UNID) we open the document:

      GET /oa/EAB_Web_Docket.nsf/0/<UNID>?OpenDocument

  and then either
    (a) follow the "Index of Filings" links to the Board's decision/order
        filings (docket-level records), or
    (b) read the attached order PDF(s) directly (filing-level records),

  where each order PDF lives at
      /oa/EAB_Web_Docket.nsf/.../<filingUNID>/$File/<name>.pdf
  and is born-digital (real text layer -> PyMuPDF, no OCR).

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import html as _htmllib
import time
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.EPA-EAB")

BASE_URL = "https://yosemite.epa.gov/oa/EAB_Web_Docket.nsf"

# Domino views that hold Board decisions / orders. Order matters only for the
# first-seen metadata; documents are de-duplicated by UNID across views.
VIEWS = [
    "Closed+Dockets",
    "Unpublished~Final~Orders",
    "Significant%20Interlocutory%20Decisions",
    "EAB+Decisions+Reviewed+by+the+Federal+Courts",
]

MIN_TEXT_CHARS = 300
VIEW_PAGE = 200  # entries per ReadViewEntries request

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# A docket page's "Index of Filings" links each filing here.
FILING_LINK_RE = re.compile(
    r'href="([^"]*Filings By Appeal Number/[0-9A-Fa-f]{32}[^"]*)"[^>]*>(.*?)</a>',
    re.I | re.S,
)
# Attached PDF on a filing/order document.
FILE_PDF_RE = re.compile(r'href="([^"]*\$File/[^"]+\.pdf)"', re.I)
# ReadViewEntries fields.
VIEWENTRY_RE = re.compile(r'<viewentry\b[^>]*\bunid="([0-9A-Fa-f]{32})"[^>]*>(.*?)</viewentry>',
                          re.I | re.S)
ENTRYDATA_RE = re.compile(
    r'<entrydata\b[^>]*\bname="([^"]*)"[^>]*>(.*?)</entrydata>', re.I | re.S)
DATETIME_RE = re.compile(r'<datetime[^>]*>(\d{8})', re.I)
TEXT_TAG_RE = re.compile(r'<text>(.*?)</text>', re.I | re.S)

# Filing descriptions that indicate a Board disposition (not a party filing).
DECISION_KW_RE = re.compile(
    r'\b(order|decision|opinion|remand|ruling|judgment|dismiss|denial|'
    r'denying|granting|final)\b', re.I)

DOCKET_META_RE = {
    "docket_number": re.compile(r'Docket Number\s*</?\w[^>]*>?\s*([A-Z0-9\-/ ]+)', re.I),
}


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _iso_from_yyyymmdd(s: str) -> str | None:
    if s and len(s) == 8 and s.isdigit():
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return None


def _strip_tags(h: str) -> str:
    h = re.sub(r"(?s)<script.*?</script>", " ", h)
    h = re.sub(r"(?s)<style.*?</style>", " ", h)
    h = re.sub(r"<[^>]+>", " ", h)
    return re.sub(r"\s+", " ", _htmllib.unescape(h)).strip()


class EPAEABScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": BROWSER_UA,
                "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=90,
        )
        self.delay = 1.0

    # ---- fetch helpers -------------------------------------------------

    def _get_text(self, url: str, retries: int = 3) -> str | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.text:
                    return resp.text
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _get_bytes(self, url: str, retries: int = 3) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.content:
                    ctype = (resp.headers.get("Content-Type") or "").lower()
                    if "pdf" in ctype or resp.content[:5] == b"%PDF-":
                        return resp.content
                    logger.warning(f"Non-PDF content ({ctype}) for {url}")
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching PDF {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    @staticmethod
    def _enc(url: str) -> str:
        """URL-encode a Domino path that may contain spaces / $File / quotes."""
        if url.startswith("http"):
            return urllib.parse.quote(url, safe=":/?&=%$~+")
        return "https://yosemite.epa.gov" + urllib.parse.quote(url, safe=":/?&=%$~+")

    # ---- discovery -----------------------------------------------------

    def _iter_view_entries(self, view: str) -> Generator[dict, None, None]:
        start = 1
        while True:
            url = (f"{BASE_URL}/{view}?ReadViewEntries"
                   f"&Start={start}&Count={VIEW_PAGE}")
            xml = self._get_text(url)
            if not xml:
                return
            entries = VIEWENTRY_RE.findall(xml)
            if not entries:
                return
            for unid, body in entries:
                cols = ENTRYDATA_RE.findall(body)
                decision_date = file_date = None
                texts = []
                for name, val in cols:
                    dm = DATETIME_RE.search(val)
                    if name.lower() == "decisiondate" and dm:
                        decision_date = _iso_from_yyyymmdd(dm.group(1))
                    elif name.lower() == "filedate" and dm:
                        file_date = _iso_from_yyyymmdd(dm.group(1))
                    else:
                        tm = TEXT_TAG_RE.search(val)
                        if tm:
                            t = _htmllib.unescape(re.sub(r"<[^>]+>", "", tm.group(1))).strip()
                            if t:
                                texts.append(t)
                yield {
                    "unid": unid.upper(),
                    "decision_date": decision_date,
                    "file_date": file_date,
                    "cols": texts,
                    "view": view,
                }
            if len(entries) < VIEW_PAGE:
                return
            start += VIEW_PAGE

    def discover_documents(self, sample: bool = False) -> Generator[dict, None, None]:
        seen: set[str] = set()
        views = VIEWS[:1] if sample else VIEWS
        for view in views:
            logger.info(f"Enumerating view {view!r}...")
            for entry in self._iter_view_entries(view):
                if entry["unid"] in seen:
                    continue
                seen.add(entry["unid"])
                yield entry
                if sample and len(seen) >= 40:
                    return
        logger.info(f"Discovered {len(seen)} unique EAB docket/order documents")

    # ---- per-document extraction ---------------------------------------

    def _pdf_text(self, pdf_url: str, slug: str) -> str:
        pdf_bytes = self._get_bytes(self._enc(pdf_url))
        if not pdf_bytes:
            return ""
        # These born-digital Domino order PDFs extract cleanly with PyMuPDF
        # (fitz) but come back empty from pdfplumber/pypdf, so prefer fitz.
        text = ""
        try:
            import fitz  # PyMuPDF
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            text = "\n".join(p.get_text() for p in doc)
            doc.close()
        except Exception as e:
            logger.warning(f"fitz extract failed ({e}); trying framework extractor")
        if len(text.strip()) < MIN_TEXT_CHARS:
            alt = extract_pdf_markdown(
                "US/EPA-EAB", slug, pdf_bytes=pdf_bytes, table="case_law",
                force=True,
            )
            if alt and len(alt.strip()) > len(text.strip()):
                text = alt
        return clean_text(text or "")

    def _collect_decision_pdfs(self, doc_html: str) -> list[str]:
        """Return absolute PDF URLs for the Board decision/order document(s)."""
        # (a) Filing/order document: PDF attached directly.
        direct = FILE_PDF_RE.findall(doc_html)
        if direct:
            # Prefer the human-named path variant (first match per unique file).
            urls, seen_names = [], set()
            for u in direct:
                u = _htmllib.unescape(u)
                name = u.rsplit("$File/", 1)[-1].lower()
                key = re.sub(r"[^a-z0-9]", "", urllib.parse.unquote(name))
                if key in seen_names:
                    continue
                seen_names.add(key)
                urls.append(u)
            return urls
        # (b) Docket page: follow "Index of Filings" links to decision filings.
        pdfs: list[str] = []
        for href, anchor in FILING_LINK_RE.findall(doc_html):
            desc = _strip_tags(anchor)
            if not DECISION_KW_RE.search(desc):
                continue
            filing_html = self._get_text(self._enc(_htmllib.unescape(href)))
            if not filing_html:
                continue
            for u in FILE_PDF_RE.findall(filing_html):
                pdfs.append(_htmllib.unescape(u))
                break  # one PDF per filing doc
        # de-dup
        out, seen = [], set()
        for u in pdfs:
            k = urllib.parse.unquote(u.rsplit("$File/", 1)[-1]).lower()
            if k in seen:
                continue
            seen.add(k)
            out.append(u)
        return out

    def _build_raw(self, entry: dict) -> dict | None:
        unid = entry["unid"]
        doc_url = f"{BASE_URL}/0/{unid}?OpenDocument"
        html = self._get_text(doc_url)
        if not html:
            return None
        page_text = _strip_tags(html[html.find("pagecontents"):]
                                if "pagecontents" in html else html)
        pdf_urls = self._collect_decision_pdfs(html)
        if not pdf_urls:
            logger.warning(f"No decision PDF for {unid}")
            return None
        parts = []
        for i, pu in enumerate(pdf_urls[:8]):
            t = self._pdf_text(pu, f"{unid}-{i}")
            if t:
                parts.append(t)
        text = "\n\n----\n\n".join(parts)
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars) for {unid}")
            return None

        # Metadata
        cols = entry.get("cols") or []
        appeal_number = party = None
        for c in cols:
            if re.match(r'^[A-Z]{2,6}[\s\-]', c) and appeal_number is None:
                appeal_number = c.strip()
            elif party is None and c.strip():
                party = c.strip()
        # Docket number from page text
        docket_number = None
        dm = re.search(r'Docket Number\s+([A-Z0-9][A-Z0-9\-/]+)', page_text)
        if dm:
            docket_number = dm.group(1).strip()
        statute = None
        sm = re.search(r'Statute\(?s?\)?\s+([A-Z][A-Za-z0-9,;&/ \.\-]+?)\s+Type', page_text)
        if sm:
            statute = sm.group(1).strip()[:200]
        # Party fallback: page <title> / caption
        if not party:
            tm = re.search(r'<title>([^<|]+)', html)
            if tm:
                party = tm.group(1).strip()

        # Filename-derived date fallback (e.g. "Issued 7-15-2026")
        date = entry.get("decision_date")
        if not date:
            fdm = re.search(r'Issued[ _-]+(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})',
                            "\n".join(pdf_urls))
            if fdm:
                mo, d, y = fdm.groups()
                date = f"{y}-{int(mo):02d}-{int(d):02d}"

        return {
            "unid": unid,
            "appeal_number": appeal_number,
            "docket_number": docket_number,
            "statute": statute,
            "party": party,
            "date": date,
            "file_date": entry.get("file_date"),
            "text": text,
            "doc_url": doc_url,
            "pdf_urls": pdf_urls,
        }

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing EPA Environmental Appeals Board (Domino docket)...")
        try:
            entries = []
            for e in self.discover_documents(sample=True):
                entries.append(e)
                if len(entries) >= 5:
                    break
            if not entries:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(entries)}+ documents (partial)")
            raw = None
            for e in entries:
                raw = self._build_raw(e)
                if raw:
                    break
            if raw and len(raw["text"]) >= MIN_TEXT_CHARS:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('appeal_number') or raw.get('docket_number')}")
            else:
                logger.error("  Text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        appeal = raw.get("appeal_number")
        party = (raw.get("party") or "").strip()
        docket = raw.get("docket_number")
        ref = appeal or docket or raw["unid"][:12]
        # Program/statute code (CAA, CWA/NPDES, RCRA, SDWA/UIC, FIFRA, TSCA...).
        statute = raw.get("statute")
        if not statute:
            pm = re.match(r"([A-Za-z]+(?:\s*§?\s*\d+\([a-z]\))?)", appeal or docket or "")
            if pm:
                statute = pm.group(1).strip()
        if party and appeal:
            title = f"In re {party} ({appeal})"
        elif party:
            title = f"In re {party}"
        elif appeal:
            title = f"EPA Environmental Appeals Board — {appeal}"
        else:
            title = f"EPA Environmental Appeals Board decision {ref}"
        slug = re.sub(r"[^A-Za-z0-9._-]+", "-",
                      (appeal or docket or raw["unid"])).strip("-")[:80]
        return {
            "_id": f"US/EPA-EAB/{raw['unid']}",
            "_source": "US/EPA-EAB",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": slug,
            "appeal_number": appeal,
            "docket_number": docket,
            "statute": statute,
            "court": "U.S. EPA Environmental Appeals Board",
            "title": title[:300],
            "text": raw["text"],
            "url": raw["doc_url"],
            "date": raw.get("date") or None,
            "file_date": raw.get("file_date") or None,
            "jurisdiction": "US",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        examined = 0
        for entry in self.discover_documents(sample=sample):
            examined += 1
            raw = self._build_raw(entry)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return
            if sample and examined >= 40:
                return

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/EPA-EAB bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = EPAEABScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
