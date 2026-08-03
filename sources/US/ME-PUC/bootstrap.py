#!/usr/bin/env python3
"""
US/ME-PUC -- Maine Public Utilities Commission Orders & Issuances

Fetches the full text of Orders issued by the Maine Public Utilities
Commission (MPUC) adjudicating utility dockets (electric, gas, water,
telecommunications). Each Order (Order, Procedural Order, Protective Order)
is an administrative adjudication / disposition of a specific docket by the
Commission = case_law. Public domain (US state government edict).

Strategy (official public case-management portal, mpuc-cms.maine.gov, a
Hyland-style "CQM.Public.WebUI" ASP.NET WebForms app behind Imperva/Incapsula
-- the same family as US/NJ-BPU and US/WI-PSC):

  1. GET /CQM.Public.WebUI/ to establish the session (Incapsula visid_incap +
     incap_ses cookies and the ASP.NET __VIEWSTATE / __EVENTVALIDATION tokens).
  2. POST __EVENTTARGET=...lbadvancedsearch -> 302 /Common/AdvanceSearch.aspx
     (the advanced document search form).
  3. POST the radio group rdblstMatterDocument=DOC (a __doPostBack that
     switches the form into "Documents" search mode).
  4. POST btnTopSearch=Search with a Date-Filed window (txtDateFiledFrom /
     txtDateFiledTo as M/D/YYYY). The server returns a server-rendered results
     grid (grdSearchedPublicDocument): one row per document carrying the
     Docket #, Case Title, Filing #, Filed Date, Document Type, Company and
     Document Title. Rows whose Document Type is an Order variant are kept.
  5. The grid pages via a __doPostBack on LnkNext; follow it until exhausted.
  6. Each document is opened via the grid's OpenDocument mechanism: set the
     hidden HdnDocumentParameter to
     "{DocRefNo}^{Ext}^{Security}^{Version}^{MatterSeq}^PublicDoc^{DocName}"
     and POST the hidden submit button HdnBtnDocumentOpen. The server stores
     the reference in session and a follow-up GET /Common/ViewDoc.aspx?
     streams the born-digital Order document (PDF, occasionally .docx).
  7. Full text is extracted from the PDF (fitz/PyMuPDF; Tesseract OCR fallback
     for the rare image-only scan) or from the .docx (word/document.xml).

Usage:
  python bootstrap.py bootstrap            # Full pull (all Orders)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import io
import json
import logging
import re
import time
import zipfile
import calendar
import html as htmllib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urlencode

import fitz  # PyMuPDF

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.ME-PUC")

BASE = "https://mpuc-cms.maine.gov/CQM.Public.WebUI/"
ADV = "https://mpuc-cms.maine.gov/CQM.Public.WebUI/Common/AdvanceSearch.aspx"
VIEWDOC = "https://mpuc-cms.maine.gov/CQM.Public.WebUI/Common/ViewDoc.aspx?"

GP = "ctl00$GridPlaceHolder$"

# Corpus floor: the MPUC electronic docket record thins out before ~2000.
FIRST_YEAR = 2000

# Document Types (grid column 4) that are Commission Orders = case_law.
ORDER_TYPES = {
    "order",
    "procedural order",
    "protective order",
}

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

OPENDOC_RE = re.compile(r"OpenGridPopupWindow\('OpenDocument','([^']+)'")


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_date(cell: str) -> Optional[str]:
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", cell or "")
    if not m:
        return None
    mm, dd, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if not (1 <= mm <= 12 and 1 <= dd <= 31 and 1900 <= yyyy <= 2100):
        return None
    return f"{yyyy:04d}-{mm:02d}-{dd:02d}"


def _month_windows():
    """Yield (start, end) 'M/D/YYYY' strings, newest month first, to FIRST_YEAR."""
    now = datetime.now(timezone.utc)
    year, month = now.year, now.month
    while year >= FIRST_YEAR:
        last = calendar.monthrange(year, month)[1]
        yield (f"{month}/1/{year}", f"{month}/{last}/{year}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1


class MEPUCScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": UA,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=90,
        )
        self.delay = 1.5

    # ---- low-level helpers --------------------------------------------------

    @staticmethod
    def _form_fields(html: str, keep_submit: Optional[str] = None) -> dict:
        """Collect all <input> name->value pairs to rebuild a WebForms POST.

        Only checked radios/checkboxes are kept; submit buttons are dropped
        unless their name contains ``keep_submit`` (so we post exactly one).
        """
        fields = {}
        for m in re.finditer(r"<input[^>]*>", html):
            tag = m.group(0)
            nm = re.search(r'name="([^"]+)"', tag)
            if not nm:
                continue
            name = nm.group(1)
            typ = (re.search(r'type="([^"]*)"', tag) or [None, "text"])[1]
            if typ in ("checkbox", "radio") and "checked" not in tag:
                continue
            if typ == "submit" and (keep_submit is None or keep_submit not in name):
                continue
            val = re.search(r'value="([^"]*)"', tag)
            fields[name] = htmllib.unescape(val.group(1)) if val else ""
        return fields

    def _post(self, url: str, data: dict, referer: str, retries: int = 4) -> Optional[str]:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                r = self.http.post(
                    url, data=data,
                    headers={"Referer": referer,
                             "Content-Type": "application/x-www-form-urlencoded"},
                )
                if r.status_code == 200 and "Error.aspx" not in r.url:
                    return r.text
                logger.warning(f"POST {url} attempt {attempt+1}: status={r.status_code} url={r.url}")
            except Exception as e:
                logger.warning(f"POST {url} attempt {attempt+1} error: {e}")
            time.sleep(1.5 * (attempt + 1))
        return None

    def _establish_doc_mode(self) -> Optional[str]:
        """GET home -> advanced search -> switch to Documents mode.

        Returns the DOC-mode form HTML (carrying the tokens for a search).
        """
        try:
            time.sleep(self.delay)
            r = self.http.get(BASE)
            if r.status_code != 200:
                logger.error(f"GET home HTTP {r.status_code}")
                return None
        except Exception as e:
            logger.error(f"GET home error: {e}")
            return None

        home = self._form_fields(r.text)
        adv = self._post(BASE, {
            "__EVENTTARGET": GP + "lbadvancedsearch",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": home.get("__VIEWSTATE", ""),
            "__VIEWSTATEGENERATOR": home.get("__VIEWSTATEGENERATOR", ""),
            "__EVENTVALIDATION": home.get("__EVENTVALIDATION", ""),
        }, referer=BASE)
        if not adv or "rdblstMatterDocument" not in adv:
            logger.error("Failed to reach advanced search form")
            return None

        fields = self._form_fields(adv)
        fields.update({
            "__EVENTTARGET": GP + "rdblstMatterDocument$1",
            "__EVENTARGUMENT": "",
            GP + "rdblstMatterDocument": "DOC",
        })
        doc = self._post(ADV, fields, referer=ADV)
        if not doc or "txtDateFiledFrom" not in doc:
            logger.error("Failed to switch to Documents search mode")
            return None
        return doc

    def _search_window(self, doc_form_html: str, start: str, end: str) -> Optional[str]:
        """From a DOC-mode form, run a Date-Filed window search -> results HTML."""
        fields = self._form_fields(doc_form_html, keep_submit="btnTopSearch")
        fields.update({
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            GP + "rdblstMatterDocument": "DOC",
            GP + "txtDateFiledFrom": start,
            GP + "txtDateFiledTo": end,
        })
        return self._post(ADV, fields, referer=ADV)

    def _next_page(self, results_html: str) -> Optional[str]:
        """Follow the results-grid pager (LnkNext) if a next page exists."""
        target = GP + "grdSearchedPublicDocument$LnkNext"
        if target.replace("$", "$") not in results_html and "LnkNext" not in results_html:
            return None
        # Only page if LnkNext is an active postback link on this page.
        if "LnkNext" not in results_html:
            return None
        fields = self._form_fields(results_html)
        fields.update({
            "__EVENTTARGET": target,
            "__EVENTARGUMENT": "",
        })
        return self._post(ADV, fields, referer=ADV)

    @staticmethod
    def _parse_rows(results_html: str) -> list:
        """Extract Order-type document rows from a results grid page."""
        h = htmllib.unescape(results_html)
        rows = []
        for m in OPENDOC_RE.finditer(h):
            qs = m.group(1)
            # enclosing <tr> for the metadata columns
            s = h.rfind("<tr", 0, m.start())
            e = h.find("</tr>", m.start())
            if s < 0 or e < 0:
                continue
            cells = [
                re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", c)).strip()
                for c in re.findall(r"<td[^>]*>(.*?)</td>", h[s:e], re.S)
            ]
            if len(cells) < 8:
                continue
            doc_type = cells[4]
            if doc_type.strip().lower() not in ORDER_TYPES:
                continue
            params = dict(kv.split("=", 1) for kv in qs.split("&") if "=" in kv)
            if "DocRefNo" not in params:
                continue
            rows.append({
                "docket_number": cells[0].strip("[] "),
                "case_title": cells[1],
                "filing_no": cells[2],
                "filed_date": parse_date(cells[3]),
                "doc_type": doc_type,
                "company": cells[5],
                "doc_title": cells[7] or params.get("DocName", ""),
                "params": params,
            })
        return rows

    def _download(self, page_html: str, params: dict) -> Optional[bytes]:
        """Open one document: POST HdnBtnDocumentOpen then GET ViewDoc.aspx."""
        hdp = "^".join([
            params.get("DocRefNo", ""),
            params.get("DocExt", "pdf"),
            params.get("DocSecurityValue", "Public"),
            params.get("VersionNo", "1"),
            params.get("MatterSeq", "0"),
            "PublicDoc",
            params.get("DocName", ""),
        ])
        fields = self._form_fields(page_html, keep_submit="HdnBtnDocumentOpen")
        fields.update({
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            GP + "HdnDocumentParameter": hdp,
            GP + "HdnBtnDocumentOpen": "",
        })
        opened = self._post(ADV, fields, referer=ADV)
        if opened is None:
            return None
        try:
            time.sleep(self.delay)
            r = self.http.get(VIEWDOC, headers={"Referer": ADV})
            ctype = r.headers.get("Content-Type", "")
            if r.status_code == 200 and (
                "pdf" in ctype or "officedocument" in ctype or "msword" in ctype
                or r.content[:4] in (b"%PDF", b"PK\x03\x04")
            ):
                return r.content
            logger.warning(f"ViewDoc unexpected: HTTP {r.status_code} type={ctype}")
        except Exception as e:
            logger.warning(f"ViewDoc GET error: {e}")
        return None

    @staticmethod
    def _extract_text(content: bytes, ext: str) -> str:
        """Extract full text from a PDF (fitz, OCR fallback) or a .docx."""
        if content[:2] == b"PK" or ext.lower() == "docx":
            try:
                with zipfile.ZipFile(io.BytesIO(content)) as z:
                    xml = z.read("word/document.xml").decode("utf-8", "replace")
                xml = re.sub(r"</w:p>", "\n", xml)
                return clean_text(re.sub(r"<[^>]+>", "", htmllib.unescape(xml)))
            except Exception as e:
                logger.warning(f"docx extract failed: {e}")
                return ""
        # PDF
        try:
            doc = fitz.open(stream=content, filetype="pdf")
        except Exception as e:
            logger.warning(f"fitz open failed: {e}")
            return ""
        text = "".join(page.get_text() for page in doc)
        if len(text.strip()) < 100:
            # image-only scan -> OCR
            try:
                import pytesseract
                from PIL import Image
                ocr = []
                for page in doc:
                    pix = page.get_pixmap(dpi=200)
                    img = Image.open(io.BytesIO(pix.tobytes("png")))
                    ocr.append(pytesseract.image_to_string(img))
                text = "\n".join(ocr)
            except Exception as e:
                logger.debug(f"OCR unavailable/failed: {e}")
        doc.close()
        return clean_text(text)

    # ---- BaseScraper API ----------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        doc_form = self._establish_doc_mode()
        if not doc_form:
            logger.error("Could not establish Documents-search session")
            return
        seen = set()
        for start, end in _month_windows():
            results = self._search_window(doc_form, start, end)
            if not results:
                continue
            page = 1
            while results:
                rows = self._parse_rows(results)
                for row in rows:
                    ref = row["params"].get("DocRefNo", "")
                    if not ref or ref in seen:
                        continue
                    seen.add(ref)
                    content = self._download(results, row["params"])
                    if not content:
                        continue
                    text = self._extract_text(content, row["params"].get("DocExt", "pdf"))
                    if len(text.strip()) < 100:
                        logger.debug(f"Insufficient text for {ref}, skipping")
                        continue
                    row["text"] = text
                    row["doc_ref"] = ref.strip("{}")
                    yield row
                if page >= 40:  # safety cap per window
                    break
                nxt = self._next_page(results)
                if not nxt or "OpenDocument" not in htmllib.unescape(nxt):
                    break
                results = nxt
                page += 1

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("filed_date") and raw["filed_date"] >= since):
                yield raw

    def normalize(self, raw: dict) -> Optional[dict]:
        text = raw.get("text", "")
        if not text or len(text.strip()) < 100:
            return None
        docket = raw.get("docket_number", "")
        title = raw.get("doc_title") or raw.get("doc_type") or "Order"
        if docket and docket not in title:
            title = f"{docket} — {title}"
        return {
            "_id": raw.get("doc_ref") or f"{docket}-{raw.get('filing_no','')}",
            "_source": "US/ME-PUC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title.strip(),
            "text": text,
            "date": raw.get("filed_date"),
            "url": BASE,
            "docket_number": docket,
            "case_title": raw.get("case_title", ""),
            "doc_type": raw.get("doc_type", ""),
            "company": raw.get("company", ""),
            "jurisdiction": "US-ME",
        }

    # ---- diagnostics --------------------------------------------------------

    def test_api(self) -> bool:
        doc_form = self._establish_doc_mode()
        if not doc_form:
            print("FAIL: could not establish Documents-search session")
            return False
        now = datetime.now(timezone.utc)
        start = f"{now.month}/1/{now.year}"
        end = f"{now.month}/{calendar.monthrange(now.year, now.month)[1]}/{now.year}"
        results = self._search_window(doc_form, start, end)
        if not results:
            print("FAIL: search returned nothing")
            return False
        rows = self._parse_rows(results)
        print(f"OK: {len(rows)} Order rows in {start}..{end}")
        for row in rows[:5]:
            print(f"  {row['filed_date']} [{row['doc_type']}] {row['docket_number']} {row['doc_title'][:50]}")
        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/ME-PUC bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MEPUCScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        return

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
