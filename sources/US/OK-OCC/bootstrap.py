#!/usr/bin/env python3
"""
US/OK-OCC -- Oklahoma Corporation Commission — Orders

Fetches the full text of Orders issued by the Oklahoma Corporation Commission
(OCC) — the state agency that adjudicates oil & gas conservation dockets
(pooling, spacing, unitization, increased density, location exceptions),
public-utility (electric/gas/water/telecom) rate & certificate cases, and
transportation matters. Each Commission Order is an administrative
adjudication of a specific case = case_law. Public domain (US state
government edict of a government body — 17 U.S.C. §105 rationale, no
copyright in state-authored edicts).

Strategy (Laserfiche WebLink 11 public repository):

  The OCC publishes its Court Clerk's electronic case files (ECF) through a
  Laserfiche WebLink 11 repository at ``public.occ.ok.gov/WebLink``
  (repo=OCC, dbid=0). The staff "Order Processing-*" Laserfiche templates are
  permission-denied to the anonymous public, but the ``ECF Document`` template
  (~355K entries) is fully viewable. Within it, the ``ECF Document Type``
  metadata field classifies each filing; the order corpus is the
  ``Final Order`` / ``Interim Order`` / ``Emergency Order`` document types.

  The anonymous JSON API is all-POST (Content-Type application/json,
  X-Requested-With: XMLHttpRequest, Referer a /WebLink/ page). A session cookie
  (WebLinkSession) is obtained by GET Browse.aspx + CookieCheck.aspx with a
  cookie jar.

    * SearchService.aspx/GetSearchListing with
      searchSyn ``{[ECF Document]:[ECF Document Type]="Final Order"}``
      returns the matching entries (+ data.hitCount total). We further
      constrain to ``& {LF:Name="*.pdf"}`` to keep only born-digital
      electronic documents (isEdoc), because the bulk of the order corpus is
      historical scanned images that Laserfiche stores with NO OCR text layer
      (GetTextHtmlForPage returns text:"" for them → not retrievable without
      OCR, which the fleet lacks).

  Full text is retrieved by downloading the born-digital PDF from the classic
  WebLink edoc URL ``/WebLink/0/edoc/{entryId}/{filename}`` and extracting text
  with PyMuPDF/fitz. Each page carries a one-line OCC Court-Clerk filing stamp
  ("Case CD ... Entry No. ... Filed in OCC Court Clerk's Office on MM/DD/YYYY -
  Page N of M"); normalize() strips those stamp lines and keeps records whose
  remaining body text is substantial (image-only PDFs carry only the stamp and
  are dropped).

  Per-document metadata (ECF Case Number, ECF Docket Date, ECF Division,
  ECF Document Type) comes from DocumentService.aspx/GetBasicDocumentInfo.

Usage:
  python bootstrap.py bootstrap            # Full pull (all born-digital Orders)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from common.base_scraper import BaseScraper  # noqa: E402

logger = logging.getLogger("legal-data-hunter")

BASE = "https://public.occ.ok.gov/WebLink"
REPO = "OCC"
DBID = 0

# ECF Document Type values that are Commission orders (case_law).
ORDER_TYPES = ["Final Order", "Interim Order", "Emergency Order"]

# The Court-Clerk filing stamp printed on every page of an e-filed PDF.
STAMP_RE = re.compile(
    r"(?im)^\s*Case\s+CD\s*[\d\-]+\s+Entry\s*No\.?\s*\d+\s+Filed\s+in\s+OCC\s+"
    r"Court\s+Clerk'?s?\s+Office\s+on\s+[\d/]+\s*-\s*Page\s+\d+\s+of\s+\d+\s*$"
)

# Minimum body-text length (after stamp removal) to count a PDF as text-bearing.
MIN_BODY_CHARS = 600


class OKOCCScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            }
        )
        self.delay = 0.4
        self._session_ready = False

    # ---- session / low-level ------------------------------------------------

    def _ensure_session(self):
        if self._session_ready:
            return
        self.session.get(f"{BASE}/Browse.aspx?dbid={DBID}&repo={REPO}", timeout=60)
        self.session.get(f"{BASE}/CookieCheck.aspx", timeout=60)
        self._session_ready = True

    def _api(self, path: str, payload: dict, retries: int = 4) -> Optional[dict]:
        self._ensure_session()
        hdr = {
            "Content-Type": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"{BASE}/Browse.aspx?dbid={DBID}&repo={REPO}",
        }
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                r = self.session.post(
                    f"{BASE}{path}", headers=hdr, data=json.dumps(payload), timeout=90
                )
                if r.status_code == 200:
                    try:
                        return json.loads(r.text)
                    except Exception:
                        return None
                logger.warning(f"HTTP {r.status_code} for {path}: {r.text[:120]}")
            except Exception as e:
                logger.warning(f"API error {path} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _edoc_bytes(self, entry_id: int, name: str, retries: int = 3) -> Optional[bytes]:
        self._ensure_session()
        url = f"{BASE}/0/edoc/{entry_id}/{name}"
        hdr = {"Referer": f"{BASE}/DocView.aspx?id={entry_id}&dbid={DBID}&repo={REPO}"}
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                r = self.session.get(url, headers=hdr, timeout=180)
                if r.status_code == 200 and r.content[:4] == b"%PDF":
                    return r.content
                if r.status_code == 200:
                    return None  # not a PDF (image placeholder / error page)
                logger.warning(f"HTTP {r.status_code} for edoc {entry_id}")
            except Exception as e:
                logger.warning(f"edoc error {entry_id} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- search / enumeration ----------------------------------------------

    def _search_page(self, syn: str, start: int, end: int) -> Optional[dict]:
        data = self._api(
            "/SearchService.aspx/GetSearchListing",
            {
                "repoName": REPO,
                "searchSyn": syn,
                "searchUuid": "",
                "sortColumn": "",
                "startIdx": start,
                "endIdx": end,
                "getNewListing": True,
                "sortOrder": 0,
                "displayInGridView": False,
            },
        )
        if not data:
            return None
        return data.get("data")

    def _enumerate_orders(self) -> Generator[dict, None, None]:
        seen = set()
        page = 100
        for doc_type in ORDER_TYPES:
            syn = f'{{[ECF Document]:[ECF Document Type]="{doc_type}"}} & {{LF:Name="*.pdf"}}'
            first = self._search_page(syn, 1, page)
            if not first:
                logger.info(f"{doc_type}: search returned nothing")
                continue
            total = first.get("hitCount") or 0
            logger.info(f"{doc_type}: {total} born-digital entries")
            start = 1
            data = first
            while start <= total:
                if data is None:
                    data = self._search_page(syn, start, min(start + page - 1, total))
                if not data:
                    break
                results = data.get("results") or []
                if not results:
                    break
                for res in results:
                    eid = res.get("entryId")
                    if eid is None or eid in seen:
                        continue
                    seen.add(eid)
                    if not res.get("isEdoc"):
                        continue
                    yield {
                        "entryId": eid,
                        "name": res.get("name") or "",
                        "doc_type": doc_type,
                        "pageCount": res.get("thumbnailPageCount"),
                    }
                start += len(results)
                data = None

    # ---- helpers ------------------------------------------------------------

    @staticmethod
    def _clean_body(text: str) -> str:
        text = STAMP_RE.sub("", text)
        # collapse excessive blank lines / whitespace but keep paragraph breaks
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _iso_date(mdY: Optional[str]) -> Optional[str]:
        if not mdY:
            return None
        for fmt in ("%m/%d/%Y", "%m/%d/%Y %I:%M:%S %p", "%Y-%m-%d"):
            try:
                return datetime.strptime(mdY.strip(), fmt).date().isoformat()
            except Exception:
                continue
        return None

    @staticmethod
    def _title_from_name(name: str, doc_type: str, case_no: str) -> str:
        # strip leading "CDxxxx-xxxxx_N_DocType_" prefix and the epoch timestamp
        base = re.sub(r"\.pdf$", "", name, flags=re.I)
        # remove epoch millisecond timestamp segment "_1648237552201-"
        base = re.sub(r"_?\d{12,}-?", " ", base)
        # remove the case/doctype prefix tokens
        base = re.sub(r"^CD[\d\-]+_\d+_", "", base)
        desc = re.sub(r"[_]+", " ", base).strip(" -_")
        desc = re.sub(r"\s+", " ", desc)
        if desc and len(desc) > 3 and not desc.lower().startswith(doc_type.lower()):
            return f"{doc_type} — {case_no} ({desc})" if case_no else f"{doc_type} ({desc})"
        return f"{doc_type} — {case_no}" if case_no else doc_type

    # ---- normalize ----------------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        if fitz is None:
            logger.error("PyMuPDF (fitz) not installed — cannot extract text")
            return None
        eid = raw["entryId"]
        name = raw["name"]
        doc_type = raw.get("doc_type") or "Order"

        # metadata
        meta_fields = {}
        info = self._api(
            "/DocumentService.aspx/GetBasicDocumentInfo",
            {"repoName": REPO, "entryId": eid},
        )
        if info:
            md = (info.get("data") or {}).get("metadata") or {}
            for f in md.get("fInfo") or []:
                vals = f.get("values") or []
                if vals:
                    meta_fields[f.get("name")] = vals[0]

        case_raw = meta_fields.get("ECF Case Number") or ""
        division = meta_fields.get("ECF Division") or ""
        case_type = meta_fields.get("ECF Case Type") or ""
        docket_date = meta_fields.get("ECF Docket Date")
        dtype = meta_fields.get("ECF Document Type") or doc_type

        # download + extract
        pdf = self._edoc_bytes(eid, name)
        if not pdf:
            return None
        try:
            doc = fitz.open(stream=pdf, filetype="pdf")
            full = "".join(p.get_text() for p in doc)
            pages = doc.page_count
            doc.close()
        except Exception as e:
            logger.warning(f"PDF parse failed for {eid}: {e}")
            return None

        body = self._clean_body(full)
        if len(body) < MIN_BODY_CHARS:
            # image-only order (no OCR text layer) — cannot capture full text
            logger.debug(f"Skip {eid}: image-only ({len(body)} body chars)")
            return None

        # case number formatting: "202001826" or "1991-160589" -> "CD <n>"
        case_no = case_raw.strip()
        if case_no and not case_no.upper().startswith(("CD", "PUD", "CO", "OG", "TR")):
            case_no = f"CD {case_no}"

        # date: prefer a "Service Date"/"dated" in body, else docket date
        date_iso = self._iso_date(docket_date)

        title = self._title_from_name(name, dtype, case_no)
        url = f"{BASE}/DocView.aspx?id={eid}&dbid={DBID}&repo={REPO}"

        return {
            "_id": f"OK-OCC-{eid}",
            "_source": "US/OK-OCC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": body,
            "date": date_iso,
            "url": url,
            "case_number": case_no or None,
            "document_type": dtype,
            "division": division or None,
            "case_type": case_type or None,
            "pages": pages,
            "entry_id": eid,
            "jurisdiction": "US-OK",
            "court": "Oklahoma Corporation Commission",
        }

    # ---- iteration ----------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._enumerate_orders()

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # WebLink search order is newest-first per type; yield everything and
        # let the loader dedup on _id (no cheap server-side "modified since").
        yield from self._enumerate_orders()

    # ---- connectivity test --------------------------------------------------

    def test_api(self) -> bool:
        try:
            self._ensure_session()
            data = self._search_page(
                '{[ECF Document]:[ECF Document Type]="Final Order"} & {LF:Name="*.pdf"}',
                1, 3,
            )
            if not data:
                logger.error("test-api: no search response")
                return False
            total = data.get("hitCount")
            results = data.get("results") or []
            logger.info(f"test-api OK: Final Order born-digital hitCount={total}, "
                        f"first={results[0].get('name') if results else None}")
            if results:
                rec = self.normalize({
                    "entryId": results[0]["entryId"],
                    "name": results[0]["name"],
                    "doc_type": "Final Order",
                })
                logger.info(f"test-api normalize: "
                            f"{'text ' + str(len(rec['text'])) + ' chars' if rec else 'image-only (skipped)'}")
            return True
        except Exception as e:
            logger.error(f"test-api FAILED: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/OK-OCC bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test-api"], help="Command"
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    scraper = OKOCCScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        return

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
