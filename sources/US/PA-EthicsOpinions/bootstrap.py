#!/usr/bin/env python3
"""
US/PA-EthicsOpinions -- Pennsylvania State Ethics Commission
Opinions of the Commission and Advices of Counsel.

Fetches the full text of the Pennsylvania State Ethics Commission's formal
Opinions and Advices of Counsel construing the Public Official and Employee
Ethics Act (65 Pa.C.S. Ch. 11) and 51 Pa. Code. Each ruling is the
Commission's written interpretation of the Ethics Act applied to a requester's
facts = doctrine. Pennsylvania state agency public record (government-edict
work).

Access (no auth, no CAPTCHA on the API):
  The Commission's public "Ethics eLibrary" is a Laserfiche WebLink 11 portal:

      https://www.ethicsrulings.pa.gov/WebLink   (repo "Ethics", dbid 0)

  Structure: Rulings > Ethics > Opinions  (folder id 36759)
             Rulings > Ethics > Advices   (folder id 34015)
  Each holds year folders (1979..present); each year folder holds the
  individual rulings as imaged documents with an OCR text layer.

  JSON API (POST, application/json):
    * FolderListingService.aspx/GetFolderListingIds  {repoName, folderId,
        sortColumn, sortAscending} -> ALL child entry ids (uncapped).
    * FolderListingService.aspx/GetFolderListing2    {repoName, folderId,
        getNewListing, start, end, sortColumn, sortAscending} -> up to 40
        children with name + page count (start/end ignored; sort to page).
    * FolderListingService.aspx/GetDocumentInfo      {repoName, dId}
        -> {pageCount, isEdoc}.
    * DocumentService.aspx/GetTextHtmlForPage        {repoName, documentId,
        pageNum, showAnn:false, searchUuid:""} -> one page's OCR text.

Strategy:
  Establish a cookie session, enumerate the Opinions and Advices year folders,
  list each year's document ids + names, then fetch every ruling's OCR text
  page-by-page and join. All rulings are doctrine.

Usage:
  python bootstrap.py bootstrap            # Full pull (all rulings)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples (newest first)
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
"""

from __future__ import annotations

import sys
import json
import html
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.PA-EthicsOpinions")

WL = "https://www.ethicsrulings.pa.gov/WebLink"
REPO = "Ethics"
ROOT_OPINIONS = 36759
ROOT_ADVICES = 34015

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

YEAR_RE = re.compile(r"^(19|20)\d{2}$")
NUMBER_RE = re.compile(r"\b(\d{2,4}-\d{1,4}[A-Za-z]?)\b")
DATE_DECIDED_RE = re.compile(
    r"DATE\s+(?:DECIDED|MAILED|ISSUED)\s*:?\s*(\d{1,2})/(\d{1,2})/((?:19|20)\d{2})",
    re.I,
)
MDY_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/((?:19|20)\d{2})\b")
MONTHS = (
    "January|February|March|April|May|June|July|August|September|October|"
    "November|December"
)
MONTH_DATE_RE = re.compile(rf"\b({MONTHS})\s+(\d{{1,2}}),\s+((?:19|20)\d{{2}})\b")
MONTH_NUM = {
    m: i + 1
    for i, m in enumerate(
        "January February March April May June July August September "
        "October November December".split()
    )
}


def _iso(y: int, m: int, d: int) -> str | None:
    if 1 <= m <= 12 and 1 <= d <= 31 and 1900 <= y <= 2100:
        return f"{y:04d}-{m:02d}-{d:02d}"
    return None


def _parse_date(text: str) -> str | None:
    m = DATE_DECIDED_RE.search(text or "")
    if m:
        iso = _iso(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        if iso:
            return iso
    m = MONTH_DATE_RE.search(text or "")
    if m:
        iso = _iso(int(m.group(3)), MONTH_NUM[m.group(1)], int(m.group(2)))
        if iso:
            return iso
    m = MDY_RE.search(text or "")
    if m:
        iso = _iso(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        if iso:
            return iso
    return None


class PAEthicsOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.6
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})
        self._ready = False

    # ------------------------------------------------------------ session
    def _ensure_session(self) -> bool:
        if self._ready:
            return True
        try:
            self.session.get(f"{WL}/Welcome.aspx?cr=1", timeout=60)
            self.session.get(
                f"{WL}/Browse.aspx?dbid=0&repo={REPO}", timeout=60
            )
        except Exception as e:
            logger.error(f"Session setup failed: {e}")
            return False
        self._ready = bool(self.session.cookies.get("WebLinkSession"))
        if not self._ready:
            logger.error("Could not obtain WebLinkSession cookie")
        return self._ready

    def _api(self, path: str, payload: dict) -> dict | None:
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"{WL}/Browse.aspx?dbid=0&repo={REPO}",
        }
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                r = self.session.post(
                    f"{WL}/{path}", data=json.dumps(payload),
                    headers=headers, timeout=60,
                )
                if r.status_code == 200:
                    try:
                        return r.json()
                    except Exception:
                        return None
            except Exception as e:
                logger.warning(f"POST {path} failed (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- enumerate
    def _folder_ids(self, folder_id: int, ascending: bool = True) -> list[int]:
        d = self._api(
            "FolderListingService.aspx/GetFolderListingIds",
            {"repoName": REPO, "folderId": folder_id,
             "sortColumn": "", "sortAscending": ascending},
        )
        if not d or "data" not in d or not isinstance(d["data"], list):
            return []
        out = []
        for x in d["data"]:
            try:
                out.append(int(x))
            except (TypeError, ValueError):
                continue
        return out

    def _listing_meta(self, folder_id: int) -> dict[int, dict]:
        """Map entryId -> {name, pageCount} via asc+desc GetFolderListing2."""
        meta: dict[int, dict] = {}
        for asc in (True, False):
            d = self._api(
                "FolderListingService.aspx/GetFolderListing2",
                {"repoName": REPO, "folderId": folder_id, "getNewListing": True,
                 "start": 0, "end": 40, "sortColumn": "Name", "sortAscending": asc},
            )
            if not d or "data" not in d:
                continue
            for r in (d["data"] or {}).get("results", []) or []:
                if not r:
                    continue
                try:
                    eid = int(r.get("entryId"))
                except (TypeError, ValueError):
                    continue
                data = r.get("data") or []
                pc = 0
                if len(data) > 1:
                    try:
                        pc = int(data[1])
                    except (TypeError, ValueError):
                        pc = 0
                meta[eid] = {
                    "name": r.get("name") or "",
                    "pages": pc,
                    "is_edoc": bool(r.get("isEdoc")),
                }
        return meta

    def _year_folders(self, root_id: int) -> list[tuple[int, int]]:
        """Return [(year, folderId), ...] for a root (Opinions/Advices)."""
        meta = self._listing_meta(root_id)
        years = []
        for eid, m in meta.items():
            name = (m["name"] or "").strip()
            if YEAR_RE.match(name):
                years.append((int(name), eid))
        years.sort(reverse=True)  # newest first
        return years

    def _page_count(self, doc_id: int) -> int:
        d = self._api(
            "FolderListingService.aspx/GetDocumentInfo",
            {"repoName": REPO, "dId": doc_id},
        )
        if d and isinstance(d.get("data"), dict):
            try:
                return int(d["data"].get("pageCount") or 0)
            except (TypeError, ValueError):
                return 0
        return 0

    def _doc_text(self, doc_id: int, pages: int) -> str:
        if pages <= 0:
            pages = self._page_count(doc_id)
        if pages <= 0:
            pages = 1
        parts = []
        for p in range(1, pages + 1):
            d = self._api(
                "DocumentService.aspx/GetTextHtmlForPage",
                {"repoName": REPO, "documentId": doc_id, "pageNum": p,
                 "showAnn": False, "searchUuid": ""},
            )
            if d and isinstance(d.get("data"), dict):
                t = d["data"].get("text") or ""
                if t.strip():
                    parts.append(t.strip())
        text = "\n".join(parts)
        # GetTextHtmlForPage wraps OCR-detected URLs in <a> tags; strip any
        # HTML tags and decode entities so text is clean plain text.
        text = re.sub(r"<[^>]+>", "", text)
        text = html.unescape(text)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    # ------------------------------------------------------------ iterate
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for series, root in (("Opinion", ROOT_OPINIONS), ("Advice", ROOT_ADVICES)):
            years = self._year_folders(root)
            logger.info(f"{series}: {len(years)} year folders")
            for year, yfid in years:
                ids = self._folder_ids(yfid)
                meta = self._listing_meta(yfid)
                logger.info(f"  {series} {year}: {len(ids)} documents")
                for did in ids:
                    m = meta.get(did, {})
                    if m.get("is_edoc") is False and m.get("pages", 0) == 0:
                        # folder-like entry with no pages; skip
                        pass
                    name = (m.get("name") or "").strip()
                    pages = m.get("pages", 0)
                    text = self._doc_text(did, pages)
                    if not text or len(text) < 120:
                        continue
                    number = None
                    nm = NUMBER_RE.search(name) or NUMBER_RE.search(text[:400])
                    if nm:
                        number = nm.group(1)
                    yield {
                        "doc_id": str(did),
                        "series": series,
                        "number": number,
                        "name": name or (f"{series} {number}" if number else str(did)),
                        "year": year,
                        "text": text,
                        "date": _parse_date(text),
                        "url": f"{WL}/DocView.aspx?id={did}&dbid=0&repo={REPO}",
                    }
                    emitted += 1
                    if emitted % 25 == 0:
                        logger.info(f"    ... {emitted} rulings emitted")
                    if sample and emitted >= 12:
                        return

    # --------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing PA State Ethics Commission eLibrary...")
        if not self._ensure_session():
            return False
        years = self._year_folders(ROOT_OPINIONS)
        if len(years) < 20:
            logger.error(f"API test FAILED: too few Opinion years ({len(years)})")
            return False
        logger.info(f"Opinion year folders: {len(years)} ({years[-1][0]}..{years[0][0]})")
        ok = 0
        # Find the most recent year folder that actually contains documents.
        year, yfid, ids = None, None, []
        for y, f in years:
            fids = self._folder_ids(f)
            if fids:
                year, yfid, ids = y, f, fids
                break
        if not ids:
            logger.error("API test FAILED: no documents in any year folder")
            return False
        meta = self._listing_meta(yfid)
        for did in ids[:4]:
            m = meta.get(did, {})
            text = self._doc_text(did, m.get("pages", 0))
            if text and len(text) > 120:
                logger.info(
                    f"  {year} doc {did} OK ({len(text)} chars, "
                    f"date={_parse_date(text)}, name='{m.get('name')}')"
                )
                ok += 1
        if ok >= 1:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        series = raw["series"]
        number = raw.get("number")
        name = raw.get("name") or ""
        if number:
            title = f"PA Ethics {series} {number}"
            rest = name.replace(number, "").strip(" -–")
            if rest and rest.lower() not in ("", "confidential"):
                title += f" ({rest})"
            elif rest.lower() == "confidential":
                title += " (Confidential)"
        else:
            title = f"PA Ethics {series}: {name}" if name else f"PA Ethics {series}"
        return {
            "_id": f"US/PA-EthicsOpinions/{series}-{raw['doc_id']}",
            "_source": "US/PA-EthicsOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": raw["doc_id"],
            "opinion_number": number,
            "series": (
                "Opinion of the Commission" if series == "Opinion"
                else "Advice of Counsel"
            ),
            "issuer": "Pennsylvania State Ethics Commission",
            "title": title,
            "text": raw["text"],
            "date": raw.get("date"),
            "year": raw.get("year"),
            "url": raw["url"],
            "jurisdiction": "US-PA",
        }

    # ------------------------------------------------------------- fetch
    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        if not self._ensure_session():
            return
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        if not self._ensure_session():
            return
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            date = raw.get("date")
            if not since or (date and date >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/PA-EthicsOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = PAEthicsOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
