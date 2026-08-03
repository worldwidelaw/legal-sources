#!/usr/bin/env python3
"""
US/CA-CPUC -- California Public Utilities Commission Decisions & Resolutions

Fetches the full text of the California Public Utilities Commission's
adjudicatory dispositions — Final Decisions and Final Resolutions — that
resolve numbered proceedings (electric/gas/water/telecom/transportation
rate cases, applications, complaints, rulemakings and enforcement) =
case_law. CPUC decisions are official California state-government works /
government edicts in the public domain.

Access (no auth, no CAPTCHA, no JavaScript required):
  The CPUC document repository at docs.cpuc.ca.gov exposes a classic
  ASP.NET WebForms search. There is NO documented REST/JSON API, but the
  server renders full result tables server-side, so the search can be
  driven with plain HTTP POSTs:

    1. GET  https://docs.cpuc.ca.gov/advancedsearchform.aspx
       -> read __VIEWSTATE / __VIEWSTATEGENERATOR / __EVENTVALIDATION.
    2. POST the search back to advancedsearchform.aspx with
       ddlCpuc01Types set to the document-type id (19 = "Final Decision",
       55 = "Final Resolution") and a PubDateFrom/PubDateTo window.
       The response is the first page of a result table (20 rows), each
       row carrying the title (decision number + short caption +
       "Proceeding: <num>"), doc type, a DIRECT PublishedDocs PDF link,
       and the published date.
    3. Page through the remaining pages by POSTing __EVENTTARGET=
       "lnkNextPage" to https://docs.cpuc.ca.gov/SearchRes.aspx with the
       viewstate carried forward from the previous page, until no
       "lnkNextPage" control remains.
    4. Download each decision PDF straight from
       docs.cpuc.ca.gov/PublishedDocs/... and extract its text with
       common.pdf_extract (born-digital, text-layer). A <200-char guard
       skips the rare scanned/empty document.

Documents issued after ~June 2000 are online, so discovery walks month by
month from the present back to FIRST_YEAR/FIRST_MONTH.

Usage:
  python bootstrap.py bootstrap            # Full pull (present -> 2000-06)
  python bootstrap.py bootstrap --sample   # ~12 recent samples
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
from common import pdf_extract

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CA-CPUC")

BASE = "https://docs.cpuc.ca.gov"
ADV_URL = BASE + "/advancedsearchform.aspx"
RES_URL = BASE + "/SearchRes.aspx"

FIRST_YEAR = 2000   # documents issued after ~June 2000 are online
FIRST_MONTH = 6

# CPUC-generated document types that are adjudicatory dispositions =
# case_law.  Mapped to the ddlCpuc01Types option values on the advanced
# search form.  (17 = Agenda Decision / 18 = Comment Decision are the
# draft/proposed versions circulated for comment; we keep only the final
# adopted dispositions to avoid duplicating drafts.)
DOC_TYPES = {
    "19": "Final Decision",
    "55": "Final Resolution",
}

MONTH_DAYS = {1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30,
              7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}

# One result <tr>: Title / Doc Type / Doc Links / Published Date.
ROW_RE = re.compile(
    r"<td class='ResultTitleTD'>(.*?)</td>\s*"
    r"<td class='ResultTypeTD'>(.*?)</td>\s*"
    r"<td class='ResultLinkTD'[^>]*>(.*?)</td>\s*"
    r"<td class='ResultDateTD'>(.*?)</td>",
    re.S,
)
PDF_HREF_RE = re.compile(r"href='([^']*\.PDF)'", re.I)
PROC_RE = re.compile(r"Proceeding:\s*([A-Za-z0-9.\-]+)", re.I)
# Decision / Resolution number at the start of a title, e.g.
#   D2406021 / D.24-06-021 / R.24-06-012 / RES E-5000
DECNUM_RE = re.compile(
    r"^\s*((?:D|R|RES)\.?\s*[A-Za-z]?-?\s*\d[\d\-]*)", re.I
)


def _clean(text: str) -> str:
    text = re.sub(r"<br\s*/?>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


class CaCpucScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
            ),
            "accept": "text/html,application/xhtml+xml",
        })
        try:  # silence the LibreSSL / unverified-TLS chatter
            requests.packages.urllib3.disable_warnings()
        except Exception:
            pass

    # ---------------------------------------------------------------- http
    def _get(self, url: str):
        for attempt in range(5):
            time.sleep(self.delay)
            try:
                r = self.session.get(url, timeout=90)
                if r.status_code == 200:
                    return r.text
                logger.warning(f"  GET {url} -> {r.status_code} "
                               f"(attempt {attempt + 1})")
            except Exception as e:
                logger.warning(f"  GET error {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    def _post(self, url: str, data: dict):
        for attempt in range(5):
            time.sleep(self.delay)
            try:
                r = self.session.post(url, data=data, timeout=120)
                if r.status_code == 200:
                    return r.text
                logger.warning(f"  POST {url} -> {r.status_code} "
                               f"(attempt {attempt + 1})")
            except Exception as e:
                logger.warning(f"  POST error {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    @staticmethod
    def _hidden(html_text: str, name: str) -> str:
        m = re.search(
            r'name="%s"[^>]*value="([^"]*)"' % re.escape(name), html_text
        )
        return m.group(1) if m else ""

    # --------------------------------------------------------- search
    def _search_first_page(self, type_id: str, start: str, end: str) -> str | None:
        """Run the advanced search; return the first result-page HTML."""
        form = self._get(ADV_URL)
        if not form:
            return None
        data = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": self._hidden(form, "__VIEWSTATE"),
            "__VIEWSTATEGENERATOR": self._hidden(form, "__VIEWSTATEGENERATOR"),
            "__EVENTVALIDATION": self._hidden(form, "__EVENTVALIDATION"),
            "DocTitle": "",
            "EfileConfirmNum": "",
            "ddlCpuc01Types": type_id,
            "ddlEfileTypes": "-1",
            "IndustryID": "-1",
            "FilingDateFrom": "",
            "FilingDateTo": "",
            "MeetDate": "",
            "ProcNum": "",
            "PubDateFrom": start,
            "PubDateTo": end,
            "SearchButton": "Search",
        }
        return self._post(ADV_URL, data)

    def _next_page(self, prev_html: str) -> str | None:
        if "lnkNextPage" not in prev_html:
            return None
        data = {
            "__EVENTTARGET": "lnkNextPage",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": self._hidden(prev_html, "__VIEWSTATE"),
            "__VIEWSTATEGENERATOR": self._hidden(prev_html, "__VIEWSTATEGENERATOR"),
            "__EVENTVALIDATION": self._hidden(prev_html, "__EVENTVALIDATION"),
        }
        return self._post(RES_URL, data)

    @staticmethod
    def _parse_rows(html_text: str, type_label: str) -> list[dict]:
        out: list[dict] = []
        for title_html, type_html, link_html, date_html in ROW_RE.findall(html_text):
            m = PDF_HREF_RE.search(link_html)
            if not m:
                continue
            href = m.group(1)
            pdf_url = href if href.startswith("http") else BASE + href
            doc_id = Path(href).stem  # e.g. 534744149
            title = _clean(title_html)
            proc = PROC_RE.search(title)
            proceeding = proc.group(1) if proc else None
            # strip the trailing "Proceeding: ..." off the caption
            caption = PROC_RE.sub("", title).strip(" -–—")
            dm = DECNUM_RE.search(caption)
            decision_number = re.sub(r"\s+", "", dm.group(1)).upper() if dm else None
            out.append({
                "doc_id": doc_id,
                "pdf_url": pdf_url,
                "title": caption or title,
                "decision_number": decision_number,
                "proceeding": proceeding,
                "document_type": _clean(type_html) or type_label,
                "pub_date": _clean(date_html),
            })
        return out

    def _month_rows(self, type_id: str, type_label: str,
                    year: int, month: int) -> list[dict]:
        last = MONTH_DAYS[month]
        if month == 2 and (year % 4 != 0 or (year % 100 == 0 and year % 400 != 0)):
            last = 28
        start = f"{month:02d}/01/{year:04d}"
        end = f"{month:02d}/{last:02d}/{year:04d}"
        html_text = self._search_first_page(type_id, start, end)
        if not html_text:
            return []
        seen: set[str] = set()
        rows: list[dict] = []
        page = 1
        while html_text and page <= 300:  # safety cap
            batch = self._parse_rows(html_text, type_label)
            fresh = [r for r in batch if r["doc_id"] not in seen]
            if not fresh:
                break
            for r in fresh:
                seen.add(r["doc_id"])
            rows.extend(fresh)
            html_text = self._next_page(html_text)
            page += 1
        logger.info(f"  {type_label} {year:04d}-{month:02d}: {len(rows)} documents")
        return rows

    # --------------------------------------------------------- discovery
    def _iter_months(self):
        now = datetime.now(timezone.utc)
        year, month = now.year, now.month
        while (year, month) >= (FIRST_YEAR, FIRST_MONTH):
            yield year, month
            month -= 1
            if month == 0:
                month = 12
                year -= 1

    # ------------------------------------------------------- build record
    def _build_raw(self, meta: dict) -> dict | None:
        text = pdf_extract.extract_pdf_markdown(
            "US/CA-CPUC", meta["doc_id"],
            pdf_url=meta["pdf_url"], table="case_law", force=True,
        )
        if not text or len(text.strip()) < 200:
            return None
        meta = dict(meta)
        meta["text"] = text.strip()
        return meta

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for year, month in self._iter_months():
            for type_id, type_label in DOC_TYPES.items():
                for meta in self._month_rows(type_id, type_label, year, month):
                    try:
                        raw = self._build_raw(meta)
                    except Exception as e:
                        logger.warning(f"  extract failed {meta['doc_id']}: {e}")
                        raw = None
                    if raw:
                        yield raw
                        emitted += 1
                        if sample and emitted >= 12:
                            return

    # --------------------------------------------------------- normalize
    @staticmethod
    def _iso_date(mdy: str | None) -> str | None:
        if not mdy:
            return None
        m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", mdy.strip())
        if not m:
            return None
        mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if not (1 <= mo <= 12 and 1 <= d <= 31 and 1900 <= y <= 2035):
            return None
        return f"{y:04d}-{mo:02d}-{d:02d}"

    def normalize(self, raw: dict) -> dict:
        doc_id = raw["doc_id"]
        dt = raw.get("document_type") or "Final Decision"
        num = raw.get("decision_number")
        caption = raw.get("title") or ""
        # Drop the trailing source filename extension CPUC leaves on captions.
        caption = re.sub(r"\.(docx?|pdf)\b", "", caption, flags=re.I).strip(" -–—")
        proc = raw.get("proceeding")
        # CPUC captions usually already begin with the decision number;
        # only prepend it when it is missing so we don't duplicate it.
        title = caption or dt
        if num and num[:5].upper() not in caption.upper():
            title = f"{num} {caption}".strip()
        title = re.sub(r"\s+", " ", title).strip()[:300]
        return {
            "_id": f"US/CA-CPUC/{doc_id}",
            "_source": "US/CA-CPUC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": doc_id,
            "decision_number": num,
            "proceeding": proc,
            "document_type": dt,
            "issuer": "California Public Utilities Commission",
            "title": title,
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": self._iso_date(raw.get("pub_date")),
            "jurisdiction": "US-CA",
        }

    # ------------------------------------------------------------- fetch
    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            iso = self._iso_date(raw.get("pub_date"))
            if not since or (iso and iso >= since):
                yield raw

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing CPUC advanced search + PDF extraction...")
        try:
            now = datetime.now(timezone.utc)
            year, month = now.year, now.month
            for _ in range(6):
                for type_id, type_label in DOC_TYPES.items():
                    rows = self._month_rows(type_id, type_label, year, month)
                    for meta in rows:
                        raw = self._build_raw(meta)
                        if raw:
                            logger.info(
                                f"  OK: {meta.get('decision_number')} "
                                f"[{meta['document_type']}] "
                                f"{len(raw['text'])} chars, "
                                f"proc={meta.get('proceeding')}")
                            logger.info("API test PASSED")
                            return True
                month -= 1
                if month == 0:
                    month, year = 12, year - 1
            logger.error("  No extractable decision found in recent months")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/CA-CPUC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CaCpucScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
