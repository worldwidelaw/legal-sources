#!/usr/bin/env python3
"""
US/FL-PERC -- Florida Public Employees Relations Commission (PERC)
Final Orders

Fetches the full text of every published Final Order issued by the
Florida Public Employees Relations Commission (PERC). PERC is Florida's
independent quasi-judicial agency that adjudicates public-sector labor
relations under Part II of Chapter 447, Florida Statutes: unfair labor
practice charges, bargaining-unit determinations, representation and
decertification elections, employee-organization registrations, and
career-service / veterans'-preference employment appeals. Each order
resolves a specific contested case = case_law. PERC orders are official
Florida state-government works in the public domain (government edicts,
17 U.S.C. § 105 analogue).

Access (no JavaScript, no CAPTCHA, no auth):
  Per section 120.53, Florida Statutes, every agency Final Order issued
  after 1 July 2015 is published on the Division of Administrative
  Hearings' "Florida Agency Indexed Orders" (FLAIO) database at
  https://www.doah.state.fl.us/FLAIO/. PERC is agency code "PER".
  The database is a stateful ASP session app:

    1. GET  /FLAIO/                             -> obtain session cookie
    2. POST /FLAIO/searchAction.asp?sT=byOther  Agency=PER + wide date
       range + DocType=ALL & Subject=ALL        -> stores the result set
       in the session and 302-redirects to /FLAIO/flaioDisplay.asp
    3. GET  /FLAIO/flaioDisplay.asp?pc={N}      -> page N of the result
       set (50 rows/page). Each row is a table of
       Agency | AgencyCaseNo | DOAH-OrderNo | IssueDate |
       <a href="https://www.doah.state.fl.us/FLAID/PER/{YEAR}/
       PER_{caseno}_{ts}.pdf">DocType</a> | Subject.

  The linked PDFs are born-digital text-layer orders on the DOAH file
  host (doah.state.fl.us/FLAID/PER/...). Text is extracted via
  common.pdf_extract.

Strategy:
  1. Establish the FLAIO session and run the Agency=PER search.
  2. Read the max page number (pc=) from the pager, walk every result
     page, and collect one document per PDF link with its row metadata
     (agency case number, DOAH order number, issue date, doc type,
     subject).
  3. Download each PDF (~1 req/s), extract its text, and emit a record.
     The issue date and case numbers come from the reliable index row;
     the order body supplies the full text.

Usage:
  python bootstrap.py bootstrap            # Full pull (2015-present)
  python bootstrap.py bootstrap --sample   # Fetch ~12 recent samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import subprocess
import tempfile
import time
from html import unescape
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common import pdf_extract

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.FL-PERC")

BASE_URL = "https://www.doah.state.fl.us"
LANDING = BASE_URL + "/FLAIO/"
SEARCH_URL = BASE_URL + "/FLAIO/searchAction.asp?sT=byOther"
PAGE_URL = BASE_URL + "/FLAIO/flaioDisplay.asp?pc={pc}"
AGENCY_CODE = "PER"
FIRST_DATE = "01/01/2015"  # FLAIO holds agency Final Orders after 1 Jul 2015

# a PERC document PDF link on the DOAH file host
PDF_HREF_RE = re.compile(
    r'href=["\'](https?://[^"\']*?/FLAID/PER/\d{4}/[^"\']+?\.pdf)["\']', re.I
)
# pager links: flaioDisplay.asp?pc=142
PC_RE = re.compile(r"flaioDisplay\.asp\?pc=(\d+)", re.I)
# result rows
ROW_RE = re.compile(r"<tr\b[^>]*>(.*?)</tr>", re.I | re.S)
CELL_RE = re.compile(r"<td\b[^>]*>(.*?)</td>", re.I | re.S)
TAG_RE = re.compile(r"<[^>]+>")
DATE_MDY_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
)


def _clean_cell(html: str) -> str:
    return unescape(TAG_RE.sub("", html)).strip()


class FLPERCScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._cookie_jar = tempfile.NamedTemporaryFile(
            prefix="flperc_cookies_", suffix=".txt", delete=False
        ).name

    # ---------------------------------------------------------------- http
    def _curl(self, url: str, post_data: str | None = None,
              want_bytes: bool = False):
        url = url.replace(" ", "%20")  # some filenames contain spaces
        cmd = [
            "curl", "-s", "-L", "--max-time", "120", "-A", UA,
            "-H", "Accept: */*",
            "-c", self._cookie_jar, "-b", self._cookie_jar,
            "-e", LANDING,
        ]
        if post_data is not None:
            cmd += ["--data", post_data]
        cmd.append(url)
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(cmd, capture_output=True, timeout=150)
                if out.returncode == 0 and out.stdout:
                    return out.stdout if want_bytes else \
                        out.stdout.decode("latin-1", "replace")
            except Exception as e:
                logger.warning(f"curl failed for {url} "
                               f"(attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # --------------------------------------------------------- discovery
    def _establish_session(self) -> str | None:
        """GET landing (cookie) then POST the Agency=PER search; return the
        first results-page HTML."""
        self._curl(LANDING)  # seed the ASP session cookie
        today = datetime.now(timezone.utc).strftime("%m/%d/%Y")
        post = (
            f"Agency={AGENCY_CODE}&AgyCaseNo=&DocNo="
            f"&IssueDate1={FIRST_DATE}&IssueDate2={today}"
            f"&UploadDate=&DOAHCaseNo=&DocType=ALL&Subject=ALL"
        )
        html = self._curl(SEARCH_URL, post_data=post)
        if not html or "FLAID/PER/" not in html:
            logger.error("PERC search returned no results page")
            return None
        return html

    @staticmethod
    def _max_page(html: str) -> int:
        pages = [int(m) for m in PC_RE.findall(html)]
        return max(pages) if pages else 1

    def _parse_rows(self, html: str) -> list[dict]:
        docs: list[dict] = []
        seen = set()
        for row_html in ROW_RE.findall(html):
            m = PDF_HREF_RE.search(row_html)
            if not m:
                continue
            pdf_url = m.group(1)
            if pdf_url in seen:
                continue
            seen.add(pdf_url)
            cells = [_clean_cell(c) for c in CELL_RE.findall(row_html)]
            # expected: [Agency, AgyCaseNo, DOAHOrderNo, IssueDate,
            #            DocType(link text), Subject]
            agy_case = cells[1] if len(cells) > 1 else None
            order_no = cells[2] if len(cells) > 2 else None
            issue_dt = cells[3] if len(cells) > 3 else None
            doc_type = cells[4] if len(cells) > 4 else None
            subject = cells[5] if len(cells) > 5 else None
            year_m = re.search(r"/FLAID/PER/(\d{4})/", pdf_url)
            fn = pdf_url.rsplit("/", 1)[-1]
            slug = re.sub(r"[^A-Za-z0-9._-]+", "-", fn)[:90]
            docs.append({
                "doc_url": pdf_url,
                "filename": fn,
                "safe_slug": slug,
                "agency_case_no": agy_case or None,
                "order_no": order_no or None,
                "issue_date_raw": issue_dt or None,
                "doc_type": doc_type or None,
                "subject": subject or None,
                "listing_year": year_m.group(1) if year_m else None,
            })
        return docs

    def discover_documents(self, sample: bool = False) -> list[dict]:
        html = self._establish_session()
        if not html:
            return []
        max_pc = self._max_page(html)
        logger.info(f"PERC search: {max_pc} result page(s)")
        out = self._parse_rows(html)
        if not sample:
            for pc in range(2, max_pc + 1):
                page = self._curl(PAGE_URL.format(pc=pc))
                if not page:
                    logger.warning(f"  page pc={pc} fetch failed")
                    continue
                rows = self._parse_rows(page)
                out.extend(rows)
                logger.info(f"  pc={pc}: {len(rows)} rows "
                            f"(total {len(out)})")
        # de-dup across pages by url, keep deterministic order
        uniq: dict[str, dict] = {}
        for d in out:
            uniq.setdefault(d["doc_url"], d)
        docs = list(uniq.values())
        docs.sort(key=lambda r: (r.get("listing_year") or "", r["filename"]))
        logger.info(f"Discovered {len(docs)} PERC order documents")
        return docs

    # ------------------------------------------------------- build record
    @staticmethod
    def _iso_date(raw: str | None, year: str | None) -> str | None:
        if raw:
            m = DATE_MDY_RE.search(raw)
            if m:
                mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
                if 1 <= mo <= 12 and 1 <= d <= 31 and 2000 <= y <= 2035:
                    return f"{y:04d}-{mo:02d}-{d:02d}"
        if year and re.fullmatch(r"\d{4}", year):
            return f"{year}-01-01"
        return None

    def _build_raw(self, doc: dict) -> dict | None:
        blob = self._curl(doc["doc_url"], want_bytes=True)
        if not blob:
            logger.warning(f"Download failed: {doc['doc_url']}")
            return None
        if blob[:4] != b"%PDF":
            logger.warning(f"Not a PDF ({blob[:8]!r}): {doc['doc_url']}")
            return None
        text = pdf_extract.extract_pdf_markdown(
            "US/FL-PERC", doc["safe_slug"], pdf_bytes=blob,
            table="case_law", force=True,
        )
        if not text or len(text.strip()) < 200:
            logger.warning(f"No usable text for {doc['doc_url']} "
                           f"({len(text) if text else 0} chars)")
            return None
        doc = dict(doc)
        doc["text"] = text.strip()
        doc["date"] = self._iso_date(doc.get("issue_date_raw"),
                                     doc.get("listing_year"))
        return doc

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing FL PERC FLAIO session + PDF extraction...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} documents (first page)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 200:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} "
                            f"chars) — {raw.get('agency_case_no')} / "
                            f"{raw.get('doc_type')}")
            else:
                logger.error("  PDF text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        cn = raw.get("agency_case_no")
        dt = raw.get("doc_type") or "Final Order"
        subj = raw.get("subject")
        title = f"PERC {cn} {dt}" if cn else f"PERC {dt}"
        if subj:
            title = f"{title} ({subj})"
        title = re.sub(r"\s+", " ", title).strip()[:300]
        return {
            "_id": f"US/FL-PERC/{raw['safe_slug']}",
            "_source": "US/FL-PERC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "filename": raw["filename"],
            "case_number": cn,
            "order_number": raw.get("order_no"),
            "order_type": dt,
            "subject": subj,
            "issuer": "Florida Public Employees Relations Commission",
            "title": title,
            "text": raw["text"],
            "url": raw["doc_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-FL",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for doc in self.discover_documents(sample=sample):
            raw = self._build_raw(doc)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/FL-PERC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = FLPERCScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
