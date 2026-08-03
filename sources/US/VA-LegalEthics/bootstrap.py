#!/usr/bin/env python3
"""
US/VA-LegalEthics -- Virginia State Bar — Legal Ethics Opinions (LEOs)

Fetches the full text of the advisory Legal Ethics Opinions ("LEOs") issued by
the Standing Committee on Legal Ethics of the Virginia State Bar. A LEO applies
the Virginia Rules of Professional Conduct to a hypothetical set of facts and
states whether the described conduct complies with or violates the ethics rules
= doctrine (the Bar's official written interpretation of the attorney-conduct
rules). Many LEOs are formally approved by the Supreme Court of Virginia.

The corpus (~350+ opinions, LEO Nos. roughly 183-1900, 1980-present) is
published by the Virginia State Bar (vsb.org) as born-digital PDFs. Distinct
from US/VA-EthicsOpinions (the Virginia Conflict of Interest & Ethics Advisory
Council, which advises public officials) and from Virginia Attorney General
opinions.

Access (no JavaScript execution needed, no CAPTCHA, no auth, browser UA):
  1. Discovery via the Telerik RadGrid "LEO - RPC Index" at
        /Site/Site/about/rules-regulations/leo-opinions.aspx
     The grid is a rule-cross-reference index (one row per opinion x cited
     Rule), so opinions repeat across rows; we collect the unique PDF hrefs
     (LEOs/{filename}.pdf) across every page. Pagination is driven by ASP.NET
     __doPostBack against the pager (page-number anchors in the visible window
     plus the "Next Pages" window-advance control), carrying __VIEWSTATE /
     __EVENTVALIDATION forward on each POST.
  2. Download each PDF from
        https://vsb.org/common/Uploaded files/LEOs/{filename}.pdf
     and extract the text layer with PyMuPDF (born-digital, no OCR).

Filenames are inconsistently zero-padded (e.g. "0847.pdf" vs "872.pdf") and some
carry letter suffixes ("0186A.pdf"), so the exact filename is taken from the
index href, never constructed from the number.

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
"""

from __future__ import annotations

import sys
import io
import json
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

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.VA-LegalEthics")

BASE = "https://vsb.org"
INDEX = BASE + "/Site/Site/about/rules-regulations/leo-opinions.aspx"
PDF_BASE = BASE + "/common/Uploaded files/LEOs/"

HREF_RE = re.compile(r"LEOs/([0-9]{1,4}[A-Za-z]?)\.pdf", re.I)
CURPAGE_RE = re.compile(r'class="rgCurrentPage"[^>]*>\s*<span>(\d+)</span>')
PAGENUM_RE = re.compile(r"Go to Page (\d+)\"[^>]*__doPostBack\(&#39;([^&]+)&#39;")
NEXTWIN_RE = re.compile(r"Next Pages\"[^>]*__doPostBack\(&#39;([^&]+)&#39;")
STATE_FIELDS = ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION")

APPROVED_RE = re.compile(
    r"Approved by the Supreme Court of Virginia\s+([A-Z][a-z]+\s+\d{1,2},\s+\d{4})"
)
DATE_RE = re.compile(r"\b([A-Z][a-z]+\s+\d{1,2},\s+\d{4})\b")
LEO_HEAD_RE = re.compile(r"LEGAL ETHICS OPINION\s+([0-9]+[A-Za-z]?)\s*[.:]?\s*(.*)", re.I)


class VALegalEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str) -> requests.Response | None:
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                r = self._session.get(url, timeout=60)
                if r.status_code == 200:
                    return r
                if r.status_code == 404:
                    return None
                logger.warning(f"GET {url} -> HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"GET failed {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    def _post(self, data: dict) -> requests.Response | None:
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                r = self._session.post(INDEX, data=data, timeout=60)
                if r.status_code == 200:
                    return r
                logger.warning(f"POST index -> HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"POST failed (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    @staticmethod
    def _state(html: str) -> dict:
        d = {}
        for name in STATE_FIELDS:
            m = re.search(r'id="%s"[^>]*value="([^"]*)"' % name, html)
            if m:
                d[name] = m.group(1)
        return d

    @staticmethod
    def _leo_files(html: str) -> set[str]:
        return {m.group(1) for m in HREF_RE.finditer(html)}

    @staticmethod
    def _curpage(html: str) -> int | None:
        m = CURPAGE_RE.search(html)
        return int(m.group(1)) if m else None

    @staticmethod
    def _pager_map(html: str) -> dict[int, str]:
        return {int(m.group(1)): m.group(2) for m in PAGENUM_RE.finditer(html)}

    @staticmethod
    def _next_window(html: str):
        m = NEXTWIN_RE.search(html)
        return m.group(1) if m else None

    @staticmethod
    def _sort_key(fn: str):
        m = re.match(r"^0*(\d+)([A-Za-z]?)$", fn)
        return (int(m.group(1)), m.group(2)) if m else (10 ** 9, fn)

    def _iter_leo_files(self, max_pages: int = 250) -> Generator[str, None, None]:
        """Paginate the RadGrid lazily, yielding each unique LEOs/{file}.pdf name
        the first time it is seen. Yielding page-by-page lets --sample stop after
        a handful of records without paginating the whole grid.
        """
        r = self._get(INDEX)
        if not r:
            logger.error("could not fetch LEO index")
            return
        html = r.text
        state = self._state(html)
        seen: set[str] = set()
        cur = self._curpage(html) or 1
        visited = {cur}

        def emit(page_html):
            page_files = sorted(self._leo_files(page_html), key=self._sort_key)
            for fn in page_files:
                if fn not in seen:
                    seen.add(fn)
                    yield fn

        yield from emit(html)
        logger.info(f"  index page {cur}: {len(seen)} unique LEO files so far")
        pages = 1
        while pages < max_pages:
            pager = self._pager_map(html)
            cand = sorted(p for p in pager if p > cur and p not in visited)
            if cand:
                target = pager[cand[0]]
            else:
                target = self._next_window(html)
                if not target:
                    break
            data = dict(state)
            data["__EVENTTARGET"] = target
            data["__EVENTARGUMENT"] = ""
            rp = self._post(data)
            if not rp:
                break
            html = rp.text
            state = self._state(html)
            nc = self._curpage(html)
            if nc is None:
                break
            if nc in visited and not cand:
                # window-advance landed on an already-seen page -> done
                break
            yield from emit(html)
            visited.add(nc)
            cur = nc
            pages += 1
            if pages % 10 == 0:
                logger.info(f"  paged to grid page {cur}: {len(seen)} unique files")
        logger.info(f"  discovery complete: {len(seen)} unique LEO PDFs "
                    f"across {len(visited)} grid pages")

    # -------------------------------------------------------- extraction
    @staticmethod
    def _leo_number(filename: str) -> str:
        """0847 -> 847 ; 0186A -> 186A ; 872 -> 872."""
        m = re.match(r"^0*(\d+)([A-Za-z]?)$", filename)
        if m:
            return m.group(1) + m.group(2).upper()
        return filename

    def _pdf_text(self, filename: str) -> str:
        url = PDF_BASE + filename + ".pdf"
        r = self._get(url)
        if not r or not r.content:
            return ""
        if fitz is None:
            logger.error("PyMuPDF (fitz) not available")
            return ""
        try:
            doc = fitz.open(stream=io.BytesIO(r.content), filetype="pdf")
        except Exception as e:
            logger.warning(f"  fitz open failed for {url}: {e}")
            return ""
        parts = [page.get_text() for page in doc]
        doc.close()
        text = "\n".join(parts)
        text = re.sub(r"[ \t]+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _title_for(number: str, text: str) -> str:
        m = LEO_HEAD_RE.search(text or "")
        if m:
            subj = re.sub(r"\s+", " ", m.group(2)).strip(" .:-–")
            # The subject often continues over several broken PDF lines; keep a
            # compact single-line heading.
            subj = subj[:200].strip()
            if subj:
                return f"LEO {number}: {subj}"
        return f"Legal Ethics Opinion {number}"

    @staticmethod
    def _parse_date(text: str) -> str | None:
        def iso(s):
            try:
                return datetime.strptime(s, "%B %d, %Y").strftime("%Y-%m-%d")
            except ValueError:
                return None
        m = APPROVED_RE.search(text or "")
        if m:
            d = iso(m.group(1))
            if d:
                return d
        # LEOs are frequently revised; take the latest date mentioned.
        dates = [iso(x) for x in DATE_RE.findall(text or "")]
        dates = [d for d in dates if d]
        return max(dates) if dates else None

    def _fetch_one(self, filename: str) -> dict | None:
        text = self._pdf_text(filename)
        if len(text) < 150:
            return None
        number = self._leo_number(filename)
        return {
            "opinion_number": number,
            "title": self._title_for(number, text),
            "text": text,
            "date": self._parse_date(text),
            "url": PDF_BASE.replace(" ", "%20") + filename + ".pdf",
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Virginia State Bar Legal Ethics Opinions...")
        r = self._get(INDEX)
        if not r:
            logger.error("API test FAILED: index unreachable")
            return False
        files = sorted(self._leo_files(r.text))[:6]
        if not files:
            logger.error("API test FAILED: no LEO PDFs found on index")
            return False
        logger.info(f"  index yields LEO files, e.g. {files}")
        ok = 0
        for fn in files:
            rec = self._fetch_one(fn)
            if rec and len(rec["text"]) > 300:
                logger.info(f"  LEO {rec['opinion_number']} OK "
                            f"({len(rec['text'])} chars) date={rec['date']}")
                ok += 1
            if ok >= 3:
                break
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["opinion_number"]
        slug = re.sub(r"[^A-Za-z0-9]+", "-", num).strip("-")
        return {
            "_id": f"US/VA-LegalEthics/{slug}",
            "_source": "US/VA-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": "Virginia State Bar — Standing Committee on Legal Ethics",
            "title": raw.get("title") or f"Legal Ethics Opinion {num}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-VA",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for fn in self._iter_leo_files():
            rec = self._fetch_one(fn)
            if not rec:
                logger.warning(f"  no text for LEOs/{fn}.pdf, skipping")
                continue
            yield rec
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

    parser = argparse.ArgumentParser(description="US/VA-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = VALegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
