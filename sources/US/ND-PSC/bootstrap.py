#!/usr/bin/env python3
"""
US/ND-PSC -- North Dakota Public Service Commission Orders

Fetches the full text of Orders issued (and adopted) by the North Dakota
Public Service Commission (NDPSC) in its formal cases -- utility rate and
certificate proceedings, grain warehouse / grain buyer matters, coal
reclamation (RC) permits, weights & measures, pipeline siting, telecom,
etc. Each Order is an administrative adjudication / disposition of a
specific case docket = case_law. Public domain (US state government edict).

Strategy (official public NDPSC case portal, apps.psc.nd.gov/cases -- a
server-rendered ASP-style search app over the Commission's document
archive; every document lives as a born-digital PDF under
https://www.psc.nd.gov/webdocs/case/{YY-NNNN}/{docket:03d}-{file:03d}.pdf):

  1. POST /cases/psdocketsearch with docketTypeCode=Order and a
     filedFromDate/filedToDate window (HTML <input type=date>, so the value
     MUST be yyyy-mm-dd -- an M/D/YYYY value 500s the endpoint). The server
     returns a results <table>: one row per Order docket carrying the Case
     Number (e.g. "PU-26-169"), the docket number + description, the page
     count, "On Behalf Of" (="Public Service Commission" for a genuine
     Commission Order; an ALJ/court name for adjudicative orders issued in
     the case), "Filed By" and the Date Filed (YYYY.MM.DD). Each row links
     to /cases/psdocketdetail?getId={YY}&getId2={caseNo}&getId3={docket}.
  2. fetch_all() walks month windows newest-first back to FIRST_YEAR so no
     single window overflows the app's ~100-row result cap.
  3. GET the psdocketdetail page for the docket; it embeds the direct
     download link(s) https://www.psc.nd.gov/webdocs/case/.../NNN-NNN.pdf.
  4. Download the Order PDF; full text via fitz/PyMuPDF. Most NDPSC Orders
     are born-digital; the minority that are image-only scans are OCR'd
     with Tesseract.

Usage:
  python bootstrap.py bootstrap            # Full pull (all Orders)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import io
import re
import json
import time
import logging
import calendar
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import fitz  # PyMuPDF
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.ND-PSC")

BASE = "https://apps.psc.nd.gov/cases"
DOCKET_SEARCH = BASE + "/psdocketsearch"
DOCKET_DETAIL = BASE + "/psdocketdetail"

# Corpus floor: the NDPSC webdocs electronic archive thins out before ~2000.
FIRST_YEAR = 2000

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

WEBDOC_RE = re.compile(r'href="(https://www\.psc\.nd\.gov/webdocs/[^"]+\.pdf)\s*"', re.I)
DOCKET_NUM_RE = re.compile(r"^\s*(\d+)\.?\s*(.*)$", re.S)


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_filed_date(val: str) -> Optional[str]:
    """Parse a 'YYYY.MM.DD' (or 'YYYY-MM-DD') filed date to ISO date."""
    if not val:
        return None
    m = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", val)
    if not m:
        return None
    try:
        y, mo, d = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
        return datetime(y, mo, d).strftime("%Y-%m-%d")
    except ValueError:
        return None


def _month_windows():
    """Yield (start, end) 'YYYY-MM-DD' strings, newest month first, to FIRST_YEAR."""
    now = datetime.now(timezone.utc)
    year, month = now.year, now.month
    while year >= FIRST_YEAR:
        last = calendar.monthrange(year, month)[1]
        yield (f"{year}-{month:02d}-01", f"{year}-{month:02d}-{last:02d}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1


class NDPSCScraper(BaseScraper):

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
                "Referer": BASE + "/psdocketsearch",
            },
            timeout=90,
        )
        self.delay = 1.2

    # ---- low-level helpers --------------------------------------------------

    def _docket_search(self, start: str, end: str) -> Optional[str]:
        """POST the docket search for docketTypeCode=Order in a date window."""
        data = {
            "casePrefix": "",
            "caseYear": "",
            "caseSequence": "",
            "caseStatusCode": "",
            "docketTypeCode": "Order",
            "docketId": "",
            "sosId": "",
            "description": "",
            "filedBy": "",
            "onBehalfOf": "",
            "filedFromDate": start,
            "filedToDate": end,
            "search": "Search",
        }
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                r = self.http.post(
                    DOCKET_SEARCH, data=data,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
                if r.status_code == 200:
                    return r.text
                logger.warning(f"docketsearch {start}..{end}: HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"docketsearch {start}..{end} error: {e}")
            time.sleep(1.5 * (attempt + 1))
        return None

    @staticmethod
    def _parse_rows(html: str):
        """Yield dicts of {case, docket_num, description, on_behalf_of,
        filed_by, filed_date, detail_url} from a docket search results page."""
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        if not table:
            return
        rows = table.find_all("tr")
        for r in rows[1:]:
            cells = r.find_all("td")
            if len(cells) < 6:
                continue
            case = cells[1].get_text(" ", strip=True)
            combo = cells[2].get_text(" ", strip=True)
            page_count = cells[3].get_text(" ", strip=True)
            on_behalf = cells[4].get_text(" ", strip=True)
            filed_by = cells[5].get_text(" ", strip=True)
            filed_date = cells[6].get_text(" ", strip=True) if len(cells) > 6 else ""
            detail = None
            for a in r.find_all("a"):
                href = a.get("href") or ""
                if "psdocketdetail" in href:
                    detail = href
                    break
            m = DOCKET_NUM_RE.match(combo)
            docket_num = m.group(1) if m else ""
            desc = (m.group(2).strip() if m else combo).strip()
            yield {
                "case": case,
                "docket_num": docket_num,
                "description": desc,
                "page_count": page_count,
                "on_behalf_of": on_behalf,
                "filed_by": filed_by,
                "filed_date": parse_filed_date(filed_date),
                "detail_url": detail,
            }

    def _pdf_urls(self, detail_href: str):
        """Resolve a psdocketdetail link to its born-digital PDF URL(s)."""
        url = detail_href
        if not url.lower().startswith("http"):
            url = BASE + "/" + url.lstrip("/")
        try:
            time.sleep(self.delay)
            r = self.http.get(url)
            if r.status_code != 200:
                logger.debug(f"docketdetail {url}: HTTP {r.status_code}")
                return []
            return [u.strip() for u in WEBDOC_RE.findall(r.text)]
        except Exception as e:
            logger.debug(f"docketdetail {url} error: {e}")
            return []

    def _download(self, url: str) -> Optional[bytes]:
        try:
            time.sleep(self.delay)
            r = self.http.get(url)
            ctype = r.headers.get("Content-Type", "")
            if r.status_code == 200 and ("pdf" in ctype.lower() or r.content[:4] == b"%PDF"):
                return r.content
            logger.debug(f"download unexpected: HTTP {r.status_code} type={ctype} {url}")
        except Exception as e:
            logger.debug(f"download error {url}: {e}")
        return None

    @staticmethod
    def _extract_text(content: bytes) -> str:
        """Extract full text from a PDF, page by page.

        Many NDPSC Orders are a born-digital cover sheet (which carries a
        text layer) followed by the signed Order as scanned image pages
        (no text layer). So OCR is applied per-page to any image-only page
        rather than only when the whole document lacks text -- otherwise
        the cover sheet's text would suppress OCR of the actual Order body.
        """
        try:
            doc = fitz.open(stream=content, filetype="pdf")
        except Exception as e:
            logger.warning(f"fitz open failed: {e}")
            return ""
        page_text = []
        ocr_pages = []
        for i, page in enumerate(doc):
            t = page.get_text()
            page_text.append(t)
            if len(t.strip()) < 50 and page.get_images():
                ocr_pages.append(i)
        if ocr_pages:
            try:
                import pytesseract
                from PIL import Image
                for i in ocr_pages:
                    pix = doc[i].get_pixmap(dpi=200)
                    img = Image.open(io.BytesIO(pix.tobytes("png")))
                    o = pytesseract.image_to_string(img)
                    if len(o.strip()) > len(page_text[i].strip()):
                        page_text[i] = o
            except Exception as e:
                logger.debug(f"OCR unavailable/failed: {e}")
        doc.close()
        return clean_text("\n".join(page_text))

    # ---- BaseScraper API ----------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        seen = set()
        for start, end in _month_windows():
            html = self._docket_search(start, end)
            if not html:
                continue
            for row in self._parse_rows(html):
                if not row.get("detail_url"):
                    continue
                key = f"{row['case']}-{row['docket_num']}"
                if key in seen:
                    continue
                seen.add(key)
                pdf_urls = self._pdf_urls(row["detail_url"])
                if not pdf_urls:
                    continue
                # Primary document is the "-010" file; fall back to first.
                primary = next((u for u in pdf_urls if u.rstrip().endswith("-010.pdf")), pdf_urls[0])
                content = self._download(primary)
                if not content:
                    continue
                text = self._extract_text(content)
                if len(text.strip()) < 100:
                    logger.debug(f"Insufficient text for {key}, skipping")
                    continue
                yield {
                    "id": key,
                    "case": row["case"],
                    "docket_num": row["docket_num"],
                    "description": row["description"],
                    "on_behalf_of": row["on_behalf_of"],
                    "filed_by": row["filed_by"],
                    "filed_date": row["filed_date"],
                    "pdf_url": primary,
                    "detail_url": (
                        row["detail_url"] if row["detail_url"].lower().startswith("http")
                        else BASE + "/" + row["detail_url"].lstrip("/")
                    ),
                    "text": text,
                }

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("filed_date") and raw["filed_date"] >= since):
                yield raw

    def normalize(self, raw: dict) -> Optional[dict]:
        text = raw.get("text", "")
        if not text or len(text.strip()) < 100:
            return None
        case = raw.get("case", "")
        desc = raw.get("description", "") or "Order"
        title = f"{case} — {desc}" if case else desc
        return {
            "_id": raw.get("id"),
            "_source": "US/ND-PSC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title.strip(),
            "text": text,
            "date": raw.get("filed_date"),
            "url": raw.get("detail_url"),
            "id": raw.get("id"),
            "case_number": case,
            "docket_number": raw.get("docket_num", ""),
            "description": raw.get("description", ""),
            "on_behalf_of": raw.get("on_behalf_of", ""),
            "pdf_url": raw.get("pdf_url", ""),
            "jurisdiction": "US-ND",
        }

    # ---- diagnostics --------------------------------------------------------

    def test_api(self) -> bool:
        now = datetime.now(timezone.utc)
        # Use a wide recent window so the connectivity test finds orders.
        start = f"{now.year - 1}-01-01"
        end = f"{now.year}-{now.month:02d}-{calendar.monthrange(now.year, now.month)[1]:02d}"
        html = self._docket_search(start, end)
        if not html:
            print("FAIL: docketsearch returned nothing")
            return False
        rows = list(self._parse_rows(html))
        print(f"OK: {len(rows)} Order dockets in {start}..{end}")
        for row in rows[:5]:
            pdfs = self._pdf_urls(row["detail_url"]) if row.get("detail_url") else []
            print(f"  {row['filed_date']} {row['case']} docket {row['docket_num']} "
                  f"[{row['on_behalf_of']}] pdf={'yes' if pdfs else 'NO'}")
        return len(rows) > 0


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/ND-PSC bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NDPSCScraper()

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
