#!/usr/bin/env python3
"""
Legal Data Hunter - UK Bar Tribunals & Adjudication Service (BTAS) Scraper

Fetches the published findings and sanctions of disciplinary tribunals convened
by the Bar Tribunals & Adjudication Service (BTAS) — the body (run by the Council
of the Inns of Court) that administers independent disciplinary tribunals hearing
allegations of professional misconduct against barristers in England & Wales,
prosecuted by the Bar Standards Board (BSB). Tribunal findings and sanctions
(disbarment, suspension, prohibition on practising, fines, reprimand) are
quasi-judicial and binding, subject to appeal to the High Court = case_law.

BTAS is the barrister-side counterpart of UK/SDT (solicitors). It is distinct
from the Bar Standards Board (the prosecutor/regulator): BTAS administers the
independent tribunals that decide.

Source: https://www.tbtas.org.uk/hearings/findings-and-sentences-of-past-hearings/
  - A paginated WordPress listing (/page/{n}/, 10 hearings/page, ~21 pages) of
    <article class="listing_item"> blocks, each a definition list (dl) carrying
    Defendant (+ Inn), Type of hearing, Panel members, Status, Dates, and the
    "Finding and sentence" summary with a link to the published findings PDF
    under /wp-content/uploads/hearings/{id}/.
  - The published-findings PDFs are born-digital (text layer) and extracted in
    full via the shared common.pdf_extract (no OCR).

Coverage: ~200 tribunal findings.

License: no Open Government Licence; the site has its own web terms & conditions.
Treated as custom terms, commercial use flagged.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
  python bootstrap.py bootstrap-fast     # Alias for full pull (fleet runner)
"""

import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common import pdf_extract

from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("UK/BTAS")

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}
_DATE_RE = re.compile(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})")
_HID_RE = re.compile(r"/hearings/(\d+)/")


class UKBTASScraper(BaseScraper):
    """Scraper for the Bar Tribunals & Adjudication Service findings listing."""

    BASE_URL = "https://www.tbtas.org.uk"
    LISTING_PATH = "/hearings/findings-and-sentences-of-past-hearings/page/{n}/"
    MAX_PAGES = 60  # safety cap (~10 hearings/page)

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=self.BASE_URL,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml",
            },
            timeout=30,
        )

    # ── Fetch helpers ────────────────────────────────────────────────
    def _get_html(self, path: str) -> Optional[str]:
        self.rate_limiter.wait()
        try:
            resp = self.client.get(path)
            if resp.status_code == 200:
                return resp.text
            if resp.status_code != 404:
                logger.warning(f"HTTP {resp.status_code} for {path}")
            return None
        except Exception as e:
            logger.warning(f"Request failed for {path}: {e}")
            return None

    def _fetch_pdf_bytes(self, pdf_url: str) -> Optional[bytes]:
        self.rate_limiter.wait()
        try:
            resp = self.client.get(pdf_url.replace(self.BASE_URL, ""))
            if resp.status_code == 200 and resp.content[:4] == b"%PDF":
                return resp.content
            logger.warning(f"PDF HTTP {resp.status_code} for {pdf_url}")
            return None
        except Exception as e:
            logger.warning(f"PDF download failed for {pdf_url}: {e}")
            return None

    def _extract_pdf(self, doc_id: str, pdf_url: str) -> str:
        pdf_bytes = self._fetch_pdf_bytes(pdf_url)
        if not pdf_bytes:
            return ""
        text = ""
        try:
            text = pdf_extract.extract_pdf_markdown(
                "UK/BTAS", doc_id, pdf_bytes=pdf_bytes,
            ) or ""
        except Exception as e:
            logger.debug(f"markdown extraction failed for {pdf_url}: {e}")
        text = text.strip()
        # BTAS findings are born-digital; if the shared markdown backend comes
        # back empty (e.g. Java opendataloader unavailable), fall back to a plain
        # PyMuPDF text extraction, which reliably reads the text layer.
        if len(text) < 200:
            try:
                import fitz
                doc = fitz.open(stream=pdf_bytes, filetype="pdf")
                try:
                    ft = "\n".join(p.get_text() for p in doc).strip()
                finally:
                    doc.close()
                if len(ft) > len(text):
                    text = ft
            except Exception as e:
                logger.debug(f"fitz fallback failed for {pdf_url}: {e}")
        return text.strip()

    # ── Listing parsing ──────────────────────────────────────────────
    @staticmethod
    def _pick_pdf(article, base) -> Optional[str]:
        """Prefer the fullest findings PDF, skipping charge sheets."""
        pdfs = [urljoin(base, a["href"]) for a in article.find_all("a", href=True)
                if a["href"].lower().endswith(".pdf")]
        if not pdfs:
            return None
        def rank(u):
            lu = u.lower()
            if "charge-sheet" in lu:
                return -1
            if "report-of-findings-and-sanction" in lu:
                return 3
            if "published-findings" in lu:
                return 2
            if "finding" in lu:
                return 1
            return 0
        pdfs.sort(key=rank, reverse=True)
        best = pdfs[0]
        return best if rank(best) >= 0 else None

    def _parse_article(self, article) -> Optional[dict]:
        fields = {}
        for dt in article.find_all("dt"):
            dd = dt.find_next_sibling("dd")
            if dd is not None:
                fields[dt.get_text(" ", strip=True)] = dd

        def_dd = fields.get("Defendant")
        defendant = ""
        inn = ""
        if def_dd is not None:
            h2 = def_dd.find("h2")
            defendant = h2.get_text(" ", strip=True) if h2 else def_dd.get_text(" ", strip=True)
            em = def_dd.find("em")
            inn = em.get_text(" ", strip=True).strip("()") if em else ""

        def _txt(label):
            dd = fields.get(label)
            return dd.get_text(" ", strip=True) if dd is not None else ""

        pdf_url = self._pick_pdf(article, self.BASE_URL)
        hid = ""
        if pdf_url:
            m = _HID_RE.search(pdf_url)
            hid = m.group(1) if m else ""

        # "Finding and sentence" dd holds a text summary (+ the pdf link text)
        fs_dd = fields.get("Finding and sentence")
        summary = ""
        if fs_dd is not None:
            for a in fs_dd.find_all("a"):
                a.extract()
            summary = fs_dd.get_text(" ", strip=True)

        return {
            "hearing_id": hid,
            "defendant": defendant,
            "inn": inn,
            "type_of_hearing": _txt("Type of hearing"),
            "panel_members": _txt("Panel members"),
            "status": _txt("Status"),
            "dates": _txt("Dates"),
            "summary": summary,
            "pdf_url": pdf_url or "",
        }

    def _iter_articles(self) -> Generator[dict, None, None]:
        for page in range(1, self.MAX_PAGES + 1):
            html = self._get_html(self.LISTING_PATH.format(n=page))
            if not html:
                logger.info(f"No page {page}; stopping.")
                break
            soup = BeautifulSoup(html, "html.parser")
            arts = soup.find_all("article", class_="listing_item")
            if not arts:
                logger.info(f"No articles on page {page}; stopping.")
                break
            for a in arts:
                yield self._parse_article(a)

    # ── Public generators ────────────────────────────────────────────
    def fetch_all(self) -> Generator[dict, None, None]:
        count = 0
        skipped = 0
        seen = set()
        for raw in self._iter_articles():
            pdf_url = raw.get("pdf_url")
            if not pdf_url:
                skipped += 1
                continue
            doc_id = raw.get("hearing_id") or pdf_url.rsplit("/", 1)[-1]
            if doc_id in seen:
                continue
            seen.add(doc_id)
            text = self._extract_pdf(doc_id, pdf_url)
            if not text:
                text = raw.get("summary", "").strip()
            if not text or len(text) < 100:
                skipped += 1
                continue
            raw["doc_id"] = doc_id
            raw["text"] = text
            count += 1
            yield raw
            if count % 25 == 0:
                logger.info(f"  {count} findings fetched ({skipped} skipped)")
        logger.info(f"Total: {count} findings with text ({skipped} skipped)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        # Listing is newest-first; walk until decisions predate `since`.
        since_iso = since.date().isoformat()
        for raw in self._iter_articles():
            pdf_url = raw.get("pdf_url")
            if not pdf_url:
                continue
            date_iso = self._parse_date(raw.get("dates", ""))
            if date_iso and date_iso < since_iso:
                continue
            doc_id = raw.get("hearing_id") or pdf_url.rsplit("/", 1)[-1]
            text = self._extract_pdf(doc_id, pdf_url) or raw.get("summary", "").strip()
            if not text or len(text) < 100:
                continue
            raw["doc_id"] = doc_id
            raw["text"] = text
            yield raw

    @staticmethod
    def _parse_date(s: str) -> Optional[str]:
        """Return the last 'D Month YYYY' in a date range as ISO."""
        matches = _DATE_RE.findall(s or "")
        if not matches:
            return None
        d, mon, y = matches[-1]
        mo = _MONTHS.get(mon.lower())
        if not mo:
            return None
        try:
            return f"{int(y):04d}-{mo:02d}-{int(d):02d}"
        except ValueError:
            return None

    def normalize(self, raw: dict) -> dict:
        text = (raw.get("text") or "").strip()
        if not text:
            return None

        defendant = raw.get("defendant", "").strip()
        title = f"BSB v {defendant} (BTAS {raw.get('doc_id', '')})" if defendant \
            else f"BTAS Tribunal Finding {raw.get('doc_id', '')}"

        return {
            "_id": f"UK/BTAS/{raw.get('doc_id', '')}",
            "_source": "UK/BTAS",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": raw.get("doc_id", ""),
            "title": title,
            "text": text,
            "defendant": defendant,
            "inn": raw.get("inn", ""),
            "type_of_hearing": raw.get("type_of_hearing", ""),
            "panel_members": raw.get("panel_members", ""),
            "status": raw.get("status", ""),
            "summary": raw.get("summary", ""),
            "date": self._parse_date(raw.get("dates", "")),
            "dates_text": raw.get("dates", ""),
            "url": "https://www.tbtas.org.uk/hearings/findings-and-sentences-of-past-hearings/",
            "pdf_url": raw.get("pdf_url", ""),
        }


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKBTASScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py bootstrap [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd in ("bootstrap", "bootstrap-fast"):
        sample = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample, sample_size=12)
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
