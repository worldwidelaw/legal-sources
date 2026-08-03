#!/usr/bin/env python3
"""
Legal Data Hunter - UK Solicitors Disciplinary Tribunal (SDT) Judgments Scraper

Fetches the published judgments of the Solicitors Disciplinary Tribunal (SDT) —
the independent statutory tribunal (constituted under s.46 of the Solicitors Act
1974) that adjudicates allegations of professional misconduct against solicitors,
registered European/foreign lawyers (RELs/RFLs) and other regulated persons in
England & Wales. SDT decisions (strike-off, suspension, fine, restrictions,
reprimand, no order) are quasi-judicial and binding, subject to appeal only to
the High Court (Administrative Court) = case_law.

The SDT is distinct from the Solicitors Regulation Authority (UK/SRA): the SRA
prosecutes; the SDT is the independent judicial body that decides.

Source: https://solicitorstribunal.org.uk/case/
  - The cases archive is a paginated WordPress listing (/case/page/{n}/,
    ~10 cases/page) linking to per-case pages /case/{id}/.
  - Each case page carries structured metadata (Case ID, SRA ID, Year,
    Publication date, Applicant, Respondent, Allegation, Outcome, Executive
    summary) plus a link to the full Final Judgment PDF under
    /wp-content/uploads/.
  - The Final Judgment PDFs are born-digital (text layer) and are extracted in
    full via the shared common.pdf_extract (no OCR needed).

Coverage: ~2,200 judgments (2016-present published online).

License: no Open Government Licence statement; the site asserts site copyright
and separate Terms & Conditions. Treated as custom terms, commercial use flagged.

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
logger = logging.getLogger("UK/SDT")

_DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")


class UKSDTScraper(BaseScraper):
    """Scraper for the Solicitors Disciplinary Tribunal judgments archive."""

    BASE_URL = "https://solicitorstribunal.org.uk"
    ARCHIVE_PATH = "/case/page/{n}/"
    MAX_PAGES = 400  # safety cap (~10 cases/page → ~4,000 cases)

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
            logger.warning(f"HTTP {resp.status_code} for {path}")
            return None
        except Exception as e:
            logger.warning(f"Request failed for {path}: {e}")
            return None

    def _fetch_pdf_bytes(self, pdf_url: str) -> Optional[bytes]:
        """Download the PDF with a browser UA (the WAF 403s the plain requests UA)."""
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

    def _extract_pdf(self, case_id: str, pdf_url: str) -> str:
        pdf_bytes = self._fetch_pdf_bytes(pdf_url)
        if not pdf_bytes:
            return ""
        try:
            text = pdf_extract.extract_pdf_markdown(
                "UK/SDT", case_id, pdf_bytes=pdf_bytes,
            )
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return ""
        return (text or "").strip()

    def _list_case_urls(self, page: int) -> list:
        """Return the list of /case/{id}/ URLs on one archive page."""
        html = self._get_html(self.ARCHIVE_PATH.format(n=page))
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")
        urls = []
        seen = set()
        for a in soup.find_all("a", href=True):
            m = re.match(r"^(?:https?://[^/]+)?/case/(\d+)/?$", a["href"])
            if m:
                cid = m.group(1)
                if cid not in seen:
                    seen.add(cid)
                    urls.append((cid, urljoin(self.BASE_URL, a["href"])))
        return urls

    # ── Case-page parsing ────────────────────────────────────────────
    @staticmethod
    def _sidebar_fields(soup) -> dict:
        """Parse the label/value sidebar (Case ID, SRA ID, Year, Publication date)."""
        out = {}
        for label in soup.select("span.fw-semibold"):
            key = label.get_text(" ", strip=True)
            val_span = label.find_next_sibling("span")
            if key and val_span:
                out[key] = val_span.get_text(" ", strip=True)
        return out

    @staticmethod
    def _table_fields(soup) -> dict:
        """Parse th→td rows from the Between and Case details tables."""
        out = {}
        for row in soup.select("table tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                out[th.get_text(" ", strip=True)] = td.get_text(" ", strip=True)
        return out

    @staticmethod
    def _find_pdf(soup, base) -> Optional[str]:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.lower().endswith(".pdf"):
                return urljoin(base, href)
        return None

    def _parse_case(self, cid: str, url: str) -> Optional[dict]:
        html = self._get_html(url.replace(self.BASE_URL, ""))
        if not html:
            return None
        soup = BeautifulSoup(html, "html.parser")
        main = soup.find("main") or soup
        sb = self._sidebar_fields(main)
        tb = self._table_fields(main)
        pdf_url = self._find_pdf(main, self.BASE_URL)

        title_tag = soup.find("title")
        title = title_tag.get_text(strip=True) if title_tag else f"Case {cid}"

        return {
            "case_id": sb.get("Case ID", cid) or cid,
            "sra_id": sb.get("SRA ID", ""),
            "year": sb.get("Year", ""),
            "publication_date": sb.get("Publication date", ""),
            "applicant": tb.get("Applicant", ""),
            "respondent": tb.get("Respondent", ""),
            "allegation": tb.get("Allegation", ""),
            "outcome": tb.get("Outcome", ""),
            "summary": tb.get("Executive summary", ""),
            "pdf_url": pdf_url,
            "page_title": title,
            "url": url,
        }

    # ── Public generators ────────────────────────────────────────────
    def fetch_all(self) -> Generator[dict, None, None]:
        count = 0
        skipped = 0
        for page in range(1, self.MAX_PAGES + 1):
            cases = self._list_case_urls(page)
            if not cases:
                logger.info(f"No cases on page {page}; stopping.")
                break
            for cid, url in cases:
                raw = self._parse_case(cid, url)
                if not raw:
                    skipped += 1
                    continue
                pdf_url = raw.get("pdf_url")
                text = self._extract_pdf(raw["case_id"], pdf_url) if pdf_url else ""
                # Fall back to on-page executive summary only if no PDF text.
                if not text:
                    text = raw.get("summary", "").strip()
                if not text or len(text) < 100:
                    skipped += 1
                    continue
                raw["text"] = text
                count += 1
                yield raw
                if count % 25 == 0:
                    logger.info(f"  {count} judgments fetched ({skipped} skipped)")
        logger.info(f"Total: {count} judgments with text ({skipped} skipped)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """The archive is newest-first; walk pages until we pass `since`."""
        for page in range(1, self.MAX_PAGES + 1):
            cases = self._list_case_urls(page)
            if not cases:
                break
            page_all_old = True
            for cid, url in cases:
                raw = self._parse_case(cid, url)
                if not raw:
                    continue
                pub = self._parse_pubdate(raw.get("publication_date", ""))
                if pub and pub >= since.date().isoformat():
                    page_all_old = False
                elif pub:
                    continue
                pdf_url = raw.get("pdf_url")
                text = self._extract_pdf(raw["case_id"], pdf_url) if pdf_url else ""
                text = (text or raw.get("summary", "")).strip()
                if not text or len(text) < 100:
                    continue
                raw["text"] = text
                yield raw
            if page_all_old and page > 1:
                break

    @staticmethod
    def _parse_pubdate(s: str) -> Optional[str]:
        m = _DATE_RE.search(s or "")
        if not m:
            return None
        d, mo, y = m.groups()
        try:
            return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
        except ValueError:
            return None

    def normalize(self, raw: dict) -> dict:
        text = (raw.get("text") or "").strip()
        if not text:
            return None

        date_iso = self._parse_pubdate(raw.get("publication_date", ""))
        if not date_iso and raw.get("year", "").isdigit():
            date_iso = f"{raw['year']}-01-01"

        applicant = raw.get("applicant", "").strip()
        respondent = raw.get("respondent", "").strip()
        if applicant and respondent:
            title = f"{applicant} v {respondent} (SDT {raw.get('case_id', '')})"
        else:
            title = raw.get("page_title", "") or f"SDT Case {raw.get('case_id', '')}"

        return {
            "_id": f"UK/SDT/{raw.get('case_id', '')}",
            "_source": "UK/SDT",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_id": raw.get("case_id", ""),
            "sra_id": raw.get("sra_id", ""),
            "title": title,
            "text": text,
            "applicant": applicant,
            "respondent": respondent,
            "allegation": raw.get("allegation", ""),
            "outcome": raw.get("outcome", ""),
            "summary": raw.get("summary", ""),
            "date": date_iso,
            "year": raw.get("year", ""),
            "url": raw.get("url", ""),
            "pdf_url": raw.get("pdf_url", ""),
        }


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKSDTScraper()

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
