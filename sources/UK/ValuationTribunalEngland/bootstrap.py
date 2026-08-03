#!/usr/bin/env python3
"""
Legal Data Hunter - Valuation Tribunal for England (VTE) Decisions Scraper

Fetches the published decisions of the Valuation Tribunal for England (VTE),
the independent statutory tribunal (constituted under the Local Government
Finance Act 1988 and the Valuation Tribunal for England (Council Tax and Rating
Appeals) (Procedure) Regulations 2009) that determines appeals about:
  - Council tax valuation (banding), liability, completion notices, penalties
    and council tax reduction schemes; and
  - Non-domestic rating (business rates) — rateable value challenges,
    completion notices, penalties and transitional certificates.
Each determination is a full, reasoned written decision, many carrying a neutral
citation of the form "[YYYY] VTE {ref}" = case_law.

Source: https://appealsearch.valuationtribunal.gov.uk/  (Appeal & decisions search)
  The search app (ASP.NET Core + Knockout SPA) exposes a server-rendered
  "Decisions" results view per appeal-type family:
    GET /Home/Decisions?AppealSearchType={CD|ND}&SearchByType=advanced
        &Skip={n}&Page={p}&PageSize={sz}&SortOn=Date&SortDesc=True
        &HearingId=00000000-0000-0000-0000-000000000000
  which paginates all decided appeals ("Showing X-Y of N results"). Each result
  row links to the decision document via
    GET /Home/Download?ApAppealNumber={id}
  which returns the born-digital decision PDF (text layer present — extracted via
  common.pdf_extract, no OCR). The trailing 2 characters of ApAppealNumber are a
  render-time decoration; the stable identifier is the appeal number shown as the
  link text (e.g. "VT00034997", "CHG100095546"). Older appeals with no published
  written decision return HTTP 404 and are skipped.

Appeal-type families with published decisions:
  CD = Council tax (valuation/banding etc.)     ~11,656 decisions
  ND = Non-domestic rating (business rates)      ~3,967 decisions

License: Crown copyright, Open Government Licence v3.0 (VTE is a Crown/arm's-
length public body; decisions are public records published under the OGL).

Usage:
  python bootstrap.py bootstrap          # Full pull
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

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

import fitz  # PyMuPDF
import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("UK/ValuationTribunalEngland")

MIN_TEXT_CHARS = 200
PAGE_SIZE = 20

_MONTHS = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05",
    "jun": "06", "jul": "07", "aug": "08", "sep": "09", "oct": "10",
    "nov": "11", "dec": "12",
}

# Appeal-type families that publish full written decisions.
APPEAL_TYPES = [
    ("CD", "Council Tax"),
    ("ND", "Non-Domestic Rating"),
]


class UKValuationTribunalEnglandScraper(BaseScraper):
    """Scraper for the Valuation Tribunal for England (VTE) decisions."""

    BASE_URL = "https://appealsearch.valuationtribunal.gov.uk"
    DECISIONS_PATH = "/Home/Decisions"
    DOWNLOAD_PATH = "/Home/Download"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; legal research)",
            "Accept": "text/html,application/xhtml+xml",
        })

    # ------------------------------------------------------------------- fetch
    def _get_listing(self, appeal_type: str, skip: int) -> Optional[str]:
        page = (skip // PAGE_SIZE) + 1
        params = {
            "AppealSearchType": appeal_type,
            "SearchByType": "advanced",
            "Skip": skip,
            "Page": page,
            "PageSize": PAGE_SIZE,
            "SortOn": "Date",
            "SortDesc": "True",
            "HearingId": "00000000-0000-0000-0000-000000000000",
        }
        self.rate_limiter.wait()
        try:
            resp = self.session.get(self.BASE_URL + self.DECISIONS_PATH,
                                    params=params, timeout=60)
            if resp.status_code == 200:
                return resp.text
            logger.warning(f"{resp.status_code} for {appeal_type} skip={skip}")
            return None
        except Exception as e:
            logger.warning(f"Listing request failed ({appeal_type} skip={skip}): {e}")
            return None

    def _download_pdf(self, ap_number: str) -> Optional[bytes]:
        """Download a decision PDF; return None on 404 (no published decision)."""
        self.rate_limiter.wait()
        try:
            resp = self.session.get(self.BASE_URL + self.DOWNLOAD_PATH,
                                    params={"ApAppealNumber": ap_number}, timeout=90)
            if resp.status_code == 200 and resp.content[:4] == b"%PDF":
                return resp.content
            if resp.status_code != 404:
                logger.warning(f"Download {resp.status_code} for {ap_number}")
            return None
        except Exception as e:
            logger.warning(f"Download failed for {ap_number}: {e}")
            return None

    @staticmethod
    def _extract_text(pdf_bytes: bytes, case_id: str = "") -> str:
        """Full text of a born-digital VTE decision PDF via PyMuPDF, with a
        shared pdfplumber/pypdf fallback. Returns "" on failure."""
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            try:
                text = "\n".join(page.get_text() for page in doc).strip()
            finally:
                doc.close()
            if len(text) >= MIN_TEXT_CHARS:
                return text
        except Exception as e:
            logger.debug(f"fitz extract failed for {case_id}: {e}")
        try:
            from common import pdf_extract as _pe
            for fn in ("_extract_with_pdfplumber", "_extract_with_pypdf"):
                f = getattr(_pe, fn, None)
                if f:
                    try:
                        t = f(pdf_bytes)
                        if t and len(t) >= MIN_TEXT_CHARS:
                            return t
                    except Exception:
                        continue
        except Exception:
            pass
        return ""

    # --------------------------------------------------------------- discovery
    @staticmethod
    def _total_results(html: str) -> int:
        m = re.search(r"of\s+([\d,]+)\s+results", html, re.I)
        return int(m.group(1).replace(",", "")) if m else 0

    def _parse_rows(self, html: str, appeal_type: str, family: str) -> list:
        soup = BeautifulSoup(html, "html.parser")
        rows = []
        table = soup.find("table", class_="govuk-table")
        if not table:
            return rows
        body = table.find("tbody")
        if not body:
            return rows
        for tr in body.find_all("tr", class_="govuk-table__row"):
            a = tr.find("a", href=re.compile(r"ApAppealNumber=", re.I))
            if not a:
                continue
            m = re.search(r"ApAppealNumber=([A-Za-z0-9]+)", a["href"])
            if not m:
                continue
            ap_number = m.group(1)
            display = a.get_text(strip=True)

            # Collect the plain-text cells: address, (rateable value), date, agent.
            cells = [c.get_text(" ", strip=True)
                     for c in tr.find_all(["td", "th"])]
            address = cells[1] if len(cells) > 1 else ""
            # The decision date is the cell matching DD-Mon-YY.
            date_iso = None
            agent = ""
            for c in cells[2:]:
                if date_iso is None and re.fullmatch(r"\d{1,2}-[A-Za-z]{3}-\d{2}", c):
                    date_iso = self._parse_date(c)
                    continue
            # Agent/billing authority is the last cell.
            if len(cells) >= 1:
                agent = cells[-1]
                if re.fullmatch(r"\d{1,2}-[A-Za-z]{3}-\d{2}", agent) or \
                   re.fullmatch(r"[\d,]+", agent):
                    agent = ""

            rows.append({
                "ap_number": ap_number,
                "case_id": display,
                "appeal_type": appeal_type,
                "family": family,
                "address": address,
                "date": date_iso,
                "agent": agent,
            })
        return rows

    @staticmethod
    def _parse_date(text: str) -> Optional[str]:
        """'17-Jul-26' -> '2026-07-17'."""
        m = re.fullmatch(r"(\d{1,2})-([A-Za-z]{3})-(\d{2})", text.strip())
        if not m:
            return None
        day, mon, yy = m.group(1), m.group(2).lower(), int(m.group(3))
        mm = _MONTHS.get(mon)
        if not mm:
            return None
        year = 2000 + yy
        return f"{year}-{mm}-{int(day):02d}"

    @staticmethod
    def _date_from_text(text: str) -> Optional[str]:
        """Fallback: 'Date: 17 July 2026' stamped in the PDF body."""
        _full = {
            "january": "01", "february": "02", "march": "03", "april": "04",
            "may": "05", "june": "06", "july": "07", "august": "08",
            "september": "09", "october": "10", "november": "11", "december": "12",
        }
        m = re.search(r"\bDate[:\s]\s*(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", text[:1500])
        if not m:
            return None
        mm = _full.get(m.group(2).lower())
        if not mm:
            return None
        return f"{m.group(3)}-{mm}-{int(m.group(1)):02d}"

    # ---------------------------------------------------------------- iteration
    def fetch_all(self) -> Generator[dict, None, None]:
        any_rows = False
        for appeal_type, family in APPEAL_TYPES:
            first = self._get_listing(appeal_type, 0)
            if not first:
                logger.warning(f"No listing for appeal type {appeal_type}")
                continue
            total = self._total_results(first)
            logger.info(f"{family} ({appeal_type}): {total} decided appeals")

            skip = 0
            pages_html = first
            while True:
                rows = self._parse_rows(pages_html, appeal_type, family)
                if not rows:
                    break
                any_rows = True
                for r in rows:
                    pdf_bytes = self._download_pdf(r["ap_number"])
                    if not pdf_bytes:
                        continue  # no published decision document (404) — skip
                    text = self._extract_text(pdf_bytes, r["case_id"])
                    if len(text) < MIN_TEXT_CHARS:
                        continue
                    raw = dict(r)
                    raw["text"] = text
                    if not raw.get("date"):
                        raw["date"] = self._date_from_text(text)
                    yield raw

                skip += PAGE_SIZE
                if skip >= total:
                    break
                pages_html = self._get_listing(appeal_type, skip)
                if not pages_html:
                    break

        if not any_rows:
            raise RuntimeError(
                "No VTE decision rows parsed for any appeal type — the search app "
                "layout may have changed or the host blocked the request (fail loud "
                "rather than emit an empty corpus)"
            )

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Decisions are date-sorted newest-first; stop once older than `since`.

        The loader upserts on the primary key, so re-yielding recent overlap is
        idempotent.
        """
        since_iso = since.strftime("%Y-%m-%d")
        for raw in self.fetch_all():
            d = raw.get("date")
            if d and d < since_iso:
                break
            yield raw

    # ---------------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        text = (raw.get("text", "") or "").strip()
        if not text:
            return None

        case_id = raw.get("case_id", "")
        family = raw.get("family", "")
        address = raw.get("address", "")

        # Neutral citation if present in the decision head, e.g. "[2026] VTE ...".
        cite = None
        mc = re.search(r"\[(\d{4})\]\s+VTE\s+[A-Za-z0-9()/\- ]+", text[:400])
        if mc:
            cite = re.sub(r"\s+", " ", mc.group(0)).strip()

        title_bits = [f"VTE {family} decision {case_id}"]
        if address:
            title_bits.append(address)
        title = " — ".join(title_bits)

        return {
            "_id": f"UK/ValuationTribunalEngland/{case_id}",
            "_source": "UK/ValuationTribunalEngland",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_id": case_id,
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "appeal_type": raw.get("appeal_type"),
            "appeal_category": family or None,
            "property_address": address or None,
            "agent": raw.get("agent") or None,
            "citation": cite,
            "url": f"{self.BASE_URL}{self.DOWNLOAD_PATH}?ApAppealNumber={raw.get('ap_number','')}",
        }


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKValuationTribunalEnglandScraper()

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
