#!/usr/bin/env python3
"""
UK/GDC -- General Dental Council -- Fitness to Practise hearing determinations.

The General Dental Council (GDC) is the UK statutory regulator for ~120,000
dentists and dental care professionals (dental nurses, hygienists, therapists,
technicians, clinical dental technicians, orthodontic therapists). Fitness to
practise and registration cases brought by the GDC are heard by the independent
Dental Professionals Hearings Service (DPHS). Its committees -- the Professional
Conduct Committee, Professional Performance Committee, Health Committee, Interim
Orders Committee -- and the Registration Appeals panels sit under the Dentists
Act 1984 and the GDC (Fitness to Practise) Rules 2006. Each concluded hearing
publishes a reasoned "PUBLIC DETERMINATION" setting out the charges/allegation,
the facts found proved, whether the registrant's fitness to practise is impaired
and the sanction/order imposed (erasure, suspension, conditions, reprimand) or
the interim order made. These are binding professional-regulator adjudications =
case law, distinct from UK/GMC (doctors), UK/SDT (solicitors), UK/BTAS
(barristers), UK/HCPTS (health & care professions), UK/NMC (nurses/midwives) and
UK/SocialWorkEngland.

Access & structure (all public, no auth):
  - dentalhearings.org is a Nuxt/Vue single-page app backed by a plain JSON API
    at https://api.dentalhearings.org :
      * GET /Hearing?futureHearings=false&page={N}  -> paginated list (20/page,
        ~630 concluded hearings across ~32 pages); each row carries hearingId,
        name, registrationNumber, profession, hearingDate, hearingType,
        outcomeSummary.
      * GET /Hearing/{hearingId}  -> full detail: venue/panel/outcome plus
        `determinationDocuments` and `chargeDocuments` arrays. Each document
        record has an `annotationId`, `filename`, `mimeType` (application/pdf).
      * The determination/charge PDFs themselves are served from public Azure
        blob storage: https://gdcolrlive1.blob.core.windows.net/annotationspublic/
        {annotationId} . These are BORN-DIGITAL PDFs with a real text layer
        (no OCR needed): a structured header (committee / hearing type / dates /
        name / registration number / case number / representation / fitness to
        practise / outcome / immediate order / committee members) followed by the
        numbered reasoned determination.
  - The API exposes a rolling window of published concluded hearings (older
    determinations are removed under the GDC publication policy), so one run
    captures the current window (~500-600 full determinations) and re-runs
    accumulate the record (the pipeline dedups on _id = the stable hearingId).

Strategy:
  - Page through /Hearing?futureHearings=false to enumerate every concluded
    hearingId; GET each detail; download each determinationDocuments PDF (the
    reasoned decision), falling back to chargeDocuments (the Notice of Hearing /
    charges) when no determination has been published; extract the text layer
    (PyMuPDF, with a shared pdfplumber/pypdf fallback) and yield full text.

Data:
  - ~500-600 full-text fitness-to-practise / registration-appeal determinations
    in the live window. Language: English. Auth: none.

Usage:
  python bootstrap.py bootstrap          # Full pull (current published window)
  python bootstrap.py bootstrap --sample # Fetch sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental (recent hearings first)
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import html
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UK.GDC")

API_BASE = "https://api.dentalhearings.org"
BLOB_BASE = "https://gdcolrlive1.blob.core.windows.net/annotationspublic/"
SITE_BASE = "https://www.dentalhearings.org"
PER_PAGE = 20

_WS_RE = re.compile(r"[ \t]+")
_MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}


def _pdf_text(pdf_bytes: bytes) -> str:
    """Full text of a born-digital determination PDF via PyMuPDF, with a shared
    pdfplumber/pypdf fallback."""
    if fitz is not None:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            try:
                text = "\n".join(page.get_text() for page in doc).strip()
            finally:
                doc.close()
            if len(text) >= 80:
                return text
        except Exception as e:
            logger.debug(f"fitz extract failed: {e}")
    try:
        from common import pdf_extract as _pe
        for fn in ("_extract_with_pdfplumber", "_extract_with_pypdf"):
            f = getattr(_pe, fn, None)
            if f:
                try:
                    t = f(pdf_bytes)
                    if t and len(t) >= 80:
                        return t
                except Exception:
                    continue
    except Exception:
        pass
    return ""


def _clean(text: str) -> str:
    text = html.unescape(text or "").replace("\r", "").replace("\xa0", " ")
    lines = [_WS_RE.sub(" ", ln).rstrip() for ln in text.split("\n")]
    out, blanks = [], 0
    for ln in lines:
        s = ln.strip()
        if s:
            blanks = 0
            out.append(s)
        else:
            blanks += 1
            if blanks <= 1:
                out.append("")
    return "\n".join(out).strip()


def _parse_date(value: Optional[str]) -> Optional[str]:
    """'16 July 2026' or '16.07.26' -> ISO 'YYYY-MM-DD'."""
    if not value:
        return None
    value = value.strip()
    m = re.match(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", value)
    if m:
        d, mon, y = int(m.group(1)), _MONTHS.get(m.group(2).lower()), int(m.group(3))
        if mon:
            return f"{y:04d}-{mon:02d}-{d:02d}"
    m = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{2,4})", value)
    if m:
        d, mon, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if y < 100:
            y += 2000
        try:
            return f"{y:04d}-{mon:02d}-{d:02d}"
        except Exception:
            return None
    return None


class GDCScraper(BaseScraper):
    """Scraper for GDC / Dental Professionals Hearings Service determinations."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=API_BASE,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-GB,en;q=0.9",
                "Origin": SITE_BASE,
                "Referer": SITE_BASE + "/",
            },
            timeout=60,
            respect_robots=False,
        )

    # -- HTTP ------------------------------------------------------------
    def _get_json(self, url: str, params: dict = None) -> Optional[Any]:
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url, params=params)
        except Exception as e:
            logger.debug(f"GET {url} failed: {e}")
            return None
        if resp.status_code != 200:
            logger.debug(f"{url}: HTTP {resp.status_code}")
            return None
        try:
            return resp.json()
        except Exception as e:
            logger.debug(f"{url}: bad json {e}")
            return None

    def _get_pdf(self, annotation_id: str) -> Optional[bytes]:
        self.rate_limiter.wait()
        url = BLOB_BASE + annotation_id
        try:
            resp = self.client.get(url)
        except Exception as e:
            logger.warning(f"GET pdf {url} failed: {e}")
            return None
        if resp.status_code != 200:
            logger.debug(f"pdf {url}: HTTP {resp.status_code}")
            return None
        body = resp.content
        if not body[:5].startswith(b"%PDF"):
            logger.debug(f"pdf {url}: not a PDF")
            return None
        return body

    # -- enumeration -----------------------------------------------------
    def _iter_hearing_ids(self) -> Generator[Dict[str, Any], None, None]:
        first = self._get_json("/Hearing",
                               {"futureHearings": "false", "page": 1})
        if not first or "hearings" not in first:
            raise RuntimeError(
                "GDC /Hearing returned no listing — api.dentalhearings.org "
                "blocked or the DPHS API contract changed")
        page_count = int(first.get("pageCount") or 1)
        total = int(first.get("totalHearingCount") or 0)
        logger.info(f"GDC concluded hearings: {total} across {page_count} pages")
        seen = set()
        page = 1
        data = first
        while True:
            for row in data.get("hearings", []):
                hid = row.get("hearingId")
                if hid and hid not in seen:
                    seen.add(hid)
                    yield row
            page += 1
            if page > page_count:
                break
            data = self._get_json("/Hearing",
                                  {"futureHearings": "false", "page": page})
            if not data:
                break

    def _document_text(self, detail: Dict[str, Any]) -> tuple[str, str]:
        """Return (full_text, document_kind) for a hearing detail, preferring the
        reasoned determination and falling back to the charge / notice PDFs."""
        for key, kind in (("determinationDocuments", "determination"),
                          ("chargeDocuments", "charge")):
            docs = detail.get(key) or []
            parts = []
            for doc in docs:
                aid = doc.get("annotationId")
                if not aid or (doc.get("mimeType") or "").lower() not in (
                        "application/pdf", ""):
                    continue
                pdf = self._get_pdf(aid)
                if not pdf:
                    continue
                t = _clean(_pdf_text(pdf))
                if len(t) >= 120:
                    parts.append(t)
            if parts:
                return ("\n\n".join(parts), kind)
        return ("", "")

    def _build_raw(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        hid = row["hearingId"]
        detail = self._get_json(f"/Hearing/{hid}")
        if not detail:
            return None
        text, kind = self._document_text(detail)
        if len(text) < 150:
            return None
        date = _parse_date(detail.get("hearingDate")) or _parse_date(
            row.get("hearingDate"))
        return {
            "hearing_id": hid,
            "text": text,
            "document_kind": kind,
            "date": date,
            "name": detail.get("name") or row.get("name"),
            "registration_number": detail.get("registrationNumber")
                                   or row.get("registrationNumber"),
            "profession": detail.get("profession") or row.get("profession"),
            "hearing_type": detail.get("hearingType") or row.get("hearingType"),
            "outcome": detail.get("outcome") or None,
            "outcome_summary": detail.get("outcomeSummary") or None,
            "venue": detail.get("hearingVenue") or None,
            "panel": detail.get("panel") or None,
        }

    # -- core ------------------------------------------------------------
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        produced = 0
        for row in self._iter_hearing_ids():
            raw = self._build_raw(row)
            if raw:
                produced += 1
                yield raw
        if produced == 0:
            raise RuntimeError(
                "GDC enumerated hearings but extracted 0 determinations — the "
                "blob document scheme or API contract changed")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        # The listing is newest-first; stop once we cross the `since` boundary.
        since_date = since.date()
        for row in self._iter_hearing_ids():
            d = _parse_date(row.get("hearingDate"))
            if d:
                try:
                    if datetime.strptime(d, "%Y-%m-%d").date() < since_date:
                        break
                except ValueError:
                    pass
            raw = self._build_raw(row)
            if raw:
                yield raw

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = _clean(raw.get("text", "") or "")
        if len(text) < 150:
            return None
        name = raw.get("name") or "GDC registrant"
        htype = raw.get("hearing_type") or "Fitness to Practise determination"
        title = f"{name} — {htype}"
        if raw.get("date"):
            title += f" ({raw['date']})"
        return {
            "_id": f"UK-GDC-{raw['hearing_id']}",
            "_source": "UK/GDC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": f"{SITE_BASE}/hearing/{raw['hearing_id']}",
            "registrant": name,
            "registration_number": raw.get("registration_number"),
            "profession": raw.get("profession"),
            "hearing_type": htype,
            "outcome": raw.get("outcome"),
            "outcome_summary": raw.get("outcome_summary"),
            "document_kind": raw.get("document_kind"),
            "court": "General Dental Council — Dental Professionals Hearings Service",
            "jurisdiction": "GB",
            "language": "en",
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing GDC / Dental Professionals Hearings Service API...")
        got = 0
        for row in self._iter_hearing_ids():
            raw = self._build_raw(row)
            if raw:
                got += 1
                print(f"  {raw.get('name')} [{raw.get('hearing_type')}] "
                      f"{raw.get('date')} ({raw.get('document_kind')}): "
                      f"{len(raw['text'])} chars - OK")
            if got >= 3:
                break
        if got == 0:
            print("  No determinations extracted — check API/blob access")


def main():
    scraper = GDCScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)
    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command in ("bootstrap", "bootstrap-fast"):
        if sample_mode:
            logger.info("Running bootstrap in sample mode")
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        else:
            logger.info("Running full bootstrap")
            stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Bootstrap complete: {stats}")
    elif command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
