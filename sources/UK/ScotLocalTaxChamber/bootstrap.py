#!/usr/bin/env python3
"""
UK/ScotLocalTaxChamber -- First-tier Tribunal for Scotland Local Taxation Chamber.

The First-tier Tribunal for Scotland Local Taxation Chamber (LTC), established on
1 April 2023, decides appeals on Scottish local-taxation matters: council tax
(banding, liability, discounts/exemptions, penalties), non-domestic (business)
rates and rateable-value/valuation-roll appeals, and water/sewerage charge
appeals. It absorbed the functions of the former Valuation Appeal Committees and
the Council Tax Reduction Review Panel. Its administration is provided by the
Scottish Courts and Tribunals Service (SCTS). Its written decisions are
adjudicative case law for the GB-SCT (Scotland) jurisdiction, NOT covered by
UK/CaseLaw (England & Wales superior courts + reserved UK tribunals, indexed via
the National Archives Find Case Law service; LTC decisions are not on that
service).

Site: https://www.localtaxationchamber.scot/ -- a React single-page app whose
"Decisions" view is populated from an Azure Blob Storage account. The decision
PDFs live in a PUBLICLY-LISTABLE container:

    https://ltcpastrauks003.blob.core.windows.net/decision-documents

(storage account + container names are read from the app's own runtime config in
the JS bundle; the container permits anonymous blob-list + blob-read, no SAS
token). Each blob is a born-digital decision PDF named like

    "Decision (Appeal) 23.00012.pdf"
    "Decision (Upper Tribunal Referral) 24.00051.pdf"
    "Decision (Review) 25.00007.pdf"

Strategy:
  - List the container's blobs via the Azure "list container" REST call
    (?restype=container&comp=list), following NextMarker if present.
  - For each blob, download the born-digital PDF and extract full text with
    PyMuPDF (pdfplumber/pypdf fallback). No OCR needed.
  - Parse the Chamber Ref (FTS/LTC/XX/YY/NNNNN), the parties (appellant /
    respondent), the tribunal member and the decision date directly from the
    PDF text; the decision type (Appeal / Review / Upper Tribunal Referral /
    Expenses Request) comes from the blob filename.
  - One record per decision PDF.

Data:
  - ~1,180 full-text decisions, 2023-present
  - Language: English
  - Auth: None (free public access)
  - Licence: SCTS terms (personal / in-house use only) -- commercial-restricted

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental (recent decisions)
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import quote
import xml.etree.ElementTree as ET

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UK.ScotLocalTaxChamber")

STORAGE_ACCOUNT = "ltcpastrauks003"
CONTAINER = "decision-documents"
BLOB_BASE = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net/{CONTAINER}"
SITE_URL = "https://www.localtaxationchamber.scot/"

# Chamber reference, e.g. FTS/LTC/CT/23/00012 (CT=Council Tax, NDR=Non-Domestic
# Rates, WA=Water, etc.).
REF_RE = re.compile(r"FTS/LTC/[A-Z]+/\d{2}/\d+", re.I)
# Decision type from the blob filename: "Decision (Appeal) 23.00012.pdf".
TYPE_RE = re.compile(r"Decision\s*\(([^)]+)\)", re.I)
# Short docket number from the filename: "23.00012".
DOCKET_RE = re.compile(r"(\d{2}\.\d{3,})")

_MONTHS = ("january february march april may june july august september "
           "october november december").split()
_MONTH_NUM = {m: i for i, m in enumerate(_MONTHS, start=1)}
TEXT_DATE_RE = re.compile(
    r"\b(\d{1,2})\s+(January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+(\d{4})\b", re.I)
# The signing/decision date is written near the end as "Date 13 July 2023" /
# "Date: 13 July 2023".
SIGN_DATE_RE = re.compile(
    r"Date\s*:?\s*(\d{1,2}\s+(?:January|February|March|April|May|June|July|"
    r"August|September|October|November|December)\s+\d{4})", re.I)

APPELLANT_RE = re.compile(
    r"Parties\s*:?\s*(.+?)\s*[\(（]\s*[“”\"']?the\s+appellant",
    re.I | re.S)
RESPONDENT_RE = re.compile(
    r"[“”\"']?the\s+appellant[”\"']?\s*[\)）]\s*(.+?)"
    r"[\(（]\s*[“”\"']?the\s+(?:respondent|local\s+authority|assessor)",
    re.I | re.S)


def _iso_from_text_date(s: str) -> Optional[str]:
    m = TEXT_DATE_RE.search(s or "")
    if not m:
        return None
    day, mon, year = m.groups()
    month = _MONTH_NUM.get(mon.lower())
    if not month:
        return None
    try:
        return f"{int(year):04d}-{month:02d}-{int(day):02d}"
    except (ValueError, TypeError):
        return None


def _decision_date(text: str) -> Optional[str]:
    """Prefer the signing 'Date <d Month yyyy>' (usually the last one in the
    document); fall back to the last date mentioned anywhere."""
    signs = SIGN_DATE_RE.findall(text or "")
    if signs:
        iso = _iso_from_text_date(signs[-1])
        if iso:
            return iso
    dates = TEXT_DATE_RE.findall(text or "")
    if dates:
        day, mon, year = dates[-1]
        month = _MONTH_NUM.get(mon.lower())
        if month:
            try:
                return f"{int(year):04d}-{month:02d}-{int(day):02d}"
            except (ValueError, TypeError):
                return None
    return None


def _party(text: str, pattern: re.Pattern) -> str:
    m = pattern.search(text or "")
    if not m:
        return ""
    raw = re.sub(r"\s+", " ", m.group(1)).strip(" ,;:“”\"'()")
    # Party name is the segment before the first comma (address follows).
    name = raw.split(",")[0].strip()
    return name[:200]


def _blob_url(name: str) -> str:
    return f"{BLOB_BASE}/{quote(name, safe='')}"


def _pdf_text(pdf_bytes: bytes) -> str:
    if fitz is not None:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            try:
                text = "\n".join(page.get_text() for page in doc).strip()
            finally:
                doc.close()
            if len(text) >= 120:
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
                    if t and len(t) >= 120:
                        return t
                except Exception:
                    continue
    except Exception:
        pass
    return ""


def _clean(text: str) -> str:
    lines = [ln.rstrip() for ln in (text or "").replace("\r", "").split("\n")]
    out, blanks = [], 0
    for ln in lines:
        if ln.strip():
            blanks = 0
            out.append(ln.strip())
        else:
            blanks += 1
            if blanks <= 1:
                out.append("")
    return "\n".join(out).strip()


# Azure blob-list XML uses no namespace on these element names.
def _iter_blob_names(xml_bytes: bytes):
    root = ET.fromstring(xml_bytes)
    blobs = root.find("Blobs")
    marker = root.findtext("NextMarker") or ""
    names = []
    if blobs is not None:
        for blob in blobs.findall("Blob"):
            name = blob.findtext("Name")
            if name:
                names.append(name)
    return names, marker.strip()


class ScotLocalTaxChamberScraper(BaseScraper):
    """Scraper for First-tier Tribunal for Scotland Local Taxation Chamber
    decisions (Azure Blob container of born-digital PDFs)."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BLOB_BASE,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "*/*",
                "Accept-Language": "en-GB,en;q=0.9",
            },
            timeout=90,
        )

    # -- discovery -------------------------------------------------------
    def _list_blobs(self) -> List[str]:
        names: List[str] = []
        marker = ""
        for _ in range(50):  # safety cap; each page returns up to 5000 blobs
            url = f"{BLOB_BASE}?restype=container&comp=list&maxresults=5000"
            if marker:
                url += f"&marker={quote(marker, safe='')}"
            self.rate_limiter.wait()
            try:
                resp = self.client.get(url)
            except Exception as e:
                logger.warning(f"list blobs failed: {e}")
                break
            if resp.status_code != 200:
                logger.warning(f"list blobs: HTTP {resp.status_code}")
                break
            page_names, marker = _iter_blob_names(resp.content)
            names.extend(page_names)
            if not marker:
                break
        # Only decision PDFs.
        return [n for n in names if n.lower().endswith(".pdf")]

    def _fetch_pdf(self, url: str) -> Optional[bytes]:
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
        except Exception as e:
            logger.warning(f"pdf {url}: {e}")
            return None
        if resp.status_code != 200:
            logger.warning(f"pdf {url}: HTTP {resp.status_code}")
            return None
        data = resp.content
        if not data[:5].startswith(b"%PDF"):
            logger.debug(f"pdf {url}: not a PDF")
            return None
        return data

    def _hydrate(self, name: str) -> Optional[Dict[str, Any]]:
        url = _blob_url(name)
        pdf = self._fetch_pdf(url)
        if not pdf:
            return None
        try:
            text = _pdf_text(pdf)
        except Exception as e:
            logger.debug(f"extract {name}: {e}")
            text = ""
        if not text:
            return None
        return {"blob_name": name, "url": url, "text": text}

    # -- core ------------------------------------------------------------
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        names = self._list_blobs()
        logger.info(f"listed {len(names)} decision PDFs")
        produced = 0
        seen = set()
        for name in names:
            if name in seen:
                continue
            seen.add(name)
            hydrated = self._hydrate(name)
            if hydrated:
                produced += 1
                yield hydrated
        if produced == 0:
            raise RuntimeError(
                "LTC decision-documents container returned 0 usable PDFs — "
                "blob container blocked, renamed, or all PDFs unreadable"
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Incremental: walk the container and emit decisions whose parsed
        decision date is on/after `since`."""
        cutoff = since.strftime("%Y-%m-%d") if since else None
        for name in self._list_blobs():
            hydrated = self._hydrate(name)
            if not hydrated:
                continue
            date = _decision_date(hydrated["text"])
            if cutoff and date and date < cutoff:
                continue
            yield hydrated

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = _clean(raw.get("text", "") or "")
        if len(text) < 200:
            return None
        name = raw.get("blob_name", "")

        ref_m = REF_RE.search(text)
        case_ref = ref_m.group(0).upper() if ref_m else ""
        dm = DOCKET_RE.search(name)
        docket = dm.group(1) if dm else ""
        tm = TYPE_RE.search(name)
        decision_type = tm.group(1).strip() if tm else "Decision"

        appellant = _party(text, APPELLANT_RE)
        respondent = _party(text, RESPONDENT_RE)
        parties = " v ".join([p for p in (appellant, respondent) if p])

        date = _decision_date(text)
        if not date and case_ref:
            # Year part of the ref (FTS/LTC/CT/23/00012 -> 2023).
            ym = re.search(r"/(\d{2})/\d+$", case_ref)
            if ym:
                date = f"20{ym.group(1)}-01-01"

        ident = case_ref.replace("/", "-") or docket or Path(name).stem
        slug = re.sub(r"[^A-Za-z0-9]+", "-", ident).strip("-")

        title = case_ref
        if parties:
            title = f"{case_ref}: {parties}".strip().lstrip(":").strip()
        if decision_type and decision_type.lower() != "appeal":
            title = f"{title} ({decision_type})" if title else decision_type
        if not title:
            title = Path(name).stem

        return {
            "_id": f"UK-ScotLocalTaxChamber-{slug}",
            "_source": "UK/ScotLocalTaxChamber",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": raw.get("url", ""),
            "case_ref": case_ref,
            "docket": docket,
            "decision_type": decision_type,
            "appellant": appellant,
            "respondent": respondent,
            "court": "First-tier Tribunal for Scotland Local Taxation Chamber",
            "jurisdiction": "GB-SCT",
            "language": "en",
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing LTC decision-documents container...")
        names = self._list_blobs()
        print(f"  listed {len(names)} decision PDFs")
        if not names:
            return
        raw = self._hydrate(names[0])
        if raw:
            rec = self.normalize(raw)
            print(f"  {rec['case_ref']} ({rec['date']}): "
                  f"{len(rec['text'])} chars extracted - OK")


def main():
    scraper = ScotLocalTaxChamberScraper()
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
