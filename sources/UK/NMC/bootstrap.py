#!/usr/bin/env python3
"""
UK/NMC -- Nursing and Midwifery Council -- Fitness to Practise outcomes.

The Nursing and Midwifery Council (NMC) is the UK statutory regulator for
~800,000 nurses, midwives and nursing associates. Its Fitness to Practise
Committee and Investigating Committee sit under the Nursing and Midwifery
Order 2001 and the NMC (Fitness to Practise) Rules 2004; their determinations
(the panel's "reasons") set out the charges/allegation, the facts found proved,
whether the registrant's fitness to practise is impaired and the sanction/order
imposed (striking-off, suspension, conditions of practice, caution) or the
interim order made. These are binding professional-regulator adjudications =
case law, distinct from UK/GMC (doctors), UK/SDT (solicitors), UK/BTAS
(barristers), UK/HCPTS (health & care professions) and UK/SocialWorkEngland.

Access & structure (all public, no auth):
  - Each concluded hearing/meeting publishes a "reasons" PDF under
    https://www.nmc.org.uk/globalassets/sitedocuments/ftpoutcomes/{year}/
    {month-year}/reasons-{name}-{type}-{PIN}-{YYYYMMDD}.pdf . These are
    born-digital PDFs with a real text layer (no OCR needed): a structured
    header (Committee / hearing type / dates / Name of Registrant / NMC PIN /
    Part(s) of the register / Relevant Location / Panel / representation),
    the charges, the reasoned determination and the outcome block
    (Facts proved / Fitness to practise / Sanction / Interim order).
  - The PDFs are indexed from monthly listing pages at
    /concerns-nurses-midwives/hearings/hearings-sanctions/hearings-{month}-{year}/ .
    The NMC keeps a rolling window of the most recent months online (older
    monthly pages are removed under its publication policy, "because decisions
    can be changed"), so a single run captures the current window (~250
    determinations across ~4 months); re-running over time accumulates the full
    record (the pipeline dedups on _id = the stable PDF stem).

Strategy:
  - For each of the last ~24 calendar months, GET the monthly listing page;
    keep the 200-OK pages and collect every ftpoutcomes "reasons-*.pdf" href.
  - Download each PDF, extract the text layer (PyMuPDF, with a shared
    pdfplumber/pypdf fallback), parse the header fields, yield full text.

Data:
  - ~250 full-text fitness-to-practise determinations in the live window,
    growing ~40-100/month. Language: English. Auth: none.

Usage:
  python bootstrap.py bootstrap          # Full pull (current live window)
  python bootstrap.py bootstrap --sample # Fetch sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental (recent months)
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
logger = logging.getLogger("legal-data-hunter.UK.NMC")

BASE_URL = "https://www.nmc.org.uk"
LISTING_TMPL = ("/concerns-nurses-midwives/hearings/hearings-sanctions/"
                "hearings-{month}-{year}/")
PDF_PREFIX = "/globalassets/sitedocuments/ftpoutcomes/"
MONTHS_BACK = 24

MONTH_NAMES = ["january", "february", "march", "april", "may", "june",
               "july", "august", "september", "october", "november", "december"]

PDF_HREF_RE = re.compile(
    r'href="(/globalassets/sitedocuments/ftpoutcomes/[^"]+?\.pdf)"', re.I)
# reasons-{name}-{type}-{pin(s)}-{YYYYMMDD}.pdf
FNAME_RE = re.compile(
    r"reasons-(?P<name>.+?)-(?P<type>[a-z]+)-(?P<pins>\d[\d-]*)-"
    r"(?P<date>\d{8})\.pdf$", re.I)

# Header field labels used inside the PDF text (value follows the colon,
# usually on the next line; runs until the next known label).
_LABELS = [
    "Name of Registrant", "Registrant", "NMC PIN", "PIN",
    "Part(s) of the register", "Part of the register", "Relevant Location",
    "Area of Registered Address", "Type of case", "Panel Members",
    "Panel Member", "Legal Assessor", "Legal Assessors", "Panel Secretary",
    "Hearings Coordinator", "Nursing and Midwifery Council", "Facts proved",
    "Facts not proved", "Facts proved by admission", "Fitness to practise",
    "Sanction", "Interim order", "Reason",
]

# Filename type-code -> human hearing-type (fallback if body parse is thin).
TYPE_CODES = {
    "icio": "Investigating Committee: Interim Order",
    "icior": "Investigating Committee: Interim Order Review",
    "ftpcsh": "Fitness to Practise Committee: Substantive Hearing",
    "ftpcsm": "Fitness to Practise Committee: Substantive Meeting",
    "ftpcsorh": "Fitness to Practise Committee: Substantive Order Review Hearing",
    "ftpcsorm": "Fitness to Practise Committee: Substantive Order Review Meeting",
    "ftpcionh": "Fitness to Practise Committee: Interim Order (New) Hearing",
    "ftpciorh": "Fitness to Practise Committee: Interim Order Review Hearing",
    "ftpcfrh": "Fitness to Practise Committee: Fraudulent/Incorrect Entry Hearing",
    "agr": "Agreed disposal / undertakings",
    "reasons": "Fitness to Practise determination",
}

_WS_RE = re.compile(r"[ \t]+")


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


def _field(text: str, label: str, maxlen: int = 240) -> Optional[str]:
    """Value of a 'Label:' header field, read until the next known label."""
    others = [re.escape(l) for l in _LABELS if l != label]
    stop = "|".join(others)
    pat = (re.escape(label) + r"\s*:\s*(.*?)(?=\n\s*(?:" + stop +
           r")\s*:?|\Z)")
    m = re.search(pat, text, re.S | re.I)
    if not m:
        return None
    v = re.sub(r"\s+", " ", m.group(1)).strip(" .;:-–")
    # drop a trailing "NMC PIN xxxx" that leaks when the colon is absent
    v = re.sub(r"\s*(?:NMC\s+)?PIN\s+[0-9A-Z]+.*$", "", v, flags=re.I).strip()
    if len(v) > maxlen:
        v = v[:maxlen].rsplit(" ", 1)[0] + "…"
    return v or None


def _hearing_type(text: str, type_code: str) -> str:
    """Committee + hearing-type from the two header lines after the NMC name."""
    lines = [ln.strip() for ln in text.split("\n")]
    for i, ln in enumerate(lines):
        if ln.lower().startswith("nursing and midwifery council"):
            head = [x for x in lines[i + 1:i + 6] if x]
            # keep the committee line + the hearing-kind line, drop dates/venue
            picked = []
            for x in head:
                if re.match(r"(name of registrant|registrant|pin)\b", x, re.I):
                    break
                if re.search(r"virtual|in[- ]person|hybrid|held\b", x, re.I):
                    continue
                if re.search(r"\bcommittee\b", x, re.I) or (
                        re.search(r"hearing|meeting|order|review|decision|removal",
                                  x, re.I) and not re.search(r"\d", x)):
                    picked.append(x)
                if len(picked) >= 2:
                    break
            if picked:
                return ": ".join(picked[:2])
            break
    return TYPE_CODES.get((type_code or "").lower(), "Fitness to Practise determination")


class NMCScraper(BaseScraper):
    """Scraper for NMC fitness-to-practise 'reasons' determination PDFs."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/pdf,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
            },
            timeout=60,
        )

    # -- HTTP ------------------------------------------------------------
    def _get_html(self, url: str) -> Optional[str]:
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
        except Exception as e:
            logger.debug(f"GET {url} failed: {e}")
            return None
        if resp.status_code != 200:
            return None
        return resp.content.decode("utf-8", "replace")

    def _get_pdf(self, url: str) -> Optional[bytes]:
        self.rate_limiter.wait()
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
    def _month_pages(self) -> List[str]:
        now = datetime.now(timezone.utc)
        y, m = now.year, now.month
        pages = []
        for _ in range(MONTHS_BACK):
            pages.append(LISTING_TMPL.format(month=MONTH_NAMES[m - 1], year=y))
            m -= 1
            if m == 0:
                m = 12
                y -= 1
        return pages

    def _pdf_urls(self) -> List[str]:
        seen, urls = set(), []
        for page in self._month_pages():
            hpage = self._get_html(BASE_URL + page)
            if not hpage:
                continue
            found = 0
            for m in PDF_HREF_RE.finditer(hpage):
                href = html.unescape(m.group(1))
                if href in seen:
                    continue
                seen.add(href)
                urls.append(href)
                found += 1
            if found:
                logger.info(f"  {page.rsplit('/', 2)[-2]}: {found} outcome PDFs")
        return urls

    def _build_raw(self, href: str) -> Optional[Dict[str, Any]]:
        fname = href.rsplit("/", 1)[-1]
        fm = FNAME_RE.search(fname)
        stem = fname[:-4] if fname.lower().endswith(".pdf") else fname
        pdf = self._get_pdf(BASE_URL + href)
        if not pdf:
            return None
        text = _clean(_pdf_text(pdf))
        if len(text) < 150:
            return None
        # date from filename (decision/publication date), fallback to header
        date = None
        pins = None
        type_code = None
        if fm:
            d = fm.group("date")
            date = f"{d[0:4]}-{d[4:6]}-{d[6:8]}"
            pins = fm.group("pins")
            type_code = fm.group("type")
        registrant = _field(text, "Name of Registrant") or _field(text, "Registrant")
        pin = (_field(text, "NMC PIN") or _field(text, "PIN")
               or (pins.replace("-", ", ") if pins else None))
        return {
            "url": BASE_URL + href,
            "stem": stem,
            "text": text,
            "date": date,
            "registrant": registrant,
            "pin": pin,
            "register_part": _field(text, "Part(s) of the register")
                             or _field(text, "Part of the register"),
            "location": _field(text, "Relevant Location")
                        or _field(text, "Area of Registered Address"),
            "hearing_type": _hearing_type(text, type_code),
            "fitness": _field(text, "Fitness to practise"),
            "sanction": _field(text, "Sanction"),
            "interim_order": _field(text, "Interim order"),
        }

    # -- core ------------------------------------------------------------
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        urls = self._pdf_urls()
        if not urls:
            raise RuntimeError(
                "NMC listing returned 0 outcome PDFs — site blocked or the "
                "hearings-sanctions monthly-page layout changed")
        produced = 0
        for href in urls:
            raw = self._build_raw(href)
            if raw:
                produced += 1
                yield raw
        if produced == 0:
            raise RuntimeError(
                "NMC found outcome PDFs but extracted 0 determinations — PDF "
                "layout changed or all downloads failed")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        since_date = since.date()
        for href in self._pdf_urls():
            fm = FNAME_RE.search(href.rsplit("/", 1)[-1])
            if fm:
                d = fm.group("date")
                try:
                    if datetime(int(d[0:4]), int(d[4:6]), int(d[6:8])).date() < since_date:
                        continue
                except ValueError:
                    pass
            raw = self._build_raw(href)
            if raw:
                yield raw

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = _clean(raw.get("text", "") or "")
        if len(text) < 150:
            return None
        registrant = raw.get("registrant") or "NMC registrant"
        htype = raw.get("hearing_type") or "Fitness to Practise determination"
        title = f"{registrant} — {htype}"
        if raw.get("date"):
            title += f" ({raw['date']})"
        return {
            "_id": f"UK-NMC-{raw['stem']}",
            "_source": "UK/NMC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw["url"],
            "registrant": registrant,
            "registration_number": raw.get("pin"),
            "register_part": raw.get("register_part"),
            "location": raw.get("location"),
            "hearing_type": htype,
            "fitness_to_practise": raw.get("fitness"),
            "sanction": raw.get("sanction"),
            "interim_order": raw.get("interim_order"),
            "court": "Nursing and Midwifery Council — Fitness to Practise Committee",
            "jurisdiction": "GB",
            "language": "en",
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing NMC hearings-sanctions enumeration...")
        urls = self._pdf_urls()
        print(f"  {len(urls)} outcome PDFs across the live window")
        got = 0
        for href in urls:
            raw = self._build_raw(href)
            if raw:
                got += 1
                print(f"  {raw.get('registrant')} [{raw.get('hearing_type')}] "
                      f"{raw.get('date')}: {len(raw['text'])} chars - OK")
            if got >= 3:
                break


def main():
    scraper = NMCScraper()
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
