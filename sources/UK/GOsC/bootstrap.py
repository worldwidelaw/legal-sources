#!/usr/bin/env python3
"""
UK/GOsC -- General Osteopathic Council -- Fitness to Practise decisions.

The General Osteopathic Council (GOsC) is the UK statutory regulator for
osteopaths under the Osteopaths Act 1993. Fitness to practise concerns are heard
by the independent Professional Conduct Committee (PCC) and Health Committee
(HC), with interim orders made by the Investigating Committee (IC/ISO). Each
concluded hearing publishes a reasoned DECISION setting out the allegation, the
facts found proved, whether the osteopath's fitness to practise is impaired, and
the sanction imposed (admonishment, conditions of practice, suspension, removal
from the Register) or the interim/undertaking order made. These are binding
professional-regulator adjudications = case law, distinct from the sibling UK
regulator sources: UK/GMC (doctors), UK/GDC (dentists), UK/GOC (opticians),
UK/GPhC (pharmacists), UK/SDT (solicitors), UK/BTAS (barristers), UK/HCPTS
(health & care professions), UK/NMC (nurses/midwives), UK/SocialWorkEngland.

Access & structure (all public, no auth):
  - osteopathy.org.uk publishes a single "Decisions" listing page:
      https://www.osteopathy.org.uk/raise-a-concern/hearings/decisions/
    grouping published cases (undertakings, interim suspension orders, hearing
    outcomes) as links to per-osteopath decision pages under
      /news-and-resources/document-library/fitness-to-practise/{slug}/
    (a few older ones live under .../document-library/about-the-gosc/{slug}/).
    The slug encodes the osteopath name, committee (PCC / HC / IC-ISO) and the
    decision date, e.g. "gosc-v-amine-el-bacha-pcc-final-determination-20-june-2024".
  - Each decision page is a thin document-library wrapper that auto-downloads the
    reasoned decision as a BORN-DIGITAL PDF linked with a relative href
    ("./{file}.pdf", content-type application/pdf, real text layer, no OCR): a
    structured header (Case No / committee / hearing date / case-of name /
    committee members / legal assessor) followed by the numbered reasoned
    decision.
  - The listing exposes a rolling window of published decisions (older ones
    removed under the GOsC fitness-to-practise publication policy), so one run
    captures the current window (~40 decisions) and re-runs accumulate the record
    (the pipeline dedups on _id = the stable decision slug).

Strategy:
  - Fetch the decisions listing page; collect every decision-page URL; for each,
    fetch the page, resolve its ".pdf" link, download the PDF, extract the text
    layer (PyMuPDF, with a shared pdfplumber/pypdf fallback) and yield full text.

Data:
  - ~40 full-text fitness-to-practise decisions in the live window.
    Language: English. Auth: none.

Usage:
  python bootstrap.py bootstrap          # Full pull (current published window)
  python bootstrap.py bootstrap --sample # Fetch sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental (recent decisions first)
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import html
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

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
logger = logging.getLogger("legal-data-hunter.UK.GOsC")

SITE_BASE = "https://www.osteopathy.org.uk"
DECISIONS_URL = SITE_BASE + "/raise-a-concern/hearings/decisions/"

_WS_RE = re.compile(r"[ \t]+")
_MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}
# per-osteopath decision pages in the document library
_DECISION_HREF_RE = re.compile(
    r'href="(https?://www\.osteopathy\.org\.uk'
    r'/news-and-resources/document-library/[^"]+)"', re.I)
# a per-decision page slug always carries a committee / decision-type token;
# policy & guidance pages do not — use this as a positive allowlist
_DECISION_SLUG_RE = re.compile(
    r"(pcc|-hc-|iso|council-decision|decision-of-council|rule-\d|"
    r"determination|website-notice|professional-conduct)", re.I)
_PDF_HREF_RE = re.compile(r'href="([^"]+\.pdf(?:\?[^"]*)?)"', re.I)


def _pdf_text(pdf_bytes: bytes) -> str:
    """Full text of a born-digital decision PDF via PyMuPDF, with a shared
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


def _strip_tags(fragment: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", " ", fragment or ""))


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


def _slug_of(url: str) -> str:
    return url.rstrip("/").rsplit("/", 1)[-1]


def _name_from_slug(slug: str) -> Optional[str]:
    """Best-effort registrant name from the decision slug."""
    s = slug
    s = re.sub(r"^gosc-v-", "", s)
    # cut at the first committee/decision-type token
    s = re.split(
        r"-(?:pcc|hc|ic|iso|professional|health|council|final|decision|review|"
        r"restoration|rule|interim|website|determination|hearing)\b", s, 1)[0]
    s = s.strip("-")
    if not s:
        return None
    name = " ".join(w.capitalize() for w in s.split("-"))
    return name or None


def _date_from_text(text: str) -> Optional[str]:
    """Find a 'DD Month YYYY' date in the decision header."""
    m = re.search(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", text[:2500])
    if m:
        mi = _MONTHS.get(m.group(2).lower())
        if mi:
            try:
                return f"{int(m.group(3)):04d}-{mi:02d}-{int(m.group(1)):02d}"
            except Exception:
                return None
    return None


def _date_from_slug(slug: str) -> Optional[str]:
    m = re.search(r"(\d{1,2})-([a-z]+)-(\d{4})$", slug)
    if m:
        mi = _MONTHS.get(m.group(2).lower())
        if mi:
            try:
                return f"{int(m.group(3)):04d}-{mi:02d}-{int(m.group(1)):02d}"
            except Exception:
                return None
    m = re.search(r"-(\d{2})(\d{2})(\d{2})$", slug)  # DDMMYY, e.g. 161225
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), 2000 + int(m.group(3))
        try:
            if 1 <= mo <= 12 and 1 <= d <= 31:
                return f"{y:04d}-{mo:02d}-{d:02d}"
        except Exception:
            return None
    return None


class GOsCScraper(BaseScraper):
    """Scraper for General Osteopathic Council fitness-to-practise decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=SITE_BASE,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
                "Accept-Language": "en-GB,en;q=0.9",
                "Referer": SITE_BASE + "/",
            },
            timeout=60,
            respect_robots=False,
        )

    # -- HTTP ------------------------------------------------------------
    def _get(self, url: str) -> Optional[bytes]:
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
        except Exception as e:
            logger.warning(f"GET {url} failed: {e}")
            return None
        if resp.status_code != 200:
            logger.debug(f"{url}: HTTP {resp.status_code}")
            return None
        return resp.content

    # -- enumeration -----------------------------------------------------
    def _list_decision_urls(self) -> List[str]:
        body = self._get(DECISIONS_URL)
        if not body:
            raise RuntimeError(
                "GOsC decisions page unreachable — osteopathy.org.uk blocked or "
                "the URL changed")
        page_html = body.decode("utf-8", errors="replace")
        urls = []
        seen = set()
        for m in _DECISION_HREF_RE.finditer(page_html):
            u = m.group(1).replace("http://", "https://").rstrip("/") + "/"
            slug = _slug_of(u)
            # keep only per-decision pages: slug carries a committee token and is
            # not a policy/guidance page
            if "policy" in slug.lower() or "guidance" in slug.lower():
                continue
            if not _DECISION_SLUG_RE.search(slug):
                continue
            if u in seen:
                continue
            seen.add(u)
            urls.append(u)
        if not urls:
            raise RuntimeError(
                "GOsC decisions page returned no decision links — the listing "
                "layout changed")
        logger.info(f"GOsC decisions: {len(urls)} decision pages")
        return urls

    def _decision_pdf_url(self, page_url: str) -> Optional[str]:
        body = self._get(page_url)
        if not body:
            return None
        page_html = body.decode("utf-8", errors="replace")
        for m in _PDF_HREF_RE.finditer(page_html):
            href = html.unescape(m.group(1))
            return urljoin(page_url, href)
        return None

    def _build_raw(self, page_url: str) -> Optional[Dict[str, Any]]:
        slug = _slug_of(page_url)
        pdf_url = self._decision_pdf_url(page_url)
        text = ""
        if pdf_url:
            pdf = self._get(pdf_url)
            if pdf and pdf[:5].startswith(b"%PDF"):
                text = _clean(_pdf_text(pdf))
        if len(text) < 150:
            return None
        date = _date_from_text(text) or _date_from_slug(slug)
        case_no = None
        mc = re.search(r"Case\s*No[:.]?\s*([0-9A-Za-z/\-]+)", text[:800])
        if mc:
            case_no = mc.group(1).strip()
        return {
            "slug": slug,
            "page_url": page_url,
            "pdf_url": pdf_url,
            "text": text,
            "date": date,
            "name": _name_from_slug(slug),
            "case_no": case_no,
        }

    # -- core ------------------------------------------------------------
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        produced = 0
        for url in self._list_decision_urls():
            raw = self._build_raw(url)
            if raw:
                produced += 1
                yield raw
        if produced == 0:
            raise RuntimeError(
                "GOsC listed decisions but extracted 0 — the decision-page PDF "
                "scheme changed")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        since_date = since.date()
        for url in self._list_decision_urls():
            raw = self._build_raw(url)
            if not raw:
                continue
            d = raw.get("date")
            if d:
                try:
                    if datetime.strptime(d, "%Y-%m-%d").date() < since_date:
                        continue
                except ValueError:
                    pass
            yield raw

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = _clean(raw.get("text", "") or "")
        if len(text) < 150:
            return None
        name = raw.get("name") or "GOsC registrant"
        # committee / decision type readable label from the slug
        slug = raw.get("slug", "")
        label = slug.replace("gosc-v-", "").replace("-", " ").strip()
        label = re.sub(r"\b\d{1,2} [a-z]+ \d{4}\b", "", label).strip().title() \
            or "Fitness to Practise decision"
        title = f"{name} — General Osteopathic Council decision"
        if raw.get("date"):
            title += f" ({raw['date']})"
        return {
            "_id": f"UK-GOsC-{raw['slug']}",
            "_source": "UK/GOsC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("page_url"),
            "pdf_url": raw.get("pdf_url"),
            "registrant": name,
            "case_number": raw.get("case_no"),
            "hearing_type": label,
            "court": "General Osteopathic Council — Professional Conduct / Health Committee",
            "jurisdiction": "GB",
            "language": "en",
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing GOsC decisions page...")
        urls = self._list_decision_urls()
        print(f"  Listed {len(urls)} decision pages")
        got = 0
        for url in urls:
            raw = self._build_raw(url)
            if raw:
                got += 1
                print(f"  {raw.get('name')} {raw.get('date')} "
                      f"[{raw.get('case_no')}]: {len(raw['text'])} chars - OK")
            if got >= 3:
                break
        if got == 0:
            print("  No decisions extracted — check PDF access")


def main():
    scraper = GOsCScraper()
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
