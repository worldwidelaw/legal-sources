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
  - osteopathy.org.uk was rebuilt on WordPress in 2026; the decisions listing
    moved from /raise-a-concern/... to
      https://www.osteopathy.org.uk/raising-a-concern/hearings/decisions/
    and every published case is now a `hearing_decision` custom post type,
    exposed through the site's open WP REST API:
      /wp-json/wp/v2/hearing_decision?per_page=100
    (id, slug, link, title, date + a `hearing_decision_type` taxonomy giving
    undertaking / interim-suspension-order / council-decision / professional-
    conduct-committee-and-health-committee-decisions). Enumerating the API is
    preferred over scraping the listing tables: it is paginated, stable and
    returns the same set the page renders.
  - Each decision post lives at /hearing-decision/{slug}/ and its body links the
    reasoned decision as a BORN-DIGITAL PDF under /wp-content/uploads/YYYY/MM/
    (content-type application/pdf, real text layer, no OCR): a structured header
    (Case No / committee / hearing date / case-of name / committee members /
    legal assessor) followed by the numbered reasoned decision.
  - The API exposes a rolling window of published decisions (older ones removed
    under the GOsC fitness-to-practise publication policy), so one run captures
    the current window (~46 decisions) and re-runs accumulate the record (the
    pipeline dedups on _id = the stable decision slug).

Strategy:
  - Page the WP REST `hearing_decision` collection; for each post fetch its page,
    resolve the ".pdf" link inside <main>, download the PDF, extract the text
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
DECISIONS_URL = SITE_BASE + "/raising-a-concern/hearings/decisions/"
API_URL = SITE_BASE + "/wp-json/wp/v2/hearing_decision"
TAXONOMY_URL = SITE_BASE + "/wp-json/wp/v2/hearing_decision_type"

_WS_RE = re.compile(r"[ \t]+")
_MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}
# per-osteopath decision pages (WordPress `hearing_decision` post type)
_DECISION_HREF_RE = re.compile(
    r'href="(https?://www\.osteopathy\.org\.uk/hearing-decision/[^"]+)"', re.I)
_PDF_HREF_RE = re.compile(r'href="([^"]+\.pdf(?:\?[^"]*)?)"', re.I)
_TITLE_DATE_RE = re.compile(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\s*$")


def _main_region(page_html: str) -> str:
    """The <main> content region, so site-wide footer/policy PDFs are ignored."""
    i = page_html.find("<main")
    if i < 0:
        return page_html
    j = page_html.find("</main>", i)
    return page_html[i:j if j > 0 else len(page_html)]


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


def _date_from_title(title: Optional[str]) -> Optional[str]:
    """WP post titles end with the decision date, e.g.
    'Ms Poonam Shah – PCC Review Decision – 01 July 2026'."""
    if not title:
        return None
    m = _TITLE_DATE_RE.search(title.strip())
    if not m:
        return None
    mi = _MONTHS.get(m.group(2).lower())
    if not mi:
        return None
    try:
        return f"{int(m.group(3)):04d}-{mi:02d}-{int(m.group(1)):02d}"
    except Exception:
        return None


def _name_from_title(title: Optional[str]) -> Optional[str]:
    """Registrant name = the segment before the first dash separator."""
    if not title:
        return None
    name = re.split(r"\s+[–—-]\s+", title.strip(), 1)[0].strip()
    name = re.sub(r"^GOsC\s+v\s+", "", name, flags=re.I).strip()
    return name or None


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
        self._types: Optional[Dict[int, str]] = None
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
    def _get_resp(self, url: str):
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
        except Exception as e:
            logger.warning(f"GET {url} failed: {e}")
            return None
        if resp.status_code != 200:
            logger.debug(f"{url}: HTTP {resp.status_code}")
            return None
        return resp

    def _get(self, url: str) -> Optional[bytes]:
        resp = self._get_resp(url)
        return resp.content if resp is not None else None

    # -- enumeration -----------------------------------------------------
    def _type_labels(self) -> Dict[int, str]:
        """hearing_decision_type term id -> human label."""
        if self._types is None:
            self._types = {}
            resp = self._get_resp(TAXONOMY_URL + "?per_page=100")
            if resp is not None:
                try:
                    for term in resp.json():
                        self._types[int(term["id"])] = _strip_tags(
                            term.get("name") or "").strip()
                except Exception as e:
                    logger.debug(f"taxonomy fetch failed: {e}")
        return self._types

    def _list_from_listing_page(self) -> List[Dict[str, Any]]:
        """Fallback enumeration: scrape the rendered decisions listing page."""
        body = self._get(DECISIONS_URL)
        if not body:
            return []
        page_html = body.decode("utf-8", errors="replace")
        out, seen = [], set()
        for m in _DECISION_HREF_RE.finditer(page_html):
            u = m.group(1).replace("http://", "https://").rstrip("/") + "/"
            if u in seen:
                continue
            seen.add(u)
            out.append({"link": u, "slug": _slug_of(u), "title": None,
                        "type": None})
        return out

    def _list_decisions(self) -> List[Dict[str, Any]]:
        """Every published hearing_decision post, newest first (WP REST API)."""
        labels = self._type_labels()
        items: List[Dict[str, Any]] = []
        seen = set()
        page, total_pages = 1, 1
        while page <= total_pages:
            resp = self._get_resp(
                f"{API_URL}?per_page=100&orderby=date&order=desc&page={page}"
                "&_fields=id,slug,link,title,date,hearing_decision_type")
            if resp is None:
                break
            try:
                batch = resp.json()
            except Exception as e:
                logger.warning(f"hearing_decision page {page} not JSON: {e}")
                break
            if not isinstance(batch, list) or not batch:
                break
            for post in batch:
                link = (post.get("link") or "").replace(
                    "http://", "https://").rstrip("/") + "/"
                if not link or link in seen:
                    continue
                seen.add(link)
                terms = post.get("hearing_decision_type") or []
                items.append({
                    "link": link,
                    "slug": post.get("slug") or _slug_of(link),
                    "title": _strip_tags(
                        (post.get("title") or {}).get("rendered") or "").strip()
                    or None,
                    "type": labels.get(terms[0]) if terms else None,
                })
            try:
                total_pages = int(resp.headers.get("X-WP-TotalPages") or 1)
            except (TypeError, ValueError):
                total_pages = 1
            page += 1
        if not items:
            logger.warning("WP REST hearing_decision returned nothing — "
                           "falling back to the rendered listing page")
            items = self._list_from_listing_page()
        if not items:
            raise RuntimeError(
                "GOsC returned no decision links — the wp-json hearing_decision "
                "API and the decisions listing both came back empty")
        logger.info(f"GOsC decisions: {len(items)} decision pages")
        return items

    def _decision_pdf_url(self, page_url: str) -> Optional[str]:
        body = self._get(page_url)
        if not body:
            return None
        page_html = _main_region(body.decode("utf-8", errors="replace"))
        for m in _PDF_HREF_RE.finditer(page_html):
            href = html.unescape(m.group(1))
            return urljoin(page_url, href)
        return None

    def _build_raw(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        page_url = item["link"]
        slug = item.get("slug") or _slug_of(page_url)
        title = item.get("title")
        pdf_url = self._decision_pdf_url(page_url)
        text = ""
        if pdf_url:
            pdf = self._get(pdf_url)
            if pdf and pdf[:5].startswith(b"%PDF"):
                text = _clean(_pdf_text(pdf))
        if len(text) < 150:
            return None
        date = (_date_from_title(title) or _date_from_slug(slug)
                or _date_from_text(text))
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
            "name": _name_from_title(title) or _name_from_slug(slug),
            "case_no": case_no,
            "post_title": title,
            "decision_type": item.get("type"),
        }

    # -- core ------------------------------------------------------------
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        produced = 0
        for item in self._list_decisions():
            raw = self._build_raw(item)
            if raw:
                produced += 1
                yield raw
        if produced == 0:
            raise RuntimeError(
                "GOsC listed decisions but extracted 0 — the decision-page PDF "
                "scheme changed")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        since_date = since.date()
        for item in self._list_decisions():
            raw = self._build_raw(item)
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
        # committee / decision type readable label: prefer the WP taxonomy term,
        # fall back to the slug with the trailing date stripped
        label = raw.get("decision_type")
        if not label:
            slug = raw.get("slug", "")
            label = slug.replace("gosc-v-", "").replace("-", " ").strip()
            label = re.sub(r"\b\d{1,2} [a-z]+ \d{4}\b", "", label).strip().title()
        label = label or "Fitness to Practise decision"
        title = raw.get("post_title") or (
            f"{name} — General Osteopathic Council decision")
        if raw.get("date") and raw["date"][:4] not in title:
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
        print("Testing GOsC hearing_decision API...")
        items = self._list_decisions()
        print(f"  Listed {len(items)} decision pages")
        got = 0
        for item in items:
            raw = self._build_raw(item)
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
