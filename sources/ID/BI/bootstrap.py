#!/usr/bin/env python3
"""
ID/BI -- Bank Indonesia Regulations (Peraturan Bank Indonesia)

Source: https://www.bi.go.id/id/publikasi/peraturan  (official BI "Peraturan" section)

Why not jdih.bi.go.id
--------------------
The former path (JDIH BI REST API at jdih.bi.go.id/api/WebJDIH/...) sits behind an
F5/BIG-IP ASM policy that answers EVERY request from a non-Indonesian address with
HTTP 200 and a "The requested URL was rejected" interstitial. Because it is a 200,
the old listing parser found zero `Detail/{id}` links and reported success with an
empty corpus (issue #1470). Verified 2026-08-21 from two independent vantages, with
and without a browser UA, on `/`, `/Web/DaftarPeraturan` and the JSON API.

www.bi.go.id publishes the same regulations (PBI / PADG / SE) as born-digital PDFs
and is reachable, so the scraper now reads from there.

Strategy:
  - The listing is an ASP.NET WebForms page (SharePoint webpart). Filter it to a
    one-year date range via the visible date pickers, then walk the DataPager "Next"
    button. Year partitioning keeps each walk short and bounds a restart's cost.
  - Listing rows already carry title / date / regulation type / BI sector.
  - Detail page `/id/publikasi/peraturan/Pages/{slug}.aspx` links the regulation PDF
    (plus optional FAQ / Ringkasan companions, which are excluded).
  - Full text extracted from the PDF via common.pdf_extract.

Document types:
  - PBI:  Peraturan Bank Indonesia (Bank Indonesia Regulations)
  - PADG: Peraturan Anggota Dewan Gubernur (Board of Governors Regulations)
  - SE:   Surat Edaran (Circular Letters)

Coverage: 2006 to present (earlier years return no rows on the live filter).
Language: Indonesian (Bahasa Indonesia).

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py bootstrap-fast     # Concurrent full pull (used by the runner)
  python bootstrap.py update             # Incremental update (recent years)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import html as htmllib
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional, Tuple

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ID.BI")

BASE = "https://www.bi.go.id"
LISTING_URL = f"{BASE}/id/publikasi/peraturan/default.aspx"
DETAIL_URL = f"{BASE}/id/publikasi/peraturan/Pages/{{slug}}.aspx"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
REQUEST_DELAY = 1.5       # seconds between listing requests
FIRST_YEAR = 2006         # earliest year the live filter returns rows for
MAX_PAGES_PER_YEAR = 400  # runaway guard on the Next-button walk

# The WAF interstitial is served with HTTP 200, so the body has to be sniffed
# rather than the status code trusted.
BLOCK_MARKERS = ("The requested URL was rejected", "URL yang Anda minta ditolak")

MONTHS_ID = {
    "januari": 1, "februari": 2, "maret": 3, "april": 4, "mei": 5, "juni": 6,
    "juli": 7, "agustus": 8, "september": 9, "oktober": 10, "nopember": 11,
    "november": 11, "desember": 12,
}

ROW_SPLIT = '<div class="media media--pers">'
SLUG_RE = re.compile(r'/id/publikasi/peraturan/Pages/([^"/?#]+?)\.aspx', re.I)
TITLE_RE = re.compile(r'class="mt-0 media__title[^"]*"[^>]*>(.*?)</a>', re.S)
SUBTITLE_RE = re.compile(r'<div class="media__subtitle">(.*?)</div>', re.S)
HIDDEN_RE = re.compile(r'<input[^>]*type="hidden"[^>]*>', re.I)
PREFIX_RE = re.compile(r'(ctl00\$ctl54\$g_[0-9a-f_]+\$ctl00)\$DataPagerPeraturan')
PDF_HREF_RE = re.compile(r'href="([^"]+?\.pdf)"', re.I)
# FAQ sheets and "Ringkasan" (executive summaries) are companions, not the regulation.
COMPANION_RE = re.compile(r'/(FAQ|Ringkasan|Summary|Sosialisasi)', re.I)

REG_TYPES = {
    "pbi": ("PBI", "Peraturan Bank Indonesia"),
    "padg": ("PADG", "Peraturan Anggota Dewan Gubernur"),
    "se": ("SE", "Surat Edaran"),
}


def strip_tags(s: str) -> str:
    """Strip HTML tags and collapse whitespace."""
    return re.sub(r"\s+", " ", htmllib.unescape(re.sub(r"<[^>]+>", " ", s))).strip()


def parse_id_date(text: str) -> Optional[str]:
    """Parse an Indonesian long date ('31 Juli 2026') to ISO 8601."""
    m = re.search(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", text or "")
    if not m:
        return None
    month = MONTHS_ID.get(m.group(2).lower())
    if not month:
        return None
    try:
        return datetime(int(m.group(3)), month, int(m.group(1))).strftime("%Y-%m-%d")
    except ValueError:
        return None


def parse_reg_number(slug: str, title: str) -> Optional[str]:
    """Recover the official regulation number from the title, else from the slug."""
    m = re.search(r"Nomor\s+([0-9]+(?:\s*/\s*[0-9A-Za-z]+)*(?:\s*Tahun\s*\d{4})?)", title or "")
    if m:
        return re.sub(r"\s+", " ", m.group(1)).strip()
    m = re.match(r"(?:PBI|PADG|SE)_(\d+)", slug, re.I)
    return m.group(1) if m else None


def reg_type_from_slug(slug: str) -> Tuple[Optional[str], Optional[str]]:
    m = re.match(r"([A-Za-z]+)_", slug)
    return REG_TYPES.get(m.group(1).lower(), (None, None)) if m else (None, None)


class BIScraper(BaseScraper):
    """
    Scraper for ID/BI -- Bank Indonesia Regulations.
    Country: ID
    URL: https://www.bi.go.id/id/publikasi/peraturan

    Data types: doctrine
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(str(source_dir))
        self.source_id = "ID/BI"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "id-ID,id;q=0.9,en;q=0.8",
        })

    # ── HTTP helpers (fail loud) ──────────────────────────────────────

    def _check(self, resp: requests.Response, what: str) -> str:
        """Assert a real HTML response, not an error page or a WAF interstitial."""
        resp.raise_for_status()
        body = resp.text
        for marker in BLOCK_MARKERS:
            if marker in body:
                raise RuntimeError(
                    f"{what}: blocked by the bi.go.id WAF (HTTP {resp.status_code} with a "
                    f"rejection interstitial) — this vantage cannot reach the source"
                )
        if len(body) < 5000:
            raise RuntimeError(f"{what}: response implausibly short ({len(body)} bytes)")
        return body

    def _get_listing(self) -> str:
        return self._check(self.session.get(LISTING_URL, timeout=60), "listing GET")

    def _post_listing(self, form: Dict[str, str], what: str) -> str:
        return self._check(self.session.post(LISTING_URL, data=form, timeout=90), what)

    @staticmethod
    def _hidden_fields(page: str) -> Dict[str, str]:
        """Collect __VIEWSTATE and friends so the postback is accepted."""
        fields: Dict[str, str] = {}
        for m in HIDDEN_RE.finditer(page):
            tag = m.group(0)
            name = re.search(r'name="([^"]+)"', tag)
            if not name:
                continue
            value = re.search(r'value="([^"]*)"', tag)
            fields[name.group(1)] = htmllib.unescape(value.group(1)) if value else ""
        return fields

    @staticmethod
    def _webpart_prefix(page: str) -> str:
        """Locate the regulation webpart's control-id prefix; raise if the page changed."""
        m = PREFIX_RE.search(page)
        if not m:
            raise RuntimeError(
                "listing page no longer contains the DataPagerPeraturan webpart — the "
                "bi.go.id regulation listing layout changed and the parser needs updating"
            )
        return m.group(1)

    # ── Listing enumeration ───────────────────────────────────────────

    @staticmethod
    def _parse_rows(page: str) -> List[Dict[str, Any]]:
        """Extract (slug, title, date, type, sector) from listing result cards."""
        rows: List[Dict[str, Any]] = []
        seen = set()
        # Cards nest three levels deep and the closing tags are ambiguous, so split on
        # the card opener and let the per-field regexes anchor within each chunk.
        for block in page.split(ROW_SPLIT)[1:]:
            slug_m = SLUG_RE.search(block)
            title_m = TITLE_RE.search(block)
            if not slug_m or not title_m:
                continue
            slug = slug_m.group(1)
            if slug in seen:
                continue
            seen.add(slug)
            subtitles = [strip_tags(s) for s in SUBTITLE_RE.findall(block)[:2]]
            jenis = sector = None
            if len(subtitles) > 1:
                parts = [p.strip() for p in subtitles[1].split("•")]
                jenis = parts[0] or None
                sector = parts[1] if len(parts) > 1 else None
            rows.append({
                "slug": slug,
                "title": strip_tags(title_m.group(1)),
                "date": parse_id_date(subtitles[0] if subtitles else ""),
                "jenis": jenis,
                "sector": sector,
            })
        return rows

    def _rows_for_year(self, year: int) -> List[Dict[str, Any]]:
        """Filter the listing to one calendar year and walk every result page."""
        base = self._get_listing()
        prefix = self._webpart_prefix(base)

        form = self._hidden_fields(base)
        form["__EVENTTARGET"] = ""
        form["__EVENTARGUMENT"] = ""
        form[f"{prefix}$TextBoxDateStart"] = f"01/01/{year}"
        form[f"{prefix}$TextBoxDateEnd"] = f"31/12/{year}"
        form[f"{prefix}$ButtonFilter"] = "Cari"
        page = self._post_listing(form, f"{year} filter POST")

        rows: List[Dict[str, Any]] = []
        seen = set()
        for page_no in range(1, MAX_PAGES_PER_YEAR + 1):
            batch = [r for r in self._parse_rows(page) if r["slug"] not in seen]
            seen.update(r["slug"] for r in batch)
            rows.extend(batch)
            # The pager renders Next as an enabled image button until the last page.
            if "aspNetDisabled next" in page or 'class="next"' not in page:
                break
            if not batch:
                break  # defensive: pager still enabled but nothing new came back
            form = self._hidden_fields(page)
            form["__EVENTTARGET"] = ""
            form["__EVENTARGUMENT"] = ""
            form[f"{prefix}$DataPagerPeraturan$ctl02$ctl00.x"] = "5"
            form[f"{prefix}$DataPagerPeraturan$ctl02$ctl00.y"] = "5"
            time.sleep(REQUEST_DELAY)
            page = self._post_listing(form, f"{year} page {page_no + 1}")

        logger.info(f"{year}: {len(rows)} regulations")
        return rows

    def _iter_rows(self, years: List[int]) -> Generator[Dict[str, Any], None, None]:
        total = 0
        for year in years:
            rows = self._rows_for_year(year)
            total += len(rows)
            for row in rows:
                row["year"] = year
                yield row
            time.sleep(REQUEST_DELAY)
        # Fail loud rather than reporting a successful empty crawl (issue #1470).
        if total == 0:
            raise RuntimeError(
                f"regulation listing yielded 0 rows across {years[-1]}-{years[0]} — "
                "bi.go.id publishes hundreds of PBI/PADG/SE regulations, so this is a "
                "block or a layout change, not an empty corpus"
            )
        logger.info(f"Discovered {total} regulations across {len(years)} years")

    # ── BaseScraper interface ─────────────────────────────────────────

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield listing rows for every year, newest first."""
        this_year = datetime.now(timezone.utc).year
        yield from self._iter_rows(list(range(this_year, FIRST_YEAR - 1, -1)))

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Yield rows from `since`'s year (default: this year) up to now."""
        this_year = datetime.now(timezone.utc).year
        start = this_year
        if since:
            try:
                start = datetime.fromisoformat(since.replace("Z", "+00:00")).year
            except ValueError:
                pass
        start = max(FIRST_YEAR, min(start, this_year))
        yield from self._iter_rows(list(range(this_year, start - 1, -1)))

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch the regulation PDF for a listing row and extract its full text."""
        slug = raw["slug"]
        url = DETAIL_URL.format(slug=slug)
        detail = self._check(self.session.get(url, timeout=60), f"detail {slug}")

        hrefs = list(dict.fromkeys(PDF_HREF_RE.findall(detail)))
        main = [h for h in hrefs if not COMPANION_RE.search(h)]
        if not main:
            logger.warning(f"{slug}: no regulation PDF on detail page, skipping")
            return None
        # Older records prefix the filename with a GUID, so prefer a filename that
        # still echoes the slug over whichever attachment happens to come first.
        stem = re.sub(r"[^a-z0-9]", "", slug.lower())[:8]
        main.sort(key=lambda h: 0 if stem and stem in re.sub(r"[^a-z0-9]", "", h.lower()) else 1)

        pdf_url = main[0] if main[0].startswith("http") else BASE + main[0]
        resp = self.session.get(pdf_url, timeout=180)
        resp.raise_for_status()
        pdf_bytes = resp.content
        if not pdf_bytes.startswith(b"%PDF-"):
            logger.warning(f"{slug}: {pdf_url} is not a PDF ({len(pdf_bytes)} bytes), skipping")
            return None

        text = extract_pdf_markdown(self.source_id, slug, pdf_bytes=pdf_bytes)
        if not text or len(text.strip()) < 200:
            logger.warning(f"{slug}: extracted only {len(text or '')} chars, skipping")
            return None

        code, type_desc = reg_type_from_slug(slug)
        title = raw.get("title") or slug

        return {
            "_id": f"ID-BI-{slug}",
            "_source": self.source_id,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": url,
            "pdf_url": pdf_url,
            "regulation_number": parse_reg_number(slug, title),
            "regulation_type": code,
            "regulation_type_desc": raw.get("jenis") or type_desc,
            "sector": raw.get("sector"),
            "year": raw.get("year"),
            "issuer": "Bank Indonesia",
            "language": "id",
        }

    def test_connection(self) -> bool:
        page = self._get_listing()
        self._webpart_prefix(page)
        rows = self._parse_rows(page)
        if not rows:
            raise RuntimeError("listing reachable but parsed 0 rows — layout changed")
        logger.info(f"Connection OK — {len(rows)} regulations on the front page")
        for r in rows[:3]:
            logger.info(f"  {r['date']} {r['slug']}: {r['title'][:80]}")
        return True


# ── CLI entrypoint ───────────────────────────────────────────────────
if __name__ == "__main__":
    scraper = BIScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode, sample_size=12)
    elif command == "bootstrap-fast":
        if sample_mode:
            scraper.bootstrap(sample_mode=True, sample_size=12)
        else:
            scraper.bootstrap_fast()
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
