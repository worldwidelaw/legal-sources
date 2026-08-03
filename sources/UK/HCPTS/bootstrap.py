#!/usr/bin/env python3
"""
UK/HCPTS -- Health and Care Professions Tribunal Service -- Hearing decisions.

The Health and Care Professions Tribunal Service (HCPTS) is the fitness to
practise adjudication service of the Health and Care Professions Council (HCPC),
the UK statutory regulator for 15 health and care professions (paramedics,
physiotherapists, dietitians, occupational therapists, radiographers, chiropodists/
podiatrists, biomedical scientists, practitioner psychologists, etc.). Its panels
sit under the Health and Social Work Professions Order 2001 and the HCPC (Conduct
and Competence Committee) (Procedure) Rules; their final-hearing determinations,
interim order decisions and review outcomes are binding fitness-to-practise case
law (allegation, the panel's reasoned findings of fact/impairment, and the order/
sanction imposed). These are NOT on Find Case Law and are distinct from the other
UK professional-regulator tribunals already covered (UK/GMC doctors, UK/SDT
solicitors, UK/BTAS barristers, UK/GC General Council, UK/GMC).

Access & structure (all public, no auth):
  - The tribunal publishes one decision page per registrant hearing at
    https://www.hcpts-uk.org/hearings/hearings/{year}/{month}/{slug}/ . The full
    catalogue of these pages is enumerable from the site's sitemap.xml (a single
    ~650 KB XML document listing every hearing URL with a <lastmod>).
  - Each decision page is clean, server-rendered HTML. A metadata block carries
    Profession / Registration Number / Hearing Type / hearing Date / Panel /
    Outcome, followed by tabbed content divs (#tab-allegation, #tab-finding,
    #tab-order, #tab-notes) holding the FULL text: the charge/allegation, the
    panel's reasoned determination (often 10k-40k chars for final hearings), and
    the order made. Born-digital HTML, no OCR.
  - Some sitemap entries point at pages whose decision is not (yet) published or
    has been removed under the Publications Policy; these render a fixed error
    page ("there has been a problem with the page") and are skipped.

Strategy:
  - GET /sitemap.xml, collect every /hearings/hearings/ URL.
  - GET each page, skip error pages, extract the metadata block + the
    allegation/finding/order/notes tab bodies, assemble the full determination
    text (dropping "No information currently available" placeholders).
  - Skip pending/not-yet-held hearings (no substantive text).

Data:
  - ~3,000+ full-text fitness-to-practise decisions (final hearings, interim
    orders, reviews). Language: English. Auth: none.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental (recently modified pages)
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UK.HCPTS")

BASE_URL = "https://www.hcpts-uk.org"
SITEMAP = "/sitemap.xml"
HEARING_PREFIX = "/hearings/hearings/"
ERROR_MARKER = "there has been a problem with the page"
PLACEHOLDER = "no information currently available"

TAG_RE = re.compile(r"<[^>]+>")
LOC_RE = re.compile(r"<loc>\s*([^<]+?)\s*</loc>", re.I)
URL_BLOCK_RE = re.compile(r"<url>(.*?)</url>", re.S | re.I)
LASTMOD_RE = re.compile(r"<lastmod>\s*([^<]+?)\s*</lastmod>", re.I)
# One tabbed content div: id="tab-NAME" ... up to the next tab div / nav / end.
TAB_RE_TMPL = (r'id="tab-{name}"[^>]*>(.*?)'
               r'(?=<div\s+id="tab-|<div[^>]*class="[^"]*tab__nav|</main|\Z)')
H1_RE = re.compile(r'<h1[^>]*class="[^"]*layout__heading[^"]*"[^>]*>(.*?)</h1>',
                   re.S | re.I)
# metadata labels in the block above the tabs
DATE_HEARING_RE = re.compile(
    r"Date and Time of hearing:\s*(?:\d{1,2}:\d{2}\s*)?(\d{1,2})/(\d{1,2})/(\d{4})",
    re.I)
DDMMYYYY_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")

_LABELS = ("Profession", "Registration Number", "Hearing Type", "Panel", "Outcome")

TAB_LABELS = [("allegation", "Allegation"), ("finding", "Finding"),
              ("order", "Order"), ("notes", "Notes")]


_ZW_RE = re.compile(r"[​‌‍﻿]")


def _strip(s: str) -> str:
    s = _ZW_RE.sub("", html.unescape(TAG_RE.sub(" ", s or "")))
    return s.replace("\xa0", " ").strip()


def _clean(text: str) -> str:
    text = html.unescape(text or "").replace("\r", "").replace("\xa0", " ")
    lines = [ln.rstrip() for ln in text.split("\n")]
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


def _block_to_text(fragment: str) -> str:
    """Strip a run of HTML to readable text with paragraph newlines."""
    h = re.sub(r"<(script|style)\b.*?</\1>", " ", fragment, flags=re.S | re.I)
    # drop the tab's own <h2> heading label
    h = re.sub(r"<h2\b.*?</h2>", " ", h, flags=re.S | re.I)
    h = re.sub(r"(?i)</(p|h1|h2|h3|h4|li|tr|div|blockquote)\s*>", "\n", h)
    h = re.sub(r"(?i)<br\s*/?>", "\n", h)
    return _clean(TAG_RE.sub("", h))


def _tab_text(page: str, name: str) -> str:
    m = re.search(TAB_RE_TMPL.format(name=name), page, re.S | re.I)
    if not m:
        return ""
    txt = _block_to_text(m.group(1))
    if not txt or txt.strip().lower().startswith(PLACEHOLDER):
        return ""
    return txt


def _meta_region(page: str) -> str:
    """Readable text of the block between the <h1> and the first tab."""
    i = page.find("layout__heading")
    j = page.find("tab-wrapper")
    if i < 0:
        return ""
    seg = page[i:j] if j > i else page[i:i + 4000]
    seg = re.sub(r"<h1\b.*?</h1>", " ", seg, flags=re.S | re.I)
    return re.sub(r"\s+", " ", _strip(seg)).strip()


def _meta_field(region: str, label: str) -> Optional[str]:
    # value runs until the next known label or end-of-region sentinel.
    others = [re.escape(l) for l in _LABELS if l != label]
    others += [r"Date and Time of hearing", r"Please note", r"Interim Order:",
               r"Location:", r"Print page"]
    stop = "|".join(others)
    m = re.search(re.escape(label) + r":\s*(.*?)(?=\s*(?:" + stop + r")\s*:?|$)",
                  region, re.I)
    if not m:
        return None
    v = m.group(1).strip(" .;:-")
    return v or None


def _hearing_date(region: str) -> Optional[str]:
    m = DATE_HEARING_RE.search(region)
    if not m:
        m = DDMMYYYY_RE.search(region)
        if not m:
            return None
    day, mon, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return f"{year:04d}-{mon:02d}-{day:02d}"
    except (ValueError, TypeError):
        return None


def _slug_id(url: str) -> str:
    path = url.split("hcpts-uk.org", 1)[-1]
    parts = [p for p in path.strip("/").split("/") if p]
    # drop the leading "hearings/hearings"
    if parts[:2] == ["hearings", "hearings"]:
        parts = parts[2:]
    return "-".join(parts)


class HCPTSScraper(BaseScraper):
    """Scraper for HCPTS fitness-to-practise hearing decisions (HTML)."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
            },
            timeout=60,
        )

    # -- HTTP ------------------------------------------------------------
    def _get(self, url: str) -> Optional[str]:
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
        except Exception as e:
            logger.warning(f"GET {url} failed: {e}")
            return None
        if resp.status_code != 200:
            logger.debug(f"GET {url}: HTTP {resp.status_code}")
            return None
        return resp.content.decode("utf-8", "replace")

    # -- enumeration -----------------------------------------------------
    def _sitemap_entries(self) -> List[Dict[str, Any]]:
        xml = self._get(BASE_URL + SITEMAP)
        if not xml:
            return []
        entries: List[Dict[str, Any]] = []
        seen = set()
        for block in URL_BLOCK_RE.finditer(xml):
            b = block.group(1)
            lm = LOC_RE.search(b)
            if not lm:
                continue
            loc = html.unescape(lm.group(1).strip())
            if HEARING_PREFIX not in loc or loc in seen:
                continue
            seen.add(loc)
            mm = LASTMOD_RE.search(b)
            entries.append({"url": loc, "lastmod": mm.group(1) if mm else None})
        return entries

    def _build_raw(self, entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        url = entry["url"]
        page = self._get(url)
        if not page or ERROR_MARKER in page.lower():
            return None
        region = _meta_region(page)
        sections = []
        for key, label in TAB_LABELS:
            body = _tab_text(page, key)
            if body:
                sections.append(f"{label}:\n{body}")
        text = "\n\n".join(sections).strip()
        if len(text) < 200:
            # pending / not-yet-held / thin holding note -- no real determination
            return None
        h1 = H1_RE.search(page)
        name = _strip(h1.group(1)) if h1 else _slug_id(url)
        return {
            "url": url,
            "lastmod": entry.get("lastmod"),
            "name": name,
            "text": text,
            "profession": _meta_field(region, "Profession"),
            "registration_number": _meta_field(region, "Registration Number"),
            "hearing_type": _meta_field(region, "Hearing Type"),
            "panel": _meta_field(region, "Panel"),
            "outcome": _meta_field(region, "Outcome"),
            "date": _hearing_date(region),
        }

    # -- core ------------------------------------------------------------
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        entries = self._sitemap_entries()
        if not entries:
            raise RuntimeError(
                "HCPTS sitemap returned 0 hearing URLs — site blocked or "
                "sitemap layout changed")
        produced = 0
        for entry in entries:
            raw = self._build_raw(entry)
            if raw:
                produced += 1
                yield raw
        if produced == 0:
            raise RuntimeError(
                "HCPTS enumerated hearing pages but extracted 0 decisions — "
                "page layout changed or all pages unpublished")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        since_date = since.date()
        for entry in self._sitemap_entries():
            lm = entry.get("lastmod")
            if lm:
                try:
                    d = datetime.fromisoformat(lm.replace("Z", "+00:00")).date()
                    if d < since_date:
                        continue
                except ValueError:
                    pass
            raw = self._build_raw(entry)
            if raw:
                yield raw

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = _clean(raw.get("text", "") or "")
        if len(text) < 200:
            return None
        name = raw.get("name") or "HCPTS registrant"
        htype = raw.get("hearing_type")
        prof = raw.get("profession")
        title_bits = [name]
        if prof:
            title_bits.append(f"({prof})")
        if htype:
            title_bits.append(f"— {htype}")
        title = " ".join(title_bits).strip()
        return {
            "_id": f"UK-HCPTS-{_slug_id(raw['url'])}",
            "_source": "UK/HCPTS",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw["url"],
            "registrant": name,
            "profession": prof,
            "registration_number": raw.get("registration_number"),
            "hearing_type": htype,
            "panel": raw.get("panel"),
            "outcome": raw.get("outcome"),
            "court": "Health and Care Professions Tribunal Service",
            "jurisdiction": "GB",
            "language": "en",
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing HCPTS sitemap enumeration...")
        entries = self._sitemap_entries()
        print(f"  {len(entries)} hearing URLs in sitemap")
        got = 0
        for entry in entries:
            raw = self._build_raw(entry)
            if raw:
                got += 1
                print(f"  {raw['name']} [{raw.get('hearing_type')}] "
                      f"{raw.get('date')}: {len(raw['text'])} chars - OK")
            if got >= 3:
                break


def main():
    scraper = HCPTSScraper()
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
