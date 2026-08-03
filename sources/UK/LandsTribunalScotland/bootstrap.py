#!/usr/bin/env python3
"""
UK/LandsTribunalScotland -- The Lands Tribunal for Scotland -- Decisions.

The Lands Tribunal for Scotland is the specialist Scottish tribunal that
determines disputes about land and property under the Lands Tribunal Act 1949 and
later statutes: discharge/variation of title conditions (real burdens), disputed
compensation on compulsory purchase, valuation-for-rating appeals, tenants'
right-to-buy references, Land Register appeals, and electronic-communications-code
(telecoms) references. Its written Opinions/Notes are adjudicative case law for
the GB-SCT (Scotland) jurisdiction, NOT covered by UK/CaseLaw (England & Wales
superior courts + reserved UK tribunals, indexed via the National Archives Find
Case Law service; the Lands Tribunal for Scotland is not on that service).

Site: http://www.lands-tribunal-scotland.org.uk (server-rendered HTML). Decisions
are browsed by subject category:

    /decisions/previous-decisions          -> lists the subject categories
    /decisions/{category}                  -> lists per-decision slugs
    /decisions/LTS.{CAT}.{YYYY}.{NN}       -> decision page: <header> with the
        neutral citation ("[YYYY] LTS N"), case reference ("LTS/{CAT}/{YYYY}/
        {NNNN}") and tribunal members, followed by the full Opinion/Note text
        inline in <main>. Born-digital HTML -- no PDF, no OCR.

Strategy:
  - Read the previous-decisions index to enumerate subject categories.
  - Page each category to collect per-decision slugs (deduped across categories).
  - On each decision page, strip <script>/<style> and tags to recover the full
    text, and parse citation / case reference / tribunal members / title / date.
  - One record per decision. Decision date is the first "<day> <Month> <year>" in
    the body (Lands Tribunal opinions carry no dates in the party/counsel header),
    falling back to the neutral-citation year.

Data:
  - ~460 full-text decisions
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UK.LandsTribunalScotland")

BASE_URL = "http://www.lands-tribunal-scotland.org.uk"
INDEX_PATH = "/decisions/previous-decisions"

TAG_RE = re.compile(r"<[^>]+>")
SCRIPT_STYLE_RE = re.compile(r"<(script|style)[^>]*>.*?</\1>", re.I | re.S)
BODY_RE = re.compile(r"<body[^>]*>(.*?)</body>", re.I | re.S)
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)
# Subject-category links on the previous-decisions index.
CATEGORY_RE = re.compile(r'href="(/decisions/[a-z][a-z0-9-]+)"', re.I)
# Per-decision slug links, e.g. /decisions/LTS.TC.2024.04 (optional .a suffix).
DECISION_RE = re.compile(r'href="(/decisions/LTS\.[A-Z]+\.\d{4}\.\d+[a-z.]*)"', re.I)

CITATION_RE = re.compile(r"Citation:\s*(\[\d{4}\]\s*LTS\s*\d+)", re.I)
# Modern header ("Case reference:") and older footer ("Case Ref:") forms.
CASEREF_RE = re.compile(r"Case ref(?:erence)?:\s*(LTS/[A-Z]+/\d{4}/\d+)", re.I)
CITYEAR_RE = re.compile(r"\[(\d{4})\]", re.I)
# Modern header: "Tribunal members: ...". Older footer: "Members sitting: ...".
MEMBERS_RE = re.compile(
    r"Tribunal members?:\s*(.*?)\s+(?:in the (?:application|appeal|reference|"
    r"matter|note)|OPINION|NOTE\b|Applicant|Appellant|Introduction)", re.I | re.S)
MEMBERS_FOOT_RE = re.compile(r"Members(?: sitting)?:\s*([^\n]+)", re.I)

_MONTH_NUM = {m: i for i, m in enumerate(
    ("january february march april may june july august september october "
     "november december").split(), start=1)}
_DATE_CORE = (r"(\d{1,2})(?:st|nd|rd|th)?\s+(January|February|March|April|May|"
              r"June|July|August|September|October|November|December)\s+(\d{4})")
DATE_RE = re.compile(r"\b" + _DATE_CORE + r"\b", re.I)
# Labelled decision-date forms, in order of preference.
DECISION_DATE_RES = [
    re.compile(r"Decision issued:\s*" + _DATE_CORE, re.I),
    re.compile(r"intimated to parties on\s*" + _DATE_CORE, re.I),
    re.compile(_DATE_CORE + r"\s+Introduction\b", re.I),
]

# Non-decision links that appear on category pages (nav, other categories).
_CATEGORY_SLUGS = {
    "disputed-compensation", "valuation-for-rating", "tenants-rights-to-buy",
    "discharge-of-land-obligations", "land-register-appeals", "title-conditions",
    "ecc", "others", "previous-decisions",
}


def _strip_all(fragment: str) -> str:
    """Strip scripts/styles and all tags from an HTML fragment -> plain text."""
    frag = SCRIPT_STYLE_RE.sub(" ", fragment or "")
    text = TAG_RE.sub(" ", frag)
    return html.unescape(text)


def _clean(text: str) -> str:
    lines = [ln.strip() for ln in (text or "").replace("\r", "").split("\n")]
    out, blanks = [], 0
    for ln in lines:
        if ln:
            blanks = 0
            out.append(ln)
        else:
            blanks += 1
            if blanks <= 1:
                out.append("")
    collapsed = "\n".join(out).strip()
    # Collapse runs of spaces/tabs left by tag removal, keep newlines.
    collapsed = re.sub(r"[ \t]{2,}", " ", collapsed)
    return collapsed


def _fmt_date(day: str, mon: str, year: str) -> Optional[str]:
    month = _MONTH_NUM.get(mon.lower())
    if not month:
        return None
    try:
        return f"{int(year):04d}-{month:02d}-{int(day):02d}"
    except (ValueError, TypeError):
        return None


def _parse_date(text: str) -> Optional[str]:
    """Decision date: prefer the labelled 'Decision issued:' / 'intimated to
    parties on' footer date (older template) or the '<date> Introduction' header
    date (modern template); only fall back to the first date in the body, which
    on older opinions can be a cited vesting/valuation date rather than the
    decision date."""
    for rx in DECISION_DATE_RES:
        m = rx.search(text or "")
        if m:
            d = _fmt_date(*m.groups())
            if d:
                return d
    m = DATE_RE.search(text or "")
    if not m:
        return None
    return _fmt_date(*m.groups())


def _slug_from_path(path: str) -> str:
    return path.rstrip("/").rsplit("/", 1)[-1]


class LandsTribunalScotlandScraper(BaseScraper):
    """Scraper for The Lands Tribunal for Scotland decisions (server-rendered
    category listings + inline-HTML full-text opinions)."""

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
            timeout=90,
        )
        self._seen: set = set()

    # -- HTTP ------------------------------------------------------------
    def _get_html(self, path_or_url: str) -> Optional[str]:
        url = path_or_url if path_or_url.startswith("http") else urljoin(BASE_URL, path_or_url)
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

    # -- discovery -------------------------------------------------------
    def _categories(self) -> List[str]:
        index = self._get_html(INDEX_PATH)
        cats: List[str] = []
        if index:
            for c in CATEGORY_RE.findall(index):
                slug = _slug_from_path(c)
                if slug in _CATEGORY_SLUGS and slug != "previous-decisions" \
                        and c not in cats:
                    cats.append(c)
        # Fall back to the known category set if the index layout changes.
        if not cats:
            cats = [f"/decisions/{s}" for s in _CATEGORY_SLUGS
                    if s != "previous-decisions"]
        return cats

    def _decision_slugs(self, category_path: str) -> List[str]:
        page = self._get_html(category_path)
        if not page:
            return []
        slugs: List[str] = []
        for s in DECISION_RE.findall(page):
            if s not in slugs:
                slugs.append(s)
        return slugs

    # -- detail ----------------------------------------------------------
    def _build_raw(self, slug_path: str, category: str) -> Optional[Dict[str, Any]]:
        detail = self._get_html(slug_path)
        if not detail:
            return None
        bm = BODY_RE.search(detail)
        body_html = bm.group(1) if bm else detail
        text = _clean(_strip_all(body_html))
        if len(text) < 200:
            return None
        tm = TITLE_RE.search(detail)
        title = _clean(_strip_all(tm.group(1))) if tm else ""
        cm = CITATION_RE.search(text)
        citation = re.sub(r"\s+", " ", cm.group(1)).strip() if cm else ""
        rm = CASEREF_RE.search(text)
        caseref = rm.group(1).strip() if rm else ""
        mm = MEMBERS_RE.search(text) or MEMBERS_FOOT_RE.search(text)
        members = re.sub(r"\s+", " ", mm.group(1)).strip() if mm else ""
        return {
            "slug": _slug_from_path(slug_path),
            "detail_path": slug_path,
            "category": category,
            "title": title,
            "citation": citation,
            "case_ref": caseref,
            "tribunal_members": members,
            "text": text,
        }

    # -- core ------------------------------------------------------------
    def _iter_all(self, limit: Optional[int] = None) -> Generator[Dict[str, Any], None, None]:
        produced = 0
        for category_path in self._categories():
            category = _slug_from_path(category_path)
            for slug in self._decision_slugs(category_path):
                if slug in self._seen:
                    continue
                self._seen.add(slug)
                raw = self._build_raw(slug, category)
                if raw:
                    produced += 1
                    yield raw
                    if limit and produced >= limit:
                        return

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        produced = 0
        for raw in self._iter_all():
            produced += 1
            yield raw
        if produced == 0:
            raise RuntimeError(
                "Lands Tribunal for Scotland listings returned 0 decisions — site "
                "blocked or layout changed"
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Incremental: the corpus is small; walk everything and emit decisions
        whose parsed date is on/after `since`."""
        cutoff = since.strftime("%Y-%m-%d") if since else None
        for raw in self._iter_all():
            date = _parse_date(raw.get("text", ""))
            if cutoff and date and date < cutoff:
                continue
            yield raw

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = raw.get("text", "") or ""
        if len(text) < 200:
            return None
        slug = raw.get("slug", "")
        citation = raw.get("citation", "")
        caseref = raw.get("case_ref", "") or slug
        title = raw.get("title", "").strip()
        if not title:
            title = citation or caseref
        date = _parse_date(text)
        if not date and citation:
            ym = CITYEAR_RE.search(citation)
            if ym:
                date = f"{ym.group(1)}-01-01"
        return {
            "_id": f"UK-LandsTribunalScotland-{slug}",
            "_source": "UK/LandsTribunalScotland",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": urljoin(BASE_URL, raw.get("detail_path", "")),
            "case_ref": caseref,
            "citation": citation,
            "category": raw.get("category", ""),
            "tribunal_members": raw.get("tribunal_members", ""),
            "court": "Lands Tribunal for Scotland",
            "jurisdiction": "GB-SCT",
            "language": "en",
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing Lands Tribunal for Scotland listings...")
        cats = self._categories()
        print(f"  {len(cats)} subject categories")
        for category_path in cats:
            slugs = self._decision_slugs(category_path)
            print(f"    {_slug_from_path(category_path)}: {len(slugs)} decisions")
            if slugs:
                raw = self._build_raw(slugs[0], _slug_from_path(category_path))
                if raw:
                    rec = self.normalize(raw)
                    print(f"      {rec['case_ref']} / {rec['citation']} "
                          f"({rec['date']}): {len(rec['text'])} chars - OK")
                    return


def main():
    scraper = LandsTribunalScotlandScraper()
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
