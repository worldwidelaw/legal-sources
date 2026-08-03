#!/usr/bin/env python3
"""
UK/JudiciaryNI -- Judiciary of Northern Ireland -- Judicial Decisions.

judiciaryni.uk is the official decisions database of the Judiciary of Northern
Ireland (Lady Chief Justice's Office / Northern Ireland Courts and Tribunals
Service). It publishes the full text of judgments of the Northern Ireland
superior courts (Court of Appeal, High Court -- King's/Queen's Bench, Chancery,
Family divisions, and the Crown Court) together with the reserved NI tribunals
(Lands Tribunal, NI Valuation Tribunal, Charity Tribunal, Care Tribunal, NI
Health & Safety Tribunal). These are binding adjudicative decisions for the
GB-NIR jurisdiction, which is NOT covered by UK/CaseLaw / UK/FindCaseLaw (England
& Wales + reserved UK-wide tribunals) nor by our Scotland sources.

Each decision is a server-rendered Drupal listing card that links to a detail
page carrying a single born-digital decision PDF under /files/judiciaryni/.

    Category (type slug)                             approx. decisions
    ------------------------------------------------------------------
    Judgments (courts)               judgments-118              ~6,600
    Summary judgments                summary-judgment-114         ~310
    Lands Tribunal                   lands-tribunal-decisions-109 ~300
    NI Valuation Tribunal            ni-valuation-tribunal-...-112 ~370
    Charity Tribunal                 charity-tribunal-...-117     ~106
    Care Tribunal                    care-tribunal-...-106         ~43
    NI Health & Safety Tribunal      ni-health-and-safety-...-116    ~2
    ------------------------------------------------------------------
    Total                                                       ~7,700 (2001-present)

Strategy:
  - For each category "type" listing, page ?page=N (Drupal 0-indexed) and parse
    each <article class="search-result decisions"> card for the detail-page
    slug, title, neutral citation, decision date (<time datetime>) and judge.
  - Fetch the detail page, extract the /files/judiciaryni/*.pdf link(s).
  - Download each born-digital PDF and extract full text with PyMuPDF (no OCR
    needed; shared pdfplumber/pypdf helper as fallback).
  - Dedup by detail slug (a decision can appear under both "judgments" and a
    court/tribunal category listing).

Data:
  - ~7,700 full-text decisions, 2001-present
  - Language: English
  - Auth: None (free public access)
  - License: Open Government Licence v3.0 (Crown copyright) -- commercial OK

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records for validation
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
from urllib.parse import urljoin, unquote

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
logger = logging.getLogger("legal-data-hunter.UK.JudiciaryNI")

BASE_URL = "https://www.judiciaryni.uk"

# Decision "type" listings (Drupal Views). Each is a paginated card listing at
# /judicial-decisions/type/{slug}?page=N.
CATEGORIES: Dict[str, str] = {
    "judgments-118": "Judgments",
    "summary-judgment-114": "Summary judgments",
    "lands-tribunal-decisions-109": "Lands Tribunal for Northern Ireland",
    "ni-valuation-tribunal-decisions-112": "Northern Ireland Valuation Tribunal",
    "charity-tribunal-decisions-117": "Charity Tribunal for Northern Ireland",
    "care-tribunal-decisions-106": "Care Tribunal for Northern Ireland",
    "ni-health-and-safety-tribunal-116": "Northern Ireland Health and Safety Tribunal",
}

MAX_PAGES = 1000            # safety ceiling per category (each holds 20 cards)

ARTICLE_RE = re.compile(
    r'<article class="search-result decisions">(.*?)</article>', re.S | re.I)
DETAIL_RE = re.compile(
    r'<h3>\s*<a\s+href="(/judicial-decisions/[^"]+)"[^>]*>(.*?)</a>', re.S | re.I)
TIME_RE = re.compile(r'<time[^>]*datetime="([^"]+)"', re.I)
CITATION_RE = re.compile(r'neutral-citation">([^<]*)</span>', re.I)
# The trailing <span> after the citation is the judge / court label.
JUDGE_RE = re.compile(
    r'neutral-citation">[^<]*</span>\s*<span>([^<]*)</span>', re.I)
PDF_RE = re.compile(r'href="(/files/judiciaryni/[^"]+?\.pdf[^"]*)"', re.I)
TAG_RE = re.compile(r"<[^>]+>")


def _strip(s: str) -> str:
    return html.unescape(TAG_RE.sub("", s or "")).strip()


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


def _slug_from_path(path: str) -> str:
    return unquote(path.rstrip("/").rsplit("/", 1)[-1])


class JudiciaryNIScraper(BaseScraper):
    """Scraper for the Judiciary of Northern Ireland judicial-decisions database
    (server-rendered Drupal card listings + born-digital PDFs)."""

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

    # -- HTTP helpers ----------------------------------------------------
    def _get_html(self, url: str) -> Optional[str]:
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

    # -- listing parsing -------------------------------------------------
    def _parse_cards(self, page_html: str, type_slug: str) -> List[Dict[str, Any]]:
        cards: List[Dict[str, Any]] = []
        for am in ARTICLE_RE.finditer(page_html):
            block = am.group(1)
            dm = DETAIL_RE.search(block)
            if not dm:
                continue
            detail_path = dm.group(1)
            title = _strip(dm.group(2))
            tm = TIME_RE.search(block)
            date = tm.group(1)[:10] if tm else None
            cm = CITATION_RE.search(block)
            citation = _strip(cm.group(1)) if cm else None
            jm = JUDGE_RE.search(block)
            judge = _strip(jm.group(1)) if jm else None
            cards.append({
                "type_slug": type_slug,
                "detail_path": detail_path,
                "slug": _slug_from_path(detail_path),
                "title": title,
                "date": date,
                "citation": citation,
                "judge": judge,
            })
        return cards

    def _build_raw(self, card: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        detail_html = self._get_html(urljoin(BASE_URL, card["detail_path"]))
        if not detail_html:
            return None
        pdf_urls: List[str] = []
        for p in PDF_RE.findall(detail_html):
            if p not in pdf_urls:
                pdf_urls.append(p)
        if not pdf_urls:
            return None
        texts: List[str] = []
        for rel in pdf_urls:
            pdf = self._fetch_pdf(urljoin(BASE_URL, rel))
            if not pdf:
                continue
            try:
                t = _pdf_text(pdf)
            except Exception as e:
                logger.debug(f"extract {rel}: {e}")
                t = ""
            if t:
                texts.append(t)
        text = "\n\n----\n\n".join(texts).strip()
        if not text:
            return None
        raw = dict(card)
        raw["pdf_urls"] = pdf_urls
        raw["text"] = text
        return raw

    # -- core ------------------------------------------------------------
    def _iter_category(self, type_slug: str, label: str,
                       limit: Optional[int] = None,
                       since_date=None) -> Generator[Dict[str, Any], None, None]:
        produced = 0
        page = 0
        while page < MAX_PAGES:
            url = f"{BASE_URL}/judicial-decisions/type/{type_slug}?page={page}"
            page_html = self._get_html(url)
            if page_html is None:
                break
            cards = self._parse_cards(page_html, type_slug)
            if not cards:
                break
            older_hit = False
            for card in cards:
                if since_date is not None:
                    d = card.get("date")
                    if d:
                        try:
                            if datetime.strptime(d, "%Y-%m-%d").date() < since_date:
                                older_hit = True
                                continue
                        except ValueError:
                            pass
                if card["slug"] in self._seen:
                    continue
                self._seen.add(card["slug"])
                raw = self._build_raw(card)
                if raw:
                    produced += 1
                    yield raw
                    if limit and produced >= limit:
                        return
            if since_date is not None and older_hit:
                break
            page += 1

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        produced = 0
        for type_slug, label in CATEGORIES.items():
            for raw in self._iter_category(type_slug, label):
                produced += 1
                yield raw
        if produced == 0:
            raise RuntimeError(
                "Judiciary NI listings returned 0 decisions — site blocked, "
                "layout changed, or all PDFs unreadable"
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        since_date = since.date()
        self._seen = set()
        for type_slug, label in CATEGORIES.items():
            for raw in self._iter_category(type_slug, label, since_date=since_date):
                yield raw

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = _clean(raw.get("text", "") or "")
        if len(text) < 200:
            return None
        slug = raw.get("slug", "")
        citation = raw.get("citation") or ""
        title = raw.get("title") or slug
        label = CATEGORIES.get(raw.get("type_slug", ""), "")
        full_title = f"{citation} {title}".strip() if citation else title
        return {
            "_id": f"UK-JudiciaryNI-{slug}",
            "_source": "UK/JudiciaryNI",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": full_title,
            "text": text,
            "date": raw.get("date"),
            "url": urljoin(BASE_URL, raw.get("detail_path", "")),
            "citation": citation or None,
            "judge": raw.get("judge") or None,
            "category": label,
            "court": "Judiciary of Northern Ireland",
            "jurisdiction": "GB-NIR",
            "language": "en",
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing Judiciary NI listings...")
        type_slug, label = next(iter(CATEGORIES.items()))
        page_html = self._get_html(
            f"{BASE_URL}/judicial-decisions/type/{type_slug}?page=0")
        print(f"  {label}: {'OK' if page_html else 'FAILED'}")
        if page_html:
            cards = self._parse_cards(page_html, type_slug)
            print(f"  parsed {len(cards)} cards on page 0")
            if cards:
                raw = self._build_raw(cards[0])
                if raw:
                    print(f"  first decision {cards[0].get('citation') or cards[0]['slug']}: "
                          f"{len(raw['text'])} chars extracted - OK")


def main():
    scraper = JudiciaryNIScraper()
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
