#!/usr/bin/env python3
"""
UK/FRC -- Financial Reporting Council -- Enforcement & Tribunal Decisions.

The Financial Reporting Council (FRC) is the UK regulator responsible for
enforcement action against statutory auditors, audit firms, accountants and
actuaries. Cases are pursued under the Audit Enforcement Procedure (AEP), the
Accountancy Scheme and the Actuarial Scheme, and conclude in a published
decision document — a Disciplinary Tribunal Report, an Executive Counsel Final
Decision Notice, or a Final Settlement Decision Notice — setting out the facts,
the misconduct/breaches found, the sanctions (fines, exclusion, reprimand,
non-audit undertakings) and the reasons. These are binding enforcement
adjudications = case law, distinct from the sectoral regulators' own notices.

Access & structure (all public, no auth):
  - frc.org.uk is a server-rendered CMS. Enforcement outcomes are indexed on:
        /library/enforcement/enforcement-cases/            (all cases)
        /library/enforcement/accountancy-scheme/
        /library/enforcement/actuarial-scheme/
        /library/enforcement/audit-enforcement-procedure/
    Each index links per-case outcome pages under /news-and-events/news/{yr}/{mo}/{slug}/
    and, in many cases, the decision PDF directly.
  - Each outcome news page carries the case title, a "Published: {date}" line and
    a link to the decision document served as a born-digital PDF at
        /documents/{id}/{name}.pdf
    (real text layer, no OCR). The stable document id is used as the record id.

Strategy:
  - Fetch the four index pages; collect (a) per-case outcome news links and
    (b) direct /documents/*.pdf decision links.
  - For each news page: extract its decision PDF + title + published date and
    yield the PDF full text (fallback to the news-page text if no PDF).
  - Also yield any direct decision PDF whose document id was not already seen.
  - De-duplicate on the /documents/{id}/ document id.

Data:
  - Full-text FRC enforcement / tribunal decisions. Language: English. Auth: none.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental (recent published first)
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
logger = logging.getLogger("legal-data-hunter.UK.FRC")

SITE_BASE = "https://www.frc.org.uk"
INDEX_PAGES = [
    "/library/enforcement/enforcement-cases/",
    "/library/enforcement/accountancy-scheme/",
    "/library/enforcement/actuarial-scheme/",
    "/library/enforcement/audit-enforcement-procedure/",
]

_WS_RE = re.compile(r"[ \t]+")
_MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}
_NEWS_RE = re.compile(r'href="(/news-and-events/news/\d{4}/\d{2}/[^"#?]+)"', re.I)
_DOC_RE = re.compile(r'href="(/documents/\d+/[^"#?]+\.pdf)"', re.I)
_DOCID_RE = re.compile(r"/documents/(\d+)/", re.I)


def _pdf_text(pdf_bytes: bytes) -> str:
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


def _title_of(page_html: str) -> Optional[str]:
    m = re.search(r"<title>(.*?)</title>", page_html, re.S | re.I)
    if not m:
        return None
    t = html.unescape(_WS_RE.sub(" ", m.group(1))).strip()
    # strip trailing site name (" | FRC", " - FRC")
    t = re.split(r"\s*[|–\-]\s*(?:FRC|Financial Reporting Council)\b", t)[0]
    return t.strip() or None


def _published_date(page_html: str) -> Optional[str]:
    m = re.search(r'datetime="(\d{4}-\d{2}-\d{2})', page_html)
    if m:
        return m.group(1)
    m = re.search(r"Published\s*:?\s*(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})",
                  _strip_tags(page_html), re.I)
    if m and m.group(2).lower() in _MONTHS:
        return (f"{int(m.group(3)):04d}-{_MONTHS[m.group(2).lower()]:02d}-"
                f"{int(m.group(1)):02d}")
    return None


def _doc_id(url: str) -> Optional[str]:
    m = _DOCID_RE.search(url)
    return m.group(1) if m else None


def _slug_title(url: str) -> str:
    slug = url.rstrip("/").rsplit("/", 1)[-1]
    slug = re.sub(r"\.pdf$", "", slug, flags=re.I)
    return _WS_RE.sub(" ", slug.replace("_", " ").replace("-", " ")).strip()


class FRCScraper(BaseScraper):
    """Scraper for Financial Reporting Council enforcement/tribunal decisions."""

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

    def _get_pdf(self, url: str) -> Optional[bytes]:
        body = self._get(url)
        if not body:
            return None
        if not body[:5].startswith(b"%PDF"):
            return None
        return body

    # -- discovery -------------------------------------------------------
    def _collect_seeds(self):
        """Return (news_links, direct_docs) discovered on the index pages."""
        news: List[str] = []
        docs: List[str] = []
        reachable = False
        seen_n, seen_d = set(), set()
        for path in INDEX_PAGES:
            body = self._get(SITE_BASE + path)
            if not body:
                continue
            reachable = True
            h = body.decode("utf-8", errors="replace")
            for href in _NEWS_RE.findall(h):
                u = urljoin(SITE_BASE, href)
                if u not in seen_n:
                    seen_n.add(u)
                    news.append(u)
            for href in _DOC_RE.findall(h):
                u = urljoin(SITE_BASE, href)
                if u not in seen_d:
                    seen_d.add(u)
                    docs.append(u)
        if not reachable:
            raise RuntimeError(
                "FRC enforcement index pages unreachable — frc.org.uk blocked "
                "or URLs changed")
        logger.info(f"FRC seeds: {len(news)} case pages, {len(docs)} direct docs")
        return news, docs

    def _raw_from_news(self, url: str, seen_docs: set) -> Optional[Dict[str, Any]]:
        body = self._get(url)
        if not body:
            return None
        h = body.decode("utf-8", errors="replace")
        pdfs = _DOC_RE.findall(h)
        title = _title_of(h)
        date = _published_date(h)
        if pdfs:
            doc_url = urljoin(SITE_BASE, pdfs[0])
            did = _doc_id(doc_url)
            if did:
                seen_docs.add(did)
            pdf = self._get_pdf(doc_url)
            if pdf:
                text = _clean(_pdf_text(pdf))
                if len(text) >= 200:
                    return {
                        "doc_id": did or url,
                        "url": doc_url,
                        "page_url": url,
                        "title": title or _slug_title(url),
                        "date": date,
                        "text": text,
                        "kind": "decision_pdf",
                    }
        # No decision PDF on this page → skip. FRC outcome pages that carry a
        # published decision always link the /documents/*.pdf; text-only pages
        # are investigation-opened announcements, not decisions, so we exclude
        # them to keep the corpus clean case law.
        return None

    def _raw_from_doc(self, url: str) -> Optional[Dict[str, Any]]:
        pdf = self._get_pdf(url)
        if not pdf:
            return None
        text = _clean(_pdf_text(pdf))
        if len(text) < 200:
            return None
        did = _doc_id(url)
        # try to find a date inside the PDF header
        date = None
        dm = re.search(r"(\d{1,2})\s+([A-Za-z]+)\s+(20\d{2})", text[:1200])
        if dm and dm.group(2).lower() in _MONTHS:
            date = (f"{int(dm.group(3)):04d}-{_MONTHS[dm.group(2).lower()]:02d}-"
                    f"{int(dm.group(1)):02d}")
        return {
            "doc_id": did or url,
            "url": url,
            "page_url": None,
            "title": _slug_title(url),
            "date": date,
            "text": text,
            "kind": "decision_pdf",
        }

    # -- core ------------------------------------------------------------
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        news, docs = self._collect_seeds()
        seen_docs: set = set()
        seen_ids: set = set()
        produced = 0
        for url in news:
            raw = self._raw_from_news(url, seen_docs)
            if raw and raw["doc_id"] not in seen_ids:
                seen_ids.add(raw["doc_id"])
                produced += 1
                yield raw
        for url in docs:
            did = _doc_id(url)
            if did and did in seen_docs:
                continue
            raw = self._raw_from_doc(url)
            if raw and raw["doc_id"] not in seen_ids:
                seen_ids.add(raw["doc_id"])
                produced += 1
                yield raw
        if produced == 0:
            raise RuntimeError(
                "FRC listed enforcement cases but extracted 0 decisions — the "
                "outcome-page / document PDF scheme changed")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        since_date = since.date()
        for raw in self.fetch_all():
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
        if len(text) < 200:
            return None
        title = raw.get("title") or "FRC enforcement decision"
        return {
            "_id": f"UK-FRC-{raw['doc_id']}",
            "_source": "UK/FRC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url"),
            "source_page": raw.get("page_url"),
            "document_kind": raw.get("kind"),
            "court": "Financial Reporting Council — Enforcement",
            "jurisdiction": "GB",
            "language": "en",
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing FRC enforcement index...")
        news, docs = self._collect_seeds()
        print(f"  {len(news)} case pages, {len(docs)} direct docs")
        got = 0
        seen: set = set()
        for url in news:
            raw = self._raw_from_news(url, seen)
            if raw:
                got += 1
                print(f"  {raw['title'][:60]} | {raw.get('date')} | "
                      f"{len(raw['text'])} chars - OK")
            if got >= 3:
                break
        if got == 0:
            print("  No decisions extracted — check PDF access")


def main():
    scraper = FRCScraper()
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
