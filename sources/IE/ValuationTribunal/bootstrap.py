#!/usr/bin/env python3
"""
IE/ValuationTribunal -- An Binse Luachála / Valuation Tribunal of Ireland (Judgments)

The Valuation Tribunal is Ireland's independent statutory body (Valuation Acts
2001-2015) that hears and determines appeals against valuations of commercial
and industrial property fixed by Tailte Éireann (formerly the Valuation Office)
for local-authority commercial rates, as well as global valuations, revision,
revaluation, vacant-site and derelict-site appeals. Each written determination
finally adjudicates a specific contested valuation appeal = case_law, and is a
public government-edict work (Irish PSI / CC BY 4.0).

Strategy:
  - Enumerate all judgment posts via the WordPress REST API:
    GET /wp-json/wp/v2/posts?per_page=100&page=N  (~3,000 posts, 2014-present)
    (the admin-ajax.php filter is AWS-WAF/CAPTCHA-gated, but the read-only
    REST API answers plain GETs.)
  - Each post's content links a born-digital "Final Determination" PDF under
    /wp-content/uploads/YYYY/MM/...pdf
  - Extract full text with PyMuPDF (fitz); parse the appeal number from the post
    title and the determination date from the PDF body.

Data:
  - ~3,000 determinations, 2014-present (plus earlier where digitised)
  - Language: English (bilingual EN/GA headers)
  - Auth: None (free public access)

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental (recent posts)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
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
except ImportError:
    fitz = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IE.ValuationTribunal")

BASE_URL = "https://www.valuationtribunal.ie"
POSTS_PATH = "/wp-json/wp/v2/posts"
PER_PAGE = 100

PDF_RE = re.compile(r'href="([^"]*/wp-content/uploads/[^"]*\.pdf)"', re.IGNORECASE)
# Appeal numbers look like VA23.5.0792 / VA14.5.467 / VA16.1.013 (dots or slashes).
APPEAL_RE = re.compile(r"\bVA\s*\d{2}[./]\d{1,2}[./]\d{1,4}[A-Z]?\b", re.IGNORECASE)
# "ISSUED ON THE 30TH DAY OF OCTOBER 2026"
ISSUED_RE = re.compile(
    r"ISSUED ON THE\s+(\d{1,2})(?:ST|ND|RD|TH)?\s+DAY OF\s+([A-Z]+)\s+(\d{4})",
    re.IGNORECASE,
)

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}


def _pdf_text(pdf_bytes: bytes) -> str:
    if not fitz:
        raise RuntimeError("PyMuPDF (fitz) is required to extract Valuation Tribunal PDFs")
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        return "\n".join(page.get_text() for page in doc)
    finally:
        doc.close()


def _clean(text: str) -> str:
    lines = [ln.rstrip() for ln in text.split("\n")]
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


def _issued_date(text: str) -> Optional[str]:
    m = ISSUED_RE.search(text)
    if not m:
        return None
    day = int(m.group(1))
    month = MONTHS.get(m.group(2).lower())
    year = int(m.group(3))
    if not month:
        return None
    try:
        return datetime(year, month, day).strftime("%Y-%m-%d")
    except ValueError:
        return None


class ValuationTribunalScraper(BaseScraper):
    """Scraper for the Irish Valuation Tribunal judgments (WordPress REST API)."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-IE,en;q=0.9",
            },
            timeout=90,
        )

    def _posts_page(self, page: int) -> Optional[List[Dict[str, Any]]]:
        url = (f"{POSTS_PATH}?per_page={PER_PAGE}&page={page}"
               "&_fields=id,date,title,content,link")
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            if resp.status_code == 400:
                # WP returns 400 (rest_post_invalid_page_number) past the last page.
                return []
            if resp.status_code != 200:
                logger.warning(f"posts page {page}: HTTP {resp.status_code}")
                return None
            data = resp.json()
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.warning(f"Error fetching posts page {page}: {e}")
            return None

    def _fetch_pdf(self, url: str) -> Optional[bytes]:
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            if resp.status_code != 200:
                logger.warning(f"pdf {url}: HTTP {resp.status_code}")
                return None
            data = resp.content
            if not data[:5].startswith(b"%PDF"):
                logger.warning(f"pdf {url}: not a PDF")
                return None
            return data
        except Exception as e:
            logger.warning(f"Error fetching pdf {url}: {e}")
            return None

    @staticmethod
    def _post_to_meta(post: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        content = (post.get("content") or {}).get("rendered", "") or ""
        m = PDF_RE.search(content)
        if not m:
            return None  # non-judgment post (news/announcement) — no determination PDF
        pdf_url = html.unescape(m.group(1))
        if pdf_url.startswith("/"):
            pdf_url = BASE_URL + pdf_url
        title = html.unescape((post.get("title") or {}).get("rendered", "")).strip()
        title = re.sub(r"\s+", " ", title.replace("–", "-").replace("&#8211;", "-"))
        am = APPEAL_RE.search(title)
        appeal_no = am.group(0).upper().replace(" ", "") if am else None
        return {
            "post_id": post.get("id"),
            "post_date": post.get("date"),
            "title": title,
            "link": post.get("link"),
            "pdf_url": pdf_url,
            "appeal_no": appeal_no,
        }

    def _iter_meta(self) -> Generator[Dict[str, Any], None, None]:
        page = 1
        any_post = False
        while True:
            posts = self._posts_page(page)
            if posts is None:
                # Transient error on this page — skip forward defensively.
                page += 1
                if page > 60:
                    break
                continue
            if not posts:
                break
            any_post = True
            for post in posts:
                meta = self._post_to_meta(post)
                if meta:
                    yield meta
            if len(posts) < PER_PAGE:
                break
            page += 1
            if page > 60:  # safety ceiling (~6,000 posts)
                break
        if not any_post:
            raise RuntimeError(
                "Valuation Tribunal REST API returned 0 posts — site blocked or API changed"
            )

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        for meta in self._iter_meta():
            pdf = self._fetch_pdf(meta["pdf_url"])
            if not pdf:
                continue
            try:
                raw_text = _pdf_text(pdf)
            except Exception as e:
                logger.warning(f"extract {meta['pdf_url']} failed: {e}")
                continue
            yield {**meta, "text": raw_text}

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        since = since if since.tzinfo else since.replace(tzinfo=timezone.utc)
        for meta in self._iter_meta():
            pd = meta.get("post_date")
            if pd:
                try:
                    d = datetime.fromisoformat(pd).replace(tzinfo=timezone.utc)
                    if d < since:
                        continue
                except ValueError:
                    pass
            pdf = self._fetch_pdf(meta["pdf_url"])
            if not pdf:
                continue
            try:
                raw_text = _pdf_text(pdf)
            except Exception:
                continue
            yield {**meta, "text": raw_text}

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        raw_text = raw.get("text", "") or ""
        text = _clean(raw_text)
        if len(text) < 200:
            return None

        appeal_no = raw.get("appeal_no")
        title = raw.get("title") or (f"Valuation Tribunal {appeal_no}" if appeal_no else "Valuation Tribunal Determination")

        # Prefer the authoritative determination date from the PDF body;
        # fall back to the WordPress publish date.
        iso_date = _issued_date(raw_text)
        if not iso_date:
            pd = raw.get("post_date")
            if pd:
                try:
                    iso_date = datetime.fromisoformat(pd).strftime("%Y-%m-%d")
                except ValueError:
                    iso_date = None

        uid = appeal_no or f"POST-{raw.get('post_id')}"

        return {
            "_id": f"VT-{uid}",
            "_source": "IE/ValuationTribunal",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"Valuation Tribunal — {title}",
            "text": text,
            "date": iso_date,
            "url": raw.get("pdf_url"),
            "appeal_no": appeal_no,
            "source_page": raw.get("link"),
            "court": "Valuation Tribunal (An Binse Luachála)",
            "jurisdiction": "IE",
            "language": "en",
        }

    def test_connection(self):
        print("Testing Valuation Tribunal REST API...")
        posts = self._posts_page(1)
        print(f"  page 1: {len(posts) if posts else 0} posts")
        if posts:
            meta = None
            for p in posts:
                meta = self._post_to_meta(p)
                if meta:
                    break
            print(f"  first judgment: {meta['appeal_no'] if meta else None} | {meta['pdf_url'] if meta else None}")
            if meta:
                pdf = self._fetch_pdf(meta["pdf_url"])
                if pdf:
                    t = _pdf_text(pdf)
                    print(f"  text {len(t)} chars; date={_issued_date(t)} - OK")
                else:
                    print("  PDF fetch FAILED")


def main():
    scraper = ValuationTribunalScraper()
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
