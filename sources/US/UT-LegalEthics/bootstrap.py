#!/usr/bin/env python3
"""
US/UT-LegalEthics -- Utah State Bar Ethics Advisory Opinion Committee — Opinions

Fetches the full text of the ethics advisory opinions issued by the Ethics
Advisory Opinion Committee ("EAOC") of the Utah State Bar. The EAOC issues formal
written opinions on the ethical propriety of the professional or personal conduct
of Bar members under the Utah Rules of Professional Conduct = doctrine (the Bar's
official written interpretation of the attorney-conduct rules).

The opinions span 1970s-present (Opinion Nos. like "90-100", "97-11", "2001-05",
"22-04", "25-01") and are published by the Utah State Bar (utahbar.org, a
WordPress site) as a custom post type. Distinct from US/UT-JudicialEthics (the
Utah Judicial Ethics Advisory Committee, which advises judges) and from Utah
Attorney General opinions.

Access (no JavaScript execution needed, no CAPTCHA, no auth, browser UA):
  1. Enumerate every opinion via the public WordPress REST API:
        GET /wp-json/wp/v2/ethics-opinions?per_page=100&page=<n>
     Each record carries {slug, title, link, date}. ~286 opinions, 3 pages.
     (The REST content field is empty — the body lives in a PDF or in the
     Elementor detail page, not in post_content.)
  2. Resolve each opinion's born-digital PDF via the media library:
        GET /wp-json/wp/v2/media?search=<slug>
     and pick the .pdf source_url matching the slug (WP stores each opinion as
     /wp-content/uploads/YYYY/MM/<slug>.pdf, with the 2-digit-year slugs mapped
     to their 4-digit-year filenames, e.g. "22-04" -> "2022-04.pdf").
  3. Download the PDF and extract the text layer (born-digital, no OCR).
     For the handful of very recent HTML-only opinions with no PDF, fall back to
     the Elementor detail page's post-content widget.

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
"""

from __future__ import annotations

import sys
import io
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.UT-LegalEthics")

BASE = "https://www.utahbar.org"
REST_OPINIONS = BASE + "/wp-json/wp/v2/ethics-opinions"
REST_MEDIA = BASE + "/wp-json/wp/v2/media"

ISSUED_RE = re.compile(r"Issued\s+([A-Z][a-z]+\.?\s+\d{1,2},\s+\d{4})")
YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")


class UTLegalEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            ),
            "Accept": "application/json, text/html",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str, params: dict | None = None) -> requests.Response | None:
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                r = self._session.get(url, params=params, timeout=60)
                if r.status_code == 200:
                    return r
                if r.status_code == 404:
                    return None
                logger.warning(f"GET {url} -> HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"GET failed {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    def _list_opinions(self) -> Generator[dict, None, None]:
        r = self._get(REST_OPINIONS, {"per_page": 100, "page": 1})
        if not r:
            logger.error("could not fetch opinions page 1")
            return
        total_pages = int(r.headers.get("X-WP-TotalPages") or 1)
        total = int(r.headers.get("X-WP-Total") or 0)
        logger.info(f"  {total} opinions across {total_pages} REST pages")
        seen: set[str] = set()

        def emit(rows):
            for row in rows:
                slug = row.get("slug")
                if slug and slug not in seen:
                    seen.add(slug)
                    yield {
                        "slug": slug,
                        "title": (row.get("title") or {}).get("rendered") or slug,
                        "link": row.get("link"),
                        "wp_date": (row.get("date") or "")[:10] or None,
                    }

        yield from emit(r.json())
        for page in range(2, total_pages + 1):
            rp = self._get(REST_OPINIONS, {"per_page": 100, "page": page})
            if not rp:
                logger.warning(f"  REST page {page} failed")
                continue
            yield from emit(rp.json())

    # ----------------------------------------------------------- pdf match
    @staticmethod
    def _slug_candidates(slug: str) -> list[str]:
        """Filename stems a slug may map to (2-digit-year -> 4-digit-year)."""
        cands = [slug]
        m = re.match(r"^(\d{2})-(\d+)$", slug)
        if m:
            yy, num = m.group(1), m.group(2)
            century = "20" if int(yy) <= 30 else "19"
            cands.append(f"{century}{yy}-{num}")
        return cands

    def _pdf_url_for(self, slug: str) -> str | None:
        r = self._get(REST_MEDIA, {"search": slug, "per_page": 15})
        if not r:
            return None
        try:
            items = r.json()
        except Exception:
            return None
        pdfs = [
            m.get("source_url") for m in items
            if (m.get("source_url") or "").lower().endswith(".pdf")
        ]
        if not pdfs:
            return None
        cands = self._slug_candidates(slug)
        for url in pdfs:
            stem = url.rsplit("/", 1)[-1].rsplit(".", 1)[0]
            if stem in cands:
                return url
        # Exactly one PDF hit and no exact stem match: trust it.
        return pdfs[0] if len(pdfs) == 1 else None

    # -------------------------------------------------------- extraction
    def _pdf_text(self, url: str) -> str:
        r = self._get(url)
        if not r or not r.content:
            return ""
        if fitz is None:
            logger.error("PyMuPDF (fitz) not available")
            return ""
        try:
            doc = fitz.open(stream=io.BytesIO(r.content), filetype="pdf")
        except Exception as e:
            logger.warning(f"  fitz open failed for {url}: {e}")
            return ""
        parts = []
        for page in doc:
            parts.append(page.get_text())
        doc.close()
        text = "\n".join(parts)
        text = re.sub(r"[ \t]+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _html_text(self, link: str) -> str:
        """Fallback for HTML-only opinions: the Elementor post-content widget."""
        from bs4 import BeautifulSoup
        r = self._get(link)
        if not r:
            return ""
        soup = BeautifulSoup(r.text, "html.parser")
        node = soup.select_one("div.elementor-widget-theme-post-content")
        if node is None:
            return ""
        text = node.get_text("\n", strip=True)
        return re.sub(r"\n{3,}", "\n\n", text).strip()

    @staticmethod
    def _parse_date(text: str, slug: str) -> str | None:
        m = ISSUED_RE.search(text or "")
        if m:
            for fmt in ("%B %d, %Y", "%b %d, %Y"):
                try:
                    return datetime.strptime(m.group(1).replace(".", ""),
                                             fmt.replace(".", "")).strftime("%Y-%m-%d")
                except ValueError:
                    continue
        # Fall back to the year encoded in the slug.
        sm = re.match(r"^(\d{2,4})-", slug)
        if sm:
            y = sm.group(1)
            if len(y) == 4:
                return f"{y}-01-01"
            yy = int(y)
            century = 2000 if yy <= 30 else 1900
            return f"{century + yy:04d}-01-01"
        return None

    def _fetch_one(self, row: dict) -> dict | None:
        slug = row["slug"]
        text = ""
        pdf_url = self._pdf_url_for(slug)
        source_url = row.get("link")
        if pdf_url:
            text = self._pdf_text(pdf_url)
            source_url = pdf_url
        if len(text) < 120:
            # HTML fallback (recent Elementor-only opinions).
            html_text = self._html_text(row["link"])
            if len(html_text) >= 120:
                text = html_text
                source_url = row["link"]
        if len(text) < 120:
            return None
        return {
            "opinion_number": slug,
            "title": row.get("title") or slug,
            "text": text,
            "date": self._parse_date(text, slug),
            "url": source_url,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Utah State Bar EAOC opinions...")
        rows = []
        for row in self._list_opinions():
            rows.append(row)
            if len(rows) >= 6:
                break
        if not rows:
            logger.error("API test FAILED: no opinions listed")
            return False
        ok = 0
        for row in rows:
            rec = self._fetch_one(row)
            if rec and len(rec["text"]) > 200:
                logger.info(f"  {rec['opinion_number']} OK "
                            f"({len(rec['text'])} chars) date={rec['date']}")
                ok += 1
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["opinion_number"]
        slug = re.sub(r"[^A-Za-z0-9]+", "-", num).strip("-")
        return {
            "_id": f"US/UT-LegalEthics/{slug}",
            "_source": "US/UT-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": "Utah State Bar — Ethics Advisory Opinion Committee",
            "title": raw.get("title") or num,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-UT",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for row in self._list_opinions():
            rec = self._fetch_one(row)
            if not rec:
                logger.warning(f"  no text for {row['slug']}, skipping")
                continue
            yield rec
            emitted += 1
            if sample and emitted >= 12:
                return

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/UT-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = UTLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
