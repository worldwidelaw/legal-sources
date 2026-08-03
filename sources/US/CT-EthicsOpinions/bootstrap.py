#!/usr/bin/env python3
"""
US/CT-EthicsOpinions -- Connecticut Office of State Ethics — Advisory Opinions

Fetches the full text of the formal Advisory Opinions issued by the Connecticut
Office of State Ethics (OSE) and its predecessor, the State Ethics Commission,
under the Codes of Ethics for Public Officials and Lobbyists (Conn. Gen. Stat.
§ 1-79 et seq.). An Advisory Opinion is the agency's written interpretation of the
conflict-of-interest, gift, revolving-door and disclosure statutes, requested by a
public official, state employee or lobbyist. Official state legal interpretation =
doctrine.

Access (no JavaScript, no CAPTCHA, no auth):
  The OSE publishes a single "numerical list and summaries" page that links every
  advisory opinion from 1993 to the present:

      https://portal.ct.gov/ethics/advisory-opinions/numerical-list-and-summaries/advisory-opinions---summaries

  Each opinion appears as an <a href> whose anchor text is "Advisory Opinion No.
  YYYY-N". The href is one of two shapes:

    * 2012–present  ->  a born-digital PDF under
        https://portal.ct.gov/-/media/ethics/advisory_opinions/{year}/...pdf
      (real text layer, extracted directly — no OCR).

    * 1993–2011     ->  an HTML detail page under
        https://portal.ct.gov/ethics/advisory-opinions/{year}/advisory-opinion-no-{yyyyn}
      whose opinion body lives in <div class="small-12 medium-8 columns"><div
      class="content"> ... </div>. We extract that block and strip the chrome.

Strategy:
  GET the list page, parse each opinion anchor to (number, url), download each
  document, extract full text (PDF text layer or HTML content div), and normalize.
  All advisory opinions are doctrine. The decision date is parsed from a "Month DD,
  YYYY" line near the top of the text, with a fallback to the opinion-number year.

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions 1993–present)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import subprocess
import time
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import _extract as _pdf_extract_bytes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CT-EthicsOpinions")

BASE_URL = "https://portal.ct.gov"
LIST_URL = (
    "https://portal.ct.gov/ethics/advisory-opinions/"
    "numerical-list-and-summaries/advisory-opinions---summaries"
)

# Every opinion anchor: capture href + anchor text.
ANCHOR_RE = re.compile(
    r'<a\s+[^>]*href="(?P<href>[^"]+)"[^>]*>(?P<text>.*?)</a>',
    re.S | re.I,
)

# Opinion number in anchor text: "Advisory Opinion No. 2024-1" (4-digit year, N).
NUM_TEXT_RE = re.compile(r"Advisory\s+Opinion\s+No\.?\s*(\d{4})-(\d+)", re.I)
# Opinion number in a PDF/HTML href: ".../advisory-opinion-no-2024-1" or "...-20051".
NUM_HREF_RE = re.compile(r"advisory[-_]?opinion[-_]?no[-_]?(\d{4})-?(\d{1,3})\b", re.I)

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}
DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|"
    r"November|December)\s+(\d{1,2}),\s+((?:19|20)\d{2})\b",
    re.I,
)


def _clean(fragment: str) -> str:
    """Strip HTML tags/entities from an HTML fragment and collapse whitespace."""
    txt = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", fragment or "", flags=re.S | re.I)
    txt = re.sub(r"<[^>]+>", " ", txt)
    txt = _html.unescape(txt)
    return re.sub(r"\s+", " ", txt).strip()


def _date_from_text(text: str) -> str | None:
    """Best-effort ISO date from the first 'Month DD, YYYY' near the top of the body."""
    m = DATE_RE.search(text[:2500])
    if m:
        mo = MONTHS[m.group(1).lower()]
        d = int(m.group(2))
        y = int(m.group(3))
        if 1 <= d <= 31:
            return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


class CTEthicsOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

    # ---------------------------------------------------------------- http
    def _curl(self, url: str, binary: bool = False):
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "60", "-A", self._ua, url],
                    capture_output=True, timeout=90,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout if binary else out.stdout.decode("utf-8", "replace")
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _abs_url(href: str) -> str:
        href = _html.unescape(href).strip()
        if href.startswith("http"):
            return quote(href, safe="/:?&=%#")
        return BASE_URL + quote(href, safe="/:?&=%#")

    @staticmethod
    def _number(text: str, href: str) -> str | None:
        """Return a normalized opinion number 'YYYY-N' from anchor text or href."""
        m = NUM_TEXT_RE.search(_html.unescape(text or ""))
        if m:
            return f"{m.group(1)}-{int(m.group(2))}"
        m = NUM_HREF_RE.search(_html.unescape(href or ""))
        if m:
            return f"{m.group(1)}-{int(m.group(2))}"
        return None

    @staticmethod
    def _year_from_number(number: str) -> str | None:
        m = re.match(r"(\d{4})-", number)
        return f"{m.group(1)}-01-01" if m else None

    def _extract_html_body(self, page_html: str) -> str:
        """Pull the opinion body out of the OSE content column, stripping chrome."""
        # The body lives in the medium-8 content column of <main>.
        m = re.search(
            r'<div class="small-12 medium-8 columns">(.*?)</main>',
            page_html, re.S | re.I,
        )
        block = m.group(1) if m else page_html
        # Prefer the inner <div class="content"> ... to page end of the column.
        m2 = re.search(r'<div class="content">(.*)', block, re.S | re.I)
        if m2:
            block = m2.group(1)
        return _clean(block)

    # ---------------------------------------------------------- discovery
    def _list_all(self) -> list[dict]:
        """Return [{number, url, is_pdf}] for each advisory opinion on the list page."""
        html = self._curl(LIST_URL)
        if not html:
            logger.error("could not fetch the advisory-opinions list page")
            return []
        seen: dict[str, dict] = {}
        for m in ANCHOR_RE.finditer(html):
            href = m.group("href")
            hl = href.lower()
            is_pdf = "/advisory_opinions/" in hl and ".pdf" in hl
            is_html = "/ethics/advisory-opinions/" in hl and re.search(
                r"/(?:19|20)\d{2}/advisory[-_]?opinion", hl
            )
            if not (is_pdf or is_html):
                continue
            caption = _clean(m.group("text"))
            number = self._number(caption, href)
            if not number or number in seen:
                continue
            seen[number] = {
                "number": number,
                "caption": caption or f"Advisory Opinion No. {number}",
                "url": self._abs_url(href),
                "is_pdf": bool(is_pdf),
            }
        return list(seen.values())

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing CT Office of State Ethics advisory opinions...")
        items = self._list_all()
        if not items:
            logger.error("API test FAILED: no opinions found on list page")
            return False
        pdfs = sum(1 for i in items if i["is_pdf"])
        logger.info(f"  discovered {len(items)} opinions ({pdfs} PDF, {len(items) - pdfs} HTML)")
        # test a couple of each kind
        sample = [i for i in items if i["is_pdf"]][:2] + [i for i in items if not i["is_pdf"]][:2]
        ok = 0
        for it in sample:
            text = self._fetch_text(it)
            if text and len(text) > 400:
                logger.info(f"  {it['number']} OK ({len(text)} chars, "
                            f"{'pdf' if it['is_pdf'] else 'html'})")
                ok += 1
        if ok:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # ---------------------------------------------------------- fetch text
    def _fetch_text(self, it: dict) -> str | None:
        if it["is_pdf"]:
            pdf = self._curl(it["url"], binary=True)
            if not pdf or pdf[:4] != b"%PDF":
                return None
            try:
                return _pdf_extract_bytes(pdf)
            except Exception as e:
                logger.warning(f"  PDF extract failed for {it['number']}: {e}")
                return None
        page = self._curl(it["url"])
        if not page:
            return None
        # Skip CT.gov soft-404 pages (dead links return a 200 "Oops!" page).
        if "That page is no longer here" in page or "Oops!" in page[:4000]:
            logger.warning(f"  {it['number']}: dead link (soft-404), skipping")
            return None
        return self._extract_html_body(page)

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw["number"]
        text = raw["text"]
        date = _date_from_text(text) or self._year_from_number(number)
        return {
            "_id": f"US/CT-EthicsOpinions/{number}",
            "_source": "US/CT-EthicsOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "issuer": "Connecticut Office of State Ethics",
            "title": raw.get("caption") or f"Advisory Opinion No. {number}",
            "text": text,
            "url": raw["url"],
            "date": date,
            "jurisdiction": "US-CT",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        items = self._list_all()
        # newest first (helps the sample land on clean recent PDFs)
        items.sort(key=lambda i: i["number"], reverse=True)
        emitted = 0
        for it in items:
            text = self._fetch_text(it)
            if not text or len(text) < 400:
                logger.warning(f"  {it['number']}: insufficient text "
                               f"({len(text) if text else 0} chars), skipping")
                continue
            yield {**it, "text": text}
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
            date = _date_from_text(raw["text"]) or self._year_from_number(raw["number"])
            if not since or (date and date >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/CT-EthicsOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CTEthicsOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
