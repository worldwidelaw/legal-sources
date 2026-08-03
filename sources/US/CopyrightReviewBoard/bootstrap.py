#!/usr/bin/env python3
"""
US/CopyrightReviewBoard -- U.S. Copyright Office Review Board — Registration Decisions

Fetches the full text of the final decisions issued by the Review Board of the
United States Copyright Office (Library of Congress). When the Copyright Office's
Registration Program refuses to register a work and the applicant files a second
request for reconsideration, the Review Board — the Office's highest-level
adjudicative body — issues a written decision that either affirms or reverses the
refusal (37 C.F.R. § 202.5). Each decision applies the Copyright Act's
originality/copyrightability standards to a specific work and is the Office's final
agency action on that application = case_law (an adjudicative decision on a specific
matter; public-domain U.S. Government work, 17 U.S.C. § 105).

Access (no JavaScript, no CAPTCHA, no auth):
  Every Review Board decision is listed in a single HTML table at

      https://www.copyright.gov/rulings-filings/review-board/

  Each row is:

      <tr>
        <td><a href="/rulings-filings/review-board/docs/{slug}.pdf">{Title}</a></td>
        <td>{Year}</td>
        <td>{Categories}</td>
        <td>{Outcome}</td>
      </tr>

  The href resolves to the born-digital, full-text PDF of the decision (real text
  layer — no OCR needed). The table holds ~750 decisions.

Strategy:
  GET the listing page, parse each table row to (slug, title, year, categories,
  outcome, pdf_url), download each PDF, extract its text layer, parse the precise
  decision date from the letter head (falling back to the table year), and
  normalize into the case_law schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (all decisions)
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
logger = logging.getLogger("legal-data-hunter.US.CopyrightReviewBoard")

BASE_URL = "https://www.copyright.gov"
LISTING_URL = "https://www.copyright.gov/rulings-filings/review-board/"

# One decision row in the listing table: the PDF anchor (title) followed by
# three <td> columns — year, categories, outcome.
ROW_RE = re.compile(
    r'<td>\s*<a\s+href="(?P<href>/rulings-filings/review-board/docs/[^"]+\.pdf)"[^>]*>'
    r'(?P<title>.*?)</a>\s*</td>\s*'
    r'<td>\s*(?P<year>\d{4})\s*</td>\s*'
    r'<td>\s*(?P<cats>.*?)\s*</td>\s*'
    r'<td>\s*(?P<outcome>.*?)\s*</td>',
    re.S | re.I,
)

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}
# "June 12, 2026" style date at the top of the decision letter.
DATE_RE = re.compile(
    r"\b(" + "|".join(_MONTHS) + r")\s+(\d{1,2}),\s+(\d{4})\b", re.I
)


def _clean(fragment: str) -> str:
    """Strip HTML tags/entities from a small HTML fragment."""
    txt = re.sub(r"<[^>]+>", " ", fragment or "")
    txt = _html.unescape(txt)
    return re.sub(r"\s+", " ", txt).strip()


class CopyrightReviewBoardScraper(BaseScraper):

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
    def _abs_pdf_url(href: str) -> str:
        """Build an absolute, percent-encoded URL for a decision PDF href."""
        return BASE_URL + quote(_html.unescape(href), safe="/:?&=%")

    @staticmethod
    def _slug(href: str) -> str:
        fname = _html.unescape(href).rsplit("/", 1)[-1]
        stem = re.sub(r"\.pdf$", "", fname, flags=re.I)
        return re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-")[:80] or "unknown"

    @staticmethod
    def _norm_date(text: str, year: str | None) -> str | None:
        # Prefer the precise date printed at the head of the decision letter.
        if text:
            m = DATE_RE.search(text[:2000])
            if m:
                mm = _MONTHS[m.group(1).lower()]
                dd = int(m.group(2))
                yyyy = int(m.group(3))
                if 1 <= dd <= 31 and 1955 <= yyyy <= 2035:
                    return f"{yyyy:04d}-{mm:02d}-{dd:02d}"
        # Fallback: the year column from the listing table.
        if year and re.fullmatch(r"\d{4}", year):
            return f"{int(year):04d}-01-01"
        return None

    # ---------------------------------------------------------- discovery
    def _list_all(self) -> list[dict]:
        """Return [{slug, title, year, categories, outcome, pdf_url}] for every decision."""
        html = self._curl(LISTING_URL)
        if not html:
            logger.error("could not fetch the Review Board listing page")
            return []
        seen: dict[str, dict] = {}
        for m in ROW_RE.finditer(html):
            href = m.group("href")
            slug = self._slug(href)
            if slug in seen:
                continue
            seen[slug] = {
                "slug": slug,
                "title": _clean(m.group("title")) or slug,
                "year": m.group("year"),
                "categories": _clean(m.group("cats")) or None,
                "outcome": _clean(m.group("outcome")) or None,
                "pdf_url": self._abs_pdf_url(href),
            }
        return list(seen.values())

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Copyright Review Board listing + PDF extraction...")
        items = self._list_all()
        if not items:
            logger.error("API test FAILED: no decisions found in listing")
            return False
        logger.info(f"  discovered {len(items)} Review Board decisions")
        ok = 0
        for it in items[:5]:
            pdf = self._curl(it["pdf_url"], binary=True)
            if not pdf:
                logger.warning(f"  no PDF for {it['slug']}")
                continue
            text = _pdf_extract_bytes(pdf)
            if text and len(text) > 400:
                date = self._norm_date(text, it["year"])
                logger.info(f"  {it['slug']} OK ({len(text)} chars) date={date}")
                ok += 1
        if ok:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        slug = raw.get("slug")
        title = raw.get("title") or slug
        date = self._norm_date(raw.get("text", ""), raw.get("year"))
        return {
            "_id": f"US/CopyrightReviewBoard/{slug}",
            "_source": "US/CopyrightReviewBoard",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"Copyright Office Review Board — {title}"[:200],
            "issuer": "Review Board of the United States Copyright Office",
            "categories": raw.get("categories"),
            "outcome": raw.get("outcome"),
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": date,
            "jurisdiction": "US",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        items = self._list_all()
        emitted = 0
        for it in items:
            pdf = self._curl(it["pdf_url"], binary=True)
            if not pdf:
                logger.warning(f"  no PDF for {it['slug']}")
                continue
            try:
                text = _pdf_extract_bytes(pdf)
            except Exception as e:
                logger.warning(f"  extract failed for {it['slug']}: {e}")
                continue
            if not text or len(text) < 400:
                logger.warning(f"  {it['slug']}: insufficient text "
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
            date = self._norm_date(raw.get("text", ""), raw.get("year"))
            if not since or (date and date >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/CopyrightReviewBoard bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CopyrightReviewBoardScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
