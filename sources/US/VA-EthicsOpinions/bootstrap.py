#!/usr/bin/env python3
"""
US/VA-EthicsOpinions -- Virginia Conflict of Interest and Ethics Advisory Council
                        — Formal Advisory Opinions

Fetches the full text of the formal advisory opinions published by the Virginia
Conflict of Interest and Ethics Advisory Council. Under Va. Code title 30,
chapter 56 (§ 30-355 et seq.) the Council issues formal advisory opinions
construing the State and Local Government Conflict of Interests Act (Va. Code
§ 2.2-3100 et seq.), the General Assembly Conflicts of Interests Act (§ 30-100
et seq.) and the lobbyist-disclosure statutes (§ 2.2-418 et seq.). Formal
advisory opinions are public record and are published on the Council's website;
they are the Council's official written interpretation of the ethics statutes =
doctrine.

Access (no JavaScript, no CAPTCHA, no auth):
  The Council's advisory-opinions page lists the formal opinions as direct
  born-digital PDF links:

      https://ethics.dls.virginia.gov/advisory-opinions.asp

  Each link is an <a href="...pdf"> whose filename carries the opinion number
  (e.g. "2016-F-001", "2015-F-004A", "2015-F-002.1") and a short caption. Some
  links live under a "Formal Advisory Opinions/" subfolder; both root-level and
  subfolder hrefs resolve directly. Every PDF has a real text layer (no OCR).

Strategy:
  GET the advisory-opinions page, parse each PDF anchor to
  (opinion_number, caption, pdf_url), download each PDF, extract its text layer,
  and normalize. The opinion number is parsed from the filename in the
  "YYYY-F-NNN[.rev|letter]" form; the decision date is parsed from the opinion
  body ("Month DD, YYYY") with a fallback to the opinion-number year.

Usage:
  python bootstrap.py bootstrap            # Full pull (all formal opinions)
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
logger = logging.getLogger("legal-data-hunter.US.VA-EthicsOpinions")

BASE_URL = "https://ethics.dls.virginia.gov"
LISTING_URL = "https://ethics.dls.virginia.gov/advisory-opinions.asp"

# Every PDF anchor on the advisory-opinions page: capture href + anchor text.
ANCHOR_RE = re.compile(
    r'<a\s+[^>]*href="(?P<href>[^"]*\.pdf[^"]*)"[^>]*>(?P<text>.*?)</a>',
    re.S | re.I,
)

# Opinion number: four-digit year, "-F-", 3+ digits, optional ".N" revision or a
# trailing letter (2015-F-004A). Filenames sometimes urlencode the spaces.
# The optional suffix is a ".N" revision or a single uppercase letter (2015-F-004A).
# The (?![a-z]) guard stops a following word (e.g. "004Legislator") being read as a
# suffix letter.
NUMBER_RE = re.compile(r"(20\d{2})-F-(\d{2,4})((?:\.\d+)|[A-Z](?![a-z]))?")

MONTHS = (
    "January|February|March|April|May|June|July|August|September|"
    "October|November|December"
)
DATE_RE = re.compile(rf"\b({MONTHS})\s+(\d{{1,2}}),\s+(20\d{{2}})\b")
_MONTH_IDX = {
    m: i + 1
    for i, m in enumerate(
        "January February March April May June July August September "
        "October November December".split()
    )
}


def _clean(fragment: str) -> str:
    """Strip HTML tags/entities from a small HTML fragment."""
    txt = re.sub(r"<[^>]+>", " ", fragment or "")
    txt = _html.unescape(txt)
    return re.sub(r"\s+", " ", txt).strip()


def _date_from_text(text: str) -> str | None:
    """First 'Month DD, YYYY' in the opinion body -> ISO date."""
    m = DATE_RE.search(text or "")
    if not m:
        return None
    mo = _MONTH_IDX.get(m.group(1).capitalize())
    d, y = int(m.group(2)), int(m.group(3))
    if mo and 1 <= d <= 31:
        return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


class VAEthicsOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
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
        """Build an absolute, percent-encoded URL for a site-relative PDF href.

        Handles hrefs that mix raw spaces and already-encoded %20 by keeping % in
        the safe set (so pre-encoded sequences are not double-encoded)."""
        href = _html.unescape(href).split("#", 1)[0]
        if href.startswith("http"):
            return quote(href, safe="/:?&=%")
        if not href.startswith("/"):
            href = "/" + href
        return BASE_URL + quote(href, safe="/:?&=%")

    @staticmethod
    def _opinion_number(href: str, caption: str) -> str | None:
        """Return the normalized opinion number (YYYY-F-NNN[.rev|letter])."""
        fname = href.rsplit("/", 1)[-1]
        for src in (_html.unescape(fname), _html.unescape(caption or "")):
            m = NUMBER_RE.search(src)
            if m:
                suffix = m.group(3) or ""
                return f"{m.group(1)}-F-{m.group(2)}{suffix.upper()}"
        return None

    @staticmethod
    def _slug_from_href(href: str) -> str:
        """Fallback stable id for opinions whose filename lacks a number."""
        fname = _html.unescape(href.rsplit("/", 1)[-1])
        fname = re.sub(r"\.pdf$", "", fname, flags=re.I)
        slug = re.sub(r"[^A-Za-z0-9]+", "-", fname).strip("-")
        return slug[:80] or "opinion"

    # ---------------------------------------------------------- discovery
    def _list_all(self) -> list[dict]:
        """Return [{opinion_number, caption, date, pdf_url}] for each opinion."""
        html = self._curl(LISTING_URL)
        if not html:
            logger.error("could not fetch the advisory-opinions page")
            return []
        seen: dict[str, dict] = {}
        for m in ANCHOR_RE.finditer(html):
            href = m.group("href")
            caption = _clean(m.group("text"))
            number = self._opinion_number(href, caption)
            key = number or self._slug_from_href(href)
            if key in seen:
                continue
            fname = _html.unescape(href.rsplit("/", 1)[-1])
            fname_disp = re.sub(r"\.pdf$", "", fname, flags=re.I)
            year = None
            ym = re.search(r"(20\d{2})", number or fname)
            if ym:
                year = f"{ym.group(1)}-01-01"
            seen[key] = {
                "opinion_number": number,
                "caption": caption or fname_disp,
                "filename_title": fname_disp,
                "date": year,  # refined from PDF body during fetch
                "pdf_url": self._abs_pdf_url(href),
            }
        return list(seen.values())

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing VA Ethics Advisory Council opinions + PDF extraction...")
        items = self._list_all()
        if not items:
            logger.error("API test FAILED: no opinions found on page")
            return False
        logger.info(f"  discovered {len(items)} formal advisory opinions")
        ok = 0
        for it in items[:5]:
            pdf = self._curl(it["pdf_url"], binary=True)
            if not pdf:
                logger.warning(f"  no PDF for {it['opinion_number']}")
                continue
            text = _pdf_extract_bytes(pdf)
            if text and len(text) > 400:
                logger.info(f"  {it['opinion_number']} OK "
                            f"({len(text)} chars) date={_date_from_text(text) or it['date']}")
                ok += 1
        if ok:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw.get("opinion_number")
        caption = raw.get("caption") or raw.get("filename_title")
        title = caption
        if number and number not in (caption or ""):
            title = f"Formal Advisory Opinion {number}: {caption}"
        _id = f"US/VA-EthicsOpinions/{number}" if number else \
            f"US/VA-EthicsOpinions/{raw.get('_slug')}"
        return {
            "_id": _id,
            "_source": "US/VA-EthicsOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "issuer": "Virginia Conflict of Interest and Ethics Advisory Council",
            "title": title,
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date"),
            "jurisdiction": "US-VA",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        items = self._list_all()
        emitted = 0
        for it in items:
            pdf = self._curl(it["pdf_url"], binary=True)
            if not pdf:
                logger.warning(f"  no PDF for {it.get('opinion_number')}")
                continue
            try:
                text = _pdf_extract_bytes(pdf)
            except Exception as e:
                logger.warning(f"  extract failed for {it.get('opinion_number')}: {e}")
                continue
            if not text or len(text) < 400:
                logger.warning(f"  {it.get('opinion_number')}: insufficient text "
                               f"({len(text) if text else 0} chars), skipping")
                continue
            date = _date_from_text(text) or it["date"]
            slug = self._slug_from_href(it["pdf_url"])
            yield {**it, "text": text, "date": date, "_slug": slug}
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

    parser = argparse.ArgumentParser(description="US/VA-EthicsOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = VAEthicsOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
