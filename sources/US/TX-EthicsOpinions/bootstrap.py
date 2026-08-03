#!/usr/bin/env python3
"""
US/TX-EthicsOpinions -- Texas Ethics Commission — Ethics Advisory Opinions

Fetches the full text of the formal Ethics Advisory Opinions of the Texas
Ethics Commission (TEC). Under Tex. Gov't Code ch. 571, the Commission issues
written advisory opinions in response to a request, authoritatively construing
the laws it administers — the campaign-finance and political-contribution rules
of the Election Code (title 15), the personal-financial-disclosure, lobby-
registration, conflict-of-interest and standards-of-conduct statutes, and the
bribery/gift provisions of the Penal Code. Each opinion states the Commission's
official interpretation of the law on the facts presented and a person who acts
in reliance on it has a defense to prosecution = doctrine (official state legal
interpretation; public-domain government edicts, 17 U.S.C. § 105 analogue).

Access (no JavaScript, no CAPTCHA, no auth):
  The opinions are published as born-digital, full-text HTML pages on the TEC
  site, organised into eight topical "parts":

      https://www.ethics.state.tx.us/opinions/part{I..VIII}/{NNN}.html

  Each part has a topic digest page that hyperlinks every opinion assigned to
  that part:

      /opinions/part{I}/digest_a.php ... /opinions/part{VIII}/digest_h.php

  The digest pages are the authoritative index of which opinion numbers exist
  and in which part directory they live (numbers are NOT contiguous within a
  part). Each opinion page is a standalone document (title = "…Advisory Opinion
  No. N", body = the full opinion text with its issue date) — no site chrome in
  the content, no OCR needed.

Strategy:
  GET the 8 digest pages, collect the unique /opinions/part*/NNN.html links,
  fetch each opinion page, strip the HTML to clean text, parse the opinion
  number from the filename and the issue date ("Month D, YYYY") from the body,
  and normalize into the doctrine schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (all advisory opinions)
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
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from bs4 import BeautifulSoup

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.TX-EthicsOpinions")

BASE_URL = "https://www.ethics.state.tx.us"

# The eight topic-digest pages that index every advisory opinion.
DIGEST_PAGES = [
    "/opinions/partI/digest_a.php",
    "/opinions/partII/digest_b.php",
    "/opinions/partIII/digest_c.php",
    "/opinions/partIV/digest_d.php",
    "/opinions/partV/digest_e.php",
    "/opinions/partVI/digest_f.php",
    "/opinions/partVII/digest_g.php",
    "/opinions/partVIII/digest_h.php",
]

# Opinion links appear in the digest pages in a couple of href shapes, e.g.
#   /opinions/partIV/456.html          (most digests)
#   /opinions/bwilson/partI/001.html   (partI digest — mirror subpath)
# Capture the part (roman) + number from anywhere in the href and rebuild the
# canonical /opinions/part{X}/{N}.html URL (confirmed to resolve for all parts).
OPINION_HREF_RE = re.compile(
    r'part([IVX]+)/(\d{1,4})\.html', re.I
)

# Opinion number from the filename (e.g. "001.html" -> 1).
NUMBER_RE = re.compile(r'/(\d{1,4})\.html$', re.I)

DATE_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2}),\s+(\d{4})",
    re.I,
)
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}


class TXEthicsScraper(BaseScraper):

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
    def _curl_text(self, url: str) -> str | None:
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "60", "-A", self._ua,
                     "-H", "Accept: text/html,*/*", url],
                    capture_output=True, timeout=90,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout.decode("utf-8", "replace")
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- parsing
    @staticmethod
    def _clean_text(html: str) -> str:
        """Strip the opinion HTML to clean, readable plain text."""
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "head", "nav", "header", "footer"]):
            tag.decompose()
        text = soup.get_text("\n")
        text = _html.unescape(text)
        text = re.sub(r"[ \t ]+", " ", text)
        text = re.sub(r"\n[ \t]*\n+", "\n\n", text)
        lines = [ln.strip() for ln in text.splitlines()]
        text = "\n".join(ln for ln in lines if ln)
        return text.strip()

    @staticmethod
    def _norm_date(text: str) -> str | None:
        m = DATE_RE.search(text)
        if not m:
            return None
        mo = MONTHS.get(m.group(1).lower())
        d = int(m.group(2))
        y = int(m.group(3))
        if mo and 1 <= d <= 31 and 1970 <= y <= 2035:
            return f"{y:04d}-{mo:02d}-{d:02d}"
        return None

    @staticmethod
    def _title_of(html: str, number: int) -> str:
        m = re.search(r"(?is)<title>\s*(.*?)\s*</title>", html)
        if m:
            t = re.sub(r"\s+", " ", _html.unescape(m.group(1))).strip()
            if t and "404" not in t:
                return t[:300]
        return f"Texas Ethics Advisory Opinion No. {number}"

    # ---------------------------------------------------------- discovery
    def _list_all(self) -> list[str]:
        seen: dict[str, str] = {}
        for page in DIGEST_PAGES:
            html = self._curl_text(urljoin(BASE_URL, page))
            if not html:
                logger.warning(f"  digest {page}: no response")
                continue
            found = 0
            for m in OPINION_HREF_RE.finditer(html):
                part = m.group(1).upper()
                number = int(m.group(2))
                key = str(number)  # dedup by opinion number
                if key not in seen:
                    # Rebuild the canonical URL (drops any mirror subpath).
                    seen[key] = f"{BASE_URL}/opinions/part{part}/{m.group(2)}.html"
                    found += 1
            logger.info(f"  digest {page}: {found} new opinions")
        # Sort by opinion number for stable ordering.
        return [seen[k] for k in sorted(seen, key=lambda x: int(x))]

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing TEC advisory-opinion index + HTML extraction...")
        urls = self._list_all()
        if not urls:
            logger.error("API test FAILED: no opinion links found in digests")
            return False
        logger.info(f"  discovered {len(urls)} advisory opinions")
        ok = 0
        for url in urls[:5]:
            html = self._curl_text(url)
            if not html:
                continue
            text = self._clean_text(html)
            if len(text) > 400:
                num_m = NUMBER_RE.search(url)
                num = int(num_m.group(1)) if num_m else "?"
                logger.info(f"  Opinion No. {num} OK ({len(text)} chars) "
                            f"date={self._norm_date(text)}")
                ok += 1
        if ok:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw.get("number")
        return {
            "_id": f"US/TX-EthicsOpinions/{number}",
            "_source": "US/TX-EthicsOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "issuer": "Texas Ethics Commission",
            "title": raw.get("title") or f"Texas Ethics Advisory Opinion No. {number}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-TX",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        urls = self._list_all()
        emitted = 0
        for url in urls:
            html = self._curl_text(url)
            if not html:
                continue
            text = self._clean_text(html)
            if len(text) < 400:
                continue
            num_m = NUMBER_RE.search(url)
            number = int(num_m.group(1)) if num_m else None
            yield {
                "number": number,
                "url": url,
                "title": self._title_of(html, number),
                "text": text,
                "date": self._norm_date(text),
            }
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

    parser = argparse.ArgumentParser(description="US/TX-EthicsOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = TXEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
