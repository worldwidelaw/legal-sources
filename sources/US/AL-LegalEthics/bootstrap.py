#!/usr/bin/env python3
"""
US/AL-LegalEthics -- Alabama State Bar — Formal Ethics Opinions

Fetches the full text of the Formal Opinions issued by the Alabama State Bar's
Office of General Counsel / Disciplinary Commission. Each opinion is the Bar's
written interpretation of the Alabama Rules of Professional Conduct in response
to a member's inquiry about contemplated attorney conduct = doctrine (advisory;
lawyer discipline itself is administered by the Bar's Disciplinary Board and the
Alabama Supreme Court). Historically numbered "RO YYYY-NN" (Retained/formal
Opinion), renamed "FO YYYY-NN" (Formal Opinion) in recent years — one continuous
series, ~134 opinions.

The Alabama State Bar is the state's UNIFIED (mandatory) bar.

Access (no JavaScript execution needed, no CAPTCHA, no auth):
  1. The opinions are listed on the public page
       https://www.alabar.org/office-of-general-counsel/formal-opinions/
     paginated via /page/N/ (20 per page, ~8 pages). Each row links to a detail
     page /office-of-general-counsel/formal-opinions/{slug}/ where {slug} is
     either the opinion number (e.g. 2013-01) or a topic slug (e.g.
     fee-splitting). The listing anchor text carries the opinion's topic title.
  2. Each detail page renders the opinion body in clean HTML inside
     <div class="content__inner"> — extracted directly with BeautifulSoup, NO
     PDF/OCR needed. The body opens with "ETHICS OPINION {RO|FO} YYYY-NN" and
     the QUESTION / ANSWER / DISCUSSION structure. (The linked PDF copies on
     alabar.org/assets/... are SCANNED images with no text layer, so the HTML
     detail page is the only clean-text path — and it is complete.)

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.AL-LegalEthics")

BASE = "https://www.alabar.org"
INDEX_PATH = "/office-of-general-counsel/formal-opinions/"
MAX_PAGES = 15  # generous ceiling (~8 real pages); loop stops on empty pages

DETAIL_RE = re.compile(
    r"/office-of-general-counsel/formal-opinions/([^/\"?#]+)/?$", re.I)

# "ETHICS OPINION FO 2023-01" / "ETHICS OPINION RO 2011-02" / "ETHICS OPINION
# R0-2015-01" (note the O/0 OCR-style variants) / "ETHICS OPINION 2013-01"
NUM_RE = re.compile(
    r"ETHICS\s+OPINION\s*(F[O0]|R[O0])?\s*[-#]?\s*((?:19|20)\d{2})\s*[-–]\s*(\d{1,3})",
    re.I)
# Number embedded in a linked PDF filename, e.g. .../FO-2026-01.pdf, .../RO-2011-02.pdf
PDF_NUM_RE = re.compile(
    r"/assets/[^\"']*?(F[O0]|R[O0])[-_ ]?((?:19|20)\d{2})[-_ ](\d{1,3})\.pdf", re.I)
# Older numeric-only PDF filenames, e.g. .../2013-01-1.pdf
PDF_PLAIN_RE = re.compile(
    r"/assets/[^\"']*?((?:19|20)\d{2})-(\d{1,3})(?:-\d+)?\.pdf", re.I)

MONTH_DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+((?:19|20)\d\d)\b",
    re.I,
)
_MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}


class ALLegalEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            ),
            "Accept": "text/html,*/*",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str) -> requests.Response | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                r = self._session.get(url, timeout=90)
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
    def _list_opinions(self) -> list[tuple[str, str, str]]:
        """Return [(slug, title, detail_url)] de-duplicated on slug,
        by walking the paginated listing until a page surfaces no new slugs."""
        out: dict[str, tuple[str, str, str]] = {}
        for page in range(1, MAX_PAGES + 1):
            url = BASE + INDEX_PATH + ("" if page == 1 else f"page/{page}/")
            r = self._get(url)
            if not r:
                break
            soup = BeautifulSoup(r.text, "html.parser")
            new_here = 0
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/page/" in href:
                    continue
                m = DETAIL_RE.search(href)
                if not m:
                    continue
                slug = m.group(1)
                if slug in ("formal-opinions",) or slug in out:
                    continue
                title = re.sub(r"\s+", " ", a.get_text(" ", strip=True))
                title = title.replace("View Opinion", "").strip(" -–—\xa0")
                durl = href if href.startswith("http") else BASE + (
                    href if href.startswith("/") else "/" + href)
                out[slug] = (slug, title, durl.split("#")[0])
                new_here += 1
            logger.info(f"  page {page}: +{new_here} (total {len(out)})")
            if new_here == 0 and page > 1:
                break
        result = list(out.values())
        logger.info(f"  discovered {len(result)} unique formal opinions")
        return result

    # -------------------------------------------------------- extraction
    @staticmethod
    def _clean(text: str) -> str:
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
        text = text.replace("​", "").replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _extract_body(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        div = soup.find("div", class_="content__inner")
        if div is None:
            return ""
        for tag in div.find_all(["script", "style", "nav", "form",
                                 "header", "footer"]):
            tag.decompose()
        for a in div.find_all("a", href=True):
            if "download" in a.get_text(" ", strip=True).lower():
                a.decompose()
        return self._clean(div.get_text("\n", strip=True))

    @staticmethod
    def _norm_prefix(p: str) -> str:
        return (p or "").upper().replace("F0", "FO").replace("R0", "RO")

    def _parse_number(self, text: str, html: str, slug: str) -> tuple[str, int] | None:
        """Return (canonical_number, year), trying in order: the body header,
        the linked PDF filename, then a numeric slug (YYYY-NN)."""
        m = NUM_RE.search(text)
        if m:
            prefix = self._norm_prefix(m.group(1))
            year, seq = int(m.group(2)), int(m.group(3))
            num = f"{prefix}-{year}-{seq:02d}" if prefix else f"{year}-{seq:02d}"
            return num, year
        pm = PDF_NUM_RE.search(html)
        if pm:
            prefix = self._norm_prefix(pm.group(1))
            year, seq = int(pm.group(2)), int(pm.group(3))
            return f"{prefix}-{year}-{seq:02d}", year
        pp = PDF_PLAIN_RE.search(html)
        if pp:
            year, seq = int(pp.group(1)), int(pp.group(2))
            return f"{year}-{seq:02d}", year
        sm = re.match(r"^((?:19|20)\d{2})-(\d{1,3})$", slug)
        if sm:
            return f"{int(sm.group(1))}-{int(sm.group(2)):02d}", int(sm.group(1))
        return None

    def _fetch_one(self, slug: str, title: str, url: str) -> dict | None:
        r = self._get(url)
        if not r or not r.text:
            return None
        text = self._extract_body(r.text)
        if len(text) < 150:
            logger.warning(f"  {slug}: insufficient text ({len(text)} chars)")
            return None

        parsed = self._parse_number(text, r.text, slug)
        if not parsed:
            logger.warning(f"  {slug}: no opinion number parsed, skipping")
            return None
        num, year = parsed

        # Date: opinions are dated by year (in the number). Prefer an explicit
        # in-range date in the body, else YYYY-01-01.
        date = f"{year}-01-01"
        for mm in MONTH_DATE_RE.finditer(text):
            mon, day, yr = mm.group(1).lower(), int(mm.group(2)), int(mm.group(3))
            if abs(yr - year) <= 1 and 1 <= day <= 31:
                date = f"{yr:04d}-{_MONTHS[mon]:02d}-{day:02d}"
                break

        return {
            "opinion_number": num,
            "title": title or f"Alabama State Bar Formal Opinion {num}",
            "text": text,
            "date": date,
            "url": url,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Alabama State Bar formal ethics opinions...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions listed")
            return False
        ok = 0
        for slug, title, url in ops[:3]:
            rec = self._fetch_one(slug, title, url)
            if rec and len(rec["text"]) > 300:
                logger.info(f"  Opinion {rec['opinion_number']} OK "
                            f"({len(rec['text'])} chars) date={rec['date']}")
                ok += 1
            else:
                logger.warning(f"  no text ({url})")
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["opinion_number"]
        return {
            "_id": f"US/AL-LegalEthics/{num}",
            "_source": "US/AL-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": "Alabama State Bar — Office of General Counsel / Disciplinary Commission",
            "title": raw.get("title") or f"Alabama State Bar Formal Opinion {num}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-AL",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        seen_nums: set[str] = set()
        for slug, title, url in self._list_opinions():
            rec = self._fetch_one(slug, title, url)
            if not rec:
                continue
            if rec["opinion_number"] in seen_nums:
                continue
            seen_nums.add(rec["opinion_number"])
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

    parser = argparse.ArgumentParser(description="US/AL-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ALLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
