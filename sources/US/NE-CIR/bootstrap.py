#!/usr/bin/env python3
"""
US/NE-CIR -- Nebraska Commission of Industrial Relations — CIR Reporter Decisions

Fetches the full text of the published decisions of the Nebraska Commission of
Industrial Relations (CIR), formerly the Court of Industrial Relations — the
state's independent quasi-judicial tribunal that resolves public-sector labor
disputes (wage/benefit comparability determinations, bargaining-unit
determination and clarification, representation controversies, and unfair /
prohibited labor practices under Neb. Rev. Stat. Ch. 48, art. 8). Each decision
resolves a specific contested case = case_law, and Nebraska state-tribunal
decisions are public-domain government works (government-edicts doctrine,
17 U.S.C. § 105 analogue).

Access (no JavaScript, no CAPTCHA, no auth):
  The CIR Reporter is published online through the State of Nebraska portal at
  https://www.nebraska.gov/ncir/reporter_and_appeals_search/. The "Reporter
  Search" landing enumerates bound reporter volumes 1..19 (1948–2007); each
  volume index

      index.cgi?dir={V}_CIR_xx&type=reporter

  lists every decision in that volume as a direct link to a born-digital HTML
  file under

      /ncir/reporter_and_appeals_search/data/reporter/{V}_CIR_xx/{V}_CIR_{page}_({year})_{caption}.html

  These HTML files are the ORIGINAL full-text decisions (FrontPage-authored,
  pure body content — no site chrome, no OCR needed). The filename encodes the
  reporter citation (volume, starting page, year) and the case caption.

Strategy:
  For each volume 1..20, GET the volume index, collect the decision .html links,
  fetch each decision page, strip the HTML to clean text, parse the citation and
  caption from the filename (and the "CASE NO." + date from the body), and
  normalize into the case_law schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (all reporter decisions)
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
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from bs4 import BeautifulSoup

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NE-CIR")

BASE_URL = "https://www.nebraska.gov"
INDEX_URL = (
    "https://www.nebraska.gov/ncir/reporter_and_appeals_search/index.cgi"
    "?dir={vol}_CIR_xx&type=reporter"
)
# Reporter volumes published online. 1..19 hold the bulk (1948–2007); 20 is
# included defensively (mostly migrated to the modern filings-opinions page).
MAX_VOLUME = 20

# Decision links look like:
#   /ncir/reporter_and_appeals_search/data/reporter/15_CIR_xx/15_CIR_84_%282005%29_...html
DECISION_HREF_RE = re.compile(
    r'(?:href=")?(/ncir/reporter_and_appeals_search/data/reporter/'
    r'\d+_CIR_xx/[^"\'>]+?\.html)',
    re.I,
)

# Parse citation + caption out of the decision filename (already URL-decoded):
#   "15_CIR_84_(2005)_State_Law_Enf_Barg_Council_v_State_of_NE.html"
#   "15_CIR_270,_278_(2006)_Central_City_Ed_Assn_v_...html"
#   "1_CIR_1_(1948).html"
# Non-greedy page capture up to the FIRST "(YYYY...)" group; the year paren may
# contain extra text (a range like "1963 to 1967" or a reversal note).
FILENAME_RE = re.compile(
    r"^(\d+)_CIR_(.+?)_\([^)]*?(\d{4})[^)]*\)[._]*(.*?)\.html$", re.I
)

# Trailing appellate / citation cruft frequently appended to a caption.
CAPTION_CUT_RE = re.compile(
    r"\s*(?:Reversed|Affirmed|Remanded|Modified|Appeal|appealed|Vacated|"
    r"\(\d{4}\)|\d+\s+Neb\.|\d+\s+CIR\b).*$",
    re.I,
)

CASE_NO_RE = re.compile(
    r"CASE\s+NOS?\.\s*(\d+(?:\s*(?:,|&|and)\s*\d+)*)", re.I
)

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


class NECIRScraper(BaseScraper):

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
        """Strip the FrontPage decision HTML to clean, readable plain text."""
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "head"]):
            tag.decompose()
        text = soup.get_text("\n")
        text = _html.unescape(text)
        # Collapse runs of blank space; keep paragraph breaks.
        text = re.sub(r"[ \t ]+", " ", text)
        text = re.sub(r"\n[ \t]*\n+", "\n\n", text)
        lines = [ln.strip() for ln in text.splitlines()]
        text = "\n".join(ln for ln in lines if ln)
        return text.strip()

    @staticmethod
    def _norm_body_date(text: str) -> str | None:
        m = DATE_RE.search(text)
        if not m:
            return None
        mo = MONTHS.get(m.group(1).lower())
        d = int(m.group(2))
        y = int(m.group(3))
        if mo and 1 <= d <= 31 and 1945 <= y <= 2035:
            return f"{y:04d}-{mo:02d}-{d:02d}"
        return None

    @classmethod
    def _parse_filename(cls, url: str) -> dict:
        name = unquote(url.rsplit("/", 1)[-1])
        m = FILENAME_RE.match(name)
        if not m:
            return {"volume": None, "page": None, "year": None, "caption": None}
        volume = int(m.group(1))
        page = m.group(2).replace("_", " ").strip()
        year = int(m.group(3))
        caption = _html.unescape(m.group(4).replace("_", " ")).strip()
        # Drop trailing appellate-history / citation cruft, then normalise the
        # "X v Y" party separator and squeeze spaces.
        caption = CAPTION_CUT_RE.sub("", caption)
        caption = re.sub(r"\bv\b\.?", "v.", caption)
        caption = re.sub(r"\s+", " ", caption).strip(" .,-&")
        return {
            "volume": volume,
            "page": page,
            "year": year,
            "caption": caption or None,
        }

    # ---------------------------------------------------------- discovery
    def _list_volume(self, vol: int) -> list[str]:
        html = self._curl_text(INDEX_URL.format(vol=vol))
        if not html:
            return []
        urls = []
        seen = set()
        for m in DECISION_HREF_RE.finditer(html):
            href = m.group(1)
            full = urljoin(BASE_URL, href)
            if full not in seen:
                seen.add(full)
                urls.append(full)
        return urls

    def _list_all(self) -> list[str]:
        all_urls = []
        for vol in range(1, MAX_VOLUME + 1):
            urls = self._list_volume(vol)
            if urls:
                logger.info(f"  volume {vol}: {len(urls)} decisions")
            all_urls.extend(urls)
        return all_urls

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing CIR Reporter index + HTML extraction...")
        urls = self._list_volume(15)
        if not urls:
            logger.error("API test FAILED: no decision links found in volume 15")
            return False
        logger.info(f"  discovered {len(urls)} decisions in volume 15")
        for url in urls[:5]:
            html = self._curl_text(url)
            if not html:
                continue
            text = self._clean_text(html)
            if len(text) > 400:
                meta = self._parse_filename(url)
                logger.info(
                    f"  {meta['volume']} CIR {meta['page']} ({meta['year']}) "
                    f"OK ({len(text)} chars) — {meta['caption']}")
                logger.info("API test PASSED")
                return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        vol = raw.get("volume")
        page = raw.get("page")
        year = raw.get("year")
        caption = raw.get("caption")
        citation = None
        if vol is not None and page is not None and year is not None:
            citation = f"{vol} CIR {page} ({year})"
        if caption and citation:
            title = f"{caption}, {citation}"
        elif caption:
            title = caption
        elif citation:
            title = f"Nebraska CIR, {citation}"
        else:
            title = "Nebraska Commission of Industrial Relations Decision"
        title = title[:300]

        # Stable id from the reporter citation (volume + starting page).
        if vol is not None and page is not None:
            slug = re.sub(r"[^0-9A-Za-z]+", "-", f"{vol}-CIR-{page}").strip("-")
        else:
            slug = re.sub(r"[^0-9A-Za-z]+", "-",
                          raw["url"].rsplit("/", 1)[-1]).strip("-")

        return {
            "_id": f"US/NE-CIR/{slug}",
            "_source": "US/NE-CIR",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "citation": citation,
            "case_number": raw.get("case_number"),
            "issuer": "Nebraska Commission of Industrial Relations",
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or (f"{year:04d}-01-01" if year else None),
            "jurisdiction": "US-NE",
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
            meta = self._parse_filename(url)
            cm = CASE_NO_RE.search(text)
            case_number = None
            if cm:
                case_number = re.sub(r"\s+", " ", cm.group(1)).strip(" ,.")
            date = self._norm_body_date(text)
            yield {
                **meta,
                "url": url,
                "text": text,
                "case_number": case_number,
                "date": date,
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

    parser = argparse.ArgumentParser(description="US/NE-CIR bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NECIRScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
