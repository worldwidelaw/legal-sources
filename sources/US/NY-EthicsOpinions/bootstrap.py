#!/usr/bin/env python3
"""
US/NY-EthicsOpinions -- New York State Commission on Ethics and Lobbying in
Government (COELIG) and its predecessors -- Formal Advisory Opinions.

Fetches the full text of the formal advisory opinions of the New York State
ethics regulator interpreting the Public Officers Law (ss 73, 73-a, 74), the
Legislative Law and the ethics/lobbying provisions those bodies administer. A
formal advisory opinion is the Commission's written interpretation of those
statutes, requested by a State officer, employee, agency or lobbyist =
doctrine. The corpus spans 1988-present across the successive regulators:

  - Commission on Ethics and Lobbying in Government (COELIG, 2022-present)
  - Joint Commission on Public Ethics (JCOPE, 2011-2022)
  - Commission on Public Integrity (2007-2011)
  - NYS Ethics Commission / Temporary Lobbying Commission (1988-2007)

Access (no JavaScript, no CAPTCHA, no auth):
  The site (ethics.ny.gov) is Drupal. The advisory-opinions Views listing

      https://ethics.ny.gov/ethics-advisory-opinions?page=N   (N = 0..~35)

  enumerates every opinion as a node link whose text is the caption ("Advisory
  Opinion No. 25-03"). Each opinion node page carries a subject/summary and a
  "Download" link that redirects to the full born-digital opinion PDF at
  /system/files/documents/YYYY/MM/advisory-opinion-{code}.pdf. Full text comes
  from that PDF (clean text layer; OCR fallback for the oldest scans).

Strategy:
  Walk all listing pages, collect the title-link for every opinion (dedup by
  slug), fetch each node page, follow its Download link to the PDF and extract
  full text via the shared common.pdf_extract._extract backend chain. The issue
  date is parsed from the first "Month DD, YYYY" in the body; it falls back to
  Jan 1 of the year encoded in the opinion number. All records are doctrine.

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples (newest first)
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

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import _extract as _pdf_extract_bytes

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover
    BeautifulSoup = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NY-EthicsOpinions")

BASE_URL = "https://ethics.ny.gov"
LISTING_URL = "https://ethics.ny.gov/ethics-advisory-opinions?page={page}"
MAX_PAGE = 40  # pager runs 0..~35; a small margin, empty pages stop the walk.

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)

# Title-link anchors read like "Advisory Opinion No. 25-03" / "Advisory Opinion 24-03".
TITLE_RE = re.compile(r"^\s*Advisory Opinion", re.I)
# Opinion number token: NN-NN (year-seq), optionally prefixed "No.".
NUM_RE = re.compile(r"(\d{2,4})\s*-\s*(\d{1,3})")

MONTHS = (
    "January February March April May June July August September "
    "October November December"
).split()
DATE_RE = re.compile(
    r"\b(" + "|".join(MONTHS) + r")\s+(\d{1,2}),?\s+((?:19|20)\d{2})\b"
)


def _year_from_yy(token: str) -> int:
    """Map an opinion-number year token to a full year.

    NY numbers use a 2-digit year: 88-99 -> 19xx (the Commission started in
    1988), 00-87 -> 20xx. A 4-digit token is used verbatim.
    """
    if len(token) == 4:
        return int(token)
    yy = int(token)
    return 1900 + yy if yy >= 88 else 2000 + yy


def _iso_from_body(text: str) -> str | None:
    m = DATE_RE.search(text or "")
    if not m:
        return None
    mo = MONTHS.index(m.group(1)) + 1
    d = int(m.group(2))
    y = int(m.group(3))
    if 1 <= d <= 31:
        return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


class NYEthicsOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})

    # ---------------------------------------------------------------- http
    def _get(self, url: str, **kw):
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                return self.session.get(url, timeout=60, allow_redirects=True, **kw)
            except Exception as e:
                logger.warning(f"GET failed for {url} (attempt {attempt + 1}): {e}")
                time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    def _parse_listing_page(self, page: int) -> list[dict]:
        r = self._get(LISTING_URL.format(page=page))
        if r is None or r.status_code != 200:
            return []
        if BeautifulSoup is None:
            logger.error("BeautifulSoup unavailable — cannot parse listing")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        rows: list[dict] = []
        for a in soup.find_all("a", href=True):
            caption = _clean(a.get_text(" ", strip=True))
            if not TITLE_RE.match(caption):
                continue
            href = a["href"]
            if not href.startswith("/advisory-opinion"):
                continue
            m = NUM_RE.search(caption) or NUM_RE.search(href)
            if not m:
                continue
            if href.startswith("/"):
                href = BASE_URL + href
            rows.append({
                "node_url": href.rstrip("/"),
                "caption": caption,
                "num_year": m.group(1),
                "num_seq": int(m.group(2)),
            })
        return rows

    def _collect_index(self) -> list[dict]:
        by_url: dict[str, dict] = {}
        empty_streak = 0
        for page in range(0, MAX_PAGE + 1):
            rows = self._parse_listing_page(page)
            if not rows:
                empty_streak += 1
                if empty_streak >= 2:
                    break
                continue
            empty_streak = 0
            for row in rows:
                by_url.setdefault(row["node_url"], row)
            logger.info(f"page {page}: {len(rows)} opinion links "
                        f"({len(by_url)} distinct so far)")
        ordered = sorted(
            by_url.values(),
            key=lambda r: (_year_from_yy(r["num_year"]), r["num_seq"]),
            reverse=True,
        )
        logger.info(f"Index collected: {len(ordered)} distinct opinions")
        return ordered

    def _find_pdf_url(self, node_html: str) -> str | None:
        """Locate the full-text PDF from an opinion node page.

        Preference: the 'Download' link (redirects to the canonical PDF). Fall
        back to any /system/files/*.pdf whose name matches the opinion number.
        """
        soup = BeautifulSoup(node_html, "html.parser")
        for a in soup.find_all("a", href=True):
            if _clean(a.get_text()).lower() == "download":
                href = a["href"]
                return BASE_URL + href if href.startswith("/") else href
        return None

    def _fetch_one(self, row: dict) -> dict | None:
        rn = self._get(row["node_url"])
        if rn is None or rn.status_code != 200:
            return None
        pdf_url = self._find_pdf_url(rn.text)
        text = ""
        if pdf_url:
            rp = self._get(pdf_url)
            if rp is not None and rp.status_code == 200 and rp.content[:5].startswith(b"%PDF"):
                text = (_pdf_extract_bytes(rp.content) or "").strip()
        # Fallback: inline node text (older opinions that are HTML, not PDF).
        if len(text) < 200:
            soup = BeautifulSoup(rn.text, "html.parser")
            for tag in soup.select("header, footer, nav, script, style, form"):
                tag.decompose()
            main = soup.select_one("main, article, .region-content")
            inline = _clean(main.get_text(" ", strip=True)) if main else ""
            if len(inline) > len(text):
                text = inline
        if len(text) < 200:
            logger.warning(f"  {row['caption']}: thin text ({len(text)} chars) — skipped")
            return None
        out = dict(row)
        out["text"] = text
        out["pdf_url"] = pdf_url
        out["date"] = _iso_from_body(text) or f"{_year_from_yy(row['num_year']):04d}-01-01"
        return out

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for row in self._collect_index():
            rec = self._fetch_one(row)
            if rec:
                yield rec
                emitted += 1
                logger.info(f"  {rec['caption']} OK ({len(rec['text'])} chars, "
                            f"date={rec['date']})")
                if sample and emitted >= 12:
                    return

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing NY ethics advisory opinions...")
        rows = self._parse_listing_page(0)
        if len(rows) < 5:
            logger.error(f"API test FAILED: listing page 0 too small ({len(rows)})")
            return False
        ok = 0
        for row in rows[:4]:
            rec = self._fetch_one(row)
            if rec and len(rec["text"]) > 200:
                logger.info(f"  {rec['caption']} OK ({len(rec['text'])} chars, "
                            f"date={rec['date']})")
                ok += 1
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: could not extract full text")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        year = _year_from_yy(raw["num_year"])
        number = f"{raw['num_year']}-{raw['num_seq']:02d}"
        return {
            "_id": f"US/NY-EthicsOpinions/{year}-{raw['num_seq']:02d}",
            "_source": "US/NY-EthicsOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": f"Advisory Opinion {number}",
            "issuer": "New York State Commission on Ethics and Lobbying in Government",
            "title": raw.get("caption") or f"Advisory Opinion {number}",
            "text": raw["text"],
            "url": raw.get("pdf_url") or raw["node_url"],
            "node_url": raw["node_url"],
            "date": raw.get("date"),
            "jurisdiction": "US-NY",
        }

    # ------------------------------------------------------------- fetch
    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            date = raw.get("date")
            if not since or (date and date >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/NY-EthicsOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NYEthicsOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
