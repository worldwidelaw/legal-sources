#!/usr/bin/env python3
"""
US/KS-EthicsOpinions -- Kansas Governmental Ethics Commission — Advisory Opinions

Fetches the full text of the advisory opinions published by the Kansas
Governmental Ethics Commission (administered via the Kansas Public Disclosure
Commission). Under K.S.A. 46-254 the Commission issues advisory opinions
construing the Kansas governmental-ethics statutes — the state
conflict-of-interest provisions (K.S.A. 46-215 et seq.), the campaign-finance
act (K.S.A. 25-4142 et seq.) and the lobbying-disclosure statutes. Each opinion
is a public record filed with the Secretary of State and published on
kansas.gov; it is the Commission's official written interpretation of the
ethics statutes = doctrine.

Access (no JavaScript, no CAPTCHA, no auth):
  Each opinion is a born-digital HTML page:

      https://www.kansas.gov/kpdc-opinion/opinion/view/{id}

  The <main id="main-content"> container holds the entire opinion body: the
  issue date, "Opinion No. YYYY-NN", the recipient, a Synopsis, the "Cited
  herein" statutes and the full opinion letter. No PDF, no OCR.

Enumeration:
  The site's three search endpoints (searchByArea / searchByKeyword /
  searchByOpinionNumber) each cap at 10 results server-side and ignore paging,
  so they expose only ~40 recent opinions. The full historical corpus is
  reached by a bounded /opinion/view/{id} scan: the ids form a near-contiguous
  block from ~660 (Opinion 1990-04) to ~2496 (Opinion 2024-03). A "miss" page
  renders the search form (no "Opinion No."); a "hit" page contains
  "Opinion No. YYYY-NN".

Strategy:
  fetch_sample walks the ~40 search-area ids (fast, spans years, validates the
  full-text extraction). fetch_all scans the bounded id range, keeps every page
  that carries an opinion number, extracts the <main> body text and normalizes.
  The decision date is parsed from the opinion body ("Month DD, YYYY") with a
  fallback to the opinion-number year.

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

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.KS-EthicsOpinions")

BASE_URL = "https://www.kansas.gov"
VIEW_URL = "https://www.kansas.gov/kpdc-opinion/opinion/view/{id}"
AREA_URL = "https://www.kansas.gov/kpdc-opinion/search/searchByArea?area={n}"

# Bounded id scan range for the full historical corpus (near-contiguous block).
SCAN_FLOOR = 640
SCAN_CEILING = 2510

# "Opinion No. YYYY-NN" — the defining marker of a real opinion page.
NUMBER_RE = re.compile(r"Opinion\s+No\.?\s*(\d{4})-(\d+)", re.I)

MONTHS = (
    "January|February|March|April|May|June|July|August|September|"
    "October|November|December"
)
DATE_RE = re.compile(rf"\b({MONTHS})\s+(\d{{1,2}}),\s+(\d{{4}})\b")
_MONTH_IDX = {
    m: i + 1
    for i, m in enumerate(
        "January February March April May June July August September "
        "October November December".split()
    )
}

VIEW_ID_RE = re.compile(r"/kpdc-opinion/opinion/view/(\d+)")


def _date_from_text(text: str) -> str | None:
    """First 'Month DD, YYYY' in the opinion body -> ISO date."""
    m = DATE_RE.search(text or "")
    if not m:
        return None
    mo = _MONTH_IDX.get(m.group(1).capitalize())
    d, y = int(m.group(2)), int(m.group(3))
    if mo and 1 <= d <= 31 and 1970 <= y <= 2100:
        return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


def _main_text(html: str) -> str:
    """Extract clean text from the <main id="main-content"> container."""
    start = html.find("<main")
    if start == -1:
        return ""
    end = html.find("</main>", start)
    frag = html[start:end] if end != -1 else html[start:]
    frag = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", frag, flags=re.S | re.I)
    frag = re.sub(r"<[^>]+>", " ", frag)
    frag = _html.unescape(frag)
    frag = re.sub(r"[ \t\r\f\v]+", " ", frag)
    frag = re.sub(r"\n\s*\n+", "\n\n", frag)
    return "\n".join(line.strip() for line in frag.splitlines() if line.strip()).strip()


class KSEthicsOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        # A plain non-browser UA; the accessKansas app serves full HTML to all.
        self._ua = "Mozilla/5.0 (compatible; LegalDataHunter/1.0)"

    # ---------------------------------------------------------------- http
    def _curl(self, url: str) -> str | None:
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "60", "-A", self._ua, url],
                    capture_output=True, timeout=90,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout.decode("utf-8", "replace")
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _parse_opinion(html: str) -> dict | None:
        """Parse a /opinion/view/{id} page into a raw record, or None on miss."""
        m = NUMBER_RE.search(html)
        if not m:
            return None
        year, seq = m.group(1), int(m.group(2))
        number = f"{year}-{seq:02d}"
        text = _main_text(html)
        if not text or NUMBER_RE.search(text) is None or len(text) < 200:
            return None
        date = _date_from_text(text) or f"{year}-01-01"
        # Title: opinion number + the Synopsis caption if present.
        title = f"Opinion No. {number}"
        sm = re.search(r"Synopsis:\s*(.+?)(?:\s*Cited herein|\s*Dear\b)", text, re.S)
        if sm:
            syn = re.sub(r"\s+", " ", sm.group(1)).strip()
            if syn:
                title = f"Opinion No. {number}: {syn[:200]}"
        return {
            "opinion_number": number,
            "title": title,
            "text": text,
            "date": date,
        }

    def _fetch_view(self, view_id: int) -> dict | None:
        html = self._curl(VIEW_URL.format(id=view_id))
        if not html:
            return None
        raw = self._parse_opinion(html)
        if raw:
            raw["view_id"] = view_id
            raw["url"] = VIEW_URL.format(id=view_id)
        return raw

    def _search_area_ids(self) -> list[int]:
        """The ~40 recent-opinion view ids exposed by the four search areas."""
        ids: list[int] = []
        seen = set()
        for area in (1, 2, 3, 4):
            html = self._curl(AREA_URL.format(n=area))
            if not html:
                continue
            for sid in VIEW_ID_RE.findall(html):
                i = int(sid)
                if i not in seen:
                    seen.add(i)
                    ids.append(i)
        return ids

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing KS Governmental Ethics Commission opinions...")
        ids = self._search_area_ids()
        if not ids:
            logger.error("API test FAILED: no opinion ids from search areas")
            return False
        logger.info(f"  discovered {len(ids)} recent opinion ids via search")
        ok = 0
        for view_id in ids[:5]:
            raw = self._fetch_view(view_id)
            if raw and len(raw["text"]) > 400:
                logger.info(f"  id {view_id} -> {raw['opinion_number']} "
                            f"({len(raw['text'])} chars) date={raw['date']}")
                ok += 1
        if ok:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw.get("opinion_number")
        _id = f"US/KS-EthicsOpinions/{number}" if number else \
            f"US/KS-EthicsOpinions/view-{raw.get('view_id')}"
        return {
            "_id": _id,
            "_source": "US/KS-EthicsOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "issuer": "Kansas Governmental Ethics Commission",
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-KS",
        }

    # ------------------------------------------------------------- fetch
    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize()).

        The ~40 recent search-area opinions are yielded first (they span years
        and give representative samples in --sample mode), then the full
        bounded id scan fills in the historical corpus, deduped by number.
        """
        seen: set[str] = set()
        hits = 0

        for view_id in self._search_area_ids():
            raw = self._fetch_view(view_id)
            if not raw or len(raw["text"]) < 400:
                continue
            num = raw["opinion_number"]
            if num in seen:
                continue
            seen.add(num)
            hits += 1
            yield raw

        for view_id in range(SCAN_FLOOR, SCAN_CEILING + 1):
            raw = self._fetch_view(view_id)
            if not raw:
                continue
            num = raw["opinion_number"]
            if num in seen:
                continue
            seen.add(num)
            hits += 1
            if hits % 100 == 0:
                logger.info(f"  scanned to id {view_id}: {hits} opinions so far")
            yield raw

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/KS-EthicsOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = KSEthicsOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
