#!/usr/bin/env python3
"""
US/DE-EthicsOpinions -- Delaware Public Integrity Commission (PIC) --
Advisory Opinion Synopses & Case Decisions.

Fetches the full text of the Delaware Public Integrity Commission's published
advisory-opinion synopses and case decisions under the State Employees',
Officers' and Officials' Code of Conduct (29 Del. C. ch. 58), the Financial
Disclosure law, the Lobbying law, and the dual-compensation policy. The PIC
publishes its opinion corpus as topic-consolidated, born-digital PDFs (each
compiling every opinion on that topic, 1991–2023, with the applicable statute
and the facts/holding of each matter). Official public records = doctrine.

Access (no CAPTCHA, no auth, no JavaScript engine needed):
  Six server-rendered WordPress listing pages on depic.delaware.gov each link
  the topic PDFs under /wp-content/uploads/. This scraper scans all six,
  collects every wp-content PDF, dedups by URL, and extracts full text via the
  shared common.pdf_extract backend (born-digital; OCR fallback for any scan).

Usage:
  python bootstrap.py bootstrap            # Full pull (all topic PDFs)
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
from typing import Generator, Optional
from urllib.parse import urljoin

import requests
from requests.utils import requote_uri
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import _extract as _pdf_extract_bytes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.DE-EthicsOpinions")

BASE_URL = "https://depic.delaware.gov"

# The six PIC listing pages that link the opinion / case-decision PDFs.
LISTING_PAGES = [
    ("Code of Conduct", "/code-of-conduct/opinion-synopsis/"),
    ("Code of Conduct", "/code-of-conduct/pic-casedecisions/"),
    ("Financial Disclosure", "/financial-disclosure/opinion-synopses/"),
    ("Lobbying", "/lobbying/lobbying-opinion-synopses/"),
    ("Compensation Policy", "/compensation-policy/case-decisions/"),
    ("Compensation Policy", "/compensation-policy/synopses-opinions/"),
]

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)

YEAR_RE = re.compile(r"((?:19|20)\d{2})")


def _title_from_slug(stem: str) -> str:
    t = stem.replace("_", " ").replace("-", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t


class DEEthicsOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})

    # ---------------------------------------------------------------- http
    def _get(self, url: str):
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                return self.session.get(url, timeout=90, allow_redirects=True)
            except Exception as e:
                logger.warning(f"GET failed for {url} (attempt {attempt + 1}): {e}")
                time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    def _collect_index(self) -> list[dict]:
        rows: list[dict] = []
        seen: set[str] = set()
        for category, path in LISTING_PAGES:
            r = self._get(urljoin(BASE_URL, path))
            if r is None or r.status_code != 200:
                logger.warning(f"listing page failed: {path}")
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if ".pdf" not in href.lower() or "/wp-content/uploads/" not in href.lower():
                    continue
                pdf_url = requote_uri(urljoin(BASE_URL, href))
                # normalise scheme (some links are http://)
                pdf_url = pdf_url.replace("http://depic", "https://depic")
                if pdf_url in seen:
                    continue
                # skip obvious non-opinion assets (favicons handled by ext already)
                stem = Path(pdf_url.split("?")[0]).stem
                if stem.lower().startswith("cropped-"):
                    continue
                seen.add(pdf_url)
                anchor = a.get_text(" ", strip=True)
                rows.append({
                    "slug": stem,
                    "pdf_url": pdf_url,
                    "category": category,
                    "anchor": anchor,
                })
        logger.info(f"Index: {len(rows)} PIC opinion/decision PDFs")
        return rows

    # ------------------------------------------------------------- fetch1
    def _fetch_one(self, row: dict) -> Optional[dict]:
        r = self._get(row["pdf_url"])
        if r is None or r.status_code != 200 or not r.content:
            logger.warning(f"  {row.get('slug')}: PDF download failed — skipped")
            return None
        if not r.content[:5].startswith(b"%PDF"):
            logger.warning(f"  {row.get('slug')}: not a PDF — skipped")
            return None
        text = (_pdf_extract_bytes(r.content) or "").strip()
        if len(text) < 200:
            logger.warning(f"  {row.get('slug')}: thin text ({len(text)} chars) — skipped")
            return None
        out = dict(row)
        out["text"] = text
        out["date"] = self._year_date(row["slug"], row.get("anchor"))
        out["pdf_final_url"] = r.url
        return out

    @staticmethod
    def _year_date(slug: str, anchor: Optional[str]) -> Optional[str]:
        # topic PDFs span a range (e.g. 1991-2023); use the latest year as the
        # coverage/publication anchor date.
        yrs = YEAR_RE.findall(f"{slug} {anchor or ''}")
        if yrs:
            y = max(int(x) for x in yrs)
            if 1980 <= y <= 2100:
                return f"{y:04d}-01-01"
        return None

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for row in self._collect_index():
            rec = self._fetch_one(row)
            if rec:
                yield rec
                emitted += 1
                logger.info(f"  {rec.get('slug')} OK ({len(rec['text'])} chars, "
                            f"date={rec['date']})")
                if sample and emitted >= 12:
                    return

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Delaware PIC opinion synopses / case decisions...")
        rows = self._collect_index()
        if len(rows) < 10:
            logger.error(f"API test FAILED: index too small ({len(rows)})")
            return False
        ok = 0
        for row in rows[:5]:
            rec = self._fetch_one(row)
            if rec and len(rec["text"]) > 200:
                logger.info(f"  {rec.get('slug')} OK ({len(rec['text'])} chars)")
                ok += 1
        if ok >= 3:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: could not extract full text")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        anchor = (raw.get("anchor") or "").strip()
        title = anchor if len(anchor) > 4 else _title_from_slug(raw["slug"])
        return {
            "_id": f"US/DE-EthicsOpinions/{raw['slug']}",
            "_source": "US/DE-EthicsOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "category": raw.get("category"),
            "document_type": "Advisory Opinion Synopsis",
            "issuer": "Delaware Public Integrity Commission",
            "title": f"Delaware PIC — {title}",
            "text": raw["text"],
            "url": raw.get("pdf_final_url") or raw["pdf_url"],
            "date": raw.get("date"),
            "jurisdiction": "US-DE",
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

    parser = argparse.ArgumentParser(description="US/DE-EthicsOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = DEEthicsOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
