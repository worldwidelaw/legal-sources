#!/usr/bin/env python3
"""
US/MT-BOPA -- Montana Board of Personnel Appeals (BOPA) Decisions

Fetches the full text of the published decisions of the Montana Board of
Personnel Appeals (BOPA), the tribunal within the Montana Department of Labor &
Industry (Employment Standards Division) that adjudicates public-sector
collective-bargaining disputes for the State of Montana, its political
subdivisions, and school districts under the Montana Public Employees Collective
Bargaining Act (Title 39, Ch. 31, MCA). BOPA resolves interest-arbitration
impasses (including firefighter and police interest arbitration), fact-finding
recommendations, and employee-classification appeals. Each numbered award /
decision resolves a specific contested case = case_law, and they are official
Montana state-government works in the public domain (government edicts).

BUILD RECIPE (builds + validates LOCALLY):
The BOPA "Case Decision Index" at erd.dli.mt.gov publishes its born-digital
decision PDFs across four HTML category pages under
  /labor-standards/collective-bargaining/board-of-personnel-appeals/
    case-decision-index/{category}
where {category} is one of: Employee-Classification-Decisions,
fact-finder-decisions, firefighter-factfinding, police-interest-arbitration-
awards. Each page lists decision PDFs whose hrefs resolve to
erd.dli.mt.gov/_docs/labor-standards/collective-bargaining/BOPA/{file}.pdf (a
handful under /labor-standards/). We fetch the four category pages, extract +
de-duplicate the PDF links (dropping a stray non-decision IT-policy link that
lives on mt.gov), download each PDF, extract full text with common.pdf_extract,
and parse the case number, decision date, and parties from the body/filename.
No auth, no CAPTCHA.

NOTE: the bulk board decisions (unfair-labor-practice / unit-determination
orders, ~cbdecNNNN legacy PDFs) are only reachable via the ebizws.mt.gov
ERD_PUBLICPORTAL search, which sits behind an F5/Shape (TSPD) bot-challenge and
cannot be enumerated without a browser; the legacy dli.mt.gov/hearings/decisions
tree serves individual PDFs but its directory index is 403 and has no Wayback
preservation. Those are a VPS/browser-only future extension.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import urljoin, unquote, urlparse

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.MT-BOPA")

BASE = "https://erd.dli.mt.gov"
INDEX = (BASE + "/labor-standards/collective-bargaining/"
         "board-of-personnel-appeals/case-decision-index/")

# The four BOPA decision categories and the human-readable label we store.
CATEGORIES = {
    "Employee-Classification-Decisions": "Employee Classification Decision",
    "fact-finder-decisions": "Fact-Finder Decision",
    "firefighter-factfinding": "Firefighter Interest Arbitration Award",
    "police-interest-arbitration-awards": "Police Interest Arbitration Award",
}

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

PDF_HREF_RE = re.compile(r'href="([^"]+?\.pdf)"', re.IGNORECASE)

# Montana labor case numbers. The modern (post-2020) format is the reliable
# one, e.g. "Case No. 2025DRS00184", "CASE NO 2024DRS00050"; older matters use
# "ULP No. 4-2010" / "Case No. 22-2011". Kept tight to avoid false positives on
# stray "No. NNNN-YYYY" citations in the body.
CASE_RE = re.compile(
    r"(?:Case|ULP|Unit\s+Determination|UD)\s+Nos?\.?\s*[:#]?\s*"
    r"(20\d{2}DRS\d{3,6}|\d{1,3}-20\d{2})",
    re.IGNORECASE)

LONGDATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2}),\s+((?:19|20)\d{2})\b")

MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}

# Filename date tokens: 2025.12.19, 8142024, 12714, 4-19-2010, 3-31-2021,
# 11142022, 892018 ...  Handled defensively in _date_from_fname.


class BOPAScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.5
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": UA})
        self._existing: set[str] = set()

    # ---------------------------------------------------------------- http
    def _get_text(self, url: str) -> str | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, timeout=(15, 120))
                resp.raise_for_status()
                return resp.text
            except Exception as e:
                logger.warning(f"GET failed ({url[:90]}) attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    def _get_bytes(self, url: str) -> bytes | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, timeout=(15, 180), stream=True)
                resp.raise_for_status()
                return resp.content
            except Exception as e:
                logger.warning(f"PDF GET failed ({url[:90]}) attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _slug_from_url(url: str) -> str:
        fname = unquote(urlparse(url).path.rsplit("/", 1)[-1])
        base = re.sub(r"(?i)\.pdf$", "", fname)
        return re.sub(r"[^A-Za-z0-9]+", "-", base).strip("-").lower()

    @staticmethod
    def _title_from_fname(url: str) -> str:
        fname = unquote(urlparse(url).path.rsplit("/", 1)[-1])
        base = re.sub(r"(?i)\.pdf$", "", fname)
        base = re.sub(r"[_\-]+", " ", base)
        base = re.sub(r"\s+", " ", base).strip()
        return base

    @staticmethod
    def _iso_from_longdate(m: re.Match) -> str | None:
        mon = MONTHS.get(m.group(1).lower())
        if not mon:
            return None
        try:
            d, y = int(m.group(2)), int(m.group(3))
        except ValueError:
            return None
        if 1 <= d <= 31 and 1970 <= y <= 2100:
            return f"{y:04d}-{mon:02d}-{d:02d}"
        return None

    @staticmethod
    def _date_from_fname(url: str) -> str | None:
        fname = unquote(urlparse(url).path.rsplit("/", 1)[-1])
        # 2025.12.19  or 2025-12-19
        m = re.search(r"(20\d{2})[.\-](\d{1,2})[.\-](\d{1,2})", fname)
        if m:
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if 1 <= mo <= 12 and 1 <= d <= 31:
                return f"{y:04d}-{mo:02d}-{d:02d}"
        # 4-19-2010 / 3-31-2021 / 12-14-2015 / 5-11-2015
        m = re.search(r"\b(\d{1,2})[.\-](\d{1,2})[.\-](20\d{2})\b", fname)
        if m:
            mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if 1 <= mo <= 12 and 1 <= d <= 31:
                return f"{y:04d}-{mo:02d}-{d:02d}"
        # Compact leading token: 11142022, 8142024, 6282019, 7132021, 892018
        m = re.match(r"(\d{5,8})[-_]", fname)
        if m:
            tok = m.group(1)
            for mlen in (2, 1):
                for dlen in (2, 1):
                    if mlen + dlen + 4 == len(tok):
                        mo = int(tok[:mlen]); d = int(tok[mlen:mlen + dlen])
                        y = int(tok[mlen + dlen:])
                        if 1 <= mo <= 12 and 1 <= d <= 31 and 2000 <= y <= 2099:
                            return f"{y:04d}-{mo:02d}-{d:02d}"
        return None

    # --------------------------------------------------------- discovery
    def discover(self) -> list[dict]:
        entries: list[dict] = []
        seen: set[str] = set()
        for cat_slug, cat_label in CATEGORIES.items():
            page = self._get_text(INDEX + cat_slug)
            if not page:
                logger.warning(f"Category page failed: {cat_slug}")
                continue
            found = 0
            for href in PDF_HREF_RE.findall(page):
                absu = urljoin(INDEX + cat_slug, href)
                # Keep only decision PDFs hosted on erd.dli.mt.gov; this drops
                # the stray non-decision IT-policy link (mt.gov/1240-X06.pdf).
                if urlparse(absu).netloc != "erd.dli.mt.gov":
                    continue
                rid = self._slug_from_url(absu)
                if rid in seen:
                    continue
                seen.add(rid)
                entries.append({"url": absu, "record_id": rid,
                                "category": cat_label})
                found += 1
            logger.info(f"  {cat_slug}: {found} decision PDFs")
        logger.info(f"Discovered {len(entries)} unique BOPA decision PDFs")
        return entries

    # ------------------------------------------------------- build record
    def _build_raw(self, entry: dict) -> dict | None:
        source_id = entry["record_id"]
        if source_id in self._existing:
            return None
        pdf_bytes = self._get_bytes(entry["url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/MT-BOPA", source_id, pdf_bytes=pdf_bytes, table="case_law"
        )
        if not text or len(text.strip()) < 400:
            logger.warning(f"No usable text for {source_id} "
                           f"({len(text or '')} chars) — skipping")
            return None
        text = text.strip()

        case_number = None
        m = CASE_RE.search(text[:3000])
        if m:
            case_number = m.group(1).upper().strip(" .,-")[:40]

        # Decision date: prefer the filename date token (these decision files
        # are named with the issue/award date, which is far more reliable than
        # scraping a body long-date that may be a CBA term or a cited date).
        # Fall back to the last long-date in the body (typically the signature
        # date of the award/recommendation).
        date = self._date_from_fname(entry["url"])
        if not date:
            spans = list(LONGDATE_RE.finditer(text))
            if spans:
                date = self._iso_from_longdate(spans[-1])

        title = "Montana BOPA — " + self._title_from_fname(entry["url"])[:180]

        return {
            "record_id": source_id,
            "case_number": case_number,
            "category": entry["category"],
            "title": _html.unescape(title)[:500],
            "text": text,
            "date": date,
            "url": entry["url"],
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Montana BOPA decision index...")
        try:
            entries = self.discover()
            if not entries:
                logger.error("  No decision PDFs discovered")
                return False
            raw = None
            for e in entries[:6]:
                raw = self._build_raw(e)
                if raw:
                    break
            if raw and raw["text"] and len(raw["text"]) > 400:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('case_number')} [{raw.get('date')}]")
                logger.info("API test PASSED")
                return True
            logger.error("  Text extraction failed")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"US/MT-BOPA/{raw['record_id']}",
            "_source": "US/MT-BOPA",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["record_id"],
            "case_number": raw.get("case_number") or None,
            "category": raw.get("category") or None,
            "issuer": "Montana Board of Personnel Appeals (BOPA)",
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-MT",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        if not sample:
            try:
                self._existing = preload_existing_ids("US/MT-BOPA", "case_law")
            except Exception as e:
                logger.warning(f"preload_existing_ids failed: {e}")
                self._existing = set()
        emitted = 0
        for entry in self.discover():
            raw = self._build_raw(entry)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 15:
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

    parser = argparse.ArgumentParser(description="US/MT-BOPA bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = BOPAScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
