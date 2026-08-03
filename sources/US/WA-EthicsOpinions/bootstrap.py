#!/usr/bin/env python3
"""
US/WA-EthicsOpinions -- Washington State Executive Ethics Board — Advisory Opinions

Fetches the full text of the formal Advisory Opinions of the Washington State
Executive Ethics Board (EEB). Under the Ethics in Public Service Act (RCW 42.52),
the Board issues written advisory opinions that authoritatively construe the
ethics statutes it administers — conflicts of interest, use of state resources
(RCW 42.52.160), gifts, special privileges, honoraria, post-employment
restrictions and related standards of conduct for Washington state officers and
employees. Each opinion states the Board's official interpretation of the law on
the facts presented and is relied on by agencies statewide = doctrine (official
state legal interpretation; public-domain state-government work).

Access (no JavaScript, no CAPTCHA, no auth):
  The opinions are indexed in a single Drupal listing table:

      https://ethics.wa.gov/advisories/advisory-opinions

  Each table row is one advisory opinion; the row's title links to a born-digital
  full-text PDF under:

      https://ethics.wa.gov/sites/default/files/public/AO%20{NN-NN}[ suffix].pdf

  The PDFs carry a real text layer (no OCR needed). The listing is the
  authoritative index of which opinion numbers exist (numbered YY-NN, 1996-present).

Strategy:
  GET the listing page, parse the table to collect (opinion_number, pdf_url) for
  every row, download each PDF, extract its text with PyMuPDF (fitz), parse the
  approval date from the body ("APPROVAL DATE: Month YYYY") with a fallback to the
  YY prefix of the opinion number, and normalize into the doctrine schema.

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
logger = logging.getLogger("legal-data-hunter.US.WA-EthicsOpinions")

BASE_URL = "https://ethics.wa.gov"
LISTING_URL = "https://ethics.wa.gov/advisories/advisory-opinions"

# Opinion number in the first cell of each listing row, e.g. "25-01", "96-09A".
NUMBER_RE = re.compile(r"^\d{2}-\d{1,2}[A-Za-z]?$")

# Approval date printed in the opinion body, e.g. "APPROVAL DATE:  January 2011"
# or "APPROVAL DATE: October 15, 1996".
APPROVAL_RE = re.compile(
    r"APPROVAL\s+DATE:\s*"
    r"(?:(?P<mon>January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(?:(?P<day>\d{1,2}),\s*)?(?P<year>\d{4}))",
    re.I,
)
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}


class WAEthicsScraper(BaseScraper):

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

    # ------------------------------------------------------------- parsing
    @staticmethod
    def _pdf_text(pdf_bytes: bytes) -> str:
        """Extract clean plain text from a born-digital PDF via PyMuPDF."""
        import fitz  # PyMuPDF
        text_parts = []
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        try:
            for page in doc:
                text_parts.append(page.get_text("text"))
                page.clean_contents()  # release per-page cache (OOM guard)
        finally:
            doc.close()
        text = "\n".join(text_parts)
        # tidy whitespace
        text = re.sub(r"[ \t ]+", " ", text)
        text = re.sub(r"\n[ \t]*\n+", "\n\n", text)
        lines = [ln.rstrip() for ln in text.splitlines()]
        return "\n".join(lines).strip()

    def _norm_date(self, text: str, number: str) -> str | None:
        m = APPROVAL_RE.search(text)
        if m:
            mo = MONTHS.get((m.group("mon") or "").lower())
            year = int(m.group("year"))
            day = int(m.group("day")) if m.group("day") else 1
            if mo and 1970 <= year <= 2035 and 1 <= day <= 31:
                return f"{year:04d}-{mo:02d}-{day:02d}"
        # Fallback: derive the year from the YY prefix of the opinion number.
        ym = re.match(r"^(\d{2})-", number)
        if ym:
            yy = int(ym.group(1))
            year = 1900 + yy if yy >= 80 else 2000 + yy
            return f"{year:04d}-01-01"
        return None

    @staticmethod
    def _subject_of(row) -> str | None:
        cells = row.find_all(["td", "th"])
        if len(cells) >= 2:
            subj = cells[1].get_text(" ", strip=True)
            if subj:
                return subj[:200]
        return None

    # ---------------------------------------------------------- discovery
    def _list_all(self) -> list[dict]:
        """Return [{number, subject, pdf_url}] for every advisory opinion."""
        html = self._curl(LISTING_URL)
        if not html:
            logger.error("could not fetch advisory-opinion listing")
            return []
        soup = BeautifulSoup(html, "html.parser")
        seen: dict[str, dict] = {}
        for row in soup.select("table tr"):
            cells = row.find_all(["td", "th"])
            if not cells:
                continue
            number = cells[0].get_text(strip=True)
            if not NUMBER_RE.match(number):
                continue
            # The row's primary document is the first PDF link under the site.
            pdf_url = None
            for a in row.find_all("a"):
                href = a.get("href", "")
                if "/sites/default/files/" in href and href.lower().endswith(".pdf"):
                    pdf_url = urljoin(BASE_URL, href)
                    break
            if not pdf_url or number in seen:
                continue
            seen[number] = {
                "number": number,
                "subject": self._subject_of(row),
                "pdf_url": pdf_url,
            }
        # Sort newest-first by (year, seq) for stable, useful ordering.
        def sort_key(n: str):
            m = re.match(r"^(\d{2})-(\d{1,2})", n)
            yy, seq = int(m.group(1)), int(m.group(2))
            year = 1900 + yy if yy >= 80 else 2000 + yy
            return (-year, -seq)
        return [seen[k] for k in sorted(seen, key=sort_key)]

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing WA Executive Ethics Board index + PDF extraction...")
        items = self._list_all()
        if not items:
            logger.error("API test FAILED: no opinions found in listing")
            return False
        logger.info(f"  discovered {len(items)} advisory opinions")
        ok = 0
        for it in items[:5]:
            pdf = self._curl(it["pdf_url"], binary=True)
            if not pdf:
                continue
            try:
                text = self._pdf_text(pdf)
            except Exception as e:
                logger.warning(f"  extract failed for {it['number']}: {e}")
                continue
            if len(text) > 400:
                logger.info(f"  AO {it['number']} OK ({len(text)} chars) "
                            f"date={self._norm_date(text, it['number'])}")
                ok += 1
        if ok:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw.get("number")
        subject = raw.get("subject")
        title = f"Washington State Advisory Opinion {number}"
        if subject:
            title += f" — {subject}"
        return {
            "_id": f"US/WA-EthicsOpinions/{number}",
            "_source": "US/WA-EthicsOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "issuer": "Washington State Executive Ethics Board",
            "subject": subject,
            "title": title[:300],
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date"),
            "jurisdiction": "US-WA",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        items = self._list_all()
        emitted = 0
        for it in items:
            pdf = self._curl(it["pdf_url"], binary=True)
            if not pdf:
                logger.warning(f"  no PDF for AO {it['number']}")
                continue
            try:
                text = self._pdf_text(pdf)
            except Exception as e:
                logger.warning(f"  extract failed for AO {it['number']}: {e}")
                continue
            if len(text) < 400:
                logger.warning(f"  AO {it['number']}: only {len(text)} chars, skipping")
                continue
            yield {
                "number": it["number"],
                "subject": it["subject"],
                "pdf_url": it["pdf_url"],
                "text": text,
                "date": self._norm_date(text, it["number"]),
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

    parser = argparse.ArgumentParser(description="US/WA-EthicsOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WAEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
