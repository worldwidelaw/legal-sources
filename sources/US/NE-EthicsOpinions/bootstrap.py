#!/usr/bin/env python3
"""
US/NE-EthicsOpinions -- Nebraska Accountability and Disclosure Commission
(NADC) -- Advisory Opinions.

Fetches the full text of the advisory opinions of the Nebraska Accountability
and Disclosure Commission (NADC) interpreting the Nebraska Political
Accountability and Disclosure Act (NPADA, Neb. Rev. Stat. ch. 49) -- the state
conflict-of-interest, campaign-finance and lobbying provisions. Each advisory
opinion is the Commission's authoritative written interpretation issued on
request and published as a public record = doctrine (pd-us).

Access (no CAPTCHA, no auth, no JavaScript engine needed):
  nadc.nebraska.gov is a server-rendered Drupal site.
    - Index:  https://nadc.nebraska.gov/advisory-opinions
              lists every opinion as /advisory-opinion-{NNN} (001..206,
              contiguous). Slugs are 3-digit zero-padded sequential ints.
    - Opinion page: https://nadc.nebraska.gov/advisory-opinion-{NNN}
              A Drupal node whose labelled fields carry the opinion number,
              Date Adopted, Subject (Conflict of Interest / Campaign Finance /
              Lobbying), "Requested by" and Summary, and whose
              <div class="field--name-body"> holds the full opinion text as
              clean born-digital HTML.

Full text:
  Most opinions carry the complete text inline in the body field (no OCR). A
  few of the newest opinions render only a summary in HTML and attach the full
  text as a born-digital/scanned PDF at
      https://nadc.nebraska.gov/sites/default/files/doc/Advisory Opinion-{NNN}.pdf
  When the HTML body is thin, the scraper reads the attached PDF via the shared
  common.pdf_extract backend (text layer; OCR fallback for scans).

All records are advisory opinions = doctrine.

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
from typing import Generator, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import _extract as _pdf_extract_bytes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NE-EthicsOpinions")

BASE_URL = "https://nadc.nebraska.gov"
INDEX_URL = f"{BASE_URL}/advisory-opinions"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)

OPINION_HREF_RE = re.compile(r"/advisory-opinion-(\d{1,4})\b")
PDF_HREF_RE = re.compile(r"/sites/default/files/doc/[^\"'<> ]*\.pdf", re.I)

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}
# "Tuesday, November 15, 1977 - 12:00"
DATE_RE = re.compile(
    r"(" + "|".join(MONTHS) + r")\s+(\d{1,2}),?\s+((?:19|20)\d{2})", re.I
)


def _iso_date(text: str) -> Optional[str]:
    m = DATE_RE.search(text or "")
    if not m:
        return None
    mo = MONTHS[m.group(1).lower()]
    d = int(m.group(2))
    y = int(m.group(3))
    if 1 <= d <= 31:
        return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


def _field(art, name: str) -> Optional[str]:
    """Text of a Drupal labelled field, with its label stripped."""
    el = art.select_one(f"div.field--name-{name}")
    if not el:
        return None
    txt = el.get_text(" ", strip=True)
    # strip the leading human label (e.g. "Date Adopted ", "Subject ")
    label = {
        "field-date-adopted": "Date Adopted",
        "field-subject": "Subject",
        "field-requested-by": "Requested by",
        "field-summary": "Summary",
        "field-opinion-number": "Opinion number",
    }.get(name)
    if label and txt.startswith(label):
        txt = txt[len(label):].strip()
    return txt or None


class NEEthicsScraper(BaseScraper):

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
                return self.session.get(url, timeout=60, allow_redirects=True)
            except Exception as e:
                logger.warning(f"GET failed for {url} (attempt {attempt + 1}): {e}")
                time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    def _opinion_numbers(self) -> list[int]:
        r = self._get(INDEX_URL)
        if r is None or r.status_code != 200:
            logger.error("Could not fetch opinions index")
            return []
        nums = sorted({int(m) for m in OPINION_HREF_RE.findall(r.text)})
        logger.info(f"Index: {len(nums)} opinions "
                    f"({nums[0]:03d}-{nums[-1]:03d})" if nums else "no opinions")
        return nums

    # ------------------------------------------------------------- fetch1
    def _fetch_one(self, num: int) -> Optional[dict]:
        url = f"{BASE_URL}/advisory-opinion-{num:03d}"
        r = self._get(url)
        if r is None or r.status_code != 200:
            logger.warning(f"  {num:03d}: page fetch failed — skipped")
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        art = soup.select_one("article") or soup
        body_el = art.select_one("div.field--name-body")
        body_text = body_el.get_text("\n", strip=True) if body_el else ""

        subject = _field(art, "field-subject")
        requested_by = _field(art, "field-requested-by")
        summary = _field(art, "field-summary")
        date_field = _field(art, "field-date-adopted")
        date = _iso_date(date_field or "")

        # Full text: prefer the inline HTML body; if thin, fall back to the
        # attached born-digital PDF (OCR fallback inside pdf_extract).
        text = body_text
        pdf_used = None
        if len(text) < 400:
            m = PDF_HREF_RE.search(r.text)
            if m:
                pdf_url = urljoin(BASE_URL, m.group(0))
                pr = self._get(pdf_url)
                if pr is not None and pr.status_code == 200 and \
                        pr.content[:5].startswith(b"%PDF"):
                    ptext = (_pdf_extract_bytes(pr.content) or "").strip()
                    if len(ptext) > len(text):
                        text = ptext
                        pdf_used = pr.url
        text = text.strip()
        if len(text) < 200:
            logger.warning(f"  {num:03d}: thin text ({len(text)} chars) — skipped")
            return None
        return {
            "number": f"{num:03d}",
            "subject": subject,
            "requested_by": requested_by,
            "summary": summary,
            "date": date,
            "url": url,
            "pdf_url": pdf_used,
            "text": text,
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        nums = self._opinion_numbers()
        # newest first so the sample skews to recent opinions
        for num in sorted(nums, reverse=True):
            rec = self._fetch_one(num)
            if rec:
                yield rec
                logger.info(f"  {rec['number']} OK ({len(rec['text'])} chars, "
                            f"date={rec['date']}, subject={rec['subject']})")
                if sample:
                    self._sample_emitted = getattr(self, "_sample_emitted", 0) + 1
                    if self._sample_emitted >= 12:
                        return

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Nebraska NADC advisory opinions...")
        nums = self._opinion_numbers()
        if len(nums) < 100:
            logger.error(f"API test FAILED: index too small ({len(nums)})")
            return False
        ok = 0
        for num in nums[:5]:
            rec = self._fetch_one(num)
            if rec and len(rec["text"]) > 200:
                logger.info(f"  {rec['number']} OK ({len(rec['text'])} chars)")
                ok += 1
        if ok >= 3:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: could not extract full text")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw.get("number")
        subj = raw.get("subject")
        title = f"Nebraska NADC Advisory Opinion No. {number}"
        if subj:
            title += f" — {subj}"
        return {
            "_id": f"US/NE-EthicsOpinions/{number}",
            "_source": "US/NE-EthicsOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "document_type": "Advisory Opinion",
            "issuer": "Nebraska Accountability and Disclosure Commission",
            "subject": subj,
            "requested_by": raw.get("requested_by"),
            "summary": raw.get("summary"),
            "title": title,
            "text": raw["text"],
            "url": raw.get("url"),
            "pdf_url": raw.get("pdf_url"),
            "date": raw.get("date"),
            "jurisdiction": "US-NE",
        }

    # ------------------------------------------------------------- fetch
    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        self._sample_emitted = 0
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        self._sample_emitted = 0
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            date = raw.get("date")
            if not since or (date and date >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/NE-EthicsOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NEEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
