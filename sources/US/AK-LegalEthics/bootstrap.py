#!/usr/bin/env python3
"""
US/AK-LegalEthics -- Alaska Bar Association — Ethics Opinions

Fetches the full text of the Ethics Opinions adopted by the Alaska Bar
Association's Ethics Committee / Board of Governors. Each opinion applies the
Alaska Rules of Professional Conduct — and, for the older opinions, the
predecessor Code of Professional Responsibility / Canons — to a stated question
to advise LAWYERS on the ethics of contemplated conduct = doctrine (advisory).

The Alaska Bar Association is the state's INTEGRATED (mandatory) bar, created by
the Alaska Integrated Bar Act and operating under the authority of the Alaska
Supreme Court to regulate the legal profession, so the opinion texts are the
work of a government-authorized body — treated as public domain under the
17 U.S.C. § 105 government-edicts rationale, consistent with the other
integrated state-bar legal-ethics sources. Published free to the public on
alaskabar.org, no login/paywall/reuse terms.

The corpus is a per-year numbered series "{year}-{N}" (older opinions use a
two-digit year, e.g. 73-1, 98-2; opinions from 2000 on use a four-digit year,
e.g. 2003-3, 2012-3, 2025-1), ~146 opinions spanning 1968-present.

Access (no JavaScript execution needed, no CAPTCHA, no auth, browser UA):
  The library landing page renders its opinion list client-side, but the
  server-side "Adopted Ethics Opinions: Chronological" page carries every
  opinion as a plain <a href=".../wp-content/uploads/{file}.pdf">{number}</a>
  anchor. The scraper parses that page: the anchor TEXT is the authoritative
  opinion number (e.g. "84-1", "2025-1"), the href is the actual PDF (whose
  filename may carry -corr / -as-modified / duplicate-upload suffixes that the
  number does not). Each PDF is born-digital (text layer) -> extracted with
  PyMuPDF, NO OCR.

  Number normalization: a two-digit year YY (68..99) -> 19YY, a four-digit
  year is used as-is; _id = "US/AK-LegalEthics/{number}".

  Date: the authoritative adoption date is the "(Adopted|Approved) by (the)
  Board of Governors on <Month DD, YYYY>" line in the PDF body; fall back to
  any "Adopted/Approved by ... on <date>" line, then to the latest in-body
  date within the opinion's year, then to {year}-01-01.

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
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import requests

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.AK-LegalEthics")

BASE = "https://alaskabar.org"
INDEX_URL = (BASE + "/ethics-discipline/ethics-opinions/"
             "adopted-ethics-opinions-chronological/")
PDF_URL = BASE + "/wp-content/uploads/{num}.pdf"

CURRENT_YEAR = 2026       # upper bound sanity for dates

MONTHS = (r"(?:January|February|March|April|May|June|July|August|"
          r"September|October|November|December)")


class AKLegalEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.4
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                "Version/16.0 Safari/605.1.15"
            ),
            "Accept": "application/pdf,text/html,*/*",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str) -> requests.Response | None:
        for attempt in range(3):
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
    @staticmethod
    def _canon_number(raw_num: str) -> str | None:
        """Normalize an opinion number token to canonical form.
        Two-digit year YY -> 19YY (all such opinions are 1968-1999)."""
        m = re.match(r"(\d{2,4})-(\d+)", raw_num)
        if not m:
            return None
        yy, n = m.group(1), int(m.group(2))
        if len(yy) == 2:
            yy = "19" + yy
        return f"{yy}-{n}"

    def _list_opinions(self) -> list[tuple[str, str]]:
        """Parse the chronological index -> [(canonical_number, pdf_url)],
        de-duplicated on canonical number, in first-seen order."""
        r = self._get(INDEX_URL)
        if not r:
            logger.error("could not fetch chronological index")
            return []
        h = r.text
        seen: dict[str, str] = {}
        for m in re.finditer(
                r'href=["\']?([^"\'> ]*?/uploads/(\d{2,4}-[^"\'> ]*?\.pdf))["\']?'
                r'[^>]*>(.*?)</a>', h, re.S | re.I):
            href, fname, atext = m.group(1), m.group(2), m.group(3)
            atext = _html.unescape(re.sub(r"<[^>]+>", "", atext)).strip()
            # authoritative number from anchor text, fall back to filename stem
            num = self._canon_number(atext) or self._canon_number(fname)
            if not num:
                continue
            if not href.lower().startswith("http"):
                href = BASE + ("" if href.startswith("/") else "/") + href.lstrip("/")
            seen.setdefault(num, href)
        opinions = list(seen.items())
        logger.info(f"  chronological index yields {len(opinions)} opinions")
        return opinions

    # -------------------------------------------------------- extraction
    @staticmethod
    def _clean(text: str) -> str:
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
        text = text.replace("​", "").replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _pdf_text(content: bytes) -> str:
        if fitz is None:
            raise RuntimeError("PyMuPDF (fitz) is required for PDF extraction")
        doc = fitz.open(stream=content, filetype="pdf")
        try:
            return "".join(page.get_text() for page in doc)
        finally:
            doc.close()

    @staticmethod
    def _make_title(num: str, text: str) -> str:
        # The subject/title follows the opinion number (as a standalone token,
        # not the "{num}.doc/.pdf" filename echo), up to the next blank line.
        yy = num.split("-")
        variants = [num]
        if len(yy[0]) == 4 and yy[0].startswith("19"):
            variants.append(f"{yy[0][2:]}-{yy[1]}")   # 1984-1 -> 84-1 body echo
        for v in variants:
            for m in re.finditer(re.escape(v) + r"(?!\S)", text):
                seg = text[m.end():m.end() + 400]
                # skip a "{num}.doc"/".pdf" filename echo
                if seg[:5].lower().startswith((".doc", ".pdf")):
                    continue
                seg = seg.lstrip("\n :.-")
                block = re.split(r"\n\n", seg, 1)[0]
                title = re.sub(r"\s+", " ", block).strip(" .:\n")
                title = re.sub(r"\s+Issue$", "", title).strip()
                if 4 < len(title) < 300:
                    return title.title() if title.isupper() else title
        return f"Alaska Bar Association Ethics Opinion {num}"

    @staticmethod
    def _parse_date(ds: str):
        try:
            return datetime.strptime(re.sub(r",", "", ds).strip(), "%B %d %Y").date()
        except Exception:
            return None

    @classmethod
    def _issue_date(cls, num: str, text: str) -> str | None:
        yr = int(num.split("-")[0])
        # 1) authoritative: (Adopted|Approved) by (the) Board of Governors on DATE
        m = re.search(
            r"(?:Adopted|Approved)\s+by\s+(?:the\s+)?Board of Governors\s+on\s+("
            + MONTHS + r"\s+\d{1,2},?\s+\d{4})", text, re.I)
        if m and cls._parse_date(m.group(1)):
            return cls._parse_date(m.group(1)).isoformat()
        # 2) any "(Adopted|Approved) by ... on DATE" -> latest
        cand = [cls._parse_date(x) for x in re.findall(
            r"(?:Adopted|Approved)\s+by[^.\n]*?on\s+(" + MONTHS
            + r"\s+\d{1,2},?\s+\d{4})", text, re.I)]
        cand = [d for d in cand if d]
        if cand:
            return max(cand).isoformat()
        # 3) latest in-body date within the opinion's year
        dates = [cls._parse_date(x) for x in re.findall(
            r"(" + MONTHS + r"\s+\d{1,2},?\s+\d{4})", text)]
        dates = [d for d in dates if d and yr - 1 <= d.year <= yr + 1]
        if dates:
            return max(dates).isoformat()
        # 4) fall back to the number's year
        if 1960 <= yr <= CURRENT_YEAR + 1:
            return f"{yr}-01-01"
        return None

    def _fetch_one(self, num: str, url: str) -> dict | None:
        r = self._get(url)
        if not r or not r.content:
            return None
        if r.content[:4] != b"%PDF" and \
                "pdf" not in r.headers.get("content-type", "").lower():
            logger.warning(f"  {num}: not a PDF ({url})")
            return None
        try:
            raw = self._pdf_text(r.content)
        except Exception as e:
            logger.warning(f"  {num}: PDF extract failed: {e}")
            return None
        text = self._clean(raw)
        if len(text) < 150:
            logger.warning(f"  {num}: insufficient text ({len(text)})")
            return None
        return {
            "opinion_number": num,
            "title": self._make_title(num, text),
            "text": text,
            "date": self._issue_date(num, text),
            "url": url,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Alaska Bar Association Ethics Opinions...")
        ok = 0
        for num, url in [("1973-1", PDF_URL.format(num="73-1")),
                         ("1998-2", PDF_URL.format(num="98-2")),
                         ("2025-1", PDF_URL.format(num="2025-1"))]:
            rec = self._fetch_one(num, url)
            if rec and len(rec["text"]) > 300:
                logger.info(f"  Opinion {num} OK ({len(rec['text'])} chars) "
                            f"date={rec['date']} title={rec['title'][:50]!r}")
                ok += 1
            else:
                logger.warning(f"  no text ({num})")
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["opinion_number"]
        return {
            "_id": f"US/AK-LegalEthics/{num}",
            "_source": "US/AK-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": "Alaska Bar Association — Ethics Committee",
            "title": raw.get("title"),
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-AK",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for num, url in self._list_opinions():
            time.sleep(self.delay)
            rec = self._fetch_one(num, url)
            if not rec:
                logger.warning(f"  no text for {num}, skipping")
                continue
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

    parser = argparse.ArgumentParser(description="US/AK-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = AKLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
