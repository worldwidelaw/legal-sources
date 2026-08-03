#!/usr/bin/env python3
"""
US/SD-LegalEthics -- State Bar of South Dakota — Ethics Opinions

Fetches the full text of the Ethics Opinions issued by the State Bar of South
Dakota's Ethics Committee. Each opinion applies the South Dakota Rules of
Professional Conduct to a stated question to advise LAWYERS on the ethics of
contemplated conduct = doctrine (advisory).

The State Bar of South Dakota is the state's INTEGRATED (unified/mandatory) bar
(SDCL ch. 16-17): all South Dakota lawyers must belong, and the Bar operates
under the authority of the South Dakota Supreme Court to regulate the legal
profession. The opinion texts are therefore the work of a government-authorized
body — treated as public domain under the 17 U.S.C. § 105 government-edicts
rationale, consistent with the other integrated state-bar legal-ethics sources.
Published free to the public on statebarofsouthdakota.com, no login/paywall/
reuse terms.

The corpus is a per-year numbered series "{year}-{N}" (four-digit year, e.g.
1995-01, 2006-01, 2020-07, 2025-01; a few carry a trailing letter, e.g.
2000-05a), ~145 opinions listed 1986-present.

Access (no JavaScript execution needed, no CAPTCHA, no auth, browser UA):
  A SINGLE public index page, /ethics-opinions/, links every opinion. The
  anchor TEXT is "{number}: {subject} (Rules ...)" and the href is either a
  clean on-site URL (/{a}_{b}_{number}/, which SERVES the opinion PDF directly)
  or a born-digital PDF on the Bar's Azure CDN
  (growthzonecmsprodeastus.azureedge.net/.../{number}.pdf). Both resolve to a
  PDF (%PDF). Each PDF is extracted with PyMuPDF, NO OCR.

  The newer opinions (~1995-present) are born-digital (text layer). The oldest
  opinions (~1986-1994, listed as a bare number with no subject) are scanned
  images with no text layer -> they yield no text and are skipped (a minority
  recoverable only on an OCR host). A handful of 2021-2023 opinions live only
  behind the members' GrowthZone portal (login) and are skipped.

  Number: the "{year}-{N}" token from the anchor text, normalized to a
  zero-padded two-digit sequence (2006-1 -> 2006-01), trailing letter preserved.
  _id = "US/SD-LegalEthics/{number}".

  Title: the subject text after the "{number}:" prefix in the anchor.

  Date: parsed from an in-body "Dated ... <Month DD, YYYY>" / "<Month DD, YYYY>"
  line within the opinion's year when present, else the number's year ->
  YYYY-01-01.

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
logger = logging.getLogger("legal-data-hunter.US.SD-LegalEthics")

BASE = "https://www.statebarofsouthdakota.com"
INDEX_URL = BASE + "/ethics-opinions/"

CURRENT_YEAR = 2026       # upper bound sanity for dates

MONTHS = (r"(?:January|February|March|April|May|June|July|August|"
          r"September|October|November|December)")

# Opinion number token: 4-digit year, dash, 1-2 digit sequence, optional letter.
NUM_RE = re.compile(r"(\d{4})-(\d{1,2})([A-Za-z]?)")


class SDLegalEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.4
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
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
                if r.status_code in (401, 403, 404):
                    return None
                logger.warning(f"GET {url} -> HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"GET failed {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    @staticmethod
    def _canon_number(raw: str) -> str | None:
        """Normalize a '{year}-{N}[letter]' token: zero-pad the sequence to two
        digits, lower-case any trailing letter. e.g. 2006-1 -> 2006-01."""
        m = NUM_RE.match(raw.strip())
        if not m:
            return None
        yy, n, suf = m.group(1), int(m.group(2)), m.group(3).lower()
        if not (1960 <= int(yy) <= CURRENT_YEAR + 1):
            return None
        return f"{yy}-{n:02d}{suf}"

    def _list_opinions(self) -> list[tuple[str, str, str]]:
        """Parse the index -> [(canonical_number, title, url)], de-duplicated on
        canonical number, in first-seen order. Skips login-gated portal links
        and the (text-layer-less) bare-number scans' hosts are still probed but
        those simply yield no text later."""
        r = self._get(INDEX_URL)
        if not r:
            logger.error("could not fetch ethics-opinions index")
            return []
        h = r.text
        seen: dict[str, tuple[str, str]] = {}
        for m in re.finditer(r'<a\b[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
                             h, re.S | re.I):
            href = m.group(1).strip()
            atext = _html.unescape(re.sub(r"<[^>]+>", " ", m.group(2)))
            atext = re.sub(r"\s+", " ", atext).strip()
            num = self._canon_number(atext)
            if not num:
                continue
            # Only follow hosts that serve the opinion PDF without auth:
            #   - on-site detail URLs   /{a}_{b}_{slug}/  (serve the PDF)
            #   - the Bar's Azure CDN    *.azureedge.net/.../{number}.pdf
            low = href.lower()
            on_site = ("statebarofsouthdakota.com/" in low
                       and re.search(r"/\d+_\d+_", low))
            azure = "azureedge.net" in low and low.endswith(".pdf")
            if not (on_site or azure):
                continue          # skip GrowthZone member portal / file:/// links
            if not low.startswith("http"):
                continue
            # title = subject after the "{number}:" prefix in the anchor text
            title = atext
            mt = re.match(r"\s*\d{4}-\d{1,2}[A-Za-z]?\s*:?\s*(.*)", atext)
            if mt and mt.group(1).strip():
                title = mt.group(1).strip()
            seen.setdefault(num, (title, href))
        opinions = [(num, t, u) for num, (t, u) in seen.items()]
        logger.info(f"  index yields {len(opinions)} candidate opinions")
        return opinions

    # -------------------------------------------------------- extraction
    @staticmethod
    def _clean(text: str) -> str:
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
        text = text.replace("​", "").replace("\xa0", " ")
        text = re.sub("[\uf0b7\uf0a7\u2022]", "-", text)  # PDF bullet glyphs -> "-"
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
    def _parse_date(ds: str):
        for fmt in ("%B %d %Y", "%B %d, %Y"):
            try:
                return datetime.strptime(re.sub(r"\s+", " ", ds).strip(), fmt).date()
            except Exception:
                pass
        try:
            return datetime.strptime(re.sub(r",", "", ds).strip(), "%B %d %Y").date()
        except Exception:
            return None

    @classmethod
    def _issue_date(cls, num: str, text: str) -> str | None:
        yr = int(num.split("-")[0])
        # latest in-body Month DD, YYYY date within +/-1 year of the number-year
        dates = [cls._parse_date(x) for x in re.findall(
            r"(" + MONTHS + r"\s+\d{1,2},?\s+\d{4})", text)]
        dates = [d for d in dates if d and yr - 1 <= d.year <= yr + 1]
        if dates:
            return max(dates).isoformat()
        if 1960 <= yr <= CURRENT_YEAR + 1:
            return f"{yr}-01-01"
        return None

    def _fetch_one(self, num: str, title: str, url: str) -> dict | None:
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
            # scanned image opinion with no text layer -> needs OCR, skip
            logger.info(f"  {num}: no text layer ({len(text)} chars), skipping")
            return None
        if not title:
            title = f"State Bar of South Dakota Ethics Opinion {num}"
        return {
            "opinion_number": num,
            "title": title,
            "text": text,
            "date": self._issue_date(num, text),
            "url": url,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing State Bar of South Dakota Ethics Opinions...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: index yielded no opinions")
            return False
        ok = 0
        for num, title, url in ops[:8]:
            rec = self._fetch_one(num, title, url)
            if rec and len(rec["text"]) > 300:
                logger.info(f"  Opinion {num} OK ({len(rec['text'])} chars) "
                            f"date={rec['date']} title={rec['title'][:50]!r}")
                ok += 1
            if ok >= 3:
                break
        if ok >= 3:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: not enough full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["opinion_number"]
        return {
            "_id": f"US/SD-LegalEthics/{num}",
            "_source": "US/SD-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": "State Bar of South Dakota — Ethics Committee",
            "title": raw.get("title"),
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-SD",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for num, title, url in self._list_opinions():
            time.sleep(self.delay)
            rec = self._fetch_one(num, title, url)
            if not rec:
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

    parser = argparse.ArgumentParser(description="US/SD-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SDLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
