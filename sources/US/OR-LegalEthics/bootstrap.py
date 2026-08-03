#!/usr/bin/env python3
"""
US/OR-LegalEthics -- Oregon State Bar — Formal Ethics Opinions

Fetches the full text of the Formal Ethics Opinions issued by the Oregon State
Bar's Legal Ethics Committee / Board of Governors. Each opinion applies the
Oregon Rules of Professional Conduct (ORPC) to a stated fact situation and
advises lawyers whether the described conduct is proper = doctrine (the Bar's
official written interpretation of the attorney-conduct rules).

The corpus is one continuous, globally-numbered series. In September 2005 the
Board of Governors re-issued the older 1991-2004 formal opinions as
"{2005}-{N}" (2005-1 .. 2005-175) conformed to the ORPC that took effect
1 Jan 2005; opinions issued since then continue the same sequential number with
the issue year as the prefix (e.g. 2011-188, 2013-189, ... 2026-208). ~208
opinions as of 2026. Distinct from US/OR-EthicsOpinions (Oregon Government
Ethics Commission — public officials) and US/OR-AGOpinions (Attorney General);
this is the state *bar*'s attorney-ethics formal-opinion series.

Access (no JavaScript execution needed, no CAPTCHA, no auth, browser UA):
  1. The "Formal Ethics Opinion Library – Table of Contents" lists EVERY opinion
     as a direct PDF link whose anchor text is "{number}: {title}":
       https://www.osbar.org/ethics/toc.html
     -> /_docs/ethics/{YYYY-NNN}.pdf
  2. Each opinion PDF is born-digital (text layer) — extracted with PyMuPDF,
     NO OCR needed. Body carries "FORMAL OPINION NO {number}", the topical
     title, and Facts / Discussion / Conclusion sections.

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
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

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
logger = logging.getLogger("legal-data-hunter.US.OR-LegalEthics")

BASE = "https://www.osbar.org"
TOC_URL = BASE + "/ethics/toc.html"
PDF_HREF_RE = re.compile(r"/_docs/ethics/(\d{4}-\d+)\.pdf", re.I)


class ORLegalEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                "Version/16.0 Safari/605.1.15"
            ),
            "Accept": "text/html,application/xhtml+xml,application/pdf",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str) -> requests.Response | None:
        for attempt in range(3):
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
        """Return [(opinion_number, title, pdf_url)], preserving TOC order,
        de-duplicated on opinion_number."""
        r = self._get(TOC_URL)
        if not r:
            logger.error("could not fetch the OSB ethics TOC")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        out: list[tuple[str, str, str]] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            m = PDF_HREF_RE.search(a["href"])
            if not m:
                continue
            num = m.group(1)
            if num in seen:
                continue
            seen.add(num)
            label = a.get_text(" ", strip=True)
            # anchor text is "{number}: {title}"
            title = label
            mt = re.match(r"\s*" + re.escape(num) + r"\s*:\s*(.+)$", label)
            if mt:
                title = mt.group(1).strip()
            out.append((num, title, urljoin(BASE, m.group(0))))
        logger.info(f"  TOC yields {len(out)} unique opinions")
        return out

    # -------------------------------------------------------- extraction
    @staticmethod
    def _clean(text: str) -> str:
        text = text.replace(" ", " ")
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

    def _fetch_one(self, num: str, title: str, url: str) -> dict | None:
        r = self._get(url)
        if not r or not r.content:
            return None
        ctype = r.headers.get("content-type", "").lower()
        if "pdf" not in ctype and not r.content[:4] == b"%PDF":
            logger.warning(f"  {num}: not a PDF ({ctype})")
            return None
        try:
            raw = self._pdf_text(r.content)
        except Exception as e:
            logger.warning(f"  {num}: PDF extract failed: {e}")
            return None
        text = self._clean(raw)
        if len(text) < 150:
            return None

        # Date: prefer an explicit "YYYY Revision" / issue year in the body;
        # else fall back to the number's year prefix.
        year = num.split("-", 1)[0]
        mrev = re.search(r"\b((?:19|20)\d\d)\s+Revision\b", text)
        date = f"{mrev.group(1) if mrev else year}-01-01"

        return {
            "opinion_number": num,
            "title": title or f"Oregon State Bar Formal Opinion No. {num}",
            "text": text,
            "date": date,
            "url": url,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Oregon State Bar Formal Ethics Opinions...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions listed")
            return False
        ok = 0
        for num, title, url in ops[:2] + ops[-1:]:
            rec = self._fetch_one(num, title, url)
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
            "_id": f"US/OR-LegalEthics/{num}",
            "_source": "US/OR-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": "Oregon State Bar — Legal Ethics Committee / Board of Governors",
            "title": raw.get("title"),
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-OR",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for num, title, url in self._list_opinions():
            rec = self._fetch_one(num, title, url)
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

    parser = argparse.ArgumentParser(description="US/OR-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ORLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
