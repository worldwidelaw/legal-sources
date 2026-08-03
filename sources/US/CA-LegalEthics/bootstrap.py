#!/usr/bin/env python3
"""
US/CA-LegalEthics -- State Bar of California (COPRAC) — Formal Ethics Opinions

Fetches the full text of the Formal Opinions issued by the State Bar of
California's Standing Committee on Professional Responsibility and Conduct
(COPRAC). Each opinion applies the California Rules of Professional Conduct (and
the State Bar Act) to a stated question and advises lawyers whether the described
conduct is proper = doctrine (the Committee's official written interpretation of
the attorney-conduct rules).

The opinions form one continuous, numbered series, "CAL {YYYY}-{N}", from
CAL 1965-1 to the present (CAL 2026-210 as of 2026-07), and are published in full
by the State Bar of California on calbar.ca.gov. Distinct from US/CA-FPPC (Fair
Political Practices Commission — political ethics of public officials) and from
California Attorney General opinions.

Access (no JavaScript execution needed, no CAPTCHA, no auth, browser UA):
  1. The single "Ethics Opinions" listing page enumerates EVERY opinion as a
     direct document link whose anchor text is the opinion number ("CAL YYYY-N").
       https://www.calbar.ca.gov/legal-professionals/ethics-compliance-practice-resources/ethics/ethics-opinions
     Older opinions (1965-2001) are born-digital HTML pages (.htm); newer ones
     (2002-present) are born-digital PDFs (.pdf). The exact href is always taken
     from the listing anchor, never constructed (filenames are irregular).
  2. HTML opinions: GET (following the 301 to /sites/default/files/...) and slice
     the <body> text. PDF opinions: download and extract the text layer with
     PyMuPDF (born-digital, no OCR).

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
"""

from __future__ import annotations

import sys
import io
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

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CA-LegalEthics")

BASE = "https://www.calbar.ca.gov"
LISTING_URL = (
    BASE + "/legal-professionals/ethics-compliance-practice-resources/"
    "ethics/ethics-opinions"
)

# Anchor text is exactly the opinion number, e.g. "CAL 2020-203" / "CAL 1965-3".
CAL_ANCHOR_RE = re.compile(r"^CAL\s+(\d{4})[-–](\d+)\s*$")
TAG_RE = re.compile(r"<[^>]+>")


class CALegalEthicsScraper(BaseScraper):

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
            "Accept": "text/html,application/xhtml+xml",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str) -> requests.Response | None:
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                r = self._session.get(url, timeout=60)
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
    def _list_opinions(self) -> list[tuple[str, str]]:
        """Return [(number 'YYYY-N', absolute_href), ...] for every opinion,
        newest-first as published, de-duplicated on opinion number."""
        r = self._get(LISTING_URL)
        if not r:
            logger.error("could not fetch the ethics-opinions listing page")
            return []
        html = r.text
        out: list[tuple[str, str]] = []
        seen: set[str] = set()
        for m in re.finditer(r'<a\s+[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
                             html, re.S | re.I):
            href = m.group(1)
            text = TAG_RE.sub("", m.group(2)).strip()
            am = CAL_ANCHOR_RE.match(text)
            if not am:
                continue
            number = f"{am.group(1)}-{am.group(2)}"
            if number in seen:
                continue
            low = href.lower().split("?")[0]
            if not (low.endswith(".pdf") or low.endswith(".htm")
                    or low.endswith(".html")):
                continue
            seen.add(number)
            out.append((number, urljoin(BASE, href)))
        logger.info(f"  listing yields {len(out)} unique opinions")
        return out

    # -------------------------------------------------------- extraction
    @staticmethod
    def _clean(text: str) -> str:
        text = re.sub(r"[ \t]+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _pdf_text(self, url: str) -> str:
        r = self._get(url)
        if not r or not r.content:
            return ""
        if fitz is None:
            logger.error("PyMuPDF (fitz) not available")
            return ""
        try:
            doc = fitz.open(stream=io.BytesIO(r.content), filetype="pdf")
        except Exception as e:
            logger.warning(f"  fitz open failed for {url}: {e}")
            return ""
        parts = [page.get_text() for page in doc]
        doc.close()
        return self._clean("\n".join(parts))

    def _html_text(self, url: str) -> str:
        r = self._get(url)
        if not r or not r.text:
            return ""
        soup = BeautifulSoup(r.text, "html.parser")
        for t in soup(["script", "style"]):
            t.decompose()
        body = soup.find("body") or soup
        return self._clean(body.get_text("\n", strip=True))

    def _fetch_one(self, number: str, url: str) -> dict | None:
        low = url.lower().split("?")[0]
        text = self._pdf_text(url) if low.endswith(".pdf") else self._html_text(url)
        if len(text) < 150:
            return None
        year = number.split("-")[0]
        return {
            "opinion_number": number,
            "title": f"California Formal Ethics Opinion No. {number}",
            "text": text,
            "date": f"{year}-01-01",
            "url": url,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing State Bar of California COPRAC ethics opinions...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions listed")
            return False
        logger.info(f"  e.g. {[n for n, _ in ops[:4]]} ... {[n for n, _ in ops[-2:]]}")
        # Probe a modern PDF and an old HTML opinion.
        probes = [ops[0]]
        old = next((o for o in reversed(ops) if o[1].lower().endswith((".htm", ".html"))), None)
        if old:
            probes.append(old)
        ok = 0
        for number, url in probes:
            rec = self._fetch_one(number, url)
            if rec and len(rec["text"]) > 300:
                logger.info(f"  CAL {number} OK ({len(rec['text'])} chars) "
                            f"date={rec['date']}")
                ok += 1
            else:
                logger.warning(f"  CAL {number} — no text ({url})")
        if ok >= 1:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["opinion_number"]
        return {
            "_id": f"US/CA-LegalEthics/{num}",
            "_source": "US/CA-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": ("State Bar of California — Standing Committee on "
                       "Professional Responsibility and Conduct (COPRAC)"),
            "title": raw.get("title") or f"California Formal Ethics Opinion No. {num}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-CA",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for number, url in self._list_opinions():
            rec = self._fetch_one(number, url)
            if not rec:
                logger.warning(f"  no text for CAL {number} ({url}), skipping")
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

    parser = argparse.ArgumentParser(description="US/CA-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CALegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
