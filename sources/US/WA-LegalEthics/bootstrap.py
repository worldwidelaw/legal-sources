#!/usr/bin/env python3
"""
US/WA-LegalEthics -- Washington State Bar Association (WSBA) — Advisory Opinions

Fetches the full text of the Advisory Opinions issued by the Washington State Bar
Association's Committee on Professional Ethics (and its predecessor Rules of
Professional Conduct Committee). Each opinion interprets the Washington Rules of
Professional Conduct (RPCs) and advises WSBA members on their ethical
obligations = doctrine (the Bar's official written interpretation of the
attorney-conduct rules).

The opinions are published in full on the WSBA Advisory Opinions portal
(ao.wsba.org). There are two numbering schemes across the corpus: older
sequential numbers (e.g. "835", "1120", some with a "W" withdrawn/variant
suffix) and newer year-based numbers (e.g. "201601" = 2016). Distinct from
US/WA-EthicsOpinions (Washington State Executive Ethics Board, which advises
public officials) and from Washington Attorney General opinions.

Access (no JavaScript execution needed, no CAPTCHA, no auth, browser UA):
  Each opinion has a clean, printable HTML view at a sequential internal id:

      https://ao.wsba.org/print.aspx?ID={id}

  The page carries labelled fields (Advisory Opinion, Year Issued, RPC(s),
  Subject) followed by the opinion body and a standard disclaimer. The portal's
  own listing is a stateful WebForms search, so the corpus is enumerated by
  walking the contiguous internal id space (1 .. ~1750); ids past the ceiling
  return an empty stub (~50 chars) and are skipped.

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

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.WA-LegalEthics")

BASE = "https://ao.wsba.org"
PRINT_URL = BASE + "/print.aspx?ID={id}"
# The internal id space runs 1..~1720 (ceiling ~2026); walk with a buffer.
DEFAULT_MAX = 1800
# Consecutive empty stubs past which we assume we've run off the end.
CONSEC_EMPTY_STOP = 60

LABELS = ("Advisory Opinion:", "Year Issued:", "RPC(s):", "Subject:")


class WALegalEthicsScraper(BaseScraper):

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
    def _get(self, url: str) -> str | None:
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                r = self._session.get(url, timeout=60)
                if r.status_code == 200 and r.text:
                    return r.text
                if r.status_code == 404:
                    return None
                logger.warning(f"GET {url} -> HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"GET failed {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # -------------------------------------------------------- extraction
    @staticmethod
    def _labelled(lines: list[str], label: str) -> str:
        for i, l in enumerate(lines):
            if l.strip() == label:
                return lines[i + 1].strip() if i + 1 < len(lines) else ""
        return ""

    def _parse(self, ident: int, html: str) -> dict | None:
        soup = BeautifulSoup(html, "html.parser")
        for t in soup(["script", "style"]):
            t.decompose()
        body = soup.find("body") or soup
        text = body.get_text("\n", strip=True)
        text = re.sub(r"[ \t]+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()

        lines = [l for l in text.split("\n")]
        number = self._labelled(lines, "Advisory Opinion:").lstrip("-").strip()
        year = self._labelled(lines, "Year Issued:").strip()
        subject = self._labelled(lines, "Subject:").strip()

        # Empty stubs carry no opinion number and almost no text.
        if not number or len(text) < 120:
            return None

        date = None
        ym = re.match(r"(\d{4})", year)
        if ym:
            date = f"{ym.group(1)}-01-01"

        title = f"WSBA Advisory Opinion {number}"
        if subject:
            title += f": {subject}"

        return {
            "opinion_number": number,
            "wsba_id": ident,
            "title": title[:300],
            "subject": subject,
            "text": text,
            "date": date,
            "url": PRINT_URL.format(id=ident),
        }

    def _fetch_one(self, ident: int) -> dict | None:
        html = self._get(PRINT_URL.format(id=ident))
        if not html:
            return None
        return self._parse(ident, html)

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing WSBA Advisory Opinions portal...")
        ok = 0
        for ident in (1, 200, 1000, 1686):
            rec = self._fetch_one(ident)
            if rec and len(rec["text"]) > 150:
                logger.info(f"  ID {ident} -> Opinion {rec['opinion_number']} "
                            f"({len(rec['text'])} chars) date={rec['date']}")
                ok += 1
            else:
                logger.warning(f"  ID {ident} — no opinion")
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: insufficient full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["opinion_number"]
        slug = re.sub(r"[^A-Za-z0-9]+", "-", num).strip("-")
        return {
            "_id": f"US/WA-LegalEthics/{slug}",
            "_source": "US/WA-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": ("Washington State Bar Association — Committee on "
                       "Professional Ethics"),
            "title": raw.get("title") or f"WSBA Advisory Opinion {num}",
            "subject": raw.get("subject"),
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-WA",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        consec_empty = 0
        for ident in range(1, DEFAULT_MAX + 1):
            rec = self._fetch_one(ident)
            if not rec:
                consec_empty += 1
                if consec_empty >= CONSEC_EMPTY_STOP:
                    logger.info(f"  {CONSEC_EMPTY_STOP} consecutive empties at "
                                f"ID {ident}; assuming end of corpus")
                    break
                continue
            consec_empty = 0
            yield rec
            emitted += 1
            if emitted % 100 == 0:
                logger.info(f"  ...{emitted} opinions so far (ID {ident})")
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

    parser = argparse.ArgumentParser(description="US/WA-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WALegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
