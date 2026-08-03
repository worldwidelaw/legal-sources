#!/usr/bin/env python3
"""
US/MA-JudicialEthics -- Massachusetts Committee on Judicial Ethics
                        — CJE Opinions

Fetches the full text of the judicial ethics opinions (formal "Letter
Opinions") issued by the Massachusetts Committee on Judicial Ethics (CJE), a
committee of the Massachusetts Supreme Judicial Court that advises judges on the
application of the Massachusetts Code of Judicial Conduct = doctrine (official
written interpretation of the judicial-conduct rules).

Access (no JavaScript, no CAPTCHA, no auth):
  The Committee publishes a Chronological Index that links every opinion, and
  each opinion has its own born-digital HTML page on mass.gov:

      https://www.mass.gov/info-details/chronological-index-of-judicial-ethics-opinions
      https://www.mass.gov/opinion/cje-opinion-no-{N}

  The opinion body sits in a `div.ma__rich-text` container (the longest one on
  the page); a short subject heading and an "issued on MM/DD/YYYY" note precede
  it. Some opinions were withheld from publication (redacted); those pages carry
  only a short explanatory note and are skipped (no full text).

  mass.gov is fronted by Akamai and serves the full server-side HTML to plain
  (non-browser) User-Agents, while a browser UA gets a JS shell — so all
  requests use a plain python-requests UA (same inversion as US/MA-EthicsOpinions).

Strategy:
  GET the chronological index, collect every /opinion/cje-opinion-no-... href,
  then fetch each opinion page and extract the subject, issue date and the full
  body text.

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
import subprocess
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.MA-JudicialEthics")

BASE = "https://www.mass.gov"
INDEX_URL = BASE + "/info-details/chronological-index-of-judicial-ethics-opinions"
HREF_RE = re.compile(r'href="(/opinion/cje-opinion-no-[0-9]{2,4}-[0-9]+)"')
DATE_RE = re.compile(r"Date:\s*(\d{1,2}/\d{1,2}/\d{4})")
# mass.gov serves the full HTML only to a plain (non-browser) UA.
UA = "python-requests/2.31.0"
WITHHELD = "decided not to publish"


class MAJudicialEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0

    # ---------------------------------------------------------------- http
    def _curl(self, url: str) -> str | None:
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--compressed", "--max-time", "60",
                     "-A", UA, url],
                    capture_output=True, timeout=90,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout.decode("utf-8", "replace")
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    def _list_opinions(self) -> list[dict]:
        """Return [{number, path}] for every opinion, in index order, deduped."""
        html = self._curl(INDEX_URL)
        if not html:
            logger.error("could not fetch the chronological index")
            return []
        out: list[dict] = []
        seen: set[str] = set()
        for path in HREF_RE.findall(html):
            number = path.rsplit("cje-opinion-no-", 1)[1]
            if number in seen:
                continue
            seen.add(number)
            out.append({"number": number, "path": path})
        return out

    # -------------------------------------------------------- extraction
    @staticmethod
    def _parse_page(html: str) -> tuple[str, str, str | None]:
        """Return (subject, body_text, date_iso) from an opinion page."""
        soup = BeautifulSoup(html, "html.parser")
        rts = soup.select("div.ma__rich-text")
        blocks = [(el.get_text("\n", strip=True)) for el in rts]
        blocks = [b for b in blocks if b]
        body = max(blocks, key=len) if blocks else ""
        # The subject heading is the short block that is neither the header note
        # nor the body/disclaimer.
        subject = ""
        for b in blocks:
            if b is body:
                continue
            if ("issued on" in b.lower() or "letter opinion" in b.lower()
                    or "relies on facts" in b.lower()):
                continue
            if 8 <= len(b) <= 200 and not subject:
                subject = b.replace("\n", " ").strip()
        page_text = soup.get_text(" ", strip=True)
        dm = DATE_RE.search(page_text)
        date_iso = None
        if dm:
            mm, dd, yy = dm.group(1).split("/")
            date_iso = f"{int(yy):04d}-{int(mm):02d}-{int(dd):02d}"
        body = re.sub(r"[ \t]+\n", "\n", body)
        body = re.sub(r"\n{3,}", "\n\n", body).strip()
        return subject, body, date_iso

    def _fetch_one(self, op: dict) -> dict | None:
        html = self._curl(BASE + op["path"])
        if not html:
            return None
        if WITHHELD in html.lower():
            return None  # opinion withheld from publication (redacted)
        subject, body, date_iso = self._parse_page(html)
        if not body or len(body) < 200:
            return None
        if not date_iso:
            # fall back to the year embedded in the opinion number
            ym = re.match(r"(\d{2,4})-", op["number"])
            if ym:
                y = int(ym.group(1))
                y = y if y >= 1000 else (1900 + y if y >= 50 else 2000 + y)
                date_iso = f"{y:04d}-01-01"
        return {
            "number": op["number"],
            "subject": subject,
            "text": body,
            "date": date_iso,
            "url": BASE + op["path"],
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Massachusetts Committee on Judicial Ethics...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions found on index")
            return False
        logger.info(f"  discovered {len(ops)} opinions")
        ok = 0
        for op in ops[:8]:
            rec = self._fetch_one(op)
            if rec and len(rec["text"]) > 200:
                logger.info(f"  CJE Opinion {rec['number']} OK "
                            f"({len(rec['text'])} chars) date={rec['date']}")
                ok += 1
            if ok >= 3:
                break
        if ok:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw["number"]
        subject = (raw.get("subject") or "").strip()
        title = f"CJE Opinion No. {number}"
        if subject:
            title += f": {subject}"
        return {
            "_id": f"US/MA-JudicialEthics/{number}",
            "_source": "US/MA-JudicialEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "issuer": "Massachusetts Committee on Judicial Ethics "
                      "(Supreme Judicial Court)",
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-MA",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        ops = self._list_opinions()
        emitted = 0
        for op in ops:
            rec = self._fetch_one(op)
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

    parser = argparse.ArgumentParser(description="US/MA-JudicialEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MAJudicialEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
