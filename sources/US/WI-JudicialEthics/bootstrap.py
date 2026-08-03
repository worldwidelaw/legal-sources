#!/usr/bin/env python3
"""
US/WI-JudicialEthics -- Wisconsin Judicial Conduct Advisory Committee
                        — Formal Advisory Opinions

Fetches the full text of the formal advisory opinions issued by the Wisconsin
Judicial Conduct Advisory Committee, a committee of the Supreme Court of
Wisconsin that renders formal advisory opinions to judges and judicial officers
on the compliance of their contemplated conduct with the Code of Judicial
Conduct (SCR Chapter 60) = doctrine (the Committee's official written
interpretation of the judicial-conduct rules).

Access (no JavaScript, no CAPTCHA, no auth):
  The Committee publishes a single index page listing every opinion in a table
  (Release date | Description "OPINION N-N: ..." | View), where each View cell
  links to a born-digital PDF:

      https://www.wicourts.gov/supreme/sc_judcond.jsp
      https://www.wicourts.gov/sc/judcond/DisplayDocument.pdf?content=pdf&seqNo=NNNN

  Each opinion PDF has a real text layer (Issue / Answer / Facts / Discussion),
  extracted directly — no OCR. wicourts.gov requires a browser User-Agent.

Strategy:
  GET the index, parse each table row (release date, opinion number + title,
  seqNo), then fetch each opinion PDF and extract its full text. The issue date
  is taken from the table's Release-date column (fallback to the opinion-number
  year).

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

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.WI-JudicialEthics")

BASE = "https://www.wicourts.gov"
INDEX_URL = BASE + "/supreme/sc_judcond.jsp"
DOC_URL = BASE + "/sc/judcond/DisplayDocument.pdf?content=pdf&seqNo={seq}"

ROW_RE = re.compile(
    r"<tr>\s*<td[^>]*>([^<]*)</td>\s*<td>(.*?)</td>\s*"
    r"<td[^>]*>(.*?)</td>\s*</tr>", re.S | re.I)
NUM_RE = re.compile(r"OPINION\s+([\w-]+)\s*:?", re.I)
SEQ_RE = re.compile(r"content=pdf&seqNo=(\d+)", re.I)
MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], start=1)}


def _detag(s: str) -> str:
    import html as _html
    s = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", _html.unescape(s)).strip()


def _parse_date(cell: str, number: str) -> str | None:
    """'Mar 13, 2019' -> '2019-03-13'; fallback to the opinion-number year."""
    m = re.match(r"([A-Za-z]{3})\w*\s+(\d{1,2}),\s+(\d{4})", cell.strip())
    if m and m.group(1)[:3].title() in MONTHS:
        mon = MONTHS[m.group(1)[:3].title()]
        return f"{int(m.group(3)):04d}-{mon:02d}-{int(m.group(2)):02d}"
    ym = re.match(r"(\d{2})-", number)  # 19-1 -> 2019
    if ym:
        yy = int(ym.group(1))
        year = 1900 + yy if yy >= 50 else 2000 + yy
        return f"{year:04d}-01-01"
    return None


class WIJudicialEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        )

    # ---------------------------------------------------------------- http
    def _curl_bytes(self, url: str) -> bytes | None:
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "60", "-A", self._ua, url],
                    capture_output=True, timeout=90,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    def _list_opinions(self) -> list[dict]:
        """Return [{number, title, date, seq}] in index order, deduped."""
        raw = self._curl_bytes(INDEX_URL)
        if not raw:
            logger.error("could not fetch the judicial-conduct index")
            return []
        html = raw.decode("utf-8", "replace")
        out: list[dict] = []
        seen: set[str] = set()
        for date_cell, desc, view in ROW_RE.findall(html):
            seqm = SEQ_RE.search(view)
            numm = NUM_RE.search(desc)
            if not seqm or not numm:
                continue
            number = numm.group(1).strip()
            if number in seen:
                continue
            seen.add(number)
            title = _detag(NUM_RE.sub("", desc, count=1)).strip(" :")
            out.append({
                "number": number,
                "title": title,
                "date": _parse_date(date_cell, number),
                "seq": seqm.group(1),
            })
        return out

    # -------------------------------------------------------- extraction
    def _fetch_one(self, op: dict) -> dict | None:
        url = DOC_URL.format(seq=op["seq"])
        raw = self._curl_bytes(url)
        if not raw or raw[:4] != b"%PDF":
            return None
        md = extract_pdf_markdown(
            url, f"US/WI-JudicialEthics/{op['number']}",
            pdf_bytes=raw, table="doctrine", force=True)
        if not md or len(md.strip()) < 150:
            return None
        return {
            "number": op["number"],
            "title": op["title"],
            "text": md.strip(),
            "date": op["date"],
            "url": url,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing WI Judicial Conduct Advisory Committee opinions...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions found on index")
            return False
        logger.info(f"  discovered {len(ops)} opinions")
        ok = 0
        for op in ops[:5]:
            rec = self._fetch_one(op)
            if rec and len(rec["text"]) > 200:
                logger.info(f"  Opinion {rec['number']} OK "
                            f"({len(rec['text'])} chars) date={rec['date']}")
                ok += 1
        if ok:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw["number"]
        title = raw.get("title") or ""
        full_title = f"Wisconsin Judicial Ethics Opinion {number}"
        if title:
            full_title += f": {title}"
        return {
            "_id": f"US/WI-JudicialEthics/{number}",
            "_source": "US/WI-JudicialEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "issuer": "Wisconsin Judicial Conduct Advisory Committee",
            "title": full_title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-WI",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        ops = self._list_opinions()
        emitted = 0
        for op in ops:
            rec = self._fetch_one(op)
            if not rec:
                logger.warning(f"  no text for opinion {op['number']}, skipping")
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

    parser = argparse.ArgumentParser(description="US/WI-JudicialEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WIJudicialEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
