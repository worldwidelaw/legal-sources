#!/usr/bin/env python3
"""
US/WA-JudicialEthics -- Washington State Ethics Advisory Committee
                        — Judicial Ethics Advisory Opinions

Fetches the full text of the Ethics Advisory Opinions issued by the Washington
State Ethics Advisory Committee (EAC). The EAC, established by the Washington
Supreme Court, issues formal advisory opinions construing the Washington Code of
Judicial Conduct (CJC). Each opinion answers a specific inquiry about the ethical
obligations of judges and judicial officers under the CJC = doctrine (the
Committee's official written interpretation of the judicial-conduct rules).

Access (no JavaScript, no CAPTCHA, no auth):
  The Committee publishes a "List of Opinion Numbers by Year" index whose links
  point to per-opinion HTML pages:

      https://www.courts.wa.gov/programs_orgs/pos_ethics/?fa=pos_ethics.byyear
      https://www.courts.wa.gov/programs_orgs/pos_ethics/?fa=pos_ethics.dispopin&mode=NNNN

  where NNNN is a four-digit code "YYSS" (two-digit year + two-digit sequence),
  e.g. mode=2001 -> Opinion 20-01, mode=8401 -> Opinion 84-01. Each opinion page
  is born-digital HTML: the opinion body sits between an "Opinion YY-SS" header
  and a footer that repeats "Opinion YY-SS" followed by the issue date
  (MM/DD/YYYY). The full text (Question / Answer / CJC analysis) is extracted
  directly from the HTML — no OCR, no PDF.

Strategy:
  GET the by-year index and enumerate every mode code. For each opinion, GET its
  page, slice out the opinion body between the header and footer markers, capture
  the issue date, and normalize. courts.wa.gov requires a browser User-Agent
  (default UAs get a 403), so all requests carry a desktop-Chrome UA.

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
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.WA-JudicialEthics")

BASE = "https://www.courts.wa.gov/programs_orgs/pos_ethics/"
INDEX_URL = BASE + "?fa=pos_ethics.byyear"
OPINION_URL = BASE + "?fa=pos_ethics.dispopin&mode={mode}"

# Enumerate the mode codes on the by-year index (four ASCII digits: YYSS).
MODE_RE = re.compile(r"mode=(\d{4})")
DATE_RE = re.compile(r"\b(\d{2})/(\d{2})/(\d{4})\b")


def _mode_to_label(mode: str) -> str:
    """mode 2001 -> '20-01'."""
    return f"{mode[:2]}-{mode[2:]}"


def _mode_year(mode: str) -> int:
    """Two-digit year prefix -> full year (84 -> 1984, 26 -> 2026)."""
    yy = int(mode[:2])
    return 1900 + yy if yy >= 50 else 2000 + yy


def _page_text(raw_html: str) -> list[str]:
    """Convert an opinion HTML page to a list of non-empty text lines."""
    body = re.sub(r"(?is)<script.*?</script>|<style.*?</style>", "", raw_html)
    # Turn block boundaries into newlines, drop remaining tags.
    body = re.sub(r"(?i)<(br|/p|/div|/td|/tr|/li|/h[1-6])[^>]*>", "\n", body)
    body = re.sub(r"<[^>]+>", " ", body)
    body = _html.unescape(body).replace("\xa0", " ")
    lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in body.splitlines()]
    return [ln for ln in lines if ln]


class WAJudicialEthicsScraper(BaseScraper):

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
    def _curl(self, url: str):
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "60", "-A", self._ua, url],
                    capture_output=True, timeout=90,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout.decode("utf-8", "replace")
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    def _list_modes(self) -> list[str]:
        """Return every opinion mode code (YYSS), newest first, de-duplicated."""
        html = self._curl(INDEX_URL)
        if not html:
            logger.error("could not fetch the by-year index")
            return []
        seen: dict[str, None] = {}
        for m in MODE_RE.finditer(html):
            seen.setdefault(m.group(1), None)
        return list(seen.keys())

    # -------------------------------------------------------- extraction
    def _extract(self, mode: str, html: str) -> dict | None:
        """Slice the opinion body + issue date out of an opinion page."""
        label = _mode_to_label(mode)
        header = f"Opinion {label}"
        # The header line may carry a trailing marker (e.g. "Opinion 26-03 (Amended)"),
        # so match by prefix; the closing footer line is the bare "Opinion YY-SS".
        header_re = re.compile(r"^Opinion\s+" + re.escape(label) + r"\b")
        lines = _page_text(html)

        # Start at the first "Opinion YY-SS ..." header line.
        try:
            start = next(i for i, ln in enumerate(lines) if header_re.match(ln))
        except StopIteration:
            return None

        # Footer nav begins at the "RECORDS" line followed by "Case Records".
        end = len(lines)
        for i in range(start + 1, len(lines)):
            if lines[i] == "RECORDS" and i + 1 < len(lines) and lines[i + 1] == "Case Records":
                end = i
                break
        # Between the body and the footer nav sits the closing "Opinion YY-SS"
        # header, the issue date (MM/DD/YYYY) and, if revised, an
        # "Amended MM/DD/YYYY" line. Walk back over those to find the body end
        # and capture the original (non-amended) issue date.
        date = None
        j = end - 1
        while j > start:
            ln = lines[j]
            if ln == header:
                j -= 1
                continue
            dm = re.fullmatch(r"(?:Amended\s+)?(\d{2})/(\d{2})/(\d{4})", ln)
            if dm:
                if not ln.startswith("Amended"):
                    mm, dd, yyyy = int(dm.group(1)), int(dm.group(2)), int(dm.group(3))
                    if 1 <= mm <= 12 and 1 <= dd <= 31:
                        date = f"{yyyy:04d}-{mm:02d}-{dd:02d}"
                j -= 1
                continue
            break
        body_lines = lines[start:j + 1]
        text = "\n".join(body_lines).strip()
        if len(text) < 150:
            return None
        if date is None:
            date = f"{_mode_year(mode):04d}-01-01"
        return {
            "mode": mode,
            "label": label,
            "text": text,
            "date": date,
            "url": OPINION_URL.format(mode=mode),
        }

    def _fetch_one(self, mode: str) -> dict | None:
        html = self._curl(OPINION_URL.format(mode=mode))
        if not html:
            return None
        return self._extract(mode, html)

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing WA Judicial Ethics Advisory Opinions...")
        modes = self._list_modes()
        if not modes:
            logger.error("API test FAILED: no opinion codes found on index")
            return False
        logger.info(f"  discovered {len(modes)} opinion codes")
        ok = 0
        for mode in modes[:5]:
            rec = self._fetch_one(mode)
            if rec and len(rec["text"]) > 200:
                logger.info(f"  Opinion {rec['label']} OK "
                            f"({len(rec['text'])} chars) date={rec['date']}")
                ok += 1
        if ok:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        label = raw["label"]
        return {
            "_id": f"US/WA-JudicialEthics/{label}",
            "_source": "US/WA-JudicialEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": label,
            "issuer": "Washington State Ethics Advisory Committee",
            "title": f"Washington Ethics Advisory Opinion {label}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-WA",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        modes = self._list_modes()
        emitted = 0
        for mode in modes:
            rec = self._fetch_one(mode)
            if not rec:
                logger.warning(f"  no text for mode={mode}, skipping")
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

    parser = argparse.ArgumentParser(description="US/WA-JudicialEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WAJudicialEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
