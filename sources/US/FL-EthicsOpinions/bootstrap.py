#!/usr/bin/env python3
"""
US/FL-EthicsOpinions -- Florida Commission on Ethics — Advisory Opinions (CEO)

Fetches the full text of the formal advisory opinions ("CEO" opinions) of the
Florida Commission on Ethics. Under Part III, ch. 112, Fla. Stat. (the Code of
Ethics for Public Officers and Employees) and § 112.322(3), the Commission
issues written advisory opinions in response to a request by a public officer,
employee or candidate, authoritatively construing the state's conflict-of-
interest, voting-conflict, gift, honoraria, financial-disclosure and
standards-of-conduct laws on the facts presented; the requestor is entitled to
rely on the opinion. Each opinion states the Commission's official
interpretation of the law = doctrine (official state legal interpretation;
public-domain government edicts, 17 U.S.C. § 105 analogue).

Access (no JavaScript, no CAPTCHA, no auth):
  Opinions are published as born-digital, full-text HTML pages:

      https://www.ethics.state.fl.us/Documents/Opinions/{YY}/CEO {YY}-{NNN}.htm

  The research index /Research/Opinions.aspx links a per-year list page for
  every year the Commission has issued opinions:

      /Research/OpinionsLists/List{YY}.aspx     (List74..List99, List00..List26)

  Each list page hyperlinks every opinion HTML file for that year. The opinion
  pages are standalone documents (the CEO number + issue date + summary + full
  text), no site chrome in the content, no OCR needed.

Strategy:
  GET /Research/Opinions.aspx, collect the List{YY}.aspx year pages, GET each,
  collect the /Documents/Opinions/{YY}/CEO ....htm links, fetch each opinion
  (URL-encoding the space), strip the HTML to clean text, parse the CEO number
  from the filename and the issue date from the body, and normalize into the
  doctrine schema.

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
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import urljoin, quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from bs4 import BeautifulSoup

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.FL-EthicsOpinions")

BASE_URL = "https://www.ethics.state.fl.us"
INDEX_URL = "https://www.ethics.state.fl.us/Research/Opinions.aspx"

# Year-list pages linked from the index (e.g. /Research/OpinionsLists/List24.aspx).
LIST_HREF_RE = re.compile(r'/Research/OpinionsLists/List(\d{2})\.aspx', re.I)

# Opinion HTML files (e.g. /Documents/Opinions/76/CEO 76-002A.htm).
OPINION_HREF_RE = re.compile(
    r'/Documents/Opinions/\d{2}/CEO[ %20]+([0-9]{2}-[0-9]{1,4}[A-Za-z]?)\.htm',
    re.I,
)

DATE_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2}),\s+(\d{4})",
    re.I,
)
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}


class FLEthicsScraper(BaseScraper):

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
    def _curl_text(self, url: str) -> str | None:
        # Percent-encode spaces (opinion filenames contain a space).
        url = url.replace(" ", "%20")
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "60", "-A", self._ua,
                     "-H", "Accept: text/html,*/*", url],
                    capture_output=True, timeout=90,
                )
                if out.returncode == 0 and out.stdout:
                    # These opinions are MS Word "Save as Web Page" exports with
                    # no charset declaration → Windows-1252 (cp1252), NOT UTF-8.
                    # Decoding as UTF-8 mangles §, em-dashes, smart quotes and
                    # nbsp (\xa0) into replacement chars, so try UTF-8 strict
                    # first and fall back to cp1252.
                    raw = out.stdout
                    try:
                        return raw.decode("utf-8")
                    except UnicodeDecodeError:
                        return raw.decode("cp1252", "replace")
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- parsing
    @staticmethod
    def _clean_text(html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "head", "nav", "header", "footer"]):
            tag.decompose()
        text = soup.get_text("\n")
        text = _html.unescape(text)
        text = re.sub(r"[ \t ]+", " ", text)
        text = re.sub(r"\n[ \t]*\n+", "\n\n", text)
        lines = [ln.strip() for ln in text.splitlines()]
        text = "\n".join(ln for ln in lines if ln)
        return text.strip()

    @staticmethod
    def _norm_date(text: str) -> str | None:
        m = DATE_RE.search(text)
        if not m:
            return None
        mo = MONTHS.get(m.group(1).lower())
        d = int(m.group(2))
        y = int(m.group(3))
        if mo and 1 <= d <= 31 and 1970 <= y <= 2035:
            return f"{y:04d}-{mo:02d}-{d:02d}"
        return None

    # ---------------------------------------------------------- discovery
    def _list_years(self) -> list[str]:
        html = self._curl_text(INDEX_URL)
        if not html:
            return []
        years = sorted({m.group(1) for m in LIST_HREF_RE.finditer(html)})
        return years

    def _list_year(self, yy: str) -> dict[str, str]:
        """Return {ceo_number: opinion_url} for a given two-digit year."""
        html = self._curl_text(
            f"{BASE_URL}/Research/OpinionsLists/List{yy}.aspx")
        out: dict[str, str] = {}
        if not html:
            return out
        for m in OPINION_HREF_RE.finditer(html):
            href = m.group(0)
            ceo = m.group(1).upper()
            if ceo not in out:
                out[ceo] = urljoin(BASE_URL, href)
        return out

    def _list_all(self) -> dict[str, str]:
        all_ops: dict[str, str] = {}
        years = self._list_years()
        for yy in years:
            ops = self._list_year(yy)
            if ops:
                logger.info(f"  year 19/20{yy}: {len(ops)} opinions")
            for ceo, url in ops.items():
                all_ops.setdefault(ceo, url)
        return all_ops

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing FL Commission on Ethics opinion index + extraction...")
        ops = self._list_year("24")
        if not ops:
            logger.error("API test FAILED: no opinion links found for 2024")
            return False
        logger.info(f"  discovered {len(ops)} opinions in 2024")
        ok = 0
        for ceo, url in list(ops.items())[:5]:
            html = self._curl_text(url)
            if not html:
                continue
            text = self._clean_text(html)
            if len(text) > 400:
                logger.info(f"  CEO {ceo} OK ({len(text)} chars) "
                            f"date={self._norm_date(text)}")
                ok += 1
        if ok:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        ceo = raw.get("ceo")
        return {
            "_id": f"US/FL-EthicsOpinions/{ceo}",
            "_source": "US/FL-EthicsOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": f"CEO {ceo}" if ceo else None,
            "issuer": "Florida Commission on Ethics",
            "title": raw.get("title") or (f"Florida Commission on Ethics Opinion CEO {ceo}"
                                          if ceo else "Florida Commission on Ethics Opinion"),
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-FL",
        }

    # ------------------------------------------------------------- fetch
    @staticmethod
    def _title_of(text: str, ceo: str) -> str:
        # The opinion body opens with "CEO YY-N—<Month D, YYYY>" (often twice)
        # then an ALL-CAPS subject heading; use the first substantive line that
        # is not the CEO/date header as the subject.
        for ln in text.splitlines():
            ln = ln.strip()
            if len(ln) < 8:
                continue
            if re.match(r"(?i)^CEO\b", ln):
                continue
            if DATE_RE.fullmatch(ln):
                continue
            # Skip the numeric header variant "YY-N -- Month D, YYYY".
            if re.match(r"^\d{2}-\d{1,4}[A-Za-z]?\s*[-–—]", ln):
                continue
            return f"CEO {ceo}: {ln[:200]}" if ceo else ln[:220]
        return f"Florida Commission on Ethics Opinion CEO {ceo}"

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        all_ops = self._list_all()
        emitted = 0
        # Newest first for a representative sample.
        for ceo in sorted(all_ops, reverse=True):
            url = all_ops[ceo]
            html = self._curl_text(url)
            if not html:
                continue
            text = self._clean_text(html)
            if len(text) < 400:
                continue
            yield {
                "ceo": ceo,
                "url": url,
                "title": self._title_of(text, ceo),
                "text": text,
                "date": self._norm_date(text),
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

    parser = argparse.ArgumentParser(description="US/FL-EthicsOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = FLEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
