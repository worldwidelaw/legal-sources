#!/usr/bin/env python3
"""
US/MD-LegalEthics -- Maryland State Bar Association, Committee on Ethics —
Ethics Docket Opinions.

Fetches the full text of the Ethics Docket opinions issued by the Maryland
State Bar Association (MSBA) Committee on Ethics. Each opinion is the
Committee's written response, upon a member's request, interpreting the
Maryland Attorneys' Rules of Professional Conduct (MARPC/MRPC) as applied to
the inquirer's contemplated conduct = doctrine. The opinions are advisory
only — they are not binding on the Maryland Supreme Court or the Attorney
Grievance Commission (which administers lawyer discipline). Numbered series
"YYYY-NN" (e.g. 1987-34, 2016-07, 2025-02), ~500 opinions 1987-present,
published free as clean HTML on msba.org.

Distinct from:
  * US/MD-EthicsOpinions — the Maryland State Ethics Commission (public
    officials / conflict-of-interest law; blocked, dsd.maryland.gov).
  * the Maryland Judiciary's Judicial Ethics Committee (judges).
  * US/MD-COMAR (regulations) and US/MD-Legislation (statutes).
This is the attorney professional-conduct advisory-opinion series that in
other states we build as US/{ST}-LegalEthics (lawyers). The MSBA is
Maryland's *voluntary* bar, so — like US/NY-LegalEthics, US/IL-LegalEthics,
US/CT-LegalEthics and US/VT-LegalEthics — the government-edicts rationale is
weaker than for an integrated bar; treated as effectively public domain
(freely published, no paywall/login/terms), caveated accordingly.

Access (no JavaScript execution needed, no CAPTCHA, no auth):
  1. A single public index page lists every opinion:
       https://www.msba.org/site/site/content/Resources-and-Tools-Content/Ethics-Opinions-and-Hotline.aspx
     Each opinion links to a detail page
       .../Resources-and-Tools-Content/Ethics-Opinions/{YEAR}/{YEAR}-{NN}.aspx
     (a handful of older entries use irregular filenames, resolved from the
     href verbatim).
  2. Each detail page renders the opinion body in clean HTML inside the
     Decisis/SmartBar content panel
       <div id="ste_container_ciOpinionTextBody_...">
     extracted directly with BeautifulSoup, NO PDF/OCR needed. The body
     carries "COMMITTEE ON ETHICS / ETHICS DOCKET NO. YYYY-NN", the subject,
     the inquiry and the Committee's conclusion.

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

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.MD-LegalEthics")

BASE = "https://www.msba.org"
INDEX_URL = (
    "https://www.msba.org/site/site/content/"
    "Resources-and-Tools-Content/Ethics-Opinions-and-Hotline.aspx"
)

# href on the index that points at an individual opinion detail page, e.g.
# .../Resources-and-Tools-Content/Ethics-Opinions/2016/2016-07.aspx
LINK_RE = re.compile(r"Ethics-Opinions/(\d{4})/[^\"'>]+?\.aspx", re.I)

# Docket number from the page <title> or body header:
#   ETHICS DOCKET NO. 2016-07
NUM_RE = re.compile(r"ETHICS\s+DOCKET\s+NO\.?\s*(\d{4})\s*[-–]\s*(\d{1,3}[A-Za-z]?)", re.I)
# Fallback: from the URL filename YYYY-NN
URLNUM_RE = re.compile(r"/(\d{4})/[^/]*?(\d{4})[-_](\d{1,3}[A-Za-z]?)", re.I)

MONTH_DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+((?:19|20)\d\d)\b",
    re.I,
)
_MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}


class MDLegalEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            ),
            "Accept": "text/html,*/*",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str) -> requests.Response | None:
        for attempt in range(4):
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
    @staticmethod
    def _norm_num(year: str, seq: str) -> str:
        """Canonical 'YYYY-NN' (zero-padded number, preserve any letter)."""
        m = re.match(r"(\d+)([A-Za-z]?)", seq)
        n, suffix = m.group(1), m.group(2)
        return f"{int(year):04d}-{int(n):02d}{suffix.lower()}"

    def _list_opinions(self) -> list[tuple[str, str, int]]:
        """Return [(opinion_number 'YYYY-NN', detail_url, year)],
        de-duplicated on opinion_number, ordered oldest-first.

        The full corpus is enumerated from the single index page — every
        opinion appears there as a link into /Ethics-Opinions/{YEAR}/."""
        r = self._get(INDEX_URL)
        if not r:
            logger.error("could not fetch the ethics opinions index page")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        out: dict[str, tuple[str, str, int]] = {}
        for a in soup.find_all("a", href=True):
            href = a["href"]
            m = LINK_RE.search(href)
            if not m:
                continue
            year = m.group(1)
            url = urljoin(INDEX_URL, href)
            # Number from the filename if it looks like YYYY-NN; else fill
            # provisionally from the year and resolve precisely at fetch time
            # off the page title.
            fm = re.search(r"(\d{4})[-_](\d{1,3}[A-Za-z]?)", href.split("/")[-1])
            if fm:
                num = self._norm_num(fm.group(1), fm.group(2))
            else:
                num = f"{year}-url{abs(hash(url)) % 1000:03d}"
            if num in out:
                continue
            out[num] = (num, url, int(year))
        result = sorted(out.values(), key=lambda x: x[0])
        logger.info(f"  discovered {len(result)} unique ethics docket opinions")
        return result

    # -------------------------------------------------------- extraction
    @staticmethod
    def _clean(text: str) -> str:
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
        text = text.replace("​", "").replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _extract_body(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        body = soup.find("div", id=re.compile(r"ste_container_ciOpinionTextBody"))
        if body is None:
            # fall back to the main content area minus obvious chrome
            body = soup.find("div", id="mainContentWrapper")
        if body is None:
            return ""
        for tag in body.find_all(["script", "style", "nav", "form", "noscript"]):
            tag.decompose()
        return self._clean(body.get_text("\n", strip=True))

    def _subject(self, text: str, num: str) -> str:
        """Best-effort short subject line following the docket-number header."""
        m = re.search(
            r"ETHICS\s+DOCKET\s+NO\.?\s*\d{4}\s*[-–]\s*\d{1,3}[A-Za-z]?\s*\n(.+)",
            text, re.I)
        if m:
            subj = m.group(1).strip()
            if 0 < len(subj) <= 200 and not subj.lower().startswith(
                    ("you ", "in your", "we have", "the committee", "on ")):
                return subj
        return ""

    def _fetch_one(self, num: str, url: str, year: int) -> dict | None:
        r = self._get(url)
        if not r or not r.text:
            return None
        text = self._extract_body(r.text)
        if len(text) < 150:
            logger.warning(f"  {num}: insufficient text ({len(text)} chars)")
            return None

        # Prefer the exact docket number carried in the page itself.
        tm = NUM_RE.search(r.text)
        if tm:
            num = self._norm_num(tm.group(1), tm.group(2))
            year = int(num[:4])

        # Date: MSBA dockets are numbered by year; use an explicit in-range
        # "Month DD, YYYY" from the body if present, else YYYY-01-01.
        date = f"{year}-01-01"
        for mm in MONTH_DATE_RE.finditer(text):
            mon, day, yr = mm.group(1).lower(), int(mm.group(2)), int(mm.group(3))
            if abs(yr - year) <= 1 and 1 <= day <= 31:
                date = f"{yr:04d}-{_MONTHS[mon]:02d}-{day:02d}"
                break

        subject = self._subject(text, num)
        title = f"MSBA Ethics Docket No. {num}"
        if subject:
            title += f" — {subject}"

        return {
            "opinion_number": num,
            "title": title,
            "subject": subject,
            "text": text,
            "date": date,
            "url": url,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Maryland State Bar ethics docket opinions...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions listed")
            return False
        ok = 0
        for num, url, year in ops[:2] + ops[-1:]:
            rec = self._fetch_one(num, url, year)
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
            "_id": f"US/MD-LegalEthics/{num}",
            "_source": "US/MD-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": "Maryland State Bar Association — Committee on Ethics",
            "title": raw.get("title") or f"MSBA Ethics Docket No. {num}",
            "subject": raw.get("subject") or None,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-MD",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        seen: set[str] = set()
        for num, url, year in self._list_opinions():
            rec = self._fetch_one(num, url, year)
            if not rec:
                logger.warning(f"  no text for {num}, skipping")
                continue
            if rec["opinion_number"] in seen:
                continue
            seen.add(rec["opinion_number"])
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

    parser = argparse.ArgumentParser(description="US/MD-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MDLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
