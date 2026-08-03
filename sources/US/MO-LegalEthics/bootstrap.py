#!/usr/bin/env python3
"""
US/MO-LegalEthics -- Advisory Committee of the Supreme Court of Missouri /
Office of Legal Ethics Counsel — Informal & Formal Ethics Opinions

Fetches the full text of the ethics advisory opinions issued to Missouri
lawyers by the Office of Legal Ethics Counsel and the Advisory Committee of
the Supreme Court of Missouri, pursuant to Missouri Supreme Court Rule
5.30(c). Each opinion interprets the Missouri Rules of Professional Conduct
(Rule 4), the discipline rules (Rule 5) and the fees-to-practice rule
(Rule 6) in response to a lawyer's inquiry about contemplated conduct =
doctrine (advisory; not binding).

Two series:
  * Informal Opinions  — numbered "YYYY-NN" (published summaries dating back
    to July 1, 1993), ~1,046 opinions.
  * Formal Opinions    — numbered by a plain running integer (e.g. 115),
    ~13 opinions.

Access (no JavaScript, CAPTCHA or authentication):
  1. Enumeration is via the site's Yoast XML sitemaps, which list every
     opinion permalink exhaustively:
       /informalopinions-sitemap.xml   (+ ...-sitemap2.xml)
       /formalopinions-sitemap.xml
     Informal permalinks are /informal-opinion/{YYYY-NN}/ and formal
     permalinks are /formal-opinion/{N}/.
  2. Each opinion page is a Divi-built layout in which every field is a
     <div class="et_pb_text_inner"> block: a labelled "Informal/Formal
     Opinion Number:", "Adoption Date:", "Rules:" and "Subject:" block, then
     the opinion body block (Question/Answer for informal opinions, the
     numbered FORMAL OPINION text for formal ones). The scraper reads the
     labelled blocks for metadata and takes the longest non-boilerplate block
     as the body — clean HTML, NO PDF/OCR.

A screen-reader artefact (" dash" inside <span class="sr-only">) is stripped
so cited rules read "4-1.15" rather than "4 dash-1.15".

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
from typing import Generator, Optional, Tuple

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.MO-LegalEthics")

BASE = "https://mo-legal-ethics.org"
SITEMAPS = [
    "/informalopinions-sitemap.xml",
    "/informalopinions-sitemap2.xml",
    "/formalopinions-sitemap.xml",
]
OPINION_RE = re.compile(
    r"https://mo-legal-ethics\.org/(informal|formal)-opinion/([^/\"<]+)/", re.I)

NUM_LABEL_RE = re.compile(r"(?:Informal|Formal)\s+Opinion\s+Number:\s*(.+)", re.I)
DATE_LABEL_RE = re.compile(r"Adoption\s+Date:\s*(.+)", re.I)
RULES_LABEL_RE = re.compile(r"Rules?:\s*(.+)", re.I)
SUBJECT_LABEL_RE = re.compile(r"Subject:\s*(.+)", re.I | re.S)

MONTH_DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+((?:19|20)\d\d)\b",
    re.I,
)
_MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}

# Text blocks that are theme chrome, not opinion content
_BOILER_PREFIXES = (
    "back to search", "home", "contact us", "site map", "privacy policy",
    "© copyright", "copyright",
)
_BOILER_CONTAINS = (
    "informal opinions are ethics advisory opinions issued",
    "formal opinions are issued by the advisory committee",
)


class MOLegalEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str) -> Optional[requests.Response]:
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
    def _list_opinions(self) -> list[Tuple[str, str, str]]:
        """Return [(series, slug, url)] de-duplicated on url, from the
        exhaustive Yoast opinion sitemaps."""
        out: dict[str, Tuple[str, str, str]] = {}
        for sm in SITEMAPS:
            r = self._get(BASE + sm)
            if not r:
                continue
            found = 0
            for m in OPINION_RE.finditer(r.text):
                series, slug = m.group(1).lower(), m.group(2)
                url = m.group(0)
                if not slug or slug in ("informal-opinion", "formal-opinion"):
                    continue
                if url in out:
                    continue
                out[url] = (series, slug, url)
                found += 1
            logger.info(f"  {sm}: +{found} (total {len(out)})")
        vals = list(out.values())
        # Interleave the handful of formal opinions evenly through the informal
        # ones so both series are represented up-front (the sample takes the
        # first N of fetch_all) and the full corpus ingests mixed.
        formal = [v for v in vals if v[0] == "formal"]
        informal = [v for v in vals if v[0] == "informal"]
        result: list[Tuple[str, str, str]] = []
        stride = max(1, len(informal) // (len(formal) + 1)) if formal else 1
        fi = 0
        for idx, item in enumerate(informal):
            if formal and idx % stride == 0 and fi < len(formal):
                result.append(formal[fi])
                fi += 1
            result.append(item)
        result.extend(formal[fi:])
        logger.info(f"  discovered {len(result)} unique opinions "
                    f"({len(formal)} formal, {len(informal)} informal)")
        return result

    # -------------------------------------------------------- extraction
    @staticmethod
    def _clean(text: str) -> str:
        # remove the screen-reader " dash" artefact around en/em dashes
        text = re.sub(r"\s*\bdash\b\s*([‐-―\-])", r"\1", text, flags=re.I)
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
        text = text.replace("​", "").replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _blocks(self, html: str) -> list[str]:
        soup = BeautifulSoup(html, "html.parser")
        blocks = []
        for div in soup.find_all("div", class_="et_pb_text_inner"):
            txt = self._clean(div.get_text("\n", strip=True))
            if txt:
                blocks.append(txt)
        return blocks

    @staticmethod
    def _is_boiler(b: str) -> bool:
        low = b.lower()
        if any(low.startswith(p) for p in _BOILER_PREFIXES):
            return True
        if any(c in low for c in _BOILER_CONTAINS):
            return True
        return False

    def _parse_opinion(self, series: str, slug: str, url: str) -> Optional[dict]:
        r = self._get(url)
        if not r or not r.text:
            return None
        blocks = self._blocks(r.text)
        if not blocks:
            return None

        number = None
        adoption = None
        rules = None
        subject = None
        labelled_idx = set()
        for i, b in enumerate(blocks):
            m = NUM_LABEL_RE.search(b)
            if m and number is None:
                number = m.group(1).strip()
                labelled_idx.add(i)
                continue
            m = DATE_LABEL_RE.search(b)
            if m and adoption is None:
                adoption = m.group(1).strip()
                labelled_idx.add(i)
                continue
            m = RULES_LABEL_RE.match(b)
            if m and rules is None:
                rules = m.group(1).strip()
                labelled_idx.add(i)
                continue
            m = SUBJECT_LABEL_RE.match(b)
            if m and subject is None:
                subject = self._clean(m.group(1))
                labelled_idx.add(i)
                continue

        # Body = the longest non-boilerplate, non-label block.
        body_candidates = [
            b for i, b in enumerate(blocks)
            if i not in labelled_idx and not self._is_boiler(b)
        ]
        if not body_candidates:
            return None
        body = max(body_candidates, key=len)

        if not number:
            # fall back to the permalink slug
            number = slug

        # Assemble full text: prepend the subject line if present.
        parts = []
        if subject:
            parts.append(f"Subject: {subject}")
        parts.append(body)
        text = "\n\n".join(parts).strip()
        if len(text) < 150:
            return None

        # Canonical number + a year for dating.
        #   modern informal: "YYYY-NN" (2025-01) -> year in the string
        #   legacy informal: all-digit "YYnnnn" (930075 = 1993, #0075)
        #   formal: a plain running integer (115) -> no year in the number,
        #           but these carry an explicit Adoption Date.
        year = None
        ym = re.search(r"(19|20)\d{2}", number)
        if ym:
            year = int(ym.group(0))
        elif re.fullmatch(r"\d{5,}", number):
            yy = int(number[:2])
            year = 1900 + yy if yy >= 50 else 2000 + yy

        # Date: prefer the explicit Adoption Date, else in-body month-date,
        # else the number's year -> YYYY-01-01, else None.
        date = None
        for src in (adoption, body):
            if not src:
                continue
            dm = MONTH_DATE_RE.search(src)
            if dm:
                mon, day, yr = dm.group(1).lower(), int(dm.group(2)), int(dm.group(3))
                if 1 <= day <= 31:
                    date = f"{yr:04d}-{_MONTHS[mon]:02d}-{day:02d}"
                    break
        if not date and year:
            date = f"{year}-01-01"

        prefix = "Informal" if series == "informal" else "Formal"
        title = subject.split("Summary")[0].strip(" ;") if subject else ""
        if not title:
            title = f"Missouri {prefix} Ethics Opinion {number}"
        title = re.sub(r"\s+", " ", title)[:300]

        return {
            "opinion_number": number,
            "series": prefix,
            "title": title,
            "text": text,
            "rules_cited": rules,
            "date": date,
            "url": url,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Missouri legal ethics opinions...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions listed")
            return False
        ok = 0
        # test one formal + first two informal
        sample = [o for o in ops if o[0] == "formal"][:1] + \
                 [o for o in ops if o[0] == "informal"][:2]
        for series, slug, url in sample:
            rec = self._parse_opinion(series, slug, url)
            if rec and len(rec["text"]) > 300:
                logger.info(f"  {rec['series']} Opinion {rec['opinion_number']} OK "
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
        series = raw.get("series", "Informal")
        return {
            "_id": f"US/MO-LegalEthics/{series[:3].upper()}-{num}",
            "_source": "US/MO-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "series": series,
            "issuer": "Advisory Committee of the Supreme Court of Missouri / "
                      "Office of Legal Ethics Counsel",
            "title": raw.get("title") or f"Missouri {series} Ethics Opinion {num}",
            "text": raw["text"],
            "rules_cited": raw.get("rules_cited"),
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-MO",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        seen: set[str] = set()
        ops = self._list_opinions()
        if sample:
            # a mix: the formal opinions first, then informal
            ops = sorted(ops, key=lambda o: (o[0] != "formal", o[2]))
        for series, slug, url in ops:
            rec = self._parse_opinion(series, slug, url)
            if not rec:
                continue
            key = f"{rec['series']}-{rec['opinion_number']}"
            if key in seen:
                continue
            seen.add(key)
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

    parser = argparse.ArgumentParser(description="US/MO-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MOLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
