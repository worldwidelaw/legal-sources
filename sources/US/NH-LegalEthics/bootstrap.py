#!/usr/bin/env python3
"""
US/NH-LegalEthics -- New Hampshire Bar Association, Ethics Committee —
Ethics Committee Advisory Opinions.

Fetches the FULL TEXT of the formal + advisory ethics opinions issued by the
New Hampshire Bar Association Ethics Committee. Each opinion is the Committee's
written interpretation of the New Hampshire Rules of Professional Conduct
(NHRPC) in response to an inquiry about contemplated attorney conduct, to
advise LAWYERS = doctrine (advisory; lawyer discipline is administered
separately by the N.H. Supreme Court Attorney Discipline Office). New Hampshire
cites its opinions by a two-part bar-year number "#{YYYY-YY}/{N}" (e.g.
"#2017-18/01", "#1990-91/10"); the older series (1970-1984) carry
"Informal/Advisory/Formal Opinion" labels. Corpus ~120 born-digital PDF
opinions, 1970-present. The New Hampshire Bar Association is the state's
UNIFIED (mandatory) bar.

Access (no JavaScript execution needed, no CAPTCHA, no auth):
  1. A handful of public index pages on nhbar.org list every opinion:
       - the master "list of all NHBA ethics opinions" page (1990-present),
       - the per-decade pages (1970-1979; 1980-1989/1983-84-thru-1980;
         1990-1999; 2000-2007).
     Two publishing schemes coexist:
       (a) OLD (1970-1984): each opinion title links DIRECTLY to a born-digital
           PDF on nhba.s3.amazonaws.com; the "#{YYYY-YY}/{N}" number precedes
           the title in the page text.
       (b) MODERN (1990-present): each opinion has a detail page
           (/ethics/opinion-YYYY-YY-NN or /YYYY-YY-NN-slug/) that shows only an
           ABSTRACT; its first "Read More" link is the born-digital full-text
           PDF on nhba.s3.amazonaws.com.
     Because the detail pages carry only the abstract, this scraper always
     resolves the "Read More" PDF so it captures the FULL opinion body, not the
     summary.
  2. Each PDF is born-digital (text layer) — extracted with PyMuPDF (fitz),
     NO OCR. Records under 200 chars are skipped (a few oldest scans have no
     text layer).

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
from urllib.parse import urljoin, quote, unquote

import requests
import fitz  # PyMuPDF
from bs4 import BeautifulSoup, NavigableString

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NH-LegalEthics")

BASE = "https://www.nhbar.org"

# Public index / hub pages that between them list every opinion. The master
# "list of all" page covers 1990-present; the decade pages cover the older,
# directly-linked S3 PDFs (1970-1984) and the 1990-2007 detail pages.
HUBS = [
    "https://www.nhbar.org/list-of-all-nhba-ethics-opinions-and-articles-with-hyperlinks/",
    "https://www.nhbar.org/ethics-1970-1979/",
    "https://www.nhbar.org/resources/ethics-1980-1989/",
    "https://www.nhbar.org/ethics-opinions-1983-84-thru-1980/",
    "https://www.nhbar.org/resources/ethics-1990-1999/",
    "https://www.nhbar.org/resources/ethics-2000-2007/",
]

# The opinion citation number, e.g. "#2017-18/01", "#1990-91/1", "1982-83/25".
NUM_RE = re.compile(r"#?((?:19|20)\d\d)-(\d\d)/(\d+)")
# A modern detail-page URL, e.g. /ethics/opinion-2017-18-01 or
# /2022-23-01-ancillary-businesses-... .
DETAIL_RE = re.compile(
    r"nhbar\.org/(?:ethics/opinion-)?((?:19|20)\d\d)-(\d\d)[-/](\d+)", re.I
)
# A born-digital opinion PDF on the NHBA S3 bucket.
S3_PDF_RE = re.compile(r"nhba\.s3\.amazonaws\.com/.*\.pdf$", re.I)
# Sidebar/marketing PDFs that share the bucket but are not opinions.
JUNK_PDF_RE = re.compile(r"sponsorship|registration|brochure|_form|flyer|agenda",
                         re.I)
# A machine-readable date inside an opinion body / index caption.
DATE_TEXT_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|"
    r"November|December)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+((?:19|20)\d\d)\b",
    re.I,
)
DATE_NUM_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/((?:19|20)?\d\d)\b")
MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July", "August",
     "September", "October", "November", "December"], start=1)}


class NHLegalEthicsScraper(BaseScraper):

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
            "Accept": "text/html,application/pdf,*/*",
        })
        self._detail_cache: dict[str, dict] = {}

    # ---------------------------------------------------------------- http
    def _get(self, url: str) -> requests.Response | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                r = self._session.get(url, timeout=90, allow_redirects=True)
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
    def _canon(year: str, yr2: str, seq: str) -> str:
        """Canonical opinion number, e.g. ('2017','18','01') -> '2017-18/1'."""
        return f"{year}-{yr2}/{int(seq)}"

    @staticmethod
    def _num_from_url(url: str) -> str | None:
        m = DETAIL_RE.search(url)
        if not m:
            return None
        return NHLegalEthicsScraper._canon(m.group(1), m.group(2), m.group(3))

    def _resolve_pdf(self, detail_url: str) -> str | None:
        """Fetch a modern detail page and return its 'Read More' opinion PDF."""
        if detail_url in self._detail_cache:
            return self._detail_cache[detail_url].get("pdf")
        r = self._get(detail_url)
        pdf = None
        date = None
        if r:
            soup = BeautifulSoup(r.text, "html.parser")
            # The first "Read More" link is always the opinion's full-text PDF.
            for a in soup.find_all("a", href=True):
                if a.get_text(strip=True).lower() == "read more" \
                        and S3_PDF_RE.search(a["href"]):
                    pdf = a["href"]
                    break
            if not pdf:  # fall back to first opinion-ish S3 PDF on the page
                for a in soup.find_all("a", href=True):
                    if S3_PDF_RE.search(a["href"]) \
                            and not JUNK_PDF_RE.search(a["href"]):
                        pdf = a["href"]
                        break
            # Inline body date (ANNOTATION/ABSTRACT header often carries one).
            body = soup.get_text("\n", strip=True)
            date = self._parse_date(body)
        self._detail_cache[detail_url] = {"pdf": pdf, "date": date}
        return pdf

    def _list_opinions(self) -> list[dict]:
        """Return [{num, pdf, title, date}] de-duplicated on PDF URL."""
        out: dict[str, dict] = {}      # keyed by S3 pdf url
        for hub in HUBS:
            r = self._get(hub)
            if not r:
                logger.warning(f"  hub unreachable: {hub}")
                continue
            before = len(out)
            self._harvest(BeautifulSoup(r.text, "html.parser"), out)
            logger.info(f"  {hub.rsplit('/', 2)[-2]}: total now {len(out)} "
                        f"(+{len(out) - before})")
        result = list(out.values())
        # Sort newest-first so a sample spans the recent, richer opinions.
        result.sort(key=lambda x: (x.get("num") or "", x["pdf"]), reverse=True)
        logger.info(f"  discovered {len(result)} unique ethics opinions")
        return result

    def _harvest(self, soup: BeautifulSoup, out: dict) -> None:
        """Walk a hub page in document order, tracking the most-recent
        '#YYYY-YY/N' text marker so directly-linked S3 PDFs (the 1970-1984
        series) inherit their opinion number; modern detail-page links are
        resolved to their 'Read More' PDF."""
        last_num = None
        for node in soup.descendants:
            if isinstance(node, NavigableString):
                m = NUM_RE.search(str(node))
                if m:
                    last_num = self._canon(m.group(1), m.group(2), m.group(3))
                continue
            if getattr(node, "name", None) != "a":
                continue
            href = node.get("href") or ""
            title = node.get_text(" ", strip=True)
            full = urljoin(BASE, href)

            if S3_PDF_RE.search(full.split("?")[0]) \
                    and not JUNK_PDF_RE.search(full):
                # Directly-linked opinion PDF (old series).
                num = None
                m = NUM_RE.search(title)
                if m:
                    num = self._canon(m.group(1), m.group(2), m.group(3))
                elif last_num:
                    num = last_num
                self._add(out, full, num, title, None)
            elif DETAIL_RE.search(full):
                # Modern detail page -> resolve its Read More PDF.
                num = self._num_from_url(full)
                pdf = self._resolve_pdf(full)
                if pdf and S3_PDF_RE.search(pdf.split("?")[0]) \
                        and not JUNK_PDF_RE.search(pdf):
                    date = self._detail_cache.get(full, {}).get("date")
                    self._add(out, pdf, num, title, date)

    @staticmethod
    def _add(out, pdf, num, title, date):
        pdf = pdf.split("#")[0]
        title = (title or "").strip()
        if pdf in out:
            cur = out[pdf]
            if not cur.get("num") and num:
                cur["num"] = num
            if len(title) > len(cur.get("title") or ""):
                cur["title"] = title
            if not cur.get("date") and date:
                cur["date"] = date
            return
        out[pdf] = {"num": num, "pdf": pdf, "title": title, "date": date}

    # -------------------------------------------------------- extraction
    @staticmethod
    def _clean(text: str) -> str:
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
        text = text.replace("​", "").replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _parse_date(text: str) -> str | None:
        m = DATE_TEXT_RE.search(text)
        if m:
            mo = MONTHS[m.group(1).lower()]
            return f"{m.group(3)}-{mo:02d}-{int(m.group(2)):02d}"
        m = DATE_NUM_RE.search(text)
        if m:
            mo, dd, yy = int(m.group(1)), int(m.group(2)), m.group(3)
            if len(yy) == 2:
                yy = ("19" if int(yy) > 40 else "20") + yy
            if 1 <= mo <= 12 and 1 <= dd <= 31:
                return f"{yy}-{mo:02d}-{dd:02d}"
        return None

    @staticmethod
    def _title_from(op: dict) -> str:
        num = op.get("num")
        t = NUM_RE.sub("", op.get("title") or "").strip(" -–—")
        # Drop label prefixes like "Formal Opinion", "Advisory Opinion".
        t = re.sub(r"^(Formal|Advisory|Informal)\s+Opinion\b[:.\s-]*", "", t,
                   flags=re.I).strip()
        if num and t:
            return f"NH Ethics Committee Advisory Opinion #{num}: {t}"
        if num:
            return f"NH Ethics Committee Advisory Opinion #{num}"
        return t or "NH Ethics Committee Advisory Opinion"

    def _fetch_one(self, op: dict) -> dict | None:
        r = self._get(quote(op["pdf"], safe="/:%?=&"))
        if not r or not r.content:
            return None
        try:
            doc = fitz.open(stream=r.content, filetype="pdf")
            text = self._clean("\n".join(p.get_text() for p in doc))
        except Exception as e:
            logger.warning(f"  {op.get('num')}: PDF parse failed: {e}")
            return None
        if len(text) < 200:
            logger.warning(f"  {op.get('num') or op['pdf']}: insufficient text "
                           f"({len(text)} chars, likely scanned) - skipping")
            return None
        # Date: index caption -> opinion body -> year from number.
        date = op.get("date") or self._parse_date(text[:2500])
        if not date and op.get("num"):
            date = f"{op['num'][:4]}-01-01"
        stem = unquote(op["pdf"].rsplit("/", 1)[-1])[:-4]
        return {
            "opinion_number": op.get("num"),
            "id_key": (op["num"].replace("/", "-") if op.get("num")
                       else re.sub(r"[^A-Za-z0-9._-]", "_", stem)[:80]),
            "title": self._title_from(op),
            "text": text,
            "date": date,
            "url": op["pdf"],
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing New Hampshire Bar ethics opinions...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions listed")
            return False
        ok = 0
        for op in ops[:2] + ops[-1:]:
            rec = self._fetch_one(op)
            if rec and len(rec["text"]) > 500:
                logger.info(f"  Opinion {rec.get('opinion_number')} OK "
                            f"({len(rec['text'])} chars) date={rec['date']}")
                ok += 1
            else:
                logger.warning(f"  no text ({op['pdf']})")
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        key = raw["id_key"]
        num = raw.get("opinion_number")
        return {
            "_id": f"US/NH-LegalEthics/{key}",
            "_source": "US/NH-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": "New Hampshire Bar Association — Ethics Committee",
            "title": raw.get("title") or "NH Ethics Committee Advisory Opinion",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-NH",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        ops = self._list_opinions()
        emitted = 0
        seen_keys: set = set()
        for op in ops:
            rec = self._fetch_one(op)
            if not rec:
                continue
            if rec["id_key"] in seen_keys:
                continue
            seen_keys.add(rec["id_key"])
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

    parser = argparse.ArgumentParser(description="US/NH-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NHLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
