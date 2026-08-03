#!/usr/bin/env python3
"""
US/NE-LegalEthics -- Nebraska Supreme Court Lawyer Ethics Advisory Opinions

Fetches the full text of the "Nebraska Ethics Advisory Opinion for Lawyers"
series issued by the Advisory Committee of the Nebraska Supreme Court (the
Lawyers' Advisory Committee). Pursuant to Rule of Discipline 5, the Committee
renders advisory opinions interpreting the Nebraska Rules of Professional
Conduct with respect to anticipatory conduct by a requesting attorney = doctrine
(advisory guidance to LAWYERS). The Committee is created by, and its members are
appointed by, the Nebraska Supreme Court, so its opinions are the work of a
body authorized by the state's highest court -> pd-us.

Distinct from US/NE-EthicsOpinions (the Nebraska Accountability and Disclosure
Commission — campaign-finance / conflict-of-interest advice to public
OFFICIALS), US/NE-Courts (appellate courts) and US/NE-Legislation.

Access (server-rendered Drupal, plain GET, no auth / JS / CAPTCHA):
  1. ENUMERATION -- the opinions are listed in a paginated Drupal view at
       /administration/professional-ethics/attorney-discipline-ethics/lawyer-ethics-opinions?page=N
     (0..14, ~20 rows/page, ~288 opinions 1968-present). Each row (a
     div.views-row-inner) carries labelled fields: the opinion Number
     (views-field-title), the Year (views-field-field-year), the Question
     Presented snippet (views-field-body) and a "Download PDF" link to the
     full opinion (views-field-field-opinion).
  2. FULL TEXT -- every opinion PDF under /sites/default/files/opinions/ is
     born-digital (text layer present for all eras, verified back to 1968) ->
     extracted with PyMuPDF, NO OCR. Filenames are irregular (25-01.pdf,
     Formal-Opinion-%2324-03.pdf, Opinion-24-01.pdf, 68-1_0.pdf) so the href
     is taken verbatim from the row and the opinion number is read from the
     row's Number field (not the filename).
  3. The body header reads "Nebraska Ethics Advisory Opinion for Lawyers /
     No. {num}" + the question presented + analysis + conclusion.

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
logger = logging.getLogger("legal-data-hunter.US.NE-LegalEthics")

BASE = "https://nebraskajudicial.gov"
LIST_URL = (
    BASE + "/administration/professional-ethics/attorney-discipline-ethics/"
    "lawyer-ethics-opinions?page={page}"
)
# The view redirects from supremecourt.nebraska.gov; 15 pages of ~20 rows.
MAX_PAGES = 40  # generous ceiling; we stop when a page has no rows

NUM_RE = re.compile(r"\b(\d{2})[-–](\d{1,3})\b")


class NELegalEthicsScraper(BaseScraper):

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
            "Accept": "text/html,application/xhtml+xml,*/*",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str):
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                r = self._session.get(url, timeout=60, allow_redirects=True)
                if r.status_code == 200:
                    return r
                if r.status_code == 404:
                    return None
                logger.warning(f"GET {url[:90]} -> HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"GET failed (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    @staticmethod
    def _field(row, cls: str) -> str | None:
        el = row.select_one(f".views-field-{cls} .field-content")
        if not el:
            el = row.select_one(f".views-field-{cls}")
        if not el:
            return None
        txt = el.get_text(" ", strip=True)
        # strip a leading "Label:" prefix if present
        txt = re.sub(r"^[A-Za-z ]+:\s*", "", txt).strip()
        return txt or None

    def _parse_page(self, html: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        out = []
        for row in soup.select("div.views-row-inner"):
            a = row.select_one(".views-field-field-opinion a[href]")
            if not a:
                # fall back to any opinion-PDF anchor in the row
                a = next((x for x in row.find_all("a", href=True)
                          if "/opinions/" in x["href"].lower()), None)
            if not a:
                continue
            number = self._field(row, "title")
            year = self._field(row, "field-year")
            question = self._field(row, "body")
            url = urljoin(BASE + "/", a["href"])
            out.append({
                "number": number,
                "year": year,
                "question": question,
                "url": url,
            })
        return out

    def _list_opinions(self) -> list[dict]:
        seen: set[str] = set()
        result: list[dict] = []
        for page in range(MAX_PAGES):
            r = self._get(LIST_URL.format(page=page))
            if not r:
                break
            rows = self._parse_page(r.text)
            if not rows:
                break
            new = 0
            for op in rows:
                key = op["url"]
                if key in seen:
                    continue
                seen.add(key)
                result.append(op)
                new += 1
            logger.info(f"  page {page}: {len(rows)} rows ({new} new)")
            if new == 0:
                break
        logger.info(f"  discovered {len(result)} opinion PDFs")
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

    @staticmethod
    def _pdf_text(content: bytes) -> str:
        if fitz is None:
            raise RuntimeError("PyMuPDF (fitz) is required for PDF extraction")
        doc = fitz.open(stream=content, filetype="pdf")
        try:
            return "".join(page.get_text() for page in doc)
        finally:
            doc.close()

    @staticmethod
    def _canon_number(op: dict) -> str | None:
        """Canonical 'YYYY-NN' from the Number field + authoritative Year."""
        num = op.get("number") or ""
        m = NUM_RE.search(num)
        if not m:
            # try the filename as a last resort
            m = NUM_RE.search(op["url"].rsplit("/", 1)[-1])
            if not m:
                return None
        seq = m.group(2)
        year = op.get("year")
        if year and year.isdigit() and len(year) == 4:
            yr = year
        else:
            yy = int(m.group(1))
            yr = f"19{yy:02d}" if yy >= 50 else f"20{yy:02d}"
        return f"{yr}-{seq}"

    @staticmethod
    def _op_date(op: dict, number: str) -> str | None:
        year = op.get("year")
        if year and year.isdigit() and len(year) == 4:
            return f"{year}-01-01"
        m = re.match(r"(\d{4})-", number)
        return f"{m.group(1)}-01-01" if m else None

    def _fetch_one(self, op: dict) -> dict | None:
        number = self._canon_number(op)
        if not number:
            logger.warning(f"  could not parse number from {op['url']}")
            return None
        r = self._get(op["url"])
        if not r or not r.content:
            return None
        if r.content[:4] != b"%PDF" and \
                "pdf" not in r.headers.get("content-type", "").lower():
            logger.warning(f"  {number}: not a PDF ({op['url']})")
            return None
        try:
            raw = self._pdf_text(r.content)
        except Exception as e:
            logger.warning(f"  {number}: PDF extract failed: {e}")
            return None
        text = self._clean(raw)
        if len(text) < 150:
            logger.info(f"  {number}: no text layer ({len(text)} chars) — "
                        f"scanned, skipping")
            return None
        return {
            "number": number,
            "title": f"Nebraska Ethics Advisory Opinion for Lawyers No. {number}",
            "text": text,
            "date": self._op_date(op, number),
            "url": op["url"],
            "question": op.get("question"),
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Nebraska Supreme Court Lawyer Ethics Opinions...")
        r = self._get(LIST_URL.format(page=0))
        if not r:
            logger.error("API test FAILED: listing page unreachable")
            return False
        rows = self._parse_page(r.text)
        if not rows:
            logger.error("API test FAILED: no rows parsed")
            return False
        logger.info(f"  page 0 has {len(rows)} rows")
        ok = 0
        for op in rows[:3]:
            rec = self._fetch_one(op)
            if rec and len(rec["text"]) > 300:
                logger.info(f"  {rec['number']} OK ({len(rec['text'])} chars) "
                            f"date={rec['date']}")
                ok += 1
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["number"]
        return {
            "_id": f"US/NE-LegalEthics/{num}",
            "_source": "US/NE-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": ("Advisory Committee of the Nebraska Supreme Court "
                       "(Lawyers' Advisory Committee)"),
            "title": raw.get("title")
            or f"Nebraska Ethics Advisory Opinion for Lawyers No. {num}",
            "text": raw["text"],
            "question_presented": raw.get("question"),
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-NE",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        seen_num: set[str] = set()
        for op in self._list_opinions():
            rec = self._fetch_one(op)
            if not rec:
                continue
            if rec["number"] in seen_num:
                continue
            seen_num.add(rec["number"])
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

    parser = argparse.ArgumentParser(description="US/NE-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NELegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
