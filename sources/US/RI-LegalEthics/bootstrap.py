#!/usr/bin/env python3
"""
US/RI-LegalEthics -- Rhode Island Supreme Court Ethics Advisory Panel Opinions

Fetches the full text of the advisory opinions issued by the Rhode Island
Supreme Court Ethics Advisory Panel, which the Supreme Court established in 1986
to give Rhode Island attorneys confidential, prospective advice on the Rhode
Island Rules of Professional Conduct. The Panel is an arm of the Supreme Court
of Rhode Island (created and governed by the Court's own rules), so its
opinions are the work of a government-authorized body -> pd-us. = doctrine
(advisory guidance to LAWYERS).

Distinct from US/RI-Courts (Rhode Island appellate courts), US/RI-Legislation
(the General Laws) and the executive Rhode Island Ethics Commission
(ethics.ri.gov), which advises public OFFICIALS, not lawyers.

Access (no JavaScript execution, no CAPTCHA, no auth):
  1. Every published opinion PDF is indexed in the Judiciary's SharePoint
     search index. The public REST search endpoint
       /_api/search/query?querytext='RIJCourt:"Ethics Advisory Panel"'
     returns one row per opinion PDF (~782 rows), each carrying the direct
     Path to the file under https://www.courts.ri.gov/Opinions/.
     Pagination is via rowlimit (<=500) + startrow.
  2. Filenames are irregular: older ones use a raw space and a 2-digit year
     ("EAP 87-03.pdf", "EAP 98-12.pdf"), newer ones a hyphen and a 4-digit
     year ("EAP-2024-01.pdf"); a few carry no "EAP" prefix ("2002-03a.pdf").
     We take the Path verbatim and URL-quote it, and parse the opinion number
     from the filename (canonicalising 2-digit years: 98 -> 1998).
  3. Each PDF from ~1998 onward is born-digital (text layer) -> extracted with
     PyMuPDF, NO OCR. Pre-1998 opinions (1986-1997) survive only as scanned
     image PDFs with no text layer (0 chars) and are correctly skipped.
     The body header reads "Rhode Island Supreme Court / Ethics Advisory Panel
     Op. {num}" (or "...Opinion No. {num}") + "Issued {Month DD, YYYY}" +
     a FACTS section and analysis.

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
from urllib.parse import quote

import requests

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
logger = logging.getLogger("legal-data-hunter.US.RI-LegalEthics")

BASE = "https://www.courts.ri.gov"
SEARCH_URL = (
    BASE + "/_api/search/query"
    "?querytext='RIJCourt:%22Ethics%20Advisory%20Panel%22'"
    "&rowlimit={rowlimit}&startrow={startrow}"
)
PAGE = 500

# Opinions before 1998 survive only as scanned image PDFs with no text layer
# (verified: every sampled 1987-1997 file extracts 0 chars). We skip them
# without downloading rather than fetch ~561 PDFs that would all be discarded.
# If the Judiciary ever OCRs the older opinions, lower this threshold.
MIN_TEXT_YEAR = 1998

MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"], start=1)}
# "Issued February 8, 2024" / "Issued November 16, 2010"
ISSUED_RE = re.compile(
    r"Issued\s+([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})", re.I)
# Opinion number in a filename: EAP 87-03 / EAP-2024-01 / 2002-03a
NUM_RE = re.compile(r"(\d{2,4})-(\d{1,3}[A-Za-z]?)")


class RILegalEthicsScraper(BaseScraper):

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
            "Accept": "application/json;odata=verbose,text/html,*/*",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str, as_json: bool = False):
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                r = self._session.get(url, timeout=60)
                if r.status_code == 200:
                    return r.json() if as_json else r
                if r.status_code == 404:
                    return None
                logger.warning(f"GET {url[:90]} -> HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"GET failed (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    @staticmethod
    def _canon_number(filename: str) -> str | None:
        """Parse & canonicalise the opinion number from a PDF filename."""
        stem = filename.rsplit("/", 1)[-1]
        stem = re.sub(r"\.pdf$", "", stem, flags=re.I)
        m = NUM_RE.search(stem)
        if not m:
            return None
        yr, seq = m.group(1), m.group(2)
        if len(yr) == 2:
            y = int(yr)
            yr = f"19{yr}" if y >= 80 else f"20{yr}"
        return f"{yr}-{seq}"

    def _list_opinions(self) -> list[dict]:
        """Walk the SharePoint search index; return [{number, url}] de-duped."""
        out: dict[str, dict] = {}
        startrow = 0
        total = None
        while True:
            data = self._get(
                SEARCH_URL.format(rowlimit=PAGE, startrow=startrow),
                as_json=True)
            if not data:
                break
            try:
                rel = (data["d"]["query"]["PrimaryQueryResult"]
                       ["RelevantResults"])
                if total is None:
                    total = rel["TotalRows"]
                rows = rel["Table"]["Rows"]["results"]
            except (KeyError, TypeError):
                logger.warning("  unexpected search response shape")
                break
            if not rows:
                break
            for row in rows:
                cells = {c["Key"]: c["Value"]
                         for c in row["Cells"]["results"]}
                path = cells.get("OriginalPath") or cells.get("Path") or ""
                if not path.lower().endswith(".pdf"):
                    continue
                num = self._canon_number(path)
                if not num or num in out:
                    continue
                fname = path.rsplit("/", 1)[-1]
                url = BASE + "/Opinions/" + quote(fname)
                out[num] = {"number": num, "url": url}
            startrow += PAGE
            if total is not None and startrow >= total:
                break
        result = sorted(out.values(), key=lambda x: self._sort_key(x["number"]))
        logger.info(f"  discovered {len(result)} indexed opinion PDFs "
                    f"(SharePoint total rows: {total})")
        return result

    @staticmethod
    def _sort_key(num: str):
        m = re.match(r"(\d{4})-(\d+)", num)
        if m:
            return (int(m.group(1)), int(m.group(2)))
        return (9999, 0)

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
    def _issue_date(text: str, number: str) -> str | None:
        m = ISSUED_RE.search(text[:1500])
        if m:
            mon = MONTHS.get(m.group(1).lower())
            day = int(m.group(2))
            yr = int(m.group(3))
            if mon and 1 <= day <= 31 and 1980 <= yr <= 2100:
                return f"{yr:04d}-{mon:02d}-{day:02d}"
        # fallback: year prefix of the opinion number
        ym = re.match(r"(\d{4})-", number)
        if ym:
            return f"{ym.group(1)}-01-01"
        return None

    def _fetch_one(self, op: dict) -> dict | None:
        r = self._get(op["url"])
        if not r or not r.content:
            return None
        if r.content[:4] != b"%PDF" and \
                "pdf" not in r.headers.get("content-type", "").lower():
            logger.warning(f"  {op['number']}: not a PDF")
            return None
        try:
            raw = self._pdf_text(r.content)
        except Exception as e:
            logger.warning(f"  {op['number']}: PDF extract failed: {e}")
            return None
        text = self._clean(raw)
        if len(text) < 150:
            # pre-1998 opinions are scanned images with no text layer
            logger.info(f"  {op['number']}: no text layer "
                        f"({len(text)} chars) — scanned, skipping")
            return None
        return {
            "number": op["number"],
            "title": (f"Rhode Island Supreme Court Ethics Advisory Panel "
                      f"Opinion {op['number']}"),
            "text": text,
            "date": self._issue_date(text, op["number"]),
            "url": op["url"],
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing RI Supreme Court Ethics Advisory Panel...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions listed")
            return False
        ok = 0
        for op in ops[-3:]:
            rec = self._fetch_one(op)
            if rec and len(rec["text"]) > 300:
                logger.info(f"  {rec['number']} OK ({len(rec['text'])} chars) "
                            f"date={rec['date']}")
                ok += 1
            else:
                logger.warning(f"  no text ({op['url']})")
        if ok >= 2:
            logger.info(f"API test PASSED ({len(ops)} opinions indexed)")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["number"]
        return {
            "_id": f"US/RI-LegalEthics/{num}",
            "_source": "US/RI-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": "Rhode Island Supreme Court — Ethics Advisory Panel",
            "title": raw.get("title")
            or f"RI Ethics Advisory Panel Opinion {num}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-RI",
        }

    # ------------------------------------------------------------- fetch
    @staticmethod
    def _op_year(number: str) -> int | None:
        m = re.match(r"(\d{4})-", number)
        return int(m.group(1)) if m else None

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for op in self._list_opinions():
            yr = self._op_year(op["number"])
            if yr is not None and yr < MIN_TEXT_YEAR:
                continue  # pre-1998: scanned image PDF, no text layer
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

    parser = argparse.ArgumentParser(description="US/RI-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = RILegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
