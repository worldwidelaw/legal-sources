#!/usr/bin/env python3
"""
US/UT-JudicialEthics -- Utah Judicial Ethics Advisory Committee -- Ethics
Advisory Opinions.

Fetches the full text of the ethics advisory opinions of the Utah Judicial
Ethics Advisory Committee, the committee established by the Utah Supreme Court
that issues written opinions construing the Utah Code of Judicial Conduct in
response to inquiries from judges and judicial officers. Each opinion is the
committee's authoritative written interpretation of the Code = doctrine. Two
published classes:

  - Informal Opinions   (caption "Informal Opinion No. YY-NN")
  - Formal Opinions     (caption "Formal Opinion No. YY-NN")

Access (no JavaScript-rendered data, no CAPTCHA, no auth):
  The Utah Courts site (utcourts.gov, Adobe Experience Manager) publishes one
  index page listing every opinion, /en/court-records-publications/publications/
  judicial-ethics-opinions.html. Each opinion is an <a> whose text is the
  opinion caption ("Informal Opinion No. 25-01") and whose href is either a
  born-digital PDF (newer opinions,
  /content/dam/.../ethics_opinions/{YYYY}/{num}.pdf) or an HTML opinion page
  (older opinions,
  /en/.../judicial-ethics-opinions/ethics-opinions/{YYYY}/{num}.html). The
  /content/dam PDF endpoint 406s a bare UA — a full browser User-Agent plus an
  Accept header serves the PDF.

Strategy:
  Parse the index page for (caption, url) anchors of both kinds, dedup by
  opinion number + class, fetch each opinion and extract full text — PDF via
  the shared common.pdf_extract backend chain, HTML via BeautifulSoup (the
  AEM "main .container" content region). The year comes from the URL path /
  number; the issue date is parsed from the opinion body. All records are
  doctrine.

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
from typing import Generator, Optional
from urllib.parse import urljoin

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import _extract as _pdf_extract_bytes

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover
    BeautifulSoup = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.UT-JudicialEthics")

BASE_URL = "https://www.utcourts.gov"
INDEX_URL = (
    BASE_URL
    + "/en/court-records-publications/publications/judicial-ethics-opinions.html"
)

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)

PDF_HREF_RE = re.compile(r"/ethics_opinions/(\d{4})/([^/\"']+)\.pdf$", re.I)
HTML_HREF_RE = re.compile(
    r"/judicial-ethics-opinions/ethics-opinions/(\d{4})/([^/\"']+)\.html$", re.I
)
# Captions: "Informal Opinion No. 25-01", "Formal Opinion No. 98-1".
CAPTION_RE = re.compile(r"\b(Informal|Formal)\s+Opinion\s+No\.?\s*([\w-]+)", re.I)

MONTHS = (
    "January February March April May June July August September "
    "October November December"
).split()
DATE_RE = re.compile(
    r"\b(" + "|".join(MONTHS) + r")\s+(\d{1,2}),?\s+((?:19|20)\d{2})\b"
)


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def _iso_from_body(text: str) -> Optional[str]:
    m = DATE_RE.search(text or "")
    if not m:
        return None
    mo = MONTHS.index(m.group(1)) + 1
    d = int(m.group(2))
    y = int(m.group(3))
    if 1 <= d <= 31:
        return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


def _decode(resp: requests.Response) -> str:
    """Decode an HTML response, tolerating the site's cp1252 bytes (§ etc.)."""
    raw = resp.content
    for enc in ("utf-8", "cp1252"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", "replace")


class UTJudicialEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": UA,
            "Accept": "text/html,application/pdf,application/xhtml+xml,*/*",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str):
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                return self.session.get(url, timeout=60, allow_redirects=True)
            except Exception as e:
                logger.warning(f"GET failed for {url} (attempt {attempt + 1}): {e}")
                time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    def _collect_index(self) -> list[dict]:
        r = self._get(INDEX_URL)
        if r is None or r.status_code != 200:
            logger.error(f"Index fetch failed: {getattr(r, 'status_code', None)}")
            return []
        if BeautifulSoup is None:
            logger.error("BeautifulSoup unavailable — cannot parse index")
            return []
        soup = BeautifulSoup(_decode(r), "html.parser")
        by_key: dict[str, dict] = {}
        for a in soup.find_all("a", href=True):
            href = a["href"]
            mp = PDF_HREF_RE.search(href)
            mh = HTML_HREF_RE.search(href)
            if not mp and not mh:
                continue
            kind = "pdf" if mp else "html"
            year = int((mp or mh).group(1))
            stem = (mp or mh).group(2)
            caption = _clean(a.get_text(" ", strip=True))
            mc = CAPTION_RE.search(caption)
            if mc:
                cls = mc.group(1).capitalize()
                number = mc.group(2)
            else:
                cls = "Informal"
                number = stem
            key = f"{cls}-{number}"
            # Prefer the PDF version when both exist for one opinion.
            if key in by_key and kind != "pdf":
                continue
            by_key[key] = {
                "opinion_class": cls,
                "number": number,
                "key": key,
                "year": year,
                "kind": kind,
                "url": urljoin(BASE_URL, href),
                "caption": caption,
            }
        ordered = sorted(
            by_key.values(),
            key=lambda r: (r["year"], r["number"]),
            reverse=True,
        )
        logger.info(f"Index collected: {len(ordered)} distinct opinions")
        return ordered

    def _extract_html(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        node = soup.select_one("main .container") or soup.find("main") or soup
        for t in node(["script", "style", "nav", "header", "footer", "aside", "form"]):
            t.decompose()
        text = node.get_text("\n", strip=True)
        return re.sub(r"\n{2,}", "\n", text).strip()

    def _fetch_one(self, row: dict) -> Optional[dict]:
        r = self._get(row["url"])
        if r is None or r.status_code != 200 or not r.content:
            return None
        if row["kind"] == "pdf":
            if not r.content[:5].startswith(b"%PDF"):
                logger.warning(f"  {row['key']}: not a PDF — skipped")
                return None
            text = (_pdf_extract_bytes(r.content) or "").strip()
        else:
            text = self._extract_html(_decode(r))
        if len(text) < 200:
            logger.warning(f"  {row['key']}: thin text ({len(text)} chars) — skipped")
            return None
        out = dict(row)
        out["text"] = text
        out["date"] = _iso_from_body(text) or f"{row['year']:04d}-01-01"
        out["final_url"] = r.url
        return out

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for row in self._collect_index():
            rec = self._fetch_one(row)
            if rec:
                yield rec
                emitted += 1
                logger.info(f"  {rec['key']} OK ({len(rec['text'])} chars, "
                            f"date={rec['date']}, {rec['kind']})")
                if sample and emitted >= 12:
                    return

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Utah Judicial Ethics Advisory Opinions...")
        rows = self._collect_index()
        if len(rows) < 20:
            logger.error(f"API test FAILED: too few opinions ({len(rows)})")
            return False
        ok = 0
        # Test a PDF and an HTML opinion.
        pdf_rows = [r for r in rows if r["kind"] == "pdf"][:2]
        html_rows = [r for r in rows if r["kind"] == "html"][:2]
        for row in pdf_rows + html_rows:
            rec = self._fetch_one(row)
            if rec and len(rec["text"]) > 200:
                logger.info(f"  {rec['key']} OK ({len(rec['text'])} chars, "
                            f"date={rec['date']}, {rec['kind']})")
                ok += 1
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: could not extract full text")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"US/UT-JudicialEthics/{raw['key']}",
            "_source": "US/UT-JudicialEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": raw["number"],
            "document_type": f"{raw['opinion_class']} Opinion",
            "issuer": "Utah Judicial Ethics Advisory Committee",
            "title": f"{raw['opinion_class']} Opinion No. {raw['number']}",
            "text": raw["text"],
            "url": raw.get("final_url") or raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-UT",
        }

    # ------------------------------------------------------------- fetch
    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            date = raw.get("date")
            if not since or (date and date >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/UT-JudicialEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = UTJudicialEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
