#!/usr/bin/env python3
"""
US/PA-Code -- The Pennsylvania Code (state administrative regulations)

Fetches the full text of every Section of the Pennsylvania Code, the
official codification of the rules and regulations of the Commonwealth of
Pennsylvania's executive-branch agencies (published by the Legislative
Reference Bureau under the Commonwealth Documents Law). The Code is the
state analogue of the federal CFR, organized by numbered Titles -> Parts
-> Chapters -> Sections. Each codified Section is a regulation =
legislation, and the Code is an official Pennsylvania state-government
work in the public domain (government edicts / edicts-of-government).

BUILD RECIPE (builds + validates LOCALLY, no CAPTCHA / JS / auth):
The official full-text database is served as static HTML at
www.pacodeandbulletin.gov.  The list of Titles is embedded as <option>
tags on the /Home/Pacode page (value="/{NNN}/{NNN}toc.html").  For each
Title the scraper fetches the Title TOC

  /secure/pacode/data/{NNN}/{NNN}toc.html

and extracts the per-Chapter TOC links (chapter{N}/chap{N}toc.html);
each Chapter TOC lists the individual Section HTML files (sX.Y.html).
Every Section HTML page carries clean born-digital full text plus <meta>
tags (title2, chapter2, section2) used to build stable ids/citations.
The latest effective/amendment date is parsed from the "Source" note in
the section body when present, else null.  No auth, no CAPTCHA.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.PA-Code")

HOST = "https://www.pacodeandbulletin.gov"
TITLE_INDEX = HOST + "/Home/Pacode"
DATA_BASE = HOST + "/secure/pacode/data"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

# <option value="/001/001toc.html">1&nbsp;&nbsp;&nbsp;GENERAL PROVISIONS</option>
TITLE_OPT_RE = re.compile(
    r'<option[^>]*value="/(\d{3})/\d{3}toc\.html"[^>]*>(.*?)</option>',
    re.IGNORECASE | re.DOTALL)
# Chapter TOC links inside a Title TOC: chapter31/chap31toc.html
CHAP_TOC_RE = re.compile(
    r'href="(chapter[0-9A-Za-z]+/chap[0-9A-Za-z]+toc\.html)"', re.IGNORECASE)
# Section HTML links inside a Chapter TOC: s31.1.html  (also handles s31.1a.html)
SECTION_RE = re.compile(r'href="(s[0-9][0-9A-Za-z.]*\.html)"', re.IGNORECASE)

META_RE = {
    "title2": re.compile(r'<meta\s+name="title2"\s+content="([^"]*)"', re.I),
    "chapter2": re.compile(r'<meta\s+name="chapter2"\s+content="([^"]*)"', re.I),
    "section2": re.compile(r'<meta\s+name="section2"\s+content="([^"]*)"', re.I),
}
PAGE_TITLE_RE = re.compile(r'<title>(.*?)</title>', re.IGNORECASE | re.DOTALL)

# Effective/amendment dates in a Source note, e.g. "effective July 1, 2019"
EFFECTIVE_RE = re.compile(
    r'effective\s+(January|February|March|April|May|June|July|August|'
    r'September|October|November|December)\s+(\d{1,2}),\s+((?:19|20)\d{2})',
    re.IGNORECASE)

FOOTER_MARKERS = (
    "No part of the information on this site may be reproduced",
    "This material has been drawn directly from the official",
)

MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}


class PACodeScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.35
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": UA})
        self._existing: set[str] = set()

    # ---------------------------------------------------------------- http
    def _get_text(self, url: str) -> str | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, timeout=(15, 90))
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                # Pages are served latin-1 / windows-1252
                resp.encoding = resp.apparent_encoding or "latin-1"
                return resp.text
            except Exception as e:
                logger.warning(f"GET failed ({url[:90]}) attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _clean_label(raw: str) -> str:
        s = re.sub(r'<[^>]+>', ' ', raw)
        s = _html.unescape(s).replace("\xa0", " ")
        return re.sub(r'\s+', ' ', s).strip()

    @classmethod
    def _title_name(cls, label: str) -> str:
        # "1     GENERAL PROVISIONS" -> "GENERAL PROVISIONS"
        s = cls._clean_label(label)
        return re.sub(r'^\d+\s+', '', s).strip()

    @staticmethod
    def _extract_body_text(html_doc: str) -> str:
        # Drop <head>, image maps and scripts before stripping tags.
        body = re.split(r'</head>', html_doc, maxsplit=1, flags=re.I)
        body = body[1] if len(body) > 1 else html_doc
        body = re.sub(r'<map\b.*?</map>', ' ', body, flags=re.I | re.S)
        body = re.sub(r'<script\b.*?</script>', ' ', body, flags=re.I | re.S)
        body = re.sub(r'<img\b[^>]*>', ' ', body, flags=re.I)
        # Preserve paragraph/line breaks as newlines for readability.
        body = re.sub(r'<br\s*/?>', '\n', body, flags=re.I)
        body = re.sub(r'</(p|h[1-6]|div|tr|li)>', '\n', body, flags=re.I)
        text = re.sub(r'<[^>]+>', ' ', body)
        text = _html.unescape(text).replace("\xa0", " ")
        # Strip the republishing-notice footer.
        for marker in FOOTER_MARKERS:
            idx = text.find(marker)
            if idx != -1:
                text = text[:idx]
        # Normalise whitespace: collapse spaces, keep paragraph breaks.
        lines = [re.sub(r'[ \t]+', ' ', ln).strip() for ln in text.splitlines()]
        text = "\n".join(ln for ln in lines if ln)
        return text.strip()

    @classmethod
    def _latest_effective(cls, text: str) -> str | None:
        best = None
        for m in EFFECTIVE_RE.finditer(text):
            mon = MONTHS.get(m.group(1).lower())
            if not mon:
                continue
            try:
                d, y = int(m.group(2)), int(m.group(3))
            except ValueError:
                continue
            if not (1 <= d <= 31 and 1900 <= y <= 2100):
                continue
            iso = f"{y:04d}-{mon:02d}-{d:02d}"
            if best is None or iso > best:
                best = iso
        return best

    # --------------------------------------------------------- discovery
    def _titles(self) -> list[tuple[str, str]]:
        """Return [(NNN, title_name), ...] from the Pacode index page."""
        html_doc = self._get_text(TITLE_INDEX)
        if not html_doc:
            return []
        out: list[tuple[str, str]] = []
        seen: set[str] = set()
        for m in TITLE_OPT_RE.finditer(html_doc):
            nnn = m.group(1)
            if nnn in seen or nnn == "000":
                continue
            seen.add(nnn)
            out.append((nnn, self._title_name(m.group(2))))
        logger.info(f"Pacode index: {len(out)} Titles")
        return out

    def _chapter_tocs(self, nnn: str) -> list[str]:
        toc = self._get_text(f"{DATA_BASE}/{nnn}/{nnn}toc.html")
        if not toc:
            return []
        rels, seen = [], set()
        for rel in CHAP_TOC_RE.findall(toc):
            rel = rel.lower()
            if rel not in seen:
                seen.add(rel)
                rels.append(rel)
        return rels

    def _sections(self, nnn: str, chap_rel: str) -> list[str]:
        """Return absolute Section HTML URLs for one chapter TOC."""
        chap_dir = chap_rel.rsplit("/", 1)[0]           # e.g. 'chapter31'
        toc = self._get_text(f"{DATA_BASE}/{nnn}/{chap_rel}")
        if not toc:
            return []
        urls, seen = [], set()
        for rel in SECTION_RE.findall(toc):
            url = f"{DATA_BASE}/{nnn}/{chap_dir}/{rel}"
            if url not in seen:
                seen.add(url)
                urls.append(url)
        return urls

    def discover(self, sample: bool = False) -> Generator[dict, None, None]:
        found = 0
        for nnn, tname in self._titles():
            n_title = 0
            for chap_rel in self._chapter_tocs(nnn):
                for sec_url in self._sections(nnn, chap_rel):
                    yield {"url": sec_url, "title_number": str(int(nnn)),
                           "title_name": tname}
                    found += 1
                    n_title += 1
                    if sample and found >= 25:
                        logger.info(f"Sample: stopped after {found} sections")
                        return
            if n_title:
                logger.info(f"Title {int(nnn)} {tname}: {n_title} sections")
        logger.info(f"Discovered {found} Pennsylvania Code sections")

    # ------------------------------------------------------- build record
    def _build_raw(self, entry: dict) -> dict | None:
        url = entry["url"]
        html_doc = self._get_text(url)
        if not html_doc:
            return None

        def meta(name: str) -> str | None:
            m = META_RE[name].search(html_doc)
            return m.group(1).strip() if m else None

        title2 = meta("title2") or entry["title_number"]
        section2 = meta("section2")
        chapter2 = meta("chapter2")
        if not section2:
            # Fall back to the filename (sX.Y.html -> X.Y)
            fm = re.search(r'/s([0-9][0-9A-Za-z.]*)\.html$', url)
            section2 = fm.group(1) if fm else None
        if not section2:
            return None

        record_id = f"{title2}-{section2}"
        if record_id in self._existing:
            return None

        text = self._extract_body_text(html_doc)
        if not text or len(text.strip()) < 40:
            logger.warning(f"Insufficient text for {record_id} "
                           f"({len(text or '')} chars) — skipping")
            return None

        pt = PAGE_TITLE_RE.search(html_doc)
        page_title = self._clean_label(pt.group(1)) if pt else None
        citation = f"{title2} Pa. Code § {section2}"
        title = page_title or f"{citation}."

        date = self._latest_effective(text)

        return {
            "record_id": record_id,
            "citation": citation,
            "title_number": title2,
            "title_name": entry.get("title_name"),
            "chapter": chapter2,
            "section": section2,
            "title": title,
            "text": text,
            "date": date,
            "url": url,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Pennsylvania Code (pacodeandbulletin.gov)...")
        try:
            entries = list(self.discover(sample=True))
            if not entries:
                logger.error("  No sections discovered")
                return False
            logger.info(f"  Discovered {len(entries)} sections (sample)")
            raw = None
            for e in entries:
                raw = self._build_raw(e)
                if raw:
                    break
            if raw and raw["text"] and len(raw["text"]) > 40:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('citation')} [{raw.get('date')}]")
            else:
                logger.error("  Text extraction failed")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"US/PA-Code/{raw['record_id']}",
            "_source": "US/PA-Code",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["record_id"],
            "citation": raw.get("citation") or None,
            "title_number": raw.get("title_number") or None,
            "title_name": raw.get("title_name") or None,
            "chapter": raw.get("chapter") or None,
            "section": raw.get("section") or None,
            "issuer": "Commonwealth of Pennsylvania (Legislative Reference Bureau)",
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-PA",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        if not sample:
            try:
                self._existing = preload_existing_ids("US/PA-Code", "legislation")
            except Exception as e:
                logger.warning(f"preload_existing_ids failed: {e}")
                self._existing = set()
        emitted = 0
        for entry in self.discover(sample=sample):
            raw = self._build_raw(entry)
            if raw:
                yield raw
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

    parser = argparse.ArgumentParser(description="US/PA-Code bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = PACodeScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
