#!/usr/bin/env python3
"""
INTL/EnergyCommunity-Cases -- Energy Community dispute settlement cases

The Energy Community (Vienna-based international organisation extending the EU
energy acquis to the Western Balkans, Ukraine, Moldova and Georgia) runs a
dispute settlement procedure under Article 91 of the Energy Community Treaty.
The Secretariat opens non-compliance cases against Contracting Parties; cases
proceed through Opening Letter / Reasoned Opinion / Ministerial Council Decision
stages. The full case registry (2008-present) is published on the EnC-LEX
portal with a per-case detail page and linked Ministerial Council Decision PDFs.

Strategy:
  - Fetch the case registry index, extract every per-case detail URL
    (/enc-lex/cases/registry/YYYY/caseNNYYCC.html)
  - For each case, fetch the detail page and extract the structured case
    summary (party, registered date, area of work, legal provision, subject
    matter, status) plus the narrative reasoning
  - Download linked Ministerial Council Decision / Reasoned Opinion PDFs and
    append their extracted full text
  - ~228 cases

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast --full
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import html
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

try:
    from common.pdf_extract import extract_pdf_markdown
except Exception:  # pragma: no cover
    extract_pdf_markdown = None


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.EnergyCommunity-Cases")

BASE = "https://www.energy-community.org"
REGISTRY_URL = f"{BASE}/enc-lex/cases/registry.html"

CASE_URL_RE = re.compile(r'/enc-lex/cases/registry/(\d{4})/case([^"\']+)\.html')
# Case code like 0112UA / 1126NM / 0811SBH -> NN, YY, CC (country code, 2 letters)
CASE_CODE_RE = re.compile(r'^(\d{2})(\d{2})S?([A-Z]{2})$')

# End markers that close the main content region on a case detail page
CONTENT_END_MARKERS = (
    '<div class="sidebar-area',
    '<div class="component-linklist-sidebar',
    'twitter.com/ener_community',
    '<footer',
)

COUNTRY_NAMES = {
    "AL": "Albania", "BH": "Bosnia and Herzegovina", "GE": "Georgia",
    "KO": "Kosovo*", "MD": "Moldova", "MO": "Moldova", "ME": "Montenegro",
    "MK": "North Macedonia", "NM": "North Macedonia", "MA": "North Macedonia",
    "RS": "Serbia", "SR": "Serbia", "UA": "Ukraine", "ML": "Moldova",
}


class EnergyCommunityCasesScraper(BaseScraper):
    """
    Scraper for INTL/EnergyCommunity-Cases -- Energy Community dispute settlement.
    Country: INTL
    URL: https://www.energy-community.org/enc-lex/cases/registry.html

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*",
        })

    # ------------------------------------------------------------------ helpers

    def _get(self, url: str) -> str:
        r = self.session.get(url, timeout=60)
        r.raise_for_status()
        return r.text

    @staticmethod
    def _clean(text: str) -> str:
        text = re.sub(r'<br\s*/?>', '\n', text)
        text = re.sub(r'</p>', '\n\n', text)
        text = re.sub(r'<[^>]+>', ' ', text)
        # entities may hide encoded markup (e.g. social-share links) — unescape
        # then strip a second time so no raw tags survive into the text field
        text = html.unescape(text)
        text = re.sub(r'<[^>]+>', ' ', text)
        # drop UI boilerplate
        text = text.replace("SHARE", " ").replace("Tweet", " ")
        # remove any dangling unterminated tag left at a cut boundary
        text = re.sub(r'<[^>]*$', '', text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n[ \t]+', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _list_cases(self) -> list:
        """Return list of (year, code, url) for every case in the registry."""
        html_text = self._get(REGISTRY_URL)
        seen = set()
        cases = []
        for m in CASE_URL_RE.finditer(html_text):
            year, code = m.group(1), m.group(2)
            url = f"{BASE}{m.group(0)}"
            if url in seen:
                continue
            seen.add(url)
            cases.append((year, code, url))
        cases.sort(key=lambda c: (c[0], c[1]))
        return cases

    def _extract_content(self, page: str) -> str:
        """Extract the main case-content region from a detail page."""
        i = page.find('content-area content-container')
        if i < 0:
            return ""
        gt = page.find('>', i)
        seg = page[gt + 1:] if gt > 0 else page[i:]
        end = len(seg)
        for marker in CONTENT_END_MARKERS:
            j = seg.find(marker)
            if 0 < j < end:
                end = j
        seg = seg[:end]
        seg = re.sub(r'<script.*?</script>', '', seg, flags=re.S)
        return self._clean(seg)

    @staticmethod
    def _extract_title(page: str) -> str:
        m = re.search(r'<title>(.*?)</title>', page, re.S)
        if not m:
            return ""
        t = html.unescape(m.group(1))
        return t.split(' - Energy Community')[0].strip()

    @staticmethod
    def _extract_pdf_links(page: str) -> list:
        links = re.findall(r'href="(/dam/[^"]+\.(?:pdf|PDF))"', page)
        out, seen = [], set()
        for l in links:
            if l not in seen:
                seen.add(l)
                out.append(BASE + l)
        return out

    @staticmethod
    def _parse_registered_date(content: str) -> Optional[str]:
        # e.g. "registered: upon complaint 04.01.2012" or "Registered ex officio"
        m = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', content)
        if m:
            d, mo, y = m.groups()
            try:
                return datetime(int(y), int(mo), int(d)).strftime("%Y-%m-%d")
            except ValueError:
                return None
        return None

    def _pdf_text(self, url: str) -> str:
        if extract_pdf_markdown is None:
            return ""
        try:
            r = self.session.get(url, timeout=120)
            if r.status_code != 200 or "pdf" not in r.headers.get("Content-Type", "").lower():
                return ""
            return extract_pdf_markdown(
                source="INTL/EnergyCommunity-Cases",
                source_id=url,
                pdf_bytes=r.content,
                table="case_law",
            ) or ""
        except Exception as e:
            logger.warning(f"PDF error {url}: {e}")
            return ""

    # ------------------------------------------------------------------- public

    def _build_case(self, year: str, code: str, url: str, with_pdfs: bool = True) -> Optional[dict]:
        try:
            page = self._get(url)
        except Exception as e:
            logger.warning(f"Fetch failed {url}: {e}")
            return None

        title = self._extract_title(page)
        content = self._extract_content(page)
        pdf_links = self._extract_pdf_links(page)

        parts = [content] if content else []
        if with_pdfs:
            for p in pdf_links[:6]:
                ptext = self._pdf_text(p)
                if ptext and len(ptext) > 100:
                    parts.append(f"\n\n--- {p.rsplit('/', 1)[-1]} ---\n\n{ptext}")
                time.sleep(1.0)

        text = "\n".join(parts).strip()
        if not text or len(text) < 60:
            logger.warning(f"Insufficient text for {code}: {len(text)} chars")
            return None

        cm = CASE_CODE_RE.match(code)
        country = cm.group(3) if cm else ""
        date = self._parse_registered_date(content)

        return {
            "_raw_id": f"ECS-{code}",
            "year": year,
            "code": code,
            "country_code": country,
            "country_name": COUNTRY_NAMES.get(country, ""),
            "title": title or f"Case ECS-{code}",
            "text": text,
            "date": date,
            "url": url,
            "pdf_links": pdf_links,
        }

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"EnC/{raw['code']}",
            "_source": "INTL/EnergyCommunity-Cases",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date") or None,
            "url": raw.get("url", ""),
            "court": "Energy Community (Article 91 dispute settlement)",
            "case_number": f"ECS-{raw['code']}",
            "country_code": raw.get("country_code", ""),
            "country_name": raw.get("country_name", ""),
            "year": raw.get("year", ""),
            "reference_documents": raw.get("pdf_links", []),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        cases = self._list_cases()
        logger.info(f"Registry lists {len(cases)} cases")
        for idx, (year, code, url) in enumerate(cases, 1):
            logger.info(f"[{idx}/{len(cases)}] ECS-{code}")
            rec = self._build_case(year, code, url)
            if rec:
                yield rec
            time.sleep(1.0)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        try:
            since_date = datetime.fromisoformat(since).date()
        except Exception:
            since_date = None
        for year, code, url in self._list_cases():
            # registry case codes encode year (YY); cheap filter on year
            cm = CASE_CODE_RE.match(code)
            if cm and since_date:
                cyear = 2000 + int(cm.group(2))
                if cyear < since_date.year:
                    continue
            rec = self._build_case(year, code, url)
            if rec:
                yield rec
            time.sleep(1.0)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="INTL/EnergyCommunity-Cases")
    subparsers = parser.add_subparsers(dest="command")

    for name in ("bootstrap", "bootstrap-fast"):
        bp = subparsers.add_parser(name, help="Full initial fetch")
        bp.add_argument("--sample", action="store_true", help="Sample mode")
        bp.add_argument("--sample-size", type=int, default=15)
        bp.add_argument("--full", action="store_true")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = EnergyCommunityCasesScraper()

    if args.command == "test":
        cases = scraper._list_cases()
        logger.info(f"Registry lists {len(cases)} cases")
        y, c, u = cases[0]
        rec = scraper._build_case(y, c, u)
        if rec:
            logger.info(f"First case {rec['title']} | text {len(rec['text'])} chars")
            logger.info(f"Preview: {rec['text'][:200]}")
            logger.info("Connectivity test passed!")
        else:
            logger.error("Failed to build first case")
            sys.exit(1)

    elif args.command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
