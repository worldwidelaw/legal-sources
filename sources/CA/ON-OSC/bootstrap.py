#!/usr/bin/env python3
"""
CA/ON-OSC — Ontario Securities Commission: Instruments, Rules & Policies

Fetches the full text of Ontario securities-law instruments, rules and policies
published by the Ontario Securities Commission (OSC) at osc.ca. These are the
national/Ontario instruments, rules, companion policies and staff notices that
govern Ontario's capital markets, grouped by the CSA numbering system.

Structure of the site (a Drupal CMS):
  1. Index page /en/securities-law/instruments-rules-policies links to the
     numbered category pages /en/securities-law/instruments-rules-policies/{N}
     (N = 0..9, the CSA subject categories).
  2. Each category page lists the instruments in that category at
     /en/securities-law/instruments-rules-policies/{N}/{number}
     (e.g. .../1/13-101).
  3. Each instrument page lists its individual documents (the current
     consolidation plus historical amendments, notices and companion policies)
     as leaf "irp_update" nodes at
     /en/securities-law/instruments-rules-policies/{N}/{number}/{slug}.
  4. Each leaf node renders a title (h1.hero__title), an effective date
     (<time datetime=...>) and one or more PDF downloads under
     /sites/default/files/pdfs/... which hold the actual full text.

The scraper enumerates categories -> instruments -> leaf documents and, for each
leaf, downloads the linked PDF(s) and extracts the text. Each leaf document
(consolidation / amendment / notice / policy) becomes one record.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch ~12 sample records for validation
  python bootstrap.py bootstrap-fast     # Concurrent full-text downloads
  python bootstrap.py update             # Incremental update (re-scan, dedup on url)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CA.ON-OSC")

BASE_URL = "https://www.osc.ca"
INDEX_URL = f"{BASE_URL}/en/securities-law/instruments-rules-policies"

REQUEST_TIMEOUT = 60
MAX_RETRIES = 3
RETRY_DELAY = 4.0
MIN_TEXT_CHARS = 200

# A leaf document URL has exactly four path segments after .../instruments-rules-policies/
#   /en/securities-law/instruments-rules-policies/{category}/{instrument}/{slug}
LEAF_RE = re.compile(
    r"^/en/securities-law/instruments-rules-policies/(\d+)/([^/]+)/([^/]+)/?$"
)
# An instrument URL: .../instruments-rules-policies/{category}/{instrument}
INSTRUMENT_RE = re.compile(
    r"^/en/securities-law/instruments-rules-policies/(\d+)/([^/]+)/?$"
)
# A category URL: .../instruments-rules-policies/{category}
CATEGORY_RE = re.compile(
    r"^/en/securities-law/instruments-rules-policies/(\d+)/?$"
)

_FILENAME_DATE_RE = re.compile(r"(\d{4})(\d{2})(\d{2})")


def _clean(text: str) -> str:
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class OSCScraper(BaseScraper):
    """
    Scraper for CA/ON-OSC — Ontario Securities Commission instruments/rules/policies.
    Country: CA (jurisdiction: CA-ON)
    URL: https://www.osc.ca/en/securities-law/instruments-rules-policies
    Data types: legislation
    Auth: none (open public materials; see README for non-commercial terms)
    """

    def __init__(self, source_dir: Optional[str] = None):
        if source_dir is None:
            source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/ZachLaik/LegalDataHunter)",
            "Accept-Language": "en-CA,en;q=0.9",
        })

    # ------------------------------------------------------------------ #
    # Low-level HTTP
    # ------------------------------------------------------------------ #
    def _get(self, url: str) -> Optional[requests.Response]:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
                if resp.status_code == 200:
                    return resp
                logger.debug("HTTP %d for %s (attempt %d)", resp.status_code, url, attempt)
            except requests.RequestException as e:
                logger.debug("GET error %s for %s (attempt %d)", e, url, attempt)
            time.sleep(RETRY_DELAY)
        return None

    def _get_soup(self, url: str) -> Optional[BeautifulSoup]:
        resp = self._get(url)
        if resp is None:
            return None
        return BeautifulSoup(resp.text, "html.parser")

    # ------------------------------------------------------------------ #
    # Enumeration helpers
    # ------------------------------------------------------------------ #
    def _discover_categories(self) -> List[str]:
        """Return the list of category page paths (.../{N}), discovered from index."""
        soup = self._get_soup(INDEX_URL)
        cats = set()
        if soup is not None:
            for a in soup.select("a[href]"):
                href = a.get("href", "").split("?")[0].split("#")[0]
                if CATEGORY_RE.match(href):
                    cats.add(href)
        if not cats:
            # Fallback to the known CSA categories 0-9.
            cats = {f"/en/securities-law/instruments-rules-policies/{n}" for n in range(10)}
        return sorted(cats, key=lambda p: int(p.rstrip("/").rsplit("/", 1)[1]))

    def _instruments_in_category(self, category_path: str) -> List[str]:
        soup = self._get_soup(urljoin(BASE_URL, category_path))
        out = []
        seen = set()
        if soup is not None:
            for a in soup.select("a[href]"):
                href = a.get("href", "").split("?")[0].split("#")[0]
                if INSTRUMENT_RE.match(href) and href not in seen:
                    seen.add(href)
                    out.append(href)
        return out

    def _leaves_in_instrument(self, instrument_path: str) -> List[str]:
        soup = self._get_soup(urljoin(BASE_URL, instrument_path))
        out = []
        seen = set()
        if soup is not None:
            for a in soup.select("a[href]"):
                href = a.get("href", "").split("?")[0].split("#")[0]
                if LEAF_RE.match(href) and href not in seen:
                    seen.add(href)
                    out.append(href)
        return out

    def _parse_leaf(self, leaf_path: str) -> Optional[Dict[str, Any]]:
        """Parse a leaf node page into {url, title, date, pdf_urls, ...}."""
        url = urljoin(BASE_URL, leaf_path)
        soup = self._get_soup(url)
        if soup is None:
            return None

        h1 = soup.select_one("h1.hero__title") or soup.select_one("h1")
        title = h1.get_text(" ", strip=True) if h1 else leaf_path.rsplit("/", 1)[-1]

        date_iso = None
        time_el = soup.select_one("time[datetime]")
        if time_el and time_el.get("datetime"):
            try:
                date_iso = datetime.fromisoformat(
                    time_el["datetime"].replace("Z", "+00:00")
                ).date().isoformat()
            except ValueError:
                date_iso = None

        # PDF downloads live under /sites/default/files/...pdf
        pdf_urls = []
        for a in soup.select("a[href]"):
            href = a.get("href", "").split("#")[0]
            if re.search(r"/sites/default/files/.+\.pdf$", href, re.IGNORECASE):
                full = urljoin(BASE_URL, href)
                if full not in pdf_urls:
                    pdf_urls.append(full)

        if date_iso is None and pdf_urls:
            m = _FILENAME_DATE_RE.search(pdf_urls[0])
            if m:
                y, mo, d = m.groups()
                if 1980 <= int(y) <= 2100 and 1 <= int(mo) <= 12 and 1 <= int(d) <= 31:
                    date_iso = f"{y}-{mo}-{d}"

        m = INSTRUMENT_RE.match("/" + leaf_path.lstrip("/").rsplit("/", 1)[0])
        category = m.group(1) if m else None
        instrument = m.group(2) if m else None

        return {
            "url": url,
            "path": leaf_path,
            "title": title,
            "date": date_iso,
            "pdf_urls": pdf_urls,
            "category": category,
            "instrument": instrument,
        }

    # ------------------------------------------------------------------ #
    # BaseScraper interface
    # ------------------------------------------------------------------ #
    def _iterate_leaves(self) -> Generator[Dict[str, Any], None, None]:
        seen_leaves = set()
        for cat in self._discover_categories():
            for instrument in self._instruments_in_category(cat):
                for leaf in self._leaves_in_instrument(instrument):
                    if leaf in seen_leaves:
                        continue
                    seen_leaves.add(leaf)
                    raw = self._parse_leaf(leaf)
                    if raw and raw.get("pdf_urls"):
                        yield raw

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield raw leaf-document references; normalize() downloads the text."""
        yield from self._iterate_leaves()

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        The OSC site has no modified-since index, so re-scan everything and let
        the leaf date gate updates. Records dedup on url downstream.
        """
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        for raw in self._iterate_leaves():
            d = raw.get("date")
            if d:
                try:
                    if datetime.fromisoformat(d).replace(tzinfo=timezone.utc) < since:
                        continue
                except ValueError:
                    pass
            yield raw

    def normalize(self, raw: dict) -> Optional[dict]:
        """Download the leaf's PDF(s) and extract full text."""
        url = raw.get("url")
        pdf_urls = raw.get("pdf_urls") or []
        if not url or not pdf_urls:
            return None

        parts: List[str] = []
        for pdf_url in pdf_urls:
            resp = self._get(pdf_url)
            if resp is None or not resp.content:
                continue
            text = extract_pdf_markdown(
                "CA/ON-OSC",
                pdf_url,
                pdf_bytes=resp.content,
                table="legislation",
                force=True,
            )
            if text:
                parts.append(text)

        text = _clean("\n\n".join(p for p in parts if p))
        if len(text) < MIN_TEXT_CHARS:
            logger.debug("Insufficient text for %s (%d chars)", url, len(text))
            return None

        return {
            "_id": raw["path"].replace("/en/securities-law/instruments-rules-policies/", "").strip("/"),
            "_source": "CA/ON-OSC",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title"),
            "text": text,
            "date": raw.get("date"),
            "url": url,
            "pdf_urls": pdf_urls,
            "instrument_number": raw.get("instrument"),
            "csa_category": raw.get("category"),
            "language": "en",
            "jurisdiction": "CA-ON",
            "issuing_body": "Ontario Securities Commission",
        }

    # ------------------------------------------------------------------ #
    # Connectivity test
    # ------------------------------------------------------------------ #
    def test_connection(self):
        print("Testing OSC instruments-rules-policies endpoints...")
        print("\n1. Discover categories...")
        cats = self._discover_categories()
        print(f"   {len(cats)} categories: {cats}")
        if not cats:
            print("   ERROR: no categories")
            return

        print("\n2. Instruments in first category...")
        instruments = self._instruments_in_category(cats[0])
        print(f"   {len(instruments)} instruments; first: {instruments[:3]}")
        if not instruments:
            print("   ERROR: no instruments")
            return

        print("\n3. Leaf documents in first instrument...")
        leaves = self._leaves_in_instrument(instruments[0])
        print(f"   {len(leaves)} leaves; first: {leaves[:3]}")
        if not leaves:
            print("   ERROR: no leaves")
            return

        print("\n4. Parse + extract first leaf with a PDF...")
        for leaf in leaves:
            raw = self._parse_leaf(leaf)
            if raw and raw.get("pdf_urls"):
                rec = self.normalize(raw)
                if rec:
                    print(f"   title: {rec['title']}")
                    print(f"   date:  {rec['date']}")
                    print(f"   pdfs:  {rec['pdf_urls']}")
                    print(f"   chars: {len(rec['text'])}")
                    print(f"   sample: {rec['text'][:300]}...")
                    break
        print("\nTest complete.")


def main():
    scraper = OSCScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] "
              "[--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: "
                  f"{stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, "
                  f"{stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))

    elif command in ("bootstrap-fast", "bootstrap_fast"):
        stats = scraper.bootstrap_fast()
        print(f"\nFast bootstrap complete: {stats['records_new']} new, "
              f"{stats.get('records_updated', 0)} updated")
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, "
              f"{stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
