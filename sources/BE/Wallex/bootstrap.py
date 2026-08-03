#!/usr/bin/env python3
"""
BE/Wallex — Walloon Region Consolidated Legislation (Wallex)

Fetches consolidated legislation of the Walloon Region (Belgium) from the
official Wallex database (wallex.wallonie.be, a Jahia CMS publishing ELI URIs).

Strategy:
  1. Enumerate acts via the JSON search action
     POST /sites/wallex/home/search/content-area/rercherce-acte.wallexSearch.do
     The action requires a homepage-primed session + AJAX headers + Referer.
     It returns acts.results[] = {path, promulgationDate (epoch ms), actId, ...},
     sorted newest-first and hard-capped at 100 results per query. We therefore
     walk backwards in time: fix promulgationDateFrom far in the past and step
     promulgationDateUntil back to the oldest date seen on each page until the
     corpus is exhausted (de-duplicating on actId across the overlapping pages).
  2. For each act, GET /fr{path}.listing.html.ajax to resolve the ELI href,
     title and date from the <div class="legal-text-status"> fragment.
  3. GET the ELI page and extract the full consolidated legal text from the
     <div class="article"> blocks (article-title + article-paragraph), which
     hold clean structured article text (no PDFs, no OCR).

The server is slow (~10-20s/request) and throttles bursts, so requests are
rate-limited with retries.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch ~12 sample records for validation
  python bootstrap.py bootstrap-fast     # Concurrent full-text downloads
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List

import requests
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BE.Wallex")

BASE_URL = "https://wallex.wallonie.be"
HOME_URL = f"{BASE_URL}/home.html"
SEARCH_URL = f"{BASE_URL}/sites/wallex/home/search/content-area/rercherce-acte.wallexSearch.do"

# Search returns the newest <=100 acts within the date window. Start the window
# far enough in the past to cover the whole corpus.
WINDOW_FROM = "01/01/1700"
PAGE_CAP = 100
REQUEST_TIMEOUT = 120
MAX_RETRIES = 3
RETRY_DELAY = 5.0
MIN_TEXT_CHARS = 80


def _epoch_ms_to_date(ms: Optional[int]) -> Optional[str]:
    """Convert epoch milliseconds to an ISO date (YYYY-MM-DD)."""
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date().isoformat()
    except (ValueError, OverflowError, OSError):
        return None


def _epoch_ms_to_ddmmyyyy(ms: int) -> str:
    """Convert epoch milliseconds to dd/mm/yyyy (the form the search expects)."""
    d = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date()
    return d.strftime("%d/%m/%Y")


class WallexScraper(BaseScraper):
    """
    Scraper for BE/Wallex — Walloon Region Consolidated Legislation.
    Country: BE (jurisdiction: BE-WAL)
    URL: https://wallex.wallonie.be
    Data types: legislation
    Auth: none (open government data)
    """

    def __init__(self, source_dir: Optional[str] = None):
        if source_dir is None:
            source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/ZachLaik/LegalDataHunter)",
            "Accept-Language": "fr-BE,fr;q=0.9",
        })
        self._primed = False

    # ------------------------------------------------------------------ #
    # Low-level HTTP helpers
    # ------------------------------------------------------------------ #
    def _prime_session(self) -> None:
        """Load the homepage once to obtain the session cookies the search needs."""
        if self._primed:
            return
        try:
            self.session.get(HOME_URL, timeout=REQUEST_TIMEOUT)
            self._primed = True
        except requests.RequestException as e:
            logger.warning("Failed to prime session: %s", e)

    def _get(self, url: str) -> Optional[requests.Response]:
        """GET with retries; returns None on persistent failure."""
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

    def _search(self, until_ddmmyyyy: str) -> List[Dict[str, Any]]:
        """Run the act-search action for promulgation dates <= until."""
        self._prime_session()
        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"{BASE_URL}/home/recherche.html",
            "Accept": "application/json, text/javascript, */*; q=0.01",
        }
        payload = {
            "searchKeyword": "",
            "promulgationDate": "range",
            "promulgationDateFrom": WINDOW_FROM,
            "promulgationDateUntil": until_ddmmyyyy,
        }
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.post(
                    SEARCH_URL, headers=headers, data=payload, timeout=REQUEST_TIMEOUT
                )
                if resp.status_code == 200:
                    return resp.json().get("acts", {}).get("results", []) or []
                logger.debug("Search HTTP %d (until=%s, attempt %d)",
                             resp.status_code, until_ddmmyyyy, attempt)
            except (requests.RequestException, ValueError) as e:
                logger.debug("Search error %s (until=%s, attempt %d)",
                             e, until_ddmmyyyy, attempt)
            time.sleep(RETRY_DELAY)
        return []

    # ------------------------------------------------------------------ #
    # Enumeration
    # ------------------------------------------------------------------ #
    def _iterate_acts(self, sample_mode: bool = False, sample_size: int = 12,
                      since: Optional[datetime] = None) -> Generator[Dict[str, Any], None, None]:
        """
        Walk the search backwards in time, yielding lightweight act references
        {path, actId, promulgationDate}. De-duplicates on actId.
        """
        seen = set()
        count = 0
        until = datetime.now(timezone.utc).date().strftime("%d/%m/%Y")
        prev_until = None

        while True:
            self.rate_limiter.wait()
            results = self._search(until)
            if not results:
                logger.info("No results for until=%s; enumeration complete", until)
                break

            new_in_page = 0
            oldest_ms = None
            for r in results:
                ms = r.get("promulgationDate")
                if ms is not None:
                    oldest_ms = ms if oldest_ms is None else min(oldest_ms, ms)

                act_id = r.get("actId")
                path = r.get("path")
                if not act_id or not path or act_id in seen:
                    continue

                # Incremental: stop once we reach acts older than `since`
                if since is not None:
                    rd = _epoch_ms_to_date(ms)
                    if rd:
                        try:
                            d = datetime.fromisoformat(rd).replace(tzinfo=timezone.utc)
                            if d < since:
                                logger.info("Reached acts older than %s; stopping", since)
                                return
                        except ValueError:
                            pass

                seen.add(act_id)
                new_in_page += 1
                count += 1
                yield {"path": path, "actId": act_id, "promulgationDate": ms}

                if sample_mode and count >= sample_size:
                    logger.info("Sample mode: collected %d act references", count)
                    return

            logger.info("Window until=%s -> %d results (%d new, total %d)",
                        until, len(results), new_in_page, count)

            if oldest_ms is None:
                break

            next_until = _epoch_ms_to_ddmmyyyy(oldest_ms)

            # Fewer than the cap means we have exhausted everything <= until.
            if len(results) < PAGE_CAP:
                break

            # Guard against a single day holding >100 acts (would loop forever):
            # if we made no progress, step the window back by one day.
            if next_until == prev_until and new_in_page == 0:
                d = datetime.fromtimestamp(oldest_ms / 1000, tz=timezone.utc).date() - timedelta(days=1)
                next_until = d.strftime("%d/%m/%Y")
                logger.warning("No progress at %s; stepping window back to %s "
                               "(a single day exceeds the %d-result cap)",
                               until, next_until, PAGE_CAP)

            prev_until = until
            until = next_until

        logger.info("Enumeration done: %d unique acts", count)

    # ------------------------------------------------------------------ #
    # Full-text extraction
    # ------------------------------------------------------------------ #
    def _resolve_listing(self, path: str) -> Optional[Dict[str, str]]:
        """Resolve a search result path to {eli_href, title, date} via listing.ajax."""
        url = f"{BASE_URL}/fr{path}.listing.html.ajax"
        resp = self._get(url)
        if resp is None:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        status = soup.select_one(".legal-text-status")
        if status is None:
            return None
        link = status.select_one("a[href]")
        if link is None:
            return None
        href = link.get("href", "").strip()
        if not href.startswith("/eli/"):
            return None
        title = link.get_text(" ", strip=True)
        time_el = status.select_one("time")
        date_text = time_el.get_text(" ", strip=True) if time_el else ""
        return {"eli_href": href, "title": title, "date_text": date_text}

    def _fetch_full_text(self, eli_href: str) -> str:
        """Fetch the ELI page and extract clean consolidated article text."""
        url = f"{BASE_URL}{eli_href}"
        resp = self._get(url)
        if resp is None:
            return ""
        soup = BeautifulSoup(resp.text, "html.parser")

        parts: List[str] = []

        # Table of contents / structural headings (titres, chapitres, sections).
        toc = soup.select_one(".text-structure")
        if toc:
            toc_text = toc.get_text("\n", strip=True)
            if toc_text:
                parts.append(toc_text)

        # Article bodies: each <div class="article"> holds an article-title
        # ("Art. N.") followed by one or more article-paragraph blocks.
        for art in soup.select(".article"):
            seg = []
            for el in art.select(".article-title, .article-paragraph"):
                t = el.get_text(" ", strip=True)
                if t:
                    seg.append(t)
            if seg:
                parts.append("\n".join(seg))

        # Fallback: some short acts render the body directly in article-paragraph
        # blocks outside an .article wrapper.
        if not any(p for p in parts if "Art" in p):
            for el in soup.select(".article-paragraph"):
                t = el.get_text(" ", strip=True)
                if t:
                    parts.append(t)

        text = "\n\n".join(p for p in parts if p)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    # ------------------------------------------------------------------ #
    # BaseScraper interface
    # ------------------------------------------------------------------ #
    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield raw act references; normalize() downloads the full text."""
        for ref in self._iterate_acts(sample_mode=False):
            yield ref

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield acts promulgated since the given date."""
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        for ref in self._iterate_acts(sample_mode=False, since=since):
            yield ref

    def normalize(self, raw: dict) -> Optional[dict]:
        """
        Resolve the ELI page and extract full consolidated text.

        Returns None if the act has no resolvable ELI page or no usable text,
        so the framework skips it.
        """
        path = raw.get("path")
        act_id = raw.get("actId")
        ms = raw.get("promulgationDate")
        if not path:
            return None

        self.rate_limiter.wait()
        listing = self._resolve_listing(path)
        if not listing:
            logger.debug("No ELI listing for %s", path)
            return None

        eli_href = listing["eli_href"]
        self.rate_limiter.wait()
        text = self._fetch_full_text(eli_href)
        if len(text) < MIN_TEXT_CHARS:
            logger.debug("Insufficient text for %s (%d chars)", eli_href, len(text))
            return None

        date_iso = _epoch_ms_to_date(ms)

        # ELI determines the act type: loi-decret = decree, arrete = order.
        if "/eli/loi-decret/" in eli_href:
            act_type = "decree"
        elif "/eli/arrete/" in eli_href:
            act_type = "order"
        else:
            act_type = "legislation"

        return {
            "_id": act_id or eli_href,
            "_source": "BE/Wallex",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": listing["title"],
            "text": text,
            "date": date_iso,
            "url": f"{BASE_URL}{eli_href}",
            "eli": eli_href,
            "act_type": act_type,
            "language": "fr",
            "jurisdiction": "BE-WAL",
        }

    # ------------------------------------------------------------------ #
    # Connectivity test
    # ------------------------------------------------------------------ #
    def test_connection(self):
        print("Testing Wallex endpoints...")
        print("\n1. Priming session (homepage)...")
        self._prime_session()
        print(f"   primed={self._primed}")

        print("\n2. Search action (latest acts)...")
        until = datetime.now(timezone.utc).date().strftime("%d/%m/%Y")
        results = self._search(until)
        print(f"   {len(results)} results (cap {PAGE_CAP})")
        if not results:
            print("   ERROR: no results")
            return
        first = results[0]
        print(f"   first path={first.get('path')} date={_epoch_ms_to_date(first.get('promulgationDate'))}")

        print("\n3. Resolve ELI listing...")
        listing = self._resolve_listing(first["path"])
        print(f"   {listing}")
        if not listing:
            print("   ERROR: could not resolve listing")
            return

        print("\n4. Fetch full text...")
        text = self._fetch_full_text(listing["eli_href"])
        print(f"   {len(text)} chars")
        print(f"   sample: {text[:300]}...")
        print("\nTest complete.")


def main():
    scraper = WallexScraper()

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

    elif command == "bootstrap-fast":
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
