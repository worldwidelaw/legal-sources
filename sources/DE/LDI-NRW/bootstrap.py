#!/usr/bin/env python3
"""
DE/LDI-NRW -- North Rhine-Westphalia Data Protection Authority (LDI NRW).

The authority runs a Drupal site with no JSON:API and a sitemap.xml that only
lists the homepage, so the corpus is discovered by crawling. Two kinds of
document are collected:

  - reports:  the annual Taetigkeitsberichte / Datenschutzberichte linked from
              /berichte. Each is a few hundred pages narrating the authority's
              enforcement casework (complaints, fines, orders) — LDI NRW does
              not publish individual Bescheide, so these are the enforcement
              record. Born-digital PDFs, extracted via common/pdf_extract.
  - guidance: the topic pages under /datenschutz, /informationsfreiheit and
              /infothek interpreting the GDPR, DSG NRW and IFG NRW. Full text
              is the Drupal <article> body.

Usage:
  python bootstrap.py bootstrap          # Fetch everything
  python bootstrap.py bootstrap-fast     # Same, invoked by the fleet wrapper
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import logging
import re
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, Optional
from urllib.parse import urljoin, urlparse, urldefrag

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DE.LDI-NRW")

BASE_URL = "https://www.ldi.nrw.de"
REPORTS_URL = f"{BASE_URL}/berichte"
HOST = "www.ldi.nrw.de"

# Crawl roots for the guidance pages. The homepage is included so newly linked
# sections are picked up without editing this list.
CRAWL_SEEDS = [
    "/",
    "/datenschutz",
    "/informationsfreiheit",
    "/infothek",
    "/infothek/recht",
    "/aktuelle-meldungen",
]

# Boilerplate that carries no doctrine: imprint, accessibility statement, the
# authority's own privacy notice, job ads, contact forms.
SKIP_PATH_RE = re.compile(
    r"^/(impressum|barrierefreiheit|datenschutzerklaerung|datenschutzhinweise"
    r"|netiquette|kontakt|beschwerde|neues_beschwerdeformular"
    r"|ueber-uns/stellenangebote|suche|search)(/|$)"
)

# Only these prefixes are crawled as guidance; everything else is navigation.
KEEP_PREFIXES = ("/datenschutz", "/informationsfreiheit", "/infothek", "/berichte")

MIN_TEXT_CHARS = 400


def _clean(text: str) -> str:
    """Collapse the whitespace Drupal's markup leaves behind."""
    text = re.sub(r"[ \t\xa0]+", " ", text)
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
    return text.strip()


def _slug(url: str) -> str:
    """Stable ID fragment from a URL path or PDF filename."""
    path = urlparse(url).path.rstrip("/")
    slug = path.rsplit("/", 1)[-1] or "home"
    slug = re.sub(r"\.pdf$", "", slug, flags=re.I)
    slug = re.sub(r"[^A-Za-z0-9]+", "-", slug).strip("-").lower()
    return slug or "home"


def _report_year(title: str, url: str) -> Optional[str]:
    """Publication year of a Taetigkeitsbericht.

    Newer filenames carry it ('31_bericht_2026.pdf'), older ones don't
    ('30.-bericht.pdf'). The reports are numbered consecutively and the Nth was
    published in year N + 1995 — verified against every dated filename in the
    series — so the ordinal in the title fills the gaps.
    """
    for candidate in (url, title):
        m = re.search(r"(19|20)\d{2}", candidate)
        if m:
            return f"{m.group(0)}-01-01"

    m = re.match(r"\s*(\d{1,2})\s*\.", title)
    if m:
        return f"{int(m.group(1)) + 1995}-01-01"
    return None


class LdiNrwScraper(BaseScraper):
    """Scraper for DE/LDI-NRW -- NRW Data Protection Authority."""

    def __init__(self):
        super().__init__(Path(__file__).parent)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/pdf",
            "Accept-Language": "de,en;q=0.8",
        })

    # ── HTTP ─────────────────────────────────────────────────────────────

    def _get(self, url: str) -> Optional[requests.Response]:
        """GET with explicit (connect, read) timeouts so no page can wedge the crawl."""
        for attempt in range(3):
            try:
                time.sleep(1.0)
                resp = self.session.get(url, timeout=(15, 45))
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"{url} attempt {attempt + 1} failed: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    # ── Discovery ────────────────────────────────────────────────────────

    def _internal_links(self, soup: BeautifulSoup, page_url: str) -> list:
        """Same-host, fragment-free links from one page."""
        out = []
        for a in soup.find_all("a", href=True):
            url = urldefrag(urljoin(page_url, a["href"]))[0]
            parsed = urlparse(url)
            if parsed.netloc != HOST or parsed.scheme not in ("http", "https"):
                continue
            out.append(url)
        return out

    def _discover_reports(self) -> list:
        """The annual Taetigkeitsberichte linked from /berichte."""
        resp = self._get(REPORTS_URL)
        if resp is None:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        seen, entries = set(), []
        for a in soup.find_all("a", href=True):
            url = urljoin(REPORTS_URL, a["href"])
            if not url.lower().endswith(".pdf") or urlparse(url).netloc != HOST:
                continue
            if url in seen:
                continue
            seen.add(url)
            title = a.get_text(strip=True) or _slug(url)
            entries.append({
                "doc_id": f"DE-LDINRW-report-{_slug(url)}",
                "title": title,
                "url": url,
                "date": _report_year(title, url),
                "doc_kind": "report",
            })

        logger.info(f"Discovered {len(entries)} report PDFs")
        return entries

    def _discover_guidance(self, limit: int = None) -> list:
        """Breadth-first crawl of the guidance sections."""
        queue = deque(urljoin(BASE_URL, p) for p in CRAWL_SEEDS)
        visited, entries = set(), []

        while queue:
            page_url = queue.popleft()
            if page_url in visited:
                continue
            visited.add(page_url)

            resp = self._get(page_url)
            if resp is None or "html" not in resp.headers.get("Content-Type", ""):
                continue
            soup = BeautifulSoup(resp.text, "html.parser")

            for link in self._internal_links(soup, page_url):
                path = urlparse(link).path
                if link in visited or link in queue:
                    continue
                if path.lower().endswith(".pdf") or SKIP_PATH_RE.match(path):
                    continue
                if path.startswith(KEEP_PREFIXES):
                    queue.append(link)

            path = urlparse(page_url).path
            if not path.startswith(KEEP_PREFIXES) or SKIP_PATH_RE.match(path):
                continue  # a seed/hub page — crawled for links, not kept

            body = soup.find("article") or soup.find("main")
            if body is None:
                continue
            text = _clean(body.get_text("\n", strip=True))
            if len(text) < MIN_TEXT_CHARS:
                continue

            h1 = soup.find("h1")
            entries.append({
                "doc_id": f"DE-LDINRW-{_slug(page_url)}",
                "title": h1.get_text(strip=True) if h1 else _slug(page_url),
                "url": page_url,
                "date": None,
                "doc_kind": "guidance",
                "text": text,
            })

            if limit and len(entries) >= limit:
                break

        logger.info(f"Discovered {len(entries)} guidance pages from {len(visited)} URLs")
        return entries

    # ── Fetch ────────────────────────────────────────────────────────────

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        existing = preload_existing_ids("DE/LDI-NRW", table="doctrine")
        count = 0

        # Reports first: they are the enforcement record and the largest texts,
        # so a truncated run still lands the most valuable documents.
        for entry in self._discover_reports():
            if entry["doc_id"] in existing:
                continue
            logger.info(f"Extracting report: {entry['title'][:70]}")
            started = time.monotonic()
            try:
                text = extract_pdf_markdown(
                    source="DE/LDI-NRW",
                    source_id=entry["doc_id"],
                    pdf_url=entry["url"],
                    table="doctrine",
                )
            except Exception as e:
                logger.warning(f"PDF extraction failed for {entry['url']}: {e}")
                text = None
            elapsed = time.monotonic() - started
            if elapsed > 60:
                logger.info(f"{entry['doc_id']} took {elapsed:.0f}s")
            if not text or len(text) < MIN_TEXT_CHARS:
                logger.warning(
                    f"Insufficient text for {entry['url']}: {len(text) if text else 0} chars"
                )
                continue
            entry["text"] = text
            count += 1
            yield entry

        for entry in self._discover_guidance():
            if entry["doc_id"] in existing:
                continue
            count += 1
            yield entry

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Guidance pages carry no modification date, so re-run the full crawl.

        The corpus is ~120 documents and the loader upserts on _id, so a full
        re-crawl is cheaper than maintaining a change feed the site doesn't offer.
        """
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw["doc_id"],
            "_source": "DE/LDI-NRW",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "doc_kind": raw.get("doc_kind", ""),
            "language": "de",
        }

    def test(self) -> bool:
        reports = self._discover_reports()
        if not reports:
            logger.error("No report PDFs found on /berichte")
            return False
        guidance = self._discover_guidance(limit=3)
        if not guidance:
            logger.error("No guidance pages discovered")
            return False
        logger.info(
            f"OK: {len(reports)} reports (e.g. {reports[0]['url']}), "
            f"guidance sample '{guidance[0]['title']}' {len(guidance[0]['text'])} chars"
        )
        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="DE/LDI-NRW data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = LdiNrwScraper()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)
    elif args.command in ("bootstrap", "bootstrap-fast"):
        sample_mode = args.sample and args.command == "bootstrap"
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "update":
        logger.info(f"Update complete: {scraper.update()}")


if __name__ == "__main__":
    main()
