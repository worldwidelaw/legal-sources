#!/usr/bin/env python3
"""
INTL/UNCITRAL-EnactmentStatus -- UNCITRAL Model Law / Convention Enactment Status Tables

Fetches the per-instrument "Status" pages from uncitral.un.org. Each status page
is a comprehensive, authoritative table maintained by the UNCITRAL Secretariat
listing every State (and sub-jurisdiction) that has enacted a UNCITRAL model law
or become a party to a UNCITRAL convention, with the year of enactment/accession
and footnotes describing reservations, declarations, and local modifications.

The full body text of each status page (intro paragraph + enactment table +
footnotes) is captured as the document text. This complements:
  - INTL/UNCITRAL-Texts  (the model-law / convention text itself)
  - INTL/UNCITRAL-CLOUT  (case law applying those texts)

Strategy:
  - Crawl the 12 subject category pages to discover instrument pages
  - On each instrument page, follow the "Status" link ({instrument}/status)
  - Parse each status page: title, intro, enactment table, footnotes
  - Dedup by normalized title (a few URLs alias to the same status page)

Usage:
  python bootstrap.py bootstrap          # Full pull (all status pages)
  python bootstrap.py bootstrap --sample # First 15 status pages
  python bootstrap.py test               # Connectivity test
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.UNCITRAL-EnactmentStatus")

BASE_URL = "https://uncitral.un.org"
TEXTS_URL = f"{BASE_URL}/en/texts"

CATEGORIES = [
    "arbitration",
    "mediation",
    "isds",
    "ecommerce",
    "salegoods",
    "msmes",
    "insolvency",
    "securityinterests",
    "onlinedispute",
    "payments",
    "procurement",
    "transportgoods",
]

# Fallback list of known status pages, used if live discovery is degraded.
FALLBACK_STATUS_URLS = [
    f"{TEXTS_URL}/arbitration/conventions/foreign_arbitral_awards/status",
    f"{TEXTS_URL}/arbitration/modellaw/commercial_arbitration/status",
    f"{TEXTS_URL}/arbitration/modellaw/commercial_conciliation/status",
    f"{TEXTS_URL}/ecommerce/conventions/electronic_communications/status",
    f"{TEXTS_URL}/ecommerce/modellaw/electronic_commerce/status",
    f"{TEXTS_URL}/ecommerce/modellaw/electronic_signatures/status",
    f"{TEXTS_URL}/ecommerce/modellaw/electronic_transferable_records/status",
    f"{TEXTS_URL}/insolvency/modellaw/cross-border_insolvency/status",
    f"{TEXTS_URL}/mediation/conventions/international_settlement_agreements/status",
    f"{TEXTS_URL}/payments/conventions/bills_of_exchange/status",
    f"{TEXTS_URL}/payments/conventions/independent_guarantees/status",
    f"{TEXTS_URL}/payments/modellaw/credit_transfers/status",
    f"{TEXTS_URL}/procurement/modellaw/procurement_of_goods_construction_and_services/status",
    f"{TEXTS_URL}/procurement/modellaw/public_procurement/status",
    f"{TEXTS_URL}/salegoods/conventions/limitation_period_international_sale_of_goods/status",
    f"{TEXTS_URL}/salegoods/conventions/sale_of_goods/cisg/status",
    f"{TEXTS_URL}/securityinterests/conventions/receivables/status",
    f"{TEXTS_URL}/securityinterests/modellaw/secured_transactions/status",
    f"{TEXTS_URL}/transportgoods/conventions/liability_of_operators_of_transport_terminals/status",
    f"{TEXTS_URL}/transportgoods/conventions/rotterdam_rules/status",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
}


class UNCITRALEnactmentStatusScraper(BaseScraper):
    SOURCE_ID = "INTL/UNCITRAL-EnactmentStatus"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _fetch_page(self, url: str) -> Optional[str]:
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=60)
                resp.raise_for_status()
                return resp.text
            except requests.RequestException as e:
                if attempt == 2:
                    logger.warning("Failed to fetch %s: %s", url, e)
                    return None
                time.sleep(2 * (attempt + 1))

    def _discover_status_urls(self) -> List[str]:
        """Crawl category -> instrument -> status link to find all status pages."""
        instrument_urls: Set[str] = set()
        for cat in CATEGORIES:
            cat_url = f"{TEXTS_URL}/{cat}"
            self.rate_limiter.wait()
            html = self._fetch_page(cat_url)
            if not html:
                continue
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if f"/en/texts/{cat}/" in href and "faq" not in href and "#" not in href:
                    instrument_urls.add(urljoin(BASE_URL, href).rstrip("/"))

        logger.info("Discovered %d instrument pages", len(instrument_urls))

        status_urls: Set[str] = set()
        for u in sorted(instrument_urls):
            self.rate_limiter.wait()
            html = self._fetch_page(u)
            if not html:
                continue
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                if a["href"].rstrip("/").endswith("/status"):
                    status_urls.add(urljoin(BASE_URL, a["href"]).rstrip("/"))
                    break

        if len(status_urls) < 10:
            logger.warning("Only %d status pages discovered; merging fallback list",
                           len(status_urls))
            status_urls.update(FALLBACK_STATUS_URLS)

        result = sorted(status_urls)
        logger.info("Found %d status pages", len(result))
        return result

    def _parse_status_page(self, url: str, html: str) -> Optional[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")

        h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else ""
        if not title:
            tt = soup.find("title")
            title = tt.get_text(strip=True) if tt else ""
        title = re.sub(r"\s+", " ", title).strip()

        content = soup.find("div", class_="field--name-body") or soup.find("article")
        if not content:
            return None
        for tag in content.find_all(["script", "style", "nav"]):
            tag.decompose()

        # Structured table extraction (State, Year, Notes)
        header_terms = {"state", "states", "treaty", "party", "parties",
                        "jurisdiction", "signature", "notes"}
        rows: List[Dict[str, str]] = []
        table = content.find("table")
        if table:
            for tr in table.find_all("tr"):
                cells = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
                cells = [c for c in cells if c != ""]
                if len(cells) >= 2 and cells[0].lower() not in header_terms:
                    entry = {"state": cells[0], "year": cells[1]}
                    if len(cells) >= 3:
                        entry["notes"] = cells[2]
                    rows.append(entry)

        full_text = content.get_text(separator="\n")
        full_text = re.sub(r"[ \t]+\n", "\n", full_text)
        full_text = re.sub(r"\n{3,}", "\n\n", full_text).strip()

        if not full_text or len(full_text) < 100:
            return None

        return {
            "url": url,
            "title": title,
            "text": full_text,
            "entries": rows,
            "num_entries": len(rows),
        }

    def _extract_year(self, title: str) -> Optional[str]:
        years = re.findall(r"\b(19\d\d|20\d\d)\b", title)
        if years:
            return f"{years[0]}-01-01"
        return None

    def test_connection(self) -> bool:
        try:
            html = self._fetch_page(
                f"{TEXTS_URL}/arbitration/modellaw/commercial_arbitration/status"
            )
            return bool(html and "tatus" in html and "<table" in html)
        except Exception as e:
            logger.error("Connection failed: %s", e)
            return False

    def fetch_all(self) -> Generator[Dict, None, None]:
        status_urls = self._discover_status_urls()
        seen_titles: Set[str] = set()

        for i, url in enumerate(status_urls):
            logger.info("[%d/%d] %s", i + 1, len(status_urls), url.split("/en/texts/")[-1])
            self.rate_limiter.wait()
            html = self._fetch_page(url)
            if not html:
                continue
            parsed = self._parse_status_page(url, html)
            if not parsed:
                logger.warning("  No usable content: %s", url)
                continue

            tkey = parsed["title"].lower()
            if tkey in seen_titles:
                logger.info("  Skipping duplicate title: %s", parsed["title"][:60])
                continue
            seen_titles.add(tkey)

            logger.info("  %d chars, %d state entries", len(parsed["text"]), parsed["num_entries"])
            yield parsed

    def fetch_updates(self, since: datetime) -> Generator[Dict, None, None]:
        # Status pages are updated whenever a new enactment is reported; a full
        # re-pull is cheap (20 pages) so updates just re-run fetch_all.
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        url = raw["url"]
        path = url.replace(f"{TEXTS_URL}/", "").replace("/status", "").strip("/")
        safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", path)

        return {
            "_id": f"UNCITRAL-Status-{safe_id}",
            "_source": "INTL/UNCITRAL-EnactmentStatus",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": self._extract_year(raw["title"]),
            "url": url,
            "num_jurisdictions": raw.get("num_entries", 0),
            "entries": raw.get("entries", []),
        }

    def run_bootstrap(self, sample: bool = False):
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for raw in self.fetch_all():
            normalized = self.normalize(raw)
            fname = re.sub(r"[^\w\-.]", "_", f"{normalized['_id'][:80]}.json")
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            count += 1
            if sample and count >= 15:
                break

        logger.info("Bootstrap complete: %d records saved", count)
        return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="INTL/UNCITRAL-EnactmentStatus Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = UNCITRALEnactmentStatusScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        scraper.run_bootstrap(sample=args.sample)
    elif args.command == "update":
        scraper.run_bootstrap(sample=False)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
