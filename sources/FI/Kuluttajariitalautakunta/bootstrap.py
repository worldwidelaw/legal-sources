#!/usr/bin/env python3
"""
FI/Kuluttajariitalautakunta — Finnish Consumer Disputes Board

Fetches consumer dispute decisions via WordPress REST API + individual page scrape.

Strategy:
  1. List decisions via /wp-json/wp/v2/paatos (100 per page, 13 pages total)
  2. For each decision, fetch the individual page to extract decision number
     (Diaarinumero) from the infobox
  3. Full text from API content field (strip HTML)
  4. Resolve taxonomy IDs (teemat, paatos-asiasanat) to human-readable names

API: https://www.kuluttajariita.fi/wp-json/wp/v2/paatos
Total: ~1,255 decisions

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
  python bootstrap.py test
"""

import sys
import re
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from html import unescape
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FI.Kuluttajariitalautakunta")

BASE_URL = "https://www.kuluttajariita.fi"
API_BASE = f"{BASE_URL}/wp-json/wp/v2"
PER_PAGE = 100


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean up whitespace."""
    text = re.sub(r"<style[^>]*>.*?</style>", "", html_text, flags=re.DOTALL)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL)
    text = re.sub(r"<[^>]+>", "\n", text)
    text = unescape(text)
    text = re.sub(r"\xa0", " ", text)
    lines = [line.strip() for line in text.splitlines()]
    text = "\n".join(line for line in lines if line)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class KuluttajariitalautakuntaScraper(BaseScraper):
    """Scraper for FI/Kuluttajariitalautakunta — Finnish Consumer Disputes Board."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=60,
        )
        self._teemat_cache = {}
        self._asiasanat_cache = {}

    def _load_taxonomy(self, taxonomy: str) -> dict:
        """Load all terms for a taxonomy into a name lookup dict."""
        terms = {}
        page = 1
        while True:
            self.rate_limiter.wait()
            url = f"{API_BASE}/{taxonomy}?per_page=100&page={page}"
            resp = self.http.get(url)
            if resp.status_code != 200:
                break
            data = resp.json()
            if not data:
                break
            for term in data:
                terms[term["id"]] = term["name"]
            if len(data) < 100:
                break
            page += 1
        return terms

    def _resolve_teemat(self, ids: list) -> list:
        """Resolve teemat (category) IDs to names."""
        if not self._teemat_cache:
            logger.info("Loading teemat taxonomy...")
            self._teemat_cache = self._load_taxonomy("teemat")
        return [self._teemat_cache.get(tid, str(tid)) for tid in ids]

    def _resolve_asiasanat(self, ids: list) -> list:
        """Resolve paatos-asiasanat (keyword) IDs to names."""
        if not self._asiasanat_cache:
            logger.info("Loading paatos-asiasanat taxonomy...")
            self._asiasanat_cache = self._load_taxonomy("paatos-asiasanat")
        return [self._asiasanat_cache.get(kid, str(kid)) for kid in ids]

    def _extract_decision_metadata(self, page_url: str) -> dict:
        """Fetch the decision page and extract metadata from the infobox."""
        meta = {}
        try:
            self.rate_limiter.wait()
            resp = self.http.get(page_url)
            if resp.status_code != 200:
                return meta
            html = resp.text

            # Decision number (Diaarinumero)
            m = re.search(
                r'id="diaarinumero-value-paatos-infobox"[^>]*>([^<]+)', html
            )
            if m:
                meta["decision_number"] = m.group(1).strip()

            # Decision date (Antopäivä)
            m = re.search(
                r'id="antopaiva-value-paatos-infobox"[^>]*>([^<]+)', html
            )
            if m:
                meta["decision_date"] = m.group(1).strip()

            # Categories (Aihealueet)
            m = re.search(
                r'id="aihealueet-value-paatos-infobox"[^>]*>([^<]+)', html
            )
            if m:
                meta["categories_text"] = m.group(1).strip()

            # Plenary decision (Täysistuntopäätös)
            m = re.search(
                r'id="taysistuntopaatos-value-paatos-infobox"[^>]*>([^<]+)', html
            )
            if m:
                val = m.group(1).strip()
                meta["plenary"] = val.lower() in ("kyllä", "yes")

            # Keywords (Asiasanat)
            m = re.search(
                r'id="asiasanat-value-paatos-infobox"[^>]*>([^<]+)', html
            )
            if m:
                meta["keywords_text"] = m.group(1).strip()

        except Exception as e:
            logger.debug(f"Could not extract page metadata from {page_url}: {e}")
        return meta

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions from the WordPress REST API."""
        page = 1
        total = None
        while True:
            self.rate_limiter.wait()
            url = f"{API_BASE}/paatos?per_page={PER_PAGE}&page={page}&orderby=date&order=asc"
            logger.info(f"Fetching API page {page}...")
            resp = self.http.get(url)
            if resp.status_code != 200:
                logger.warning(f"API returned {resp.status_code} on page {page}")
                break

            if total is None:
                total = int(resp.headers.get("X-WP-Total", 0))
                total_pages = int(resp.headers.get("X-WP-TotalPages", 0))
                logger.info(f"Total decisions: {total}, pages: {total_pages}")

            data = resp.json()
            if not data:
                break

            for item in data:
                # Fetch individual page for decision number
                page_meta = self._extract_decision_metadata(item["link"])
                item["_page_meta"] = page_meta
                yield item

            if len(data) < PER_PAGE:
                break
            page += 1

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions modified since the given date."""
        since_iso = since.strftime("%Y-%m-%dT%H:%M:%S")
        page = 1
        while True:
            self.rate_limiter.wait()
            url = (
                f"{API_BASE}/paatos?per_page={PER_PAGE}&page={page}"
                f"&modified_after={since_iso}&orderby=modified&order=asc"
            )
            resp = self.http.get(url)
            if resp.status_code != 200:
                break
            data = resp.json()
            if not data:
                break
            for item in data:
                page_meta = self._extract_decision_metadata(item["link"])
                item["_page_meta"] = page_meta
                yield item
            if len(data) < PER_PAGE:
                break
            page += 1

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a WP paatos post into a standardized record."""
        content_html = raw.get("content", {}).get("rendered", "")
        text = strip_html(content_html)

        if not text or len(text) < 50:
            return None

        title = raw.get("title", {}).get("rendered", "")
        title = unescape(title)

        page_meta = raw.get("_page_meta", {})
        decision_number = page_meta.get("decision_number", "")

        # Parse decision date from page metadata (DD.MM.YYYY format)
        decision_date = None
        raw_date_str = page_meta.get("decision_date", "")
        if raw_date_str:
            try:
                dt = datetime.strptime(raw_date_str, "%d.%m.%Y")
                decision_date = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Fallback to API date
        if not decision_date:
            api_date = raw.get("date", "")
            if api_date:
                decision_date = api_date[:10]

        # Resolve taxonomies
        teemat_ids = raw.get("teemat", [])
        asiasanat_ids = raw.get("paatos-asiasanat", [])
        categories = self._resolve_teemat(teemat_ids) if teemat_ids else []
        keywords = self._resolve_asiasanat(asiasanat_ids) if asiasanat_ids else []

        # Fallback categories/keywords from page metadata
        if not categories and page_meta.get("categories_text"):
            categories = [c.strip() for c in page_meta["categories_text"].split(",")]
        if not keywords and page_meta.get("keywords_text"):
            keywords = [k.strip() for k in page_meta["keywords_text"].split(",")]

        plenary = page_meta.get("plenary", False)

        # Use decision_number as _id if available, otherwise WP post ID
        _id = decision_number if decision_number else f"wp-{raw['id']}"

        return {
            "_id": _id,
            "_source": "FI/Kuluttajariitalautakunta",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": decision_date,
            "url": raw.get("link", ""),
            "decision_number": decision_number,
            "categories": categories,
            "keywords": keywords,
            "plenary": plenary,
            "slug": raw.get("slug", ""),
            "wp_id": raw["id"],
            "language": "fi",
        }


def main():
    scraper = KuluttajariitalautakuntaScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, "
                  f"{stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
