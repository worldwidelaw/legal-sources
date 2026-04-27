#!/usr/bin/env python3
"""
CoE/CODICES -- Venice Commission CODICES Database Fetcher

Fetches constitutional court decisions from the CODICES REST API.

Strategy:
  - Use POST /api/search to paginate through all ~12,000+ precis
  - Fetch each precis detail for metadata and summary text
  - Fetch full text HTML via fulltext translations + static files
  - Normalize into standard schema

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import time
import re
import html as htmlmod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CoE.CODICES")

API_BASE = "https://codices.coe.int/api"

# Default search body for POST /api/search — all array fields must be present
SEARCH_DEFAULTS = {
    "Text": "",
    "Type": "PRECIS",
    "Country": "",
    "Continent": "",
    "Page": 0,
    "Size": 500,
    "TreePathList": [],
    "CountryFilterList": [],
    "ThesaurusFilterList": [],
    "LanguageCode": "",
    "WithProximity": False,
    "ProximitySize": 0,
    "ReferenceCode": "",
    "DecisionNumber": "",
    "Title": "",
    "StartDate": "",
    "EndDate": "",
    "Group": "",
    "ThesaurusIndexNumber": "",
    "ThesaurusText": "",
    "AlphaIndexText": "",
    "WithThesaurusChildren": False,
}


class CODICESScraper(BaseScraper):
    """Scraper for CoE/CODICES -- Venice Commission constitutional decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        try:
            from common.http_client import HttpClient
            self.client = HttpClient(timeout=60)
        except ImportError:
            self.client = None

    def _http_get(self, url: str) -> Optional[str]:
        """HTTP GET returning response text."""
        for attempt in range(3):
            try:
                if self.client:
                    resp = self.client.get(url)
                    if resp.status_code == 200:
                        return resp.text
                    if resp.status_code in (404, 500):
                        return None
                    logger.warning(f"HTTP {resp.status_code} for {url[:100]}")
                else:
                    import urllib.request
                    req = urllib.request.Request(url, headers={
                        "User-Agent": "Mozilla/5.0",
                        "Accept": "application/json",
                    })
                    with urllib.request.urlopen(req, timeout=60) as resp:
                        return resp.read().decode("utf-8", errors="replace")
            except Exception as e:
                if "404" in str(e) or "500" in str(e):
                    return None
                logger.warning(f"Attempt {attempt+1} GET failed for {url[:100]}: {e}")
                time.sleep(2 * (attempt + 1))
        return None

    def _get_json(self, url: str) -> Optional[Any]:
        """GET and parse JSON."""
        text = self._http_get(url)
        if not text:
            return None
        try:
            return json.loads(text, strict=False)
        except json.JSONDecodeError:
            return None

    def _post_json(self, url: str, body: dict) -> Optional[Any]:
        """POST JSON and parse response."""
        payload = json.dumps(body).encode("utf-8")
        for attempt in range(3):
            try:
                if self.client:
                    resp = self.client.post(url, json_data=body)
                    if resp.status_code == 200:
                        return resp.json()
                    logger.warning(f"POST HTTP {resp.status_code} for {url[:100]}")
                else:
                    import urllib.request
                    req = urllib.request.Request(url, data=payload, headers={
                        "User-Agent": "Mozilla/5.0",
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                    })
                    with urllib.request.urlopen(req, timeout=60) as resp:
                        return json.loads(resp.read().decode("utf-8", errors="replace"))
            except Exception as e:
                logger.warning(f"POST attempt {attempt+1} failed for {url[:100]}: {e}")
                time.sleep(2 * (attempt + 1))
        return None

    def _search_all_ids(self, page_size: int = 500) -> List[str]:
        """Use POST /api/search to get all precis IDs via pagination by continent."""
        all_ids = []
        seen = set()
        # Search by continent to avoid the 10,000-result API cap
        continents = ["EUR", "AMR", "AFR", "ASI"]
        for continent in continents:
            page = 0
            while True:
                body = dict(SEARCH_DEFAULTS)
                body["Page"] = page
                body["Size"] = page_size
                body["Continent"] = continent
                time.sleep(1)
                result = self._post_json(f"{API_BASE}/search", body)
                if not result:
                    logger.error(f"Search {continent} page {page} failed")
                    break
                items = result.get("searchResult", [])
                if not items:
                    break
                for item in items:
                    item_id = item.get("id")
                    if item_id and item_id not in seen:
                        seen.add(item_id)
                        all_ids.append(item_id)
                logger.info(f"Search {continent} page {page}: {len(items)} items (total: {len(all_ids)})")
                if not result.get("hasMoreChildren", False):
                    break
                page += 1
        return all_ids

    def _get_country_tree(self, country_code: str) -> Optional[dict]:
        """Get precis tree for a country."""
        return self._get_json(f"{API_BASE}/precis/tree?countryCode={country_code}")

    def _fetch_precis(self, guid: str) -> Optional[dict]:
        """Fetch a single precis by GUID."""
        return self._get_json(f"{API_BASE}/precis/{guid}")

    def _clean_html(self, raw_html: str) -> str:
        """Strip HTML tags and clean text."""
        text = re.sub(r'<script[^>]*>.*?</script>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', text)
        text = htmlmod.unescape(text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _get_full_text_from_precis(self, precis: dict) -> Optional[str]:
        """Get full text HTML from fullTexts -> fullTextTranslations -> filePath."""
        full_texts = precis.get("fullTexts", [])
        if not full_texts:
            return None

        for ft in full_texts:
            ftt = ft.get("fullTextTranslations", {})
            if not ftt:
                continue
            # Prefer English, then any available language
            for lang in ["eng", "fra"] + list(ftt.keys()):
                if lang in ftt:
                    file_path = ftt[lang].get("filePath", "")
                    if not file_path:
                        continue
                    time.sleep(0.5)
                    html_content = self._http_get(f"{API_BASE}/staticFiles/{file_path}")
                    if html_content and len(html_content) > 100:
                        clean = self._clean_html(html_content)
                        if len(clean) > 200:
                            return clean
        return None

    def _extract_summary(self, precis: dict) -> str:
        """Extract summary from precisTranslations dict (EN preferred, then FR)."""
        translations = precis.get("precisTranslations", {})
        if not isinstance(translations, dict):
            return ""

        for lang in ["eng", "fra"]:
            t = translations.get(lang)
            if not t or not isinstance(t, dict):
                continue
            parts = []
            for field in ["headNote", "summary", "crossReference"]:
                val = t.get(field, "")
                if val:
                    cleaned = self._clean_html(val) if "<" in val else val
                    parts.append(cleaned)
            if parts:
                return "\n\n".join(parts)

        # Try any language
        for lang, t in translations.items():
            if not isinstance(t, dict):
                continue
            text = t.get("summary", "") or t.get("text", "")
            if text:
                return self._clean_html(text) if "<" in text else text
        return ""

    def _collect_tree_ids(self, node: dict) -> List[str]:
        """Recursively collect all leaf node IDs from a tree."""
        ids = []
        children = node.get("children", [])
        if children:
            for child in children:
                ids.extend(self._collect_tree_ids(child))
        else:
            # Leaf node
            node_id = node.get("id")
            if node_id and node_id != "00000000-0000-0000-0000-000000000000":
                ids.append(node_id)
        return ids

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        ref_code = raw.get("referenceCode", "")
        decision_date = raw.get("decisionDate", "")
        date = decision_date[:10] if decision_date else ""
        text = raw.get("_text", "")
        summary = raw.get("_summary", "")
        country = raw.get("_country", "")

        # Build title from translations
        translations = raw.get("precisTranslations", {})
        title = ""
        if isinstance(translations, dict):
            for lang in ["eng", "fra"]:
                t = translations.get(lang)
                if t and isinstance(t, dict):
                    title = t.get("title", "")
                    if title:
                        break
        if not title:
            title = f"CODICES {ref_code}"

        # Combine full text + summary
        combined_text = text
        if summary and not text:
            combined_text = summary
        elif summary and text:
            combined_text = f"{text}\n\n--- SUMMARY ---\n\n{summary}"

        # Get decision number from translations
        decision_number = ""
        if isinstance(translations, dict):
            for lang in ["eng", "fra"]:
                t = translations.get(lang)
                if t and isinstance(t, dict):
                    decision_number = t.get("decisionNumber", "")
                    if decision_number:
                        break

        return {
            "_id": f"CoE-CODICES-{ref_code}",
            "_source": "CoE/CODICES",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": combined_text,
            "date": date,
            "url": f"https://codices.coe.int/codices/precis/{raw.get('id', '')}",
            "reference_code": ref_code,
            "country": country,
            "decision_number": decision_number,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all decisions via POST /api/search pagination + detail fetch."""
        logger.info("Fetching all precis IDs via search API...")
        all_ids = self._search_all_ids()
        logger.info(f"Found {len(all_ids)} precis IDs to fetch")

        count = 0
        skipped = 0
        for i, guid in enumerate(all_ids):
            time.sleep(1)
            precis = self._fetch_precis(guid)
            if not precis:
                skipped += 1
                continue

            full_text = self._get_full_text_from_precis(precis)
            summary = self._extract_summary(precis)

            if not full_text and not summary:
                logger.warning(f"No text for {precis.get('referenceCode', '?')}")
                skipped += 1
                continue

            precis["_text"] = full_text or ""
            precis["_summary"] = summary
            # Extract country from referenceCode (e.g., "GER-2024-1-001" -> "GER")
            ref = precis.get("referenceCode", "")
            precis["_country"] = ref.split("-")[0] if ref else ""

            count += 1
            if count % 100 == 0:
                logger.info(f"Progress: {count} fetched, {skipped} skipped, {i+1}/{len(all_ids)} processed")
            yield precis

        logger.info(f"Completed: {count} decisions fetched, {skipped} skipped")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent decisions."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test using search API."""
        # Test search endpoint
        body = dict(SEARCH_DEFAULTS)
        body["Page"] = 0
        body["Size"] = 5
        result = self._post_json(f"{API_BASE}/search", body)
        if not result:
            logger.error("Search endpoint failed")
            return False

        items = result.get("searchResult", [])
        tree = result.get("tree", {})
        total = tree.get("precisTree", {}).get("resultCount", 0)
        logger.info(f"Search OK: {len(items)} items returned, {total} total precis")

        if not items:
            logger.error("No search results")
            return False

        # Fetch first precis detail
        guid = items[0].get("id")
        precis = self._fetch_precis(guid)
        if not precis:
            logger.error("Could not fetch precis detail")
            return False

        ref = precis.get("referenceCode", "?")
        logger.info(f"Precis OK: {ref}")

        full_text = self._get_full_text_from_precis(precis)
        if full_text:
            logger.info(f"Full text OK: {len(full_text)} chars")
        summary = self._extract_summary(precis)
        if summary:
            logger.info(f"Summary OK: {len(summary)} chars")

        return bool(full_text or summary)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CoE/CODICES data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CODICESScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
if __name__ == "__main__":
    main()
