#!/usr/bin/env python3
"""
IS/Reglugerdir — Icelandic Regulations (Reglugerðir)

Fetches regulations from the island.is GraphQL API.

Strategy:
  - Query getRegulationsYears to discover available years
  - Paginate getRegulationsSearch per year to collect all regulation names
  - Fetch each regulation via getRegulation query
  - ~2,487 in-force regulations

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10 sample records
  python bootstrap.py update             # Incremental update
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IS.Reglugerdir")

# GraphQL queries
QUERY_YEARS = """
query GetRegulationsYears {
  getRegulationsYears
}
"""

QUERY_SEARCH = """
query GetRegulationsSearch($input: GetRegulationsSearchInput!) {
  getRegulationsSearch(input: $input) {
    data {
      name
      title
      publishedDate
      ministry {
        name
      }
    }
    paging {
      page
      pages
    }
  }
}
"""

QUERY_REGULATION = """
query GetRegulation($input: GetRegulationInput!) {
  getRegulation(input: $input) {
    name
    title
    text
    signatureDate
    publishedDate
    effectiveDate
    ministry {
      name
    }
    lawChapters {
      name
      slug
    }
    history {
      title
      name
      date
    }
  }
}
"""


def strip_html(html_text: str) -> str:
    """Strip HTML tags and extract clean text from regulation body."""
    if not html_text:
        return ""

    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_text, 'html.parser')
        for element in soup(['script', 'style']):
            element.decompose()
        text = soup.get_text(separator='\n', strip=True)
    except ImportError:
        text = re.sub(r'<br\s*/?>', '\n', html_text)
        text = re.sub(r'<p[^>]*>', '\n\n', text)
        text = re.sub(r'</p>', '', text)
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&lt;', '<', text)
        text = re.sub(r'&gt;', '>', text)

    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)
    return text.strip()


class ReglugerdiScraper(BaseScraper):
    """
    Scraper for IS/Reglugerdir — Icelandic Regulations via island.is GraphQL API.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.graphql_url = self.config.get("api", {}).get("base_url", "https://island.is/api/graphql")
        self.base_url = "https://island.is/reglugerdir"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; LegalSourcesBot/1.0; worldwidelaw/legal-sources)",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def _graphql_request(self, query: str, variables: dict = None) -> Optional[dict]:
        """Execute a GraphQL request against the island.is API."""
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        try:
            resp = self.session.post(self.graphql_url, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            if "errors" in data:
                logger.warning(f"GraphQL errors: {data['errors']}")
                return None

            return data.get("data")

        except requests.RequestException as e:
            logger.error(f"Request error: {e}")
            return None

    def _get_available_years(self) -> list[int]:
        """Get list of available regulation years."""
        data = self._graphql_request(QUERY_YEARS)
        if data and "getRegulationsYears" in data:
            return sorted(data["getRegulationsYears"], reverse=True)
        return []

    def _get_regulation_names(self, year: int = None, page: int = 1) -> tuple[list[str], int]:
        """Get regulation names for a given year. Returns (names, total_pages)."""
        variables = {"input": {"page": page}}
        if year:
            variables["input"]["year"] = year

        data = self._graphql_request(QUERY_SEARCH, variables)
        if not data or "getRegulationsSearch" not in data:
            return [], 0

        search_result = data["getRegulationsSearch"]
        names = [r["name"] for r in search_result.get("data", []) if r.get("name")]
        total_pages = search_result.get("paging", {}).get("pages", 0)

        return names, total_pages

    def _get_all_regulation_names(self, max_names: int = None) -> list[str]:
        """Get all regulation names across all years."""
        all_names = []

        logger.info("Fetching available years...")
        years = self._get_available_years()

        if not years:
            logger.info("No years returned, trying paginated search...")
            page = 1
            while True:
                if max_names and len(all_names) >= max_names:
                    break
                names, total_pages = self._get_regulation_names(page=page)
                if not names:
                    break
                all_names.extend(names)
                logger.info(f"  Page {page}/{total_pages}: {len(names)} regulations")
                if page >= total_pages:
                    break
                page += 1
                time.sleep(0.5)
            return all_names[:max_names] if max_names else all_names

        logger.info(f"Found {len(years)} years: {years[0]}..{years[-1]}")

        for year in years:
            if max_names and len(all_names) >= max_names:
                break

            page = 1
            while True:
                names, total_pages = self._get_regulation_names(year=year, page=page)
                if not names:
                    break
                all_names.extend(names)
                if page >= total_pages:
                    break
                page += 1
                time.sleep(0.3)

            logger.info(f"  Year {year}: {len(all_names)} total so far")
            time.sleep(0.5)

            if len(all_names) > 3000:
                logger.info(f"Safety limit reached at {len(all_names)} names")
                break

        logger.info(f"Total regulation names discovered: {len(all_names)}")
        return all_names[:max_names] if max_names else all_names

    def _fetch_regulation(self, name: str) -> Optional[dict]:
        """Fetch a single regulation by its name/number."""
        variables = {"input": {"name": name}}

        data = self._graphql_request(QUERY_REGULATION, variables)
        if not data or "getRegulation" not in data:
            return None

        reg = data["getRegulation"]
        if not reg:
            return None

        raw_text = reg.get("text", "")
        text = strip_html(raw_text)

        if not text or len(text) < 20:
            return None

        return {
            "name": reg.get("name", name),
            "title": reg.get("title", ""),
            "text": text,
            "signatureDate": reg.get("signatureDate"),
            "publishedDate": reg.get("publishedDate"),
            "effectiveDate": reg.get("effectiveDate"),
            "ministry": reg.get("ministry"),
            "lawChapters": reg.get("lawChapters"),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all regulations."""
        all_names = self._get_all_regulation_names()

        logger.info(f"Processing {len(all_names)} regulations...")

        for i, name in enumerate(all_names):
            logger.info(f"[{i+1}/{len(all_names)}] Fetching {name}...")

            raw = self._fetch_regulation(name)
            if raw:
                yield raw

            time.sleep(1.0)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield regulations published since `since`."""
        for raw in self.fetch_all():
            date_str = raw.get("publishedDate") or raw.get("signatureDate") or ""
            if date_str:
                try:
                    if "T" in date_str:
                        date_str = date_str.split("T")[0]
                    doc_date = datetime.fromisoformat(date_str)
                    if doc_date >= since:
                        yield raw
                except (ValueError, TypeError):
                    yield raw
            else:
                yield raw

    def normalize(self, raw: dict) -> dict:
        """Transform raw regulation data into standardized schema."""
        reg_name = raw.get("name", "")
        reg_id = reg_name.replace("/", "_")

        # Determine date
        date = raw.get("signatureDate") or raw.get("publishedDate") or ""
        if date and "T" in date:
            date = date.split("T")[0]

        record = {
            "_id": reg_id,
            "_source": "IS/Reglugerdir",
            "_type": "regulation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", f"Reglugerð nr. {reg_name}"),
            "text": raw["text"],
            "date": date,
            "url": f"{self.base_url}/nr/{reg_name}",
            "language": "isl",
            "regulation_number": reg_name,
        }

        ministry = raw.get("ministry")
        if ministry and ministry.get("name"):
            record["ministry"] = ministry["name"]

        law_chapters = raw.get("lawChapters")
        if law_chapters:
            record["law_chapters"] = [
                {"name": ch.get("name", ""), "slug": ch.get("slug", "")}
                for ch in law_chapters
            ]

        effective_date = raw.get("effectiveDate")
        if effective_date:
            if "T" in effective_date:
                effective_date = effective_date.split("T")[0]
            record["effective_date"] = effective_date

        return record


# ── CLI entry point ──────────────────────────────────────────────────

def main():
    scraper = ReglugerdiScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
