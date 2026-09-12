#!/usr/bin/env python3
"""
INTL/IEA-IRENA-PolicyDB -- IEA/IRENA Renewable Energy Policies Database

Fetches ~13K renewable energy policy records from the IEA/IRENA joint database
via their public REST API. Covers 190+ countries.

Endpoint: GET https://api.iea.org/v3/policies
  - Returns all policies in a single JSON array (no pagination)
  - No authentication required
  - Each policy has title, description (HTML), status, year, countries

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast     # Alias for bootstrap
  python bootstrap.py update             # Fetch all (API has no date filter)
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import html
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.IEA-IRENA-PolicyDB")

SOURCE_ID = "INTL/IEA-IRENA-PolicyDB"
SAMPLE_DIR = Path(__file__).parent / "sample"

API_URL = "https://api.iea.org/v3/policies"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "application/json",
}


def strip_html(text: str) -> str:
    """Remove HTML tags, decode entities, normalize whitespace."""
    if not text:
        return ""
    text = re.sub(r"<br\s*/?\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<p[^>]*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<div[^>]*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p>|</div>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"^\s+", "", text, flags=re.MULTILINE)
    return text.strip()


def parse_date(date_str: str, year: Optional[int] = None) -> Optional[str]:
    """Parse various date formats to ISO 8601."""
    if not date_str:
        if year:
            return f"{year}-01-01"
        return None
    date_str = date_str.strip()
    # Skip Excel serial date numbers (pure digits, no valid date meaning)
    if date_str.isdigit() and len(date_str) <= 5:
        if year:
            return f"{year}-01-01"
        return None
    for fmt in ("%d-%m-%Y", "%d %B %Y", "%d %b %Y", "%Y-%m-%d",
                "%d %B, %Y", "%B %d, %Y", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Try extracting year only
    m = re.search(r"(\d{4})", date_str)
    if m:
        return f"{m.group(1)}-01-01"
    if year:
        return f"{year}-01-01"
    return None


class IEAIRENAPolicyScraper(BaseScraper):
    """Scraper for INTL/IEA-IRENA-PolicyDB -- IEA/IRENA policy database."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _fetch_policies(self) -> list:
        """Fetch all policies from the API."""
        logger.info("Fetching all policies from %s ...", API_URL)
        resp = self.session.get(API_URL, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        logger.info("Received %d policies", len(data))
        return data

    def normalize(self, doc: dict) -> Optional[dict]:
        """Transform a RAW API record into the standard schema.

        Returns None for records with insufficient text (framework skips them).
        NOTE: fetch_all() yields RAW docs; the framework calls this once. Do not
        call normalize() inside fetch_all() or records get double-normalized.
        """
        policy_id = str(doc.get("policyId", ""))
        title = doc.get("title", "").strip()
        description = strip_html(doc.get("description", ""))
        status = doc.get("status", "")
        year = doc.get("year")

        # Build text from description + metadata
        text_parts = []
        if description:
            text_parts.append(description)
        if status:
            text_parts.append(f"\nStatus: {status}")
        if doc.get("jurisdiction"):
            text_parts.append(f"Jurisdiction: {doc['jurisdiction']}")
        if doc.get("mandatory") == "1":
            text_parts.append("Type: Mandatory")

        text = "\n".join(text_parts)

        # Countries
        countries = [c.get("name", "") for c in doc.get("countries", [])]
        country_codes = [c.get("iso3", "") for c in doc.get("countries", [])]

        # Date
        date = parse_date(doc.get("datePromulgated", ""), year)

        # URL
        url = doc.get("learnMore", "")
        if not url:
            url = f"https://www.iea.org/policies?policyId={policy_id}"

        if len(text) < 30:
            return None

        return {
            "_id": f"INTL-IEA-IRENA-{policy_id}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "language": doc.get("learnMoreLanguage", "en") or "en",
            "policy_id": policy_id,
            "status": status,
            "year": year,
            "countries": countries,
            "country_codes": country_codes,
            "jurisdiction": doc.get("jurisdiction", ""),
            "technologies": [t.get("name", "") if isinstance(t, dict) else str(t)
                            for t in doc.get("technologies", [])],
            "topics": [t.get("name", "") if isinstance(t, dict) else str(t)
                       for t in doc.get("topics", [])],
            "policy_types": [t.get("name", "") if isinstance(t, dict) else str(t)
                             for t in doc.get("policyTypes", [])],
            "date_modified": doc.get("dateModified", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all IEA/IRENA policies as RAW docs (framework normalizes)."""
        policies = self._fetch_policies()
        for doc in policies:
            yield doc
        logger.info("Total raw policies yielded: %d", len(policies))

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield RAW policies modified since a date (filters client-side)."""
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
        policies = self._fetch_policies()
        count = 0
        for doc in policies:
            modified = doc.get("dateModified", "")
            if modified and modified[:10] >= since:
                yield doc
                count += 1
        logger.info("Update complete: %d raw records since %s", count, since)


def main():
    scraper = IEAIRENAPolicyScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing connectivity...")
        resp = scraper.session.get(API_URL + "?page=1&per_page=1", timeout=30)
        resp.raise_for_status()
        data = resp.json()
        logger.info("API OK — %d total policies", len(data))
        if data:
            sample_rec = scraper.normalize(data[0])
            logger.info("Sample: %s | text length: %d",
                        sample_rec["title"], len(sample_rec["text"]))
        return

    if command in ("bootstrap", "bootstrap-fast"):
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        limit = 15 if sample else None
        count = 0
        for doc in scraper.fetch_all():
            record = scraper.normalize(doc)
            if record is None:
                continue
            count += 1
            safe_id = record["_id"].replace("/", "_")[:100]
            fname = SAMPLE_DIR / f"{safe_id}.json"
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            if count % 500 == 0:
                logger.info("Saved %d records...", count)
            if limit and count >= limit:
                break
        logger.info("Bootstrap complete: %d records saved to %s",
                     count, SAMPLE_DIR)

    elif command == "update":
        since = (sys.argv[2] if len(sys.argv) > 2
                 and not sys.argv[2].startswith("-") else "2025-01-01")
        count = sum(1 for _ in scraper.fetch_updates(since))
        logger.info("Update complete: %d records since %s", count, since)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
