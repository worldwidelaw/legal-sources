#!/usr/bin/env python3
"""
OECD/LegalInstruments -- OECD Legal Instruments Compendium

Fetches OECD legal instruments (decisions, recommendations, declarations,
conventions) with full text from the OECD Legal Instruments API.

Strategy:
  - List all instruments via /api/instruments?lang=en
  - For each, fetch detail via /api/instruments/{id}?lang=en
  - Download full text HTML from bodyText.ref URIs
  - Strip HTML tags to extract clean text

API: https://legalinstruments.oecd.org/api/instruments
No auth required. ~500 instruments.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Incremental update
  python bootstrap.py test-api             # Quick API connectivity test
"""

import sys
import json
import logging
import re
import html as html_module
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.OECD.LegalInstruments")

BASE_URL = "https://legalinstruments.oecd.org"

# Type ID to name mapping (from API responses)
TYPE_MAP = {
    "1": "Act",
    "2": "Decision",
    "3": "Substantive Outcome Document",
    "4": "Recommendation",
    "5": "Declaration",
    "6": "Convention",
    "7": "Arrangement",
    "8": "International Agreement",
    "9": "Other",
}

STATUS_MAP = {
    "1": "In force",
    "2": "Abrogated",
    "3": "Repealed",
    "4": "Not yet in force",
}


class OECDScraper(BaseScraper):
    """
    Scraper for OECD/LegalInstruments -- OECD Legal Instruments Compendium.
    Country: OECD (international)
    URL: https://legalinstruments.oecd.org

    Data types: legislation
    Auth: none (open data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json",
            },
            timeout=60,
        )

    # -- API helpers --------------------------------------------------------

    def _get_json(self, endpoint, params=None):
        """Fetch a JSON endpoint with retry."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(endpoint, params=params)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Error fetching {endpoint}: {e}")
            time.sleep(3)
            try:
                resp = self.client.get(endpoint, params=params)
                resp.raise_for_status()
                return resp.json()
            except Exception as e2:
                logger.error(f"Retry failed for {endpoint}: {e2}")
                return None

    def _fetch_html_text(self, uri):
        """Fetch an HTML document and extract clean text."""
        if not uri:
            return ""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(uri)
            if resp.status_code != 200:
                logger.debug(f"Non-200 for {uri}: {resp.status_code}")
                return ""
            content = resp.text
            # Strip HTML tags
            text = re.sub(r'<[^>]+>', ' ', content)
            text = html_module.unescape(text)
            # Normalize whitespace
            text = re.sub(r'\s+', ' ', text).strip()
            # Add paragraph breaks where there were block elements
            text = re.sub(r'\s{2,}', '\n\n', text)
            return text
        except Exception as e:
            logger.debug(f"Error fetching HTML {uri}: {e}")
            return ""

    def _get_instrument_text(self, instrument_id):
        """Fetch full text for a single instrument."""
        detail = self._get_json(f"/api/instruments/{instrument_id}", params={"lang": "en"})
        if not detail:
            return "", detail

        body_text = detail.get("bodyText", {})
        refs = body_text.get("ref", [])

        # Find English HTML reference
        text = ""
        for ref in refs:
            if ref.get("lang") == "en" and ref.get("format") == "html":
                uri = ref.get("uri", "")
                if uri:
                    text = self._fetch_html_text(uri)
                    break

        # Fallback to French if no English
        if not text:
            for ref in refs:
                if ref.get("lang") == "fr" and ref.get("format") == "html":
                    uri = ref.get("uri", "")
                    if uri:
                        text = self._fetch_html_text(uri)
                        break

        return text, detail

    # -- Core fetch methods -------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all OECD legal instruments with full text."""
        instruments = self._get_json("/api/instruments", params={"lang": "en"})
        if not instruments:
            logger.error("Failed to fetch instruments list")
            return

        logger.info(f"Found {len(instruments)} instruments")

        for i, inst in enumerate(instruments):
            inst_id = inst.get("id")
            key = inst.get("key", "")
            title = inst.get("title", "")

            logger.info(f"[{i+1}/{len(instruments)}] Fetching {key}: {title[:60]}")

            text, detail = self._get_instrument_text(inst_id)

            yield {
                "id": inst_id,
                "key": key,
                "title": title,
                "adoption_date": inst.get("adoptionDate"),
                "in_force_date": inst.get("inForceDate"),
                "status": inst.get("status", {}),
                "type": inst.get("type", {}),
                "text": text,
                "detail": detail,
            }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield instruments updated since the given date."""
        instruments = self._get_json("/api/instruments", params={"lang": "en"})
        if not instruments:
            return

        for inst in instruments:
            last_pub = inst.get("lastPublishDate", "")
            if last_pub:
                try:
                    pub_date = datetime.fromisoformat(last_pub.replace("Z", "+00:00"))
                    if pub_date < since:
                        continue
                except (ValueError, TypeError):
                    pass

            inst_id = inst.get("id")
            text, detail = self._get_instrument_text(inst_id)

            yield {
                "id": inst_id,
                "key": inst.get("key", ""),
                "title": inst.get("title", ""),
                "adoption_date": inst.get("adoptionDate"),
                "in_force_date": inst.get("inForceDate"),
                "status": inst.get("status", {}),
                "type": inst.get("type", {}),
                "text": text,
                "detail": detail,
            }

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw instrument data into standard schema."""
        text = raw.get("text", "")
        if not text:
            key = raw.get("key", "?")
            logger.debug(f"Skipping {key}: no text content")
            return None

        key = raw.get("key", "")
        inst_id = raw.get("id", "")

        # Extract type and status names
        type_info = raw.get("type", {})
        type_id = str(type_info.get("id", ""))
        type_name = type_info.get("name", "") or TYPE_MAP.get(type_id, "Unknown")

        status_info = raw.get("status", {})
        status_id = str(status_info.get("id", ""))
        status_name = status_info.get("name", "") or STATUS_MAP.get(status_id, "Unknown")

        # Extract title from detail if available (multilingual)
        detail = raw.get("detail")
        title = raw.get("title", "")
        if detail:
            title_info = detail.get("title", {})
            names = title_info.get("name", [])
            for name in names:
                if name.get("lang") == "en":
                    title = name.get("value", title)
                    break

        # Extract blurb/summary
        blurb = ""
        if detail:
            blurb_info = detail.get("blurb", {})
            texts = blurb_info.get("text", [])
            for t in texts:
                if t.get("lang") == "en" and t.get("value"):
                    blurb = re.sub(r'<[^>]+>', ' ', t["value"])
                    blurb = html_module.unescape(blurb).strip()
                    blurb = re.sub(r'\s+', ' ', blurb)
                    break

        date_iso = raw.get("adoption_date") or raw.get("in_force_date")
        url = f"https://legalinstruments.oecd.org/en/instruments/{key.replace('/', '-')}" if key else ""

        return {
            "_id": f"OECD/LegalInstruments/{key}",
            "_source": "OECD/LegalInstruments",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "instrument_key": key,
            "instrument_id": inst_id,
            "title": title,
            "text": text,
            "blurb": blurb,
            "date": date_iso,
            "in_force_date": raw.get("in_force_date"),
            "instrument_type": type_name,
            "status_name": status_name,
            "url": url,
        }

    def test_api(self):
        """Quick API connectivity and content test."""
        logger.info("Testing OECD Legal Instruments API...")

        instruments = self._get_json("/api/instruments", params={"lang": "en"})
        if not instruments:
            logger.error("FAIL: Could not fetch instruments list")
            return False
        logger.info(f"OK: {len(instruments)} instruments found")

        # Test detail + full text
        test = instruments[0]
        test_id = test.get("id")
        logger.info(f"Testing instrument {test.get('key')}: {test.get('title', '')[:60]}")

        text, detail = self._get_instrument_text(test_id)
        logger.info(f"OK: Text length: {len(text)} chars")
        if len(text) > 100:
            logger.info(f"   Preview: {text[:200]}...")
            return True
        else:
            logger.warning("WARN: Short or empty text")
            return len(text) > 0


# -- CLI entry point --------------------------------------------------------

if __name__ == "__main__":
    scraper = OECDScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(json.dumps(stats, indent=2, default=str))

    elif command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=30)
        stats = scraper.bootstrap(sample_mode=False)
        print(json.dumps(stats, indent=2, default=str))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
