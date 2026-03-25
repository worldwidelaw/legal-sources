#!/usr/bin/env python3
"""
NO/Stortinget -- Norwegian Parliament Data Service

Fetches legislative decisions (lovvedtak), committee recommendations
(innstillinger), and debate transcripts (referater) from Stortinget's
open data API.

Strategy:
  - Bootstrap: Iterates through sessions, lists publications by type,
    then fetches full-text XML for each publication.
  - Update: Fetches only recent sessions.
  - Sample: Fetches 15 records across publication types for validation.

API: https://data.stortinget.no/eksport/
Docs: https://data.stortinget.no/dokumentasjon-og-hjelp/teknisk-dokumentasjon/

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Incremental update (recent sessions)
  python bootstrap.py test-api             # Quick API connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from xml.etree import ElementTree as ET

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NO.Stortinget")

API_BASE = "https://data.stortinget.no/eksport"

# XML namespace used by Stortinget API
NS = {"st": "http://data.stortinget.no"}

# Publication types to fetch, mapped to data type
PUB_TYPE_MAP = {
    "lovvedtak": "legislation",
    "innstilling": "doctrine",
    "referat": "doctrine",
}


class StortingetScraper(BaseScraper):
    """
    Scraper for NO/Stortinget -- Norwegian Parliament Data Service.
    Country: NO
    URL: https://data.stortinget.no

    Data types: legislation + doctrine
    Auth: none (open data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/xml",
            },
            timeout=60,
        )

    # -- API helpers --------------------------------------------------------

    def _get_xml(self, endpoint, params=None):
        """Fetch an XML endpoint and return the parsed ElementTree root."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(endpoint, params=params)
            # 404 is expected for older publications without full text
            if resp.status_code == 404:
                pub_id = (params or {}).get("publikasjonid", "?")
                logger.debug(f"404 for {endpoint} ({pub_id}) — no full text available")
                return None
            resp.raise_for_status()
            text = resp.text
            # Remove default namespace to simplify element access
            text = re.sub(r'\s*xmlns="[^"]*"', '', text, count=1)
            return ET.fromstring(text)
        except Exception as e:
            logger.error(f"Error fetching {endpoint} params={params}: {e}")
            # Retry once for non-404 errors
            time.sleep(3)
            try:
                resp = self.client.get(endpoint, params=params)
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                text = resp.text
                text = re.sub(r'\s*xmlns="[^"]*"', '', text, count=1)
                return ET.fromstring(text)
            except Exception as e2:
                logger.error(f"Retry failed for {endpoint}: {e2}")
                return None

    def _get_sessions(self):
        """Return list of session IDs from oldest to newest."""
        root = self._get_xml("/sesjoner")
        if root is None:
            return []
        sessions = []
        for sesjon in root.iter("sesjon"):
            sid = sesjon.findtext("id")
            if sid:
                sessions.append(sid)
        # Sort chronologically (oldest first)
        sessions.sort()
        return sessions

    def _list_publications(self, pub_type, session_id):
        """List all publication IDs/metadata for a given type and session."""
        root = self._get_xml(
            "/publikasjoner",
            params={"publikasjontype": pub_type, "sesjonid": session_id},
        )
        if root is None:
            return []

        pubs = []
        for pub in root.iter("publikasjon"):
            pub_id = pub.findtext("id")
            title = pub.findtext("tittel") or ""
            # 'dato' is often 0001-01-01; use 'tilgjengelig_dato' as fallback
            date = pub.findtext("dato") or ""
            if date.startswith("0001"):
                date = pub.findtext("tilgjengelig_dato") or ""
            if pub_id:
                pubs.append({
                    "id": pub_id,
                    "title": title,
                    "date": date,
                    "type": pub_type,
                    "session_id": session_id,
                })
        return pubs

    def _fetch_publication_text(self, pub_id):
        """Fetch the full text of a single publication by ID."""
        root = self._get_xml(
            "/publikasjon",
            params={"publikasjonid": pub_id},
        )
        if root is None:
            return ""

        # Extract all text content recursively, ignoring XML tags
        return self._extract_text(root)

    def _extract_text(self, element):
        """Recursively extract all text from an XML element tree."""
        parts = []
        if element.text:
            parts.append(element.text.strip())
        for child in element:
            # Skip metadata-like tags, keep content tags
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            child_text = self._extract_text(child)
            if child_text:
                parts.append(child_text)
            if child.tail:
                parts.append(child.tail.strip())
        return "\n".join(p for p in parts if p)

    def _clean_text(self, text):
        """Clean extracted text: normalize whitespace, remove artifacts."""
        if not text:
            return ""
        # Remove excessive blank lines
        text = re.sub(r'\n{3,}', '\n\n', text)
        # Normalize whitespace within lines
        lines = []
        for line in text.split('\n'):
            line = line.strip()
            if line:
                lines.append(line)
            else:
                lines.append('')
        return '\n'.join(lines).strip()

    # -- Core fetch methods -------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all publications across all sessions and types.

        Iterates newest-to-oldest so sample mode gets recent (more complete) data.
        """
        sessions = self._get_sessions()
        if not sessions:
            logger.error("No sessions found")
            return

        logger.info(f"Found {len(sessions)} sessions: {sessions[0]} to {sessions[-1]}")

        # Newest first — recent sessions have better full-text coverage
        for session_id in reversed(sessions):
            for pub_type in PUB_TYPE_MAP:
                logger.info(f"Fetching {pub_type} for session {session_id}")
                pubs = self._list_publications(pub_type, session_id)
                logger.info(f"  Found {len(pubs)} {pub_type} publications")

                for pub_meta in pubs:
                    full_text = self._fetch_publication_text(pub_meta["id"])
                    pub_meta["text"] = full_text
                    yield pub_meta

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield publications from recent sessions only."""
        sessions = self._get_sessions()
        if not sessions:
            return

        # Only check the last 2 sessions for updates
        recent = sessions[-2:]
        logger.info(f"Checking recent sessions for updates: {recent}")

        for session_id in recent:
            for pub_type in PUB_TYPE_MAP:
                pubs = self._list_publications(pub_type, session_id)
                for pub_meta in pubs:
                    # Parse date if available
                    date_str = pub_meta.get("date", "")
                    if date_str:
                        try:
                            pub_date = datetime.fromisoformat(
                                date_str.replace("Z", "+00:00")
                            )
                            if pub_date < since:
                                continue
                        except (ValueError, TypeError):
                            pass

                    full_text = self._fetch_publication_text(pub_meta["id"])
                    pub_meta["text"] = full_text
                    yield pub_meta

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw publication data into standard schema."""
        pub_id = raw.get("id", "")
        text = self._clean_text(raw.get("text", ""))

        if not text:
            logger.debug(f"Skipping {pub_id}: no text content")
            return None

        pub_type = raw.get("type", "")
        data_type = PUB_TYPE_MAP.get(pub_type, "doctrine")

        # Parse date
        date_raw = raw.get("date", "")
        date_iso = None
        if date_raw:
            try:
                dt = datetime.fromisoformat(date_raw.replace("Z", "+00:00"))
                date_iso = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                date_iso = date_raw[:10] if len(date_raw) >= 10 else None

        title = raw.get("title", "").strip()
        if not title:
            title = f"{pub_type} {pub_id}"

        session_id = raw.get("session_id", "")
        url = f"https://data.stortinget.no/eksport/publikasjon?publikasjonid={pub_id}"

        return {
            "_id": f"NO/Stortinget/{pub_id}",
            "_source": "NO/Stortinget",
            "_type": data_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "publication_id": pub_id,
            "title": title,
            "text": text,
            "date": date_iso,
            "session_id": session_id,
            "publication_type": pub_type,
            "url": url,
        }

    def test_api(self):
        """Quick API connectivity and content test."""
        logger.info("Testing Stortinget API connectivity...")

        # Test sessions endpoint
        sessions = self._get_sessions()
        if not sessions:
            logger.error("FAIL: Could not fetch sessions")
            return False
        logger.info(f"OK: {len(sessions)} sessions available ({sessions[0]} to {sessions[-1]})")

        # Test publications list (use a session we know has data)
        # Future sessions may be empty, so try from recent backwards
        test_session = None
        for s in reversed(sessions):
            pubs_check = self._list_publications("lovvedtak", s)
            if pubs_check:
                test_session = s
                pubs = pubs_check
                break
        if not test_session:
            logger.error("FAIL: No sessions with lovvedtak found")
            return False
        logger.info(f"OK: {len(pubs)} lovvedtak in {test_session}")

        if pubs:
            # Test individual publication fetch
            test_pub = pubs[0]
            text = self._fetch_publication_text(test_pub["id"])
            logger.info(f"OK: Publication {test_pub['id']} text length: {len(text)} chars")
            if len(text) > 100:
                logger.info(f"   Preview: {text[:200]}...")
                return True
            else:
                logger.warning(f"WARN: Text seems short for {test_pub['id']}")
                return True

        return True


# -- CLI entry point --------------------------------------------------------

if __name__ == "__main__":
    scraper = StortingetScraper()

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
