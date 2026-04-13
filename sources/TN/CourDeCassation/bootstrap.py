#!/usr/bin/env python3
"""
TN/CourDeCassation -- Tunisia Court of Cassation Jurisprudence

Fetches Tunisian Court of Cassation (Cour de Cassation) decisions from Juricaf,
the francophone supreme court jurisprudence database maintained by AHJUCAF.

Strategy:
  - List page: /recherche/+/facet_pays:Tunisie (paginated, 10 per page)
  - Parse decision IDs from listing HTML (href="/arret/TUNISIE-...")
  - Fetch each decision page and extract full text from <article id="textArret">
  - Extract metadata from <meta> tags (dc.date, docketnumber, title, keywords)
  - 28 decisions available (2005-2019), all in French, ODBL license

Note: Official Tunisian judiciary sites (e-justice.tn, cassation.tn,
judiciarytunisia.tn) are all unreachable. Juricaf is the only accessible
source with full text.

Usage:
  python bootstrap.py bootstrap          # Full initial pull (all 28 decisions)
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import html
import logging
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
logger = logging.getLogger("legal-data-hunter.TN.CourDeCassation")

BASE_URL = "https://juricaf.org"
SEARCH_URL = "/recherche/+/facet_pays%3ATunisie"

# All known Tunisian decision IDs on Juricaf (28 total, as of 2026-04).
# Listing pages beyond page 1 are blocked by Anubis anti-bot, so we
# hardcode the full corpus. Individual decision pages are not blocked.
KNOWN_DECISION_IDS = [
    "TUNISIE-COURDECASSATION-20190411-80956",
    "TUNISIE-COURDECASSATION-20190304-60341",
    "TUNISIE-COURDECASSATION-20180529-594492018",
    "TUNISIE-COURDECASSATION-20180426-491452017",
    "TUNISIE-COURDECASSATION-20180406-6655466555",
    "TUNISIE-COURDECASSATION-20180326-507120017",
    "TUNISIE-COURDECASSATION-20171208-548042017",
    "TUNISIE-COURDECASSATION-20171130-4686146783",
    "TUNISIE-COURDECASSATION-20171109-17073",
    "TUNISIE-COURDECASSATION-20171106-430902016",
    "TUNISIE-COURDECASSATION-20170612-417052016",
    "TUNISIE-COURDECASSATION-20170330-436712016",
    "TUNISIE-COURDECASSATION-20151009-195842014",
    "TUNISIE-COURDECASSATION-20150102-201521606",
    "TUNISIE-COURDECASSATION-20141204-6069",
    "TUNISIE-COURDECASSATION-20140429-201280576",
    "TUNISIE-COURDECASSATION-20140213-20132831",
    "TUNISIE-COURDECASSATION-20130122-73983",
    "TUNISIE-COURDECASSATION-20121217-797392012",
    "TUNISIE-COURDECASSATION-20120403-59509",
    "TUNISIE-COURDECASSASTION-20111122-51633",
    "TUNISIE-COURDAPPELDESFAX-20110317-39552",
    "TUNISIE-COURDECASSATION-20080313-20241",
    "TUNISIE-COURDECASSATION-20060126-5216",
    "TUNISIE-COURDECASSATION-20051229-19318",
    "TUNISIE-COURDECASSATION-20051229-33412004",
    "TUNISIE-COURDECASSATION-20050628-2005933",
    "TUNISIE-COURDECASSATION-20050127-29038",
]


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities, preserving meaningful whitespace."""
    if not text:
        return ""
    text = html.unescape(text)
    # Replace <br> and block-level tags with newlines
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<(?:p|div|h[1-6]|li|tr)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</(?:p|div|h[1-6]|li|tr)>', '\n', text, flags=re.IGNORECASE)
    # Remove all remaining HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Normalize whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n', '\n\n', text)
    return text.strip()


class CourDeCassationScraper(BaseScraper):
    """
    Scraper for TN/CourDeCassation -- Tunisian Court of Cassation via Juricaf.
    Country: TN
    URL: https://juricaf.org
    Data types: case_law
    Auth: none (Open access, ODBL license)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Accept": "text/html,application/xhtml+xml",
            },
            timeout=60,
        )

    def _get_decision_ids(self) -> list:
        """Return all known Tunisian decision IDs.

        Juricaf's listing pages trigger anti-bot (Anubis) after page 1,
        but individual decision pages are accessible. Since the corpus is
        small and stable (28 decisions), we use a hardcoded list.
        """
        logger.info(f"Using {len(KNOWN_DECISION_IDS)} known decision IDs")
        return list(KNOWN_DECISION_IDS)

    def _fetch_decision(self, decision_id: str) -> Optional[dict]:
        """Fetch a single decision page and extract content + metadata."""
        url = f"/arret/{decision_id}"
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            if resp.status_code != 200:
                logger.warning(f"Decision {decision_id}: HTTP {resp.status_code}")
                return None

            page_html = resp.text

            # Extract metadata from <meta> tags
            title = ""
            m = re.search(r'<meta\s+name="title"\s+content="([^"]*)"', page_html)
            if m:
                title = html.unescape(m.group(1))

            date = ""
            m = re.search(r'<meta\s+name="dc\.date"\s+content="([^"]*)"', page_html)
            if m:
                date = m.group(1)  # Already YYYY-MM-DD

            case_number = ""
            m = re.search(r'<meta\s+name="docketnumber"\s+content="([^"]*)"', page_html)
            if m:
                case_number = m.group(1)

            keywords = ""
            m = re.search(r'<meta\s+name="dc\.description"\s+content="([^"]*)"', page_html)
            if m:
                keywords = html.unescape(m.group(1))

            court = ""
            m = re.search(r'<meta\s+name="dc\.creator"\s+content="([^"]*)"', page_html)
            if m:
                court = html.unescape(m.group(1))

            chamber = ""
            m = re.search(r'Formation : <a[^>]*>([^<]+)</a>', page_html)
            if m:
                chamber = html.unescape(m.group(1))

            urn_lex = ""
            m = re.search(r'Identifiant URN:LEX : (urn:lex;[^\s<]+)', page_html)
            if m:
                urn_lex = m.group(1)

            # Extract full text from <article id="textArret">
            text = ""
            m = re.search(r'<article\s+id="textArret">(.*?)</article>', page_html, re.DOTALL)
            if m:
                text = strip_html(m.group(1))

            if not text:
                logger.warning(f"No text found for {decision_id}")
                return None

            return {
                "decision_id": decision_id,
                "title": title,
                "date": date,
                "case_number": case_number,
                "court": court,
                "chamber": chamber,
                "keywords": keywords,
                "urn_lex": urn_lex,
                "text": text,
                "url": f"{BASE_URL}/arret/{decision_id}",
            }

        except Exception as e:
            logger.error(f"Error fetching decision {decision_id}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all Tunisian court decisions from Juricaf."""
        decision_ids = self._get_decision_ids()
        for i, did in enumerate(decision_ids, 1):
            logger.info(f"Fetching decision {i}/{len(decision_ids)}: {did}")
            raw = self._fetch_decision(did)
            if raw:
                yield raw

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions updated since given date (re-fetches all, small corpus)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a raw decision into the standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 100:
            return None

        title = raw.get("title", "")
        if not title:
            return None

        date = raw.get("date", "")
        case_number = raw.get("case_number", "")
        decision_id = raw.get("decision_id", "")

        # Build unique ID from decision_id
        _id = f"TN/CourDeCassation/{decision_id}"

        return {
            "_id": _id,
            "_source": "TN/CourDeCassation",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": raw.get("url", ""),
            "case_number": case_number,
            "court": raw.get("court", ""),
            "chamber": raw.get("chamber", ""),
            "keywords": raw.get("keywords", ""),
            "urn_lex": raw.get("urn_lex", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="TN/CourDeCassation bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Full bootstrap or sample")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--full", action="store_true", help="Full fetch (all decisions)")

    sub.add_parser("update", help="Incremental update")
    sub.add_parser("test", help="Connectivity test")

    args = parser.parse_args()

    scraper = CourDeCassationScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        ids = scraper._get_decision_ids()
        if ids:
            logger.info(f"OK: {len(ids)} known decisions")
            raw = scraper._fetch_decision(ids[0])
            if raw:
                logger.info(f"Title: {raw['title']}")
                logger.info(f"Text length: {len(raw['text'])} chars")
            else:
                logger.error("FAILED: Could not fetch decision")
                sys.exit(1)
        else:
            logger.error("FAILED: No decision IDs found")
            sys.exit(1)

    elif args.command == "bootstrap":
        sample_mode = args.sample and not args.full
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        logger.info(f"Bootstrap complete: {stats}")

    elif args.command == "update":
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Update complete: {stats}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
