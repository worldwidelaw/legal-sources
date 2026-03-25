#!/usr/bin/env python3
"""
INTL/ICRC-IHL -- ICRC International Humanitarian Law Databases

Fetches IHL treaties, customary IHL rules, and national practice documents.

Strategy:
  - Drupal JSON:API at ihl-databases.icrc.org/jsonapi
  - Treaties: /node/treaty with include=field_treaty_content for full article text
  - Customary IHL Rules: /node/rule with full body text
  - National Practice: /node/national_practice with summaries
  - No authentication required
  - ~6,634 total documents

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html.parser import HTMLParser

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.ICRC-IHL")

API_BASE = "https://ihl-databases.icrc.org/jsonapi"
SITE_BASE = "https://ihl-databases.icrc.org"
PAGE_SIZE = 50


class HTMLStripper(HTMLParser):
    """Strip HTML tags and return plain text."""

    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_data(self, data):
        self.parts.append(data)

    def get_text(self):
        return " ".join(self.parts).strip()


def strip_html(html: str) -> str:
    """Remove HTML tags from a string."""
    if not html:
        return ""
    s = HTMLStripper()
    s.feed(html)
    text = s.get_text()
    # Clean up whitespace
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class ICRCIHLScraper(BaseScraper):
    """
    Scraper for INTL/ICRC-IHL -- ICRC International Humanitarian Law Databases.
    Country: INTL
    URL: https://ihl-databases.icrc.org

    Data types: legislation, case_law, doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research)",
            "Accept": "application/vnd.api+json",
        })
        self._sample_mode = False

    def _api_get(self, url: str, params: dict = None, timeout: int = 60) -> dict:
        """Make a JSON:API request."""
        r = self.session.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()

    def _paginate(self, url: str, limit: int = PAGE_SIZE, max_items: int = None) -> Generator[dict, None, None]:
        """Paginate through a JSON:API endpoint."""
        offset = 0
        yielded = 0
        while True:
            data = self._api_get(f"{url}?page[limit]={limit}&page[offset]={offset}")
            items = data.get("data", [])
            if not items:
                break

            for item in items:
                yield item
                yielded += 1
                if max_items and yielded >= max_items:
                    return

            total = data.get("meta", {}).get("count")
            offset += limit
            if total and offset >= total:
                break
            if len(items) < limit:
                break

            time.sleep(0.5)

    # ----- Treaties -----

    def _fetch_treaty_text(self, uuid: str) -> str:
        """Fetch full treaty text by including article content paragraphs."""
        url = f"{API_BASE}/node/treaty/{uuid}?include=field_treaty_content"
        data = self._api_get(url, timeout=120)

        parts = []
        # Add presentation text
        attrs = data.get("data", {}).get("attributes", {})
        pres = attrs.get("field_treaty_presentation", "")
        if pres:
            parts.append(strip_html(pres))

        # Add article content from included paragraphs
        included = data.get("included", [])
        for inc in included:
            if inc.get("type") == "paragraph--treaty_content":
                iattrs = inc.get("attributes", {})
                title = iattrs.get("field_treaty_content_title", "")
                content = iattrs.get("field_treaty_content_content", "")
                if title:
                    parts.append(f"\n{title}")
                if content:
                    parts.append(strip_html(content))

        return "\n\n".join(parts).strip()

    def _fetch_treaties(self, max_items: int = None) -> Generator[dict, None, None]:
        """Fetch all IHL treaties with full text."""
        logger.info("Fetching IHL treaties...")
        count = 0
        for item in self._paginate(f"{API_BASE}/node/treaty", max_items=max_items):
            uuid = item["id"]
            attrs = item.get("attributes", {})
            title = attrs.get("title", "")
            logger.info(f"Treaty: {title[:80]}")

            text = self._fetch_treaty_text(uuid)
            if not text or len(text) < 50:
                logger.warning(f"Skipping treaty (no text): {title}")
                continue

            path = attrs.get("field_path", "")
            yield {
                "uuid": uuid,
                "title": title,
                "text": text,
                "date": attrs.get("field_treaty_date_of_adoption"),
                "url": f"{SITE_BASE}/en{path}" if path else f"{SITE_BASE}/en/ihl-treaties",
                "sub_database": "treaty",
                "data_type": "legislation",
                "short_title": attrs.get("field_short_title", ""),
                "treaty_number": attrs.get("field_treaty_number", ""),
                "in_force": attrs.get("field_treaty_in_force", False),
                "historical": attrs.get("field_historical", False),
            }
            count += 1
            time.sleep(1)

        logger.info(f"Fetched {count} treaties")

    # ----- Customary IHL Rules -----

    def _fetch_rules(self, max_items: int = None) -> Generator[dict, None, None]:
        """Fetch all customary IHL rules."""
        logger.info("Fetching customary IHL rules...")
        count = 0
        # Rules are fewer, fetch larger pages
        for item in self._paginate(f"{API_BASE}/node/rule", limit=200, max_items=max_items):
            attrs = item.get("attributes", {})
            title = attrs.get("title", "")
            body = attrs.get("body", "")

            text = strip_html(body) if body else ""
            if not text or len(text) < 50:
                logger.warning(f"Skipping rule (no text): {title}")
                continue

            rule_num = attrs.get("field_rule_number", "")
            path = attrs.get("field_path", "")

            yield {
                "uuid": item["id"],
                "title": f"Rule {rule_num}: {title}" if rule_num else title,
                "text": text,
                "date": None,
                "url": f"{SITE_BASE}/en{path}" if path else f"{SITE_BASE}/en/customary-ihl",
                "sub_database": "rule",
                "data_type": "doctrine",
                "rule_number": rule_num,
            }
            count += 1

        logger.info(f"Fetched {count} rules")

    # ----- National Practice -----

    def _fetch_national_practice(self, max_items: int = None) -> Generator[dict, None, None]:
        """Fetch national practice documents."""
        logger.info("Fetching national practice documents...")
        count = 0
        for item in self._paginate(f"{API_BASE}/node/national_practice", max_items=max_items):
            attrs = item.get("attributes", {})
            title = attrs.get("field_np_english_title", "") or attrs.get("title", "")
            summary = attrs.get("field_np_summary", "")

            text = strip_html(summary) if summary else ""
            if not text or len(text) < 30:
                continue

            # Determine data_type from category
            cat_country = attrs.get("_category_and_country", "")
            if "case-law" in str(cat_country).lower():
                data_type = "case_law"
            elif "manual" in str(cat_country).lower():
                data_type = "doctrine"
            else:
                data_type = "legislation"

            date = attrs.get("field_np_decision_date") or attrs.get("field_np_promulgation_date")
            path = attrs.get("field_path", "")

            yield {
                "uuid": item["id"],
                "title": title,
                "text": text,
                "date": date,
                "url": f"{SITE_BASE}/en{path}" if path else f"{SITE_BASE}/en/national-practice",
                "sub_database": "national_practice",
                "data_type": data_type,
                "original_title": attrs.get("field_np_original_title", ""),
                "state_body": attrs.get("field_np_state_body", ""),
            }
            count += 1

            if count % 100 == 0:
                logger.info(f"National practice: {count} fetched...")

        logger.info(f"Fetched {count} national practice documents")

    # ----- Main interface -----

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw record into standard schema."""
        text = raw.get("text", "").strip()
        if not text:
            return None

        title = raw.get("title", "").strip()
        if not title:
            return None

        sub_db = raw.get("sub_database", "unknown")
        uuid = raw.get("uuid", "")

        return {
            "_id": f"ICRC-IHL-{sub_db}-{uuid}" if uuid else f"ICRC-IHL-{hash(title)}",
            "_source": "INTL/ICRC-IHL",
            "_type": raw.get("data_type", "legislation"),
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "sub_database": sub_db,
            "rule_number": raw.get("rule_number"),
            "treaty_number": raw.get("treaty_number"),
            "short_title": raw.get("short_title"),
            "original_title": raw.get("original_title"),
            "state_body": raw.get("state_body"),
            "in_force": raw.get("in_force"),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all records from all three sub-databases."""
        if self._sample_mode:
            # In sample mode, fetch a balanced mix
            yield from self._fetch_treaties(max_items=5)
            yield from self._fetch_rules(max_items=5)
            yield from self._fetch_national_practice(max_items=5)
        else:
            yield from self._fetch_treaties()
            yield from self._fetch_rules()
            yield from self._fetch_national_practice()

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recently changed records."""
        since_iso = since.isoformat() if isinstance(since, datetime) else str(since)
        logger.info(f"Fetching updates since {since_iso}")
        # JSON:API filter by changed date
        for content_type in ["treaty", "rule", "national_practice"]:
            url = f"{API_BASE}/node/{content_type}"
            offset = 0
            while True:
                filter_url = (
                    f"{url}?page[limit]={PAGE_SIZE}&page[offset]={offset}"
                    f"&filter[changed][condition][path]=changed"
                    f"&filter[changed][condition][operator]=%3E%3D"
                    f"&filter[changed][condition][value]={since_iso}"
                )
                data = self._api_get(filter_url)
                items = data.get("data", [])
                if not items:
                    break

                for item in items:
                    if content_type == "treaty":
                        yield from self._fetch_treaties(max_items=1)
                    elif content_type == "rule":
                        attrs = item.get("attributes", {})
                        body = attrs.get("body", "")
                        text = strip_html(body) if body else ""
                        if text:
                            yield {
                                "uuid": item["id"],
                                "title": attrs.get("title", ""),
                                "text": text,
                                "date": None,
                                "url": f"{SITE_BASE}/en{attrs.get('field_path', '')}",
                                "sub_database": "rule",
                                "data_type": "doctrine",
                                "rule_number": attrs.get("field_rule_number"),
                            }
                    else:
                        attrs = item.get("attributes", {})
                        summary = attrs.get("field_np_summary", "")
                        text = strip_html(summary) if summary else ""
                        if text:
                            yield {
                                "uuid": item["id"],
                                "title": attrs.get("field_np_english_title", ""),
                                "text": text,
                                "date": attrs.get("field_np_decision_date"),
                                "url": f"{SITE_BASE}/en{attrs.get('field_path', '')}",
                                "sub_database": "national_practice",
                                "data_type": "legislation",
                            }

                if len(items) < PAGE_SIZE:
                    break
                offset += PAGE_SIZE
                time.sleep(0.5)


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/ICRC-IHL data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = ICRCIHLScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            # Test treaties
            data = scraper._api_get(f"{API_BASE}/node/treaty?page[limit]=1")
            t_count = data.get("meta", {}).get("count", 0)
            logger.info(f"Treaties: {t_count}")

            # Test rules
            data = scraper._api_get(f"{API_BASE}/node/rule?page[limit]=1")
            r_count = data.get("meta", {}).get("count", 0)
            logger.info(f"Rules: {r_count}")

            # Test national practice
            data = scraper._api_get(f"{API_BASE}/node/national_practice?page[limit]=1")
            np_count = data.get("meta", {}).get("count", 0)
            logger.info(f"National Practice: {np_count}")

            logger.info(f"Total: {t_count + r_count + np_count} documents")
            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        if args.sample:
            scraper._sample_mode = True
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
